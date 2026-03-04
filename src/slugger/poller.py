"""Background poller daemon for tracking SLURM job state changes."""

from __future__ import annotations

import json
import logging
from logging.handlers import RotatingFileHandler
import os
import signal
import time
from pathlib import Path

from slugger.config import SLUGGER_DIR, ensure_slugger_dir, load_config
from slugger.models import JobStatus, TERMINAL_STATES, SluggerConfig
from slugger.gsheet_sync import update_row
from slugger.slurm import query_active_jobs, query_job_details, _SLURM_STATE_MAP
from slugger.store import get_active_job_ids, get_job, save_job, update_job_locked, prune_terminal_jobs

logger = logging.getLogger("slugger")

PID_FILE = SLUGGER_DIR / "poller.pid"
LOG_FILE = SLUGGER_DIR / "slugger.log"

# Graceful shutdown flag — set by signal handler, checked in poll loop
_shutdown = False

# Prune terminal jobs every N poll cycles to bound store growth
_PRUNE_EVERY_N_CYCLES = 100


def _setup_logging() -> None:
    ensure_slugger_dir()
    handler = RotatingFileHandler(
        LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3
    )
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    )
    root = logging.getLogger("slugger")
    root.setLevel(logging.INFO)
    root.addHandler(handler)


def _write_pid() -> bool:
    """Write PID file with process identity for reliable staleness detection.

    Returns True if this process now owns the PID file.
    """
    content = json.dumps({"pid": os.getpid(), "start": time.time()})
    try:
        fd = os.open(str(PID_FILE), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        if is_running():
            return False  # Another daemon is active
        PID_FILE.unlink(missing_ok=True)
        try:
            fd = os.open(str(PID_FILE), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            # Another process won the race — let it be the poller
            return False
    try:
        os.write(fd, content.encode())
    finally:
        os.close(fd)
    return True


def _remove_pid() -> None:
    PID_FILE.unlink(missing_ok=True)


def _is_slugger_process(pid: int) -> bool:
    """Verify that a PID belongs to a slugger poller (not a recycled PID)."""
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            cmdline = f.read().decode("utf-8", errors="replace")
        return "slugger" in cmdline
    except OSError:
        return False


def get_poller_pid() -> int | None:
    """Return PID of running poller, or None."""
    if not PID_FILE.exists():
        return None

    # Reject symlinks to prevent symlink attacks
    if PID_FILE.is_symlink():
        PID_FILE.unlink()
        return None

    try:
        raw = PID_FILE.read_text().strip()
        # Support both legacy (plain PID) and new (JSON) formats
        if raw.startswith("{"):
            data = json.loads(raw)
            pid = int(data["pid"])
        else:
            pid = int(raw)
    except (ValueError, OSError, json.JSONDecodeError, KeyError):
        return None

    # Check if process is alive
    try:
        os.kill(pid, 0)
    except OSError:
        # Process is dead — stale PID file
        _remove_pid()
        return None

    # Verify it's actually a slugger process (prevent PID recycling false positive)
    if not _is_slugger_process(pid):
        _remove_pid()
        return None

    return pid


def is_running() -> bool:
    return get_poller_pid() is not None


def stop_poller() -> bool:
    """Stop the running poller. Returns True if stopped."""
    pid = get_poller_pid()
    if pid is None:
        return False

    try:
        os.kill(pid, signal.SIGTERM)
        # Wait briefly for process to exit
        for _ in range(10):
            try:
                os.kill(pid, 0)
                time.sleep(0.1)
            except OSError:
                break
        _remove_pid()
        return True
    except OSError:
        _remove_pid()
        return False


def _daemonize() -> None:
    """Double-fork to detach from terminal."""
    try:
        pid = os.fork()
    except OSError as e:
        raise RuntimeError(f"Failed to daemonize: {e}") from e

    if pid > 0:
        return  # Parent returns

    # Decouple from parent
    os.setsid()

    # Second fork
    pid = os.fork()
    if pid > 0:
        os._exit(0)

    # Redirect stdio to /dev/null
    devnull = os.open(os.devnull, os.O_RDWR)
    os.dup2(devnull, 0)
    os.dup2(devnull, 1)
    os.dup2(devnull, 2)
    os.close(devnull)

    # Ignore SIGHUP — daemon should not die on terminal hangup
    signal.signal(signal.SIGHUP, signal.SIG_IGN)

    # Run the polling loop
    _setup_logging()
    if not _write_pid():
        # Another poller owns the PID file — exit to prevent orphan daemon
        os._exit(0)

    def _handle_sigterm(_signum, _frame):
        global _shutdown
        logger.info("Poller received SIGTERM, shutting down gracefully")
        _shutdown = True

    signal.signal(signal.SIGTERM, _handle_sigterm)

    logger.info("Poller started (PID %d)", os.getpid())

    try:
        _poll_loop()
    except Exception:
        logger.exception("Poller crashed")
    finally:
        _remove_pid()
        os._exit(0)  # Ensure daemon never returns to caller


def start_poller() -> bool:
    """Start the poller daemon. Returns True if started (or already running)."""
    if is_running():
        return True

    ensure_slugger_dir()
    _daemonize()
    return True


def _poll_loop() -> None:
    """Main polling loop — reloads config each cycle, checks shutdown flag."""
    cycle_count = 0
    while not _shutdown:
        config = load_config()
        try:
            _poll_once(config)
        except Exception:
            logger.exception("Poll cycle error")

        cycle_count += 1
        # Periodically prune old terminal jobs to bound store growth
        if cycle_count % _PRUNE_EVERY_N_CYCLES == 0:
            try:
                pruned = prune_terminal_jobs()
                if pruned > 0:
                    logger.info("Pruned %d old terminal jobs from store", pruned)
            except Exception:
                logger.exception("Prune error")

        # Sleep in small increments to respond to shutdown quickly
        deadline = time.monotonic() + config.poll_interval_sec
        while not _shutdown and time.monotonic() < deadline:
            time.sleep(min(1.0, deadline - time.monotonic()))


def _poll_once(config: SluggerConfig) -> None:
    """Run a single poll cycle using locked read-modify-write for each job."""
    active_ids = get_active_job_ids()
    if not active_ids:
        return

    # Check squeue for running/pending jobs
    squeue_states = query_active_jobs()

    if squeue_states is None:
        # squeue failed (transient SLURM issue) — skip entire cycle
        logger.warning("squeue failed, skipping poll cycle")
        return

    for job_id in active_ids:
        if _shutdown:
            return

        job = get_job(job_id)
        if job is None:
            continue

        if job_id in squeue_states:
            _handle_active_job(job, squeue_states[job_id], config)
        else:
            _handle_finished_job(job, config)


def _handle_active_job(job, sq_info: dict[str, str], config: SluggerConfig) -> None:
    """Handle a job that is still in squeue."""
    new_status = _SLURM_STATE_MAP.get(sq_info["state"].upper(), JobStatus.UNKNOWN)
    node = sq_info.get("node", "") or None
    # Don't overwrite valid status with UNKNOWN from unrecognized squeue states
    changed = (new_status != job.status and new_status != JobStatus.UNKNOWN)
    node_changed = (node and node != job.node)

    if not (changed or node_changed):
        return

    old_status = job.status
    updates: dict[str, object] = {}
    if changed:
        updates["status"] = new_status
    if node_changed:
        updates["node"] = node

    # Enrich with sacct details when transitioning to RUNNING
    if changed and new_status == JobStatus.RUNNING:
        details = query_job_details(job.job_id)
        if details:
            for k, v in details.items():
                if v is not None and k != "job_id":
                    updates.setdefault(k, v)

    # Use locked read-modify-write to prevent lost updates
    updated = update_job_locked(job.job_id, lambda j: j.with_update(**updates))
    if updated:
        update_row(updated, config)
        if changed:
            logger.info("Job %s: %s -> %s", job.job_id, old_status.value, new_status.value)


def _handle_finished_job(job, config: SluggerConfig) -> None:
    """Handle a job that disappeared from squeue — check sacct for final state."""
    details = query_job_details(job.job_id)
    if details:
        updates = {k: v for k, v in details.items() if v is not None and k != "job_id"}
        updated = update_job_locked(job.job_id, lambda j: j.with_update(**updates))
        if updated:
            update_row(updated, config)
            logger.info("Job %s finished: %s", job.job_id, updated.status.value)
    else:
        # sacct also returned nothing — don't mark UNKNOWN immediately,
        # wait for next cycle (transient SLURM issue)
        logger.warning("Job %s: not in squeue, no sacct data — will retry next cycle", job.job_id)
