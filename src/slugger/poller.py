"""Background poller daemon for tracking SLURM job state changes."""

from __future__ import annotations

import logging
import os
import signal
import sys
import time
from pathlib import Path

from slugger.config import SLUGGER_DIR, ensure_slugger_dir, load_config
from slugger.models import JobStatus
from slugger.gsheet_sync import update_row
from slugger.slurm import query_active_jobs, query_job_details
from slugger.store import get_active_job_ids, get_job, save_job

logger = logging.getLogger("slugger")

PID_FILE = SLUGGER_DIR / "poller.pid"
LOG_FILE = SLUGGER_DIR / "slugger.log"

# Terminal states that don't need further polling
TERMINAL_STATES = {
    JobStatus.COMPLETED,
    JobStatus.FAILED,
    JobStatus.TIMEOUT,
    JobStatus.CANCELLED,
}


def _setup_logging() -> None:
    ensure_slugger_dir()
    handler = logging.FileHandler(LOG_FILE)
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    )
    root = logging.getLogger("slugger")
    root.setLevel(logging.INFO)
    root.addHandler(handler)


def _write_pid() -> bool:
    """Write PID file with restrictive permissions.

    Returns True if this process now owns the PID file.
    """
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
        os.write(fd, str(os.getpid()).encode())
    finally:
        os.close(fd)
    return True


def _remove_pid() -> None:
    PID_FILE.unlink(missing_ok=True)


def get_poller_pid() -> int | None:
    """Return PID of running poller, or None."""
    if not PID_FILE.exists():
        return None

    # Reject symlinks to prevent symlink attacks
    if PID_FILE.is_symlink():
        PID_FILE.unlink()
        return None

    try:
        pid = int(PID_FILE.read_text().strip())
    except (ValueError, OSError):
        return None

    # Check if process is alive
    try:
        os.kill(pid, 0)
        return pid
    except OSError:
        # Stale PID file
        _remove_pid()
        return None


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

    # Run the polling loop
    _setup_logging()
    if not _write_pid():
        # Another poller owns the PID file — exit to prevent orphan daemon
        os._exit(0)

    def handle_sigterm(_signum, _frame):
        logger.info("Poller stopping (SIGTERM)")
        _remove_pid()
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_sigterm)

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
    """Main polling loop — reloads config each cycle for live updates."""
    while True:
        config = load_config()
        try:
            _poll_once(config)
        except Exception:
            logger.exception("Poll cycle error")

        time.sleep(config.poll_interval_sec)


def _poll_once(config) -> None:
    """Run a single poll cycle."""
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
        job = get_job(job_id)
        if job is None:
            continue

        if job_id in squeue_states:
            # Job is still in queue — update state if changed
            new_state_str = squeue_states[job_id]
            new_status = _map_squeue_state(new_state_str)
            # Don't overwrite valid status with UNKNOWN from unrecognized squeue states
            if new_status != job.status and new_status != JobStatus.UNKNOWN:
                old_status = job.status
                job = job.with_update(status=new_status)
                save_job(job)
                update_row(job, config)
                logger.info("Job %s: %s -> %s", job_id, old_status.value, new_status.value)
        else:
            # Job disappeared from squeue — check sacct for final state
            details = query_job_details(job_id)
            if details:
                updates = {k: v for k, v in details.items() if v is not None and k != "job_id"}
                job = job.with_update(**updates)
                save_job(job)
                update_row(job, config)
                logger.info("Job %s finished: %s", job_id, job.status.value)
            else:
                # sacct also returned nothing — don't mark UNKNOWN immediately,
                # wait for next cycle (transient SLURM issue)
                logger.warning("Job %s: not in squeue, no sacct data — will retry next cycle", job_id)


def _map_squeue_state(state: str) -> JobStatus:
    mapping = {
        "PENDING": JobStatus.PENDING,
        "RUNNING": JobStatus.RUNNING,
        "COMPLETING": JobStatus.RUNNING,
        "CONFIGURING": JobStatus.PENDING,
        "SUSPENDED": JobStatus.PENDING,
        "REQUEUED": JobStatus.PENDING,
        "RESIZING": JobStatus.RUNNING,
    }
    return mapping.get(state.upper(), JobStatus.UNKNOWN)
