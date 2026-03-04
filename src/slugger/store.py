"""Local JSON store for job persistence with atomic writes and proper locking."""

from __future__ import annotations

import contextlib
import fcntl
import json
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Callable, Optional

from slugger.config import ensure_slugger_dir
from slugger.models import Job, JobStatus, TERMINAL_STATES

logger = logging.getLogger("slugger")

# Maximum number of terminal (finished) jobs to keep in the store.
# Active jobs are never pruned.
_MAX_TERMINAL_JOBS = 5000


def _jobs_path() -> Path:
    return ensure_slugger_dir() / "jobs.json"


def _lock_path() -> Path:
    return _jobs_path().with_suffix(".lock")


@contextlib.contextmanager
def _shared_lock():
    """Acquire a shared (read) lock on the jobs store."""
    lp = _lock_path()
    ensure_slugger_dir()
    with open(lp, "w") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_SH)
        try:
            yield
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


@contextlib.contextmanager
def _exclusive_lock():
    """Acquire an exclusive (write) lock on the jobs store."""
    lp = _lock_path()
    ensure_slugger_dir()
    with open(lp, "w") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


def _load_raw_unlocked() -> list[dict]:
    """Load raw job data from disk. Caller must hold at least a shared lock."""
    path = _jobs_path()
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text())
        return data if isinstance(data, list) else []
    except json.JSONDecodeError:
        # Preserve corrupted file for recovery
        backup = path.with_suffix(".json.corrupt")
        if not backup.exists():
            shutil.copy2(path, backup)
        logger.warning("Corrupted jobs.json — backup saved to %s", backup)
        return []
    except OSError:
        return []


def _load_raw() -> list[dict]:
    """Load raw job data with a shared lock for consistency."""
    with _shared_lock():
        return _load_raw_unlocked()


def save_job(job: Job) -> None:
    """Save or update a job in the store (atomic, exclusive-locked)."""
    path = _jobs_path()

    with _exclusive_lock():
        jobs = _load_raw_unlocked()
        new_data = job.to_dict()
        updated = any(j.get("job_id") == job.job_id for j in jobs)
        result = [
            new_data if j.get("job_id") == job.job_id else j
            for j in jobs
        ]
        if not updated:
            result = [*result, new_data]
        _save_raw_unlocked(result, path)


def update_job_locked(job_id: str, updater: Callable[[Job], Job]) -> Optional[Job]:
    """Read-modify-write a single job under exclusive lock.

    Prevents lost updates when poller and CLI modify the same job concurrently.
    Returns the updated Job, or None if not found.
    """
    path = _jobs_path()

    with _exclusive_lock():
        jobs = _load_raw_unlocked()
        target_idx = None
        for i, j in enumerate(jobs):
            if j.get("job_id") == job_id:
                target_idx = i
                break

        if target_idx is None:
            return None

        old_job = Job.from_dict(jobs[target_idx])
        new_job = updater(old_job)
        jobs[target_idx] = new_job.to_dict()
        _save_raw_unlocked(jobs, path)
        return new_job


def prune_terminal_jobs(max_terminal: int = _MAX_TERMINAL_JOBS) -> int:
    """Remove oldest terminal jobs exceeding the cap. Returns count pruned."""
    path = _jobs_path()

    with _exclusive_lock():
        jobs = _load_raw_unlocked()
        terminal_indices = [
            i for i, j in enumerate(jobs)
            if j.get("status") in {s.value for s in TERMINAL_STATES}
        ]

        if len(terminal_indices) <= max_terminal:
            return 0

        # Remove oldest terminal jobs (they appear first in the list)
        to_remove = set(terminal_indices[:len(terminal_indices) - max_terminal])
        pruned = [j for i, j in enumerate(jobs) if i not in to_remove]
        _save_raw_unlocked(pruned, path)
        return len(to_remove)


def _save_raw_unlocked(jobs: list[dict], path: Path) -> None:
    """Write jobs to disk atomically. Caller must hold exclusive lock."""
    fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        os.fchmod(fd, 0o600)
        with open(fd, "w") as f:
            json.dump(jobs, f, indent=2, ensure_ascii=False)
        Path(tmp_path).replace(path)
    except Exception:
        Path(tmp_path).unlink(missing_ok=True)
        raise


def get_job(job_id: str) -> Optional[Job]:
    """Get a single job by ID."""
    for j in _load_raw():
        if j.get("job_id") == job_id:
            return Job.from_dict(j)
    return None


def get_latest_job() -> Optional[Job]:
    """Get the most recently submitted job."""
    jobs = _load_raw()
    if not jobs:
        return None
    return Job.from_dict(jobs[-1])


def list_jobs(
    limit: int = 0,
    include_completed: bool = False,
    project: str = "",
) -> list[Job]:
    """List jobs, most recent first.

    Args:
        limit: Max number of jobs to return (0 = all).
        include_completed: Include completed/failed/cancelled jobs.
        project: Filter by project name (empty = all projects).
    """
    raw = list(reversed(_load_raw()))

    result: list[Job] = []
    for j in raw:
        job = Job.from_dict(j)
        if project and job.project != project:
            continue
        if not include_completed and job.status in TERMINAL_STATES:
            continue
        result.append(job)
        if limit and len(result) >= limit:
            break

    return result


def list_all_jobs(limit: int = 0, project: str = "") -> list[Job]:
    """List all jobs regardless of status, most recent first.

    Args:
        limit: Max number of jobs to return (0 = all).
        project: Filter by project name (empty = all projects).
    """
    raw = list(reversed(_load_raw()))

    result: list[Job] = []
    for j in raw:
        job = Job.from_dict(j)
        if project and job.project != project:
            continue
        result.append(job)
        if limit and len(result) >= limit:
            break
    return result


def get_active_job_ids(project: str = "") -> list[str]:
    """Get IDs of jobs in PENDING or RUNNING status.

    Args:
        project: Filter by project name (empty = all projects).
    """
    return [
        j["job_id"]
        for j in _load_raw()
        if j.get("status") in ("PENDING", "RUNNING")
        and (not project or j.get("project", "") == project)
    ]
