"""Local JSON store for job persistence with atomic writes."""

from __future__ import annotations

import fcntl
import json
import os
import tempfile
from pathlib import Path
from typing import Optional

from slugger.config import ensure_slugger_dir
from slugger.models import Job, JobStatus

# Terminal states excluded from active listings
TERMINAL_STATES = {
    JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.TIMEOUT, JobStatus.CANCELLED,
}


def _jobs_path() -> Path:
    return ensure_slugger_dir() / "jobs.json"


def _load_raw() -> list[dict]:
    path = _jobs_path()
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text())
        return data if isinstance(data, list) else []
    except (json.JSONDecodeError, OSError):
        return []


def save_job(job: Job) -> None:
    """Save or update a job in the store."""
    path = _jobs_path()
    lock_path = path.with_suffix(".lock")
    ensure_slugger_dir()

    with open(lock_path, "w") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            # Re-read inside lock to prevent lost updates
            jobs = _load_raw()
            new_data = job.to_dict()
            updated = any(j.get("job_id") == job.job_id for j in jobs)
            result = [
                new_data if j.get("job_id") == job.job_id else j
                for j in jobs
            ]
            if not updated:
                result = [*result, new_data]
            _save_raw_unlocked(result, path)
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


def _save_raw_unlocked(jobs: list[dict], path: Path) -> None:
    """Write jobs to disk without acquiring lock (caller must hold lock)."""
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

    jobs = [Job.from_dict(j) for j in raw]
    if project:
        jobs = [j for j in jobs if j.project == project]
    if limit:
        jobs = jobs[:limit]
    return jobs


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
