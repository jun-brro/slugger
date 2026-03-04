"""Data models for slugger."""

from __future__ import annotations

from dataclasses import dataclass, field, fields, asdict
from enum import Enum
from typing import Optional


class JobStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"
    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class Job:
    job_id: str
    script: str
    status: JobStatus = JobStatus.PENDING
    job_name: str = ""
    submit_time: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    elapsed: Optional[str] = None
    node: Optional[str] = None
    partition: Optional[str] = None
    gpus: Optional[int] = None
    cpus: Optional[int] = None
    memory_mb: Optional[int] = None
    exit_code: Optional[str] = None
    stdout_path: Optional[str] = None
    stderr_path: Optional[str] = None
    project: str = ""
    sheet_row: Optional[int] = None
    sbatch_args: list[str] = field(default_factory=list)

    def with_update(self, **kwargs: object) -> Job:
        """Return a new Job with updated fields."""
        current = asdict(self)
        current.update(kwargs)
        if isinstance(current["status"], str):
            current["status"] = JobStatus(current["status"])
        return Job(**current)

    def to_dict(self) -> dict:
        data = asdict(self)
        data["status"] = self.status.value
        return data

    @classmethod
    def from_dict(cls, data: dict) -> Job:
        data = {**data}
        data["status"] = JobStatus(data.get("status", "UNKNOWN"))
        # Only pass known fields — ignore legacy/unknown keys
        known = {f.name for f in fields(cls)}
        filtered = {k: v for k, v in data.items() if k in known}
        return cls(**filtered)


@dataclass(frozen=True)
class SluggerConfig:
    google_credentials: str = ""
    spreadsheet_id: str = ""
    poll_interval_sec: int = 30

    @property
    def sheet_configured(self) -> bool:
        return bool(self.google_credentials and self.spreadsheet_id)
