"""SLURM command wrappers: sbatch, squeue, sacct, and directive parsing.

Parses both #SBATCH (standard SLURM) and #SLUGGER (slugger-specific) directives
from batch scripts to extract job metadata for tracking.
"""

from __future__ import annotations

import re
import shlex
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Optional

from slugger.models import Job, JobStatus

# ── #SLUGGER directive mapping ──────────────────────────────────────────────

DIRECTIVE_MAP: dict[str, str] = {
    "--gpus": "--gres=gpu:{value}",
    "--partition": "--partition={value}",
    "--time": "--time={value}",
    "--cpus": "--cpus-per-task={value}",
    "--mem": "--mem={value}",
    "--name": "--job-name={value}",
}

# ── Security ────────────────────────────────────────────────────────────────

# sbatch flags that enable arbitrary command execution
_BLOCKED_SBATCH_ARGS = {"--wrap", "--export-file"}
_BLOCKED_SBATCH_PREFIXES = ("--wrap=", "--export-file=")

# Regex for validating #SLUGGER directive values
_SAFE_DIRECTIVE_VALUE = re.compile(r"^[a-zA-Z0-9._:/-]+$")

# Regex for validating SLURM job IDs (numeric only)
_VALID_JOB_ID = re.compile(r"^\d+$")

# ── #SBATCH parsing regex ───────────────────────────────────────────────────

# Matches: #SBATCH --key=value  or  #SBATCH --key="quoted value"
# Handles both --key=value and --key value forms
_SBATCH_LINE = re.compile(r"^\s*#SBATCH\s+(.+)$")


def _validate_sbatch_args(args: list[str]) -> list[str]:
    """Reject dangerous sbatch arguments that could enable command execution."""
    validated: list[str] = []
    for arg in args:
        lower = arg.lower()
        if lower in _BLOCKED_SBATCH_ARGS or any(
            lower.startswith(p) for p in _BLOCKED_SBATCH_PREFIXES
        ):
            raise ValueError(f"Blocked dangerous sbatch argument: {arg.split('=')[0]}")
        validated.append(arg)
    return validated


def validate_job_id(job_id: str) -> bool:
    """Check that a job ID is a valid numeric SLURM job ID."""
    return bool(_VALID_JOB_ID.match(job_id))


# ── #SBATCH directive extraction ────────────────────────────────────────────

def parse_sbatch_directives(script_path: str) -> dict[str, str]:
    """Parse #SBATCH directives from a script into a key-value dict.

    Handles:
      #SBATCH --job-name="EMB - Train Qwen3-VL-8B (MMDiT-v2; D4S8)"
      #SBATCH --gpus=2
      #SBATCH --partition=sjw_alinlab
      #SBATCH --output=slurm_out/%j-name.out

    Returns dict like {"--job-name": "EMB - Train...", "--gpus": "2", ...}
    """
    path = Path(script_path)
    if not path.exists():
        return {}

    directives: dict[str, str] = {}
    content = path.read_text()

    for line in content.splitlines():
        m = _SBATCH_LINE.match(line)
        if not m:
            continue

        rest = m.group(1).strip()
        # Use shlex to handle quoted values properly
        try:
            tokens = shlex.split(rest)
        except ValueError:
            # Malformed quotes — fall back to simple split
            tokens = rest.split()

        for token in tokens:
            if "=" in token:
                key, _, val = token.partition("=")
                directives[key] = val

    return directives


def _extract_sbatch_metadata(directives: dict[str, str]) -> dict[str, object]:
    """Extract Job-relevant metadata from parsed #SBATCH directives.

    Returns dict with optional keys: job_name, partition, gpus, cpus.
    """
    meta: dict[str, object] = {}

    # Job name: --job-name or -J
    job_name = directives.get("--job-name") or directives.get("-J")
    if job_name:
        meta["job_name"] = job_name

    # Partition
    partition = directives.get("--partition") or directives.get("-p")
    if partition:
        meta["partition"] = partition

    # GPUs: --gpus=N (simple SLURM syntax) or --gres=gpu:N
    gpus_str = directives.get("--gpus") or directives.get("--gpus-per-node")
    if gpus_str:
        try:
            meta["gpus"] = int(gpus_str)
        except ValueError:
            pass

    if "gpus" not in meta:
        gres = directives.get("--gres", "")
        gres_match = re.match(r"gpu(?::\w+)?:(\d+)", gres)
        if gres_match:
            meta["gpus"] = int(gres_match.group(1))

    # CPUs
    cpus_str = directives.get("--cpus-per-task")
    if cpus_str:
        try:
            meta["cpus"] = int(cpus_str)
        except ValueError:
            pass

    return meta


# ── #SLUGGER directive parsing ──────────────────────────────────────────────

def parse_slugger_directives(script_path: str) -> tuple[list[str], str]:
    """Parse #SLUGGER directives from a script.

    Returns:
        Tuple of (sbatch_args, project_name). project_name is empty if not specified.
    """
    path = Path(script_path)
    if not path.exists():
        return [], ""

    sbatch_args: list[str] = []
    project = ""
    content = path.read_text()

    for line in content.splitlines():
        stripped = line.strip()
        if not stripped.startswith("#SLUGGER"):
            continue

        tokens = stripped[len("#SLUGGER"):].split()
        i = 0
        while i < len(tokens):
            token = tokens[i]
            if token == "--project" and i + 1 < len(tokens):
                raw_project = tokens[i + 1]
                # Only accept safe project names; invalid ones fall back to cwd
                if _SAFE_DIRECTIVE_VALUE.match(raw_project):
                    project = raw_project
                i += 2
            elif token in DIRECTIVE_MAP and i + 1 < len(tokens):
                value = tokens[i + 1]
                if not _SAFE_DIRECTIVE_VALUE.match(value):
                    raise ValueError(f"Invalid directive value: {value!r}")
                sbatch_args.append(DIRECTIVE_MAP[token].format(value=value))
                i += 2
            else:
                i += 1

    return sbatch_args, project


# ── Job submission ──────────────────────────────────────────────────────────

def submit_job(
    script_path: str,
    extra_args: list[str] | None = None,
    project: str = "",
) -> Job:
    """Submit a job via sbatch and return a Job object.

    Metadata priority: CLI extra_args > #SLUGGER directives > #SBATCH directives.

    Args:
        script_path: Path to the batch script.
        extra_args: Additional sbatch arguments from CLI passthrough.
        project: Project name (overrides directive if non-empty).
    """
    path = Path(script_path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Script not found: {path}")

    # Parse both directive types
    directive_args, directive_project = parse_slugger_directives(str(path))
    sbatch_directives = parse_sbatch_directives(str(path))
    sbatch_meta = _extract_sbatch_metadata(sbatch_directives)

    resolved_project = project or directive_project
    all_args = _validate_sbatch_args(directive_args + (extra_args or []))

    cmd = ["sbatch"] + all_args + [str(path)]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)

    if result.returncode != 0:
        raise RuntimeError(f"sbatch failed: {result.stderr.strip()}")

    # Parse job ID from "Submitted batch job 12345"
    match = re.search(r"Submitted batch job (\d+)", result.stdout)
    if not match:
        raise RuntimeError(f"Could not parse job ID from: {result.stdout.strip()}")

    job_id = match.group(1)
    now = datetime.now().isoformat(timespec="seconds")

    # Extract metadata: CLI/SLUGGER args override #SBATCH defaults
    job_name = _extract_job_name(all_args, "") or sbatch_meta.get("job_name", path.name)
    partition = _extract_arg_value(all_args, "--partition") or sbatch_meta.get("partition")
    gpus = _extract_gpu_count(all_args)
    if gpus is None:
        gpus = sbatch_meta.get("gpus")

    # Resolve output/error paths — %j → job_id, make absolute
    stdout_path = _resolve_slurm_path(sbatch_directives.get("--output", ""), job_id)
    stderr_path = _resolve_slurm_path(sbatch_directives.get("--error", ""), job_id)

    return Job(
        job_id=job_id,
        script=str(path),
        status=JobStatus.PENDING,
        job_name=job_name,
        submit_time=now,
        partition=partition,
        gpus=gpus,
        stdout_path=stdout_path or None,
        stderr_path=stderr_path or None,
        project=resolved_project,
        sbatch_args=all_args,
    )


# ── SLURM queries ──────────────────────────────────────────────────────────

def query_active_jobs() -> dict[str, str] | None:
    """Query squeue for active jobs. Returns {job_id: state}, or None on failure."""
    cmd = ["squeue", "--me", "--noheader", "--format=%i|%T"]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)

    if result.returncode != 0:
        return None  # squeue failed — caller should skip this cycle

    jobs: dict[str, str] = {}
    for line in result.stdout.strip().splitlines():
        parts = line.strip().split("|")
        if len(parts) == 2:
            jobs[parts[0].strip()] = parts[1].strip()
    return jobs


def query_job_details(job_id: str) -> Optional[dict]:
    """Query sacct for detailed job info. Returns parsed dict or None."""
    if not validate_job_id(job_id):
        return None
    sacct_fields = "JobID,JobName,State,Partition,NodeList,AllocTRES,Start,End,Elapsed,ExitCode"
    cmd = [
        "sacct",
        "-j", job_id,
        "--noheader",
        "--parsable2",
        f"--format={sacct_fields}",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)

    if result.returncode != 0 or not result.stdout.strip():
        return None

    # Take the first line (main job, not steps)
    for line in result.stdout.strip().splitlines():
        parts = line.split("|")
        if len(parts) < 10:
            continue
        # Skip job steps like "12345.batch", "12345.0"
        raw_id = parts[0].strip()
        if "." in raw_id:
            continue

        gpus, cpus, memory_mb = parse_alloc_tres(parts[5])

        return {
            "job_id": raw_id,
            "job_name": parts[1].strip(),
            "status": _map_slurm_state(parts[2].strip()),
            "partition": parts[3].strip() or None,
            "node": parts[4].strip() or None,
            "gpus": gpus,
            "cpus": cpus,
            "memory_mb": memory_mb,
            "start_time": _clean_time(parts[6]),
            "end_time": _clean_time(parts[7]),
            "elapsed": parts[8].strip() or None,
            "exit_code": parts[9].strip() or None,
        }

    return None


# ── Parsing helpers ─────────────────────────────────────────────────────────

def parse_alloc_tres(tres_str: str) -> tuple[Optional[int], Optional[int], Optional[int]]:
    """Parse AllocTRES string like 'cpu=24,gres/gpu=2,mem=300G'.

    Returns (gpus, cpus, memory_mb).
    """
    gpus: Optional[int] = None
    cpus: Optional[int] = None
    memory_mb: Optional[int] = None

    if not tres_str.strip():
        return gpus, cpus, memory_mb

    for part in tres_str.split(","):
        part = part.strip()
        if part.startswith("gres/gpu="):
            try:
                gpus = int(part.split("=")[1])
            except (ValueError, IndexError):
                pass
        elif part.startswith("cpu="):
            try:
                cpus = int(part.split("=")[1])
            except (ValueError, IndexError):
                pass
        elif part.startswith("mem="):
            memory_mb = _parse_memory(part.split("=")[1])

    return gpus, cpus, memory_mb


def _parse_memory(mem_str: str) -> Optional[int]:
    """Convert memory string like '300G', '32000M', '1T' to MB."""
    mem_str = mem_str.strip()
    if not mem_str:
        return None

    match = re.match(r"(\d+(?:\.\d+)?)\s*([KMGT]?)", mem_str, re.IGNORECASE)
    if not match:
        return None

    value = float(match.group(1))
    unit = match.group(2).upper()

    multipliers = {"": 1, "K": 1 / 1024, "M": 1, "G": 1024, "T": 1024 * 1024}
    result = value * multipliers.get(unit, 1)
    return max(1, int(result)) if result > 0 else 0


def _map_slurm_state(state: str) -> JobStatus:
    """Map SLURM state strings to JobStatus enum."""
    state = state.split()[0] if state else ""  # Handle "CANCELLED by ..."
    state = state.rstrip("+")  # SLURM appends + when not all processes exited cleanly
    mapping = {
        "PENDING": JobStatus.PENDING,
        "RUNNING": JobStatus.RUNNING,
        "COMPLETED": JobStatus.COMPLETED,
        "FAILED": JobStatus.FAILED,
        "TIMEOUT": JobStatus.TIMEOUT,
        "CANCELLED": JobStatus.CANCELLED,
        "CANCELED": JobStatus.CANCELLED,
        "NODE_FAIL": JobStatus.FAILED,
        "BOOT_FAIL": JobStatus.FAILED,
        "PREEMPTED": JobStatus.CANCELLED,
        "OUT_OF_MEMORY": JobStatus.FAILED,
        "DEADLINE": JobStatus.TIMEOUT,
        "REQUEUED": JobStatus.PENDING,
    }
    return mapping.get(state.upper(), JobStatus.UNKNOWN)


def _extract_job_name(args: list[str], fallback: str) -> str:
    """Extract job name from sbatch args, using fallback if not found."""
    name = _extract_arg_value(args, "--job-name")
    if name:
        return name
    # Check -J shorthand
    for i, arg in enumerate(args):
        if arg == "-J" and i + 1 < len(args):
            return args[i + 1]
    return fallback


def _extract_arg_value(args: list[str], prefix: str) -> Optional[str]:
    """Extract value from sbatch args like --partition=sjw."""
    for arg in args:
        if arg.startswith(f"{prefix}="):
            return arg.split("=", 1)[1]
    return None


def _extract_gpu_count(args: list[str]) -> Optional[int]:
    """Extract GPU count from --gres=gpu:N or --gres=gpu:type:N."""
    for arg in args:
        match = re.match(r"--gres=gpu(?::\w+)?:(\d+)", arg)
        if match:
            return int(match.group(1))
    return None


def _resolve_slurm_path(raw: str, job_id: str) -> str:
    """Resolve SLURM output path: replace %j with job_id, make absolute."""
    if not raw:
        return ""
    resolved = raw.replace("%j", job_id)
    p = Path(resolved)
    if not p.is_absolute():
        p = Path.cwd() / p
    return str(p)


def _clean_time(time_str: str) -> Optional[str]:
    """Clean up SLURM time fields, returning None for 'Unknown'."""
    t = time_str.strip()
    if not t or t.lower() == "unknown":
        return None
    return t
