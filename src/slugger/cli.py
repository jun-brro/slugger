"""Slugger CLI: SLURM job manager with Google Sheets logging."""

from __future__ import annotations

import logging
import os
import re
import shutil
from pathlib import Path
from typing import Optional

import typer
from rich.panel import Panel

from slugger.config import CONFIG_PATH, CREDENTIALS_PATH, ensure_slugger_dir, load_config
from slugger.models import SluggerConfig
from slugger.display import console, show_job_detail, show_job_list, show_submit_result
from slugger.gsheet_sync import create_row, update_row
from slugger.slurm import submit_job, validate_job_id
from slugger.store import get_job, get_latest_job, list_all_jobs, save_job

app = typer.Typer(
    name="slugger",
    help="SLURM job manager with Google Sheets logging.",
    add_completion=False,
    no_args_is_help=True,
)

poller_app = typer.Typer(help="Background poller management.")
app.add_typer(poller_app, name="poller")

logger = logging.getLogger("slugger")


def _resolve_project(project_opt: str) -> str:
    """Resolve project name: explicit flag > cwd basename.

    Sanitizes for Google Sheets worksheet title compatibility.
    """
    name = project_opt if project_opt else Path.cwd().name
    if not name:
        name = "default"
    # Remove characters invalid in Google Sheets worksheet titles
    name = re.sub(r'[:\\/*?\[\]]', '_', name)
    return name[:100]


@app.command()
def submit(
    script: str = typer.Argument(help="Path to batch script"),
    sbatch_args: Optional[list[str]] = typer.Argument(None, help="Extra sbatch arguments"),
    project: str = typer.Option("", "--project", "-p", help="Project name (default: cwd name)"),
) -> None:
    """Submit a SLURM job and record it."""
    try:
        # Pass raw CLI flag — submit_job handles priority: CLI > #SLUGGER > ""
        job = submit_job(script, sbatch_args, project=project)
    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)
    except ValueError as e:
        console.print(f"[red]Invalid argument:[/red] {e}")
        raise typer.Exit(1)
    except RuntimeError as e:
        console.print(f"[red]sbatch failed:[/red] {e}")
        raise typer.Exit(1)

    # Resolve project: CLI flag > #SLUGGER directive > cwd name
    final_project = _resolve_project(job.project)
    if final_project != job.project:
        job = job.with_update(project=final_project)

    save_job(job)

    # Sync to Google Sheets (best-effort)
    config = load_config()
    if config.sheet_configured:
        row = create_row(job, config)
        if row:
            job = job.with_update(sheet_row=row)
            save_job(job)

    show_submit_result(job)

    # Auto-start poller
    from slugger.poller import is_running, start_poller

    if not is_running():
        start_poller()
        console.print("[dim]Poller started in background.[/dim]")


@app.command()
def status(
    job_id: Optional[str] = typer.Argument(None, help="Job ID (default: latest)"),
) -> None:
    """Show job status."""
    if job_id and not validate_job_id(job_id):
        console.print(f"[red]Invalid job ID:[/red] {job_id}")
        raise typer.Exit(1)
    job = get_job(job_id) if job_id else get_latest_job()

    if job is None:
        console.print("[red]Job not found.[/red]")
        raise typer.Exit(1)

    show_job_detail(job)


@app.command(name="list")
def list_cmd(
    all_jobs: bool = typer.Option(False, "-a", "--all", help="Include completed jobs"),
    limit: int = typer.Option(20, "-n", "--limit", help="Max number of jobs"),
    project: str = typer.Option("", "--project", "-p", help="Project name filter"),
    all_projects: bool = typer.Option(False, "--all-projects", help="Show jobs from all projects"),
) -> None:
    """List tracked jobs."""
    from slugger.store import list_jobs

    # Determine project filter
    if all_projects:
        proj_filter = ""
    else:
        proj_filter = _resolve_project(project)

    if all_jobs:
        jobs = list_all_jobs(limit=limit, project=proj_filter)
    else:
        jobs = list_jobs(limit=limit, project=proj_filter)

    show_job_list(jobs, show_project=all_projects)


@app.command()
def sync(
    job_id: Optional[str] = typer.Argument(None, help="Job ID (default: all active)"),
    project: str = typer.Option("", "--project", "-p", help="Project name filter"),
) -> None:
    """Force sync job(s) to Google Sheets."""
    config = load_config()
    if not config.sheet_configured:
        console.print("[red]Google Sheets not configured. Run 'slugger login' first.[/red]")
        raise typer.Exit(1)

    if job_id:
        if not validate_job_id(job_id):
            console.print(f"[red]Invalid job ID:[/red] {job_id}")
            raise typer.Exit(1)
        _sync_one(job_id, config)
    else:
        from slugger.store import get_active_job_ids

        proj_filter = _resolve_project(project)
        active = get_active_job_ids(project=proj_filter)
        if not active:
            console.print("[dim]No active jobs to sync.[/dim]")
            return
        for jid in active:
            _sync_one(jid, config)


def _sync_one(job_id: str, config: SluggerConfig) -> None:
    job = get_job(job_id)
    if job is None:
        console.print(f"[red]Job {job_id} not found.[/red]")
        return

    if job.sheet_row:
        ok = update_row(job, config)
        label = "updated" if ok else "update failed"
    else:
        row = create_row(job, config)
        if row:
            job = job.with_update(sheet_row=row)
            save_job(job)
        ok = row is not None
        label = "created" if ok else "creation failed"

    style = "green" if ok else "red"
    console.print(f"[{style}]Job {job_id}: {label}.[/{style}]")


@app.command()
def login() -> None:
    """Link Google Sheets (paste service account JSON path)."""
    ensure_slugger_dir()

    console.print(Panel(
        "[bold]Google Sheets Setup[/bold]\n\n"
        "1. Go to [link=https://console.cloud.google.com]console.cloud.google.com[/link]\n"
        "2. Create project → Enable [bold]Google Sheets API[/bold]\n"
        "3. Create Service Account → download JSON key\n"
        "4. Share your spreadsheet with the service account email",
        border_style="cyan",
    ))
    console.print()

    # Step 1: Credentials JSON
    default_creds = str(CREDENTIALS_PATH) if CREDENTIALS_PATH.exists() else ""
    creds_path = typer.prompt(
        "Path to service account JSON",
        default=default_creds,
        show_default=bool(default_creds),
    )

    src = Path(creds_path).expanduser().resolve()
    if not src.exists():
        console.print(f"[red]File not found:[/red] {src}")
        raise typer.Exit(1)

    # Copy to ~/.slugger/credentials.json with restrictive permissions
    if src != CREDENTIALS_PATH:
        shutil.copy2(src, CREDENTIALS_PATH)
        os.chmod(CREDENTIALS_PATH, 0o600)
        console.print(f"[dim]Copied to {CREDENTIALS_PATH}[/dim]")

    # Step 2: Verify credentials and list spreadsheets or ask for ID
    spreadsheet_id = ""
    sheet_title = ""

    try:
        import gspread
        gc = gspread.service_account(filename=str(CREDENTIALS_PATH))

        console.print("\n[dim]Credentials OK.[/dim]")
        console.print("[dim]Paste the spreadsheet URL or ID.[/dim]\n")

        raw = typer.prompt("Spreadsheet URL or ID")
        spreadsheet_id = _parse_spreadsheet_id(raw)

        if not re.match(r'^[a-zA-Z0-9_-]+$', spreadsheet_id):
            console.print("[red]Invalid spreadsheet ID format.[/red]")
            raise typer.Exit(1)

        sh = gc.open_by_key(spreadsheet_id)
        sheet_title = sh.title

        # Ensure headers on default sheet
        ws = sh.sheet1
        if ws.acell("A1").value != "Job ID":
            ws.update("A1", [[
                "Job ID", "Job Name", "Status", "Script", "Submit Time",
                "Start Time", "End Time", "Elapsed", "Node", "Partition",
                "GPUs", "CPUs", "Memory (MB)", "Exit Code",
            ]])
            console.print("[green]Headers created.[/green]")
        else:
            console.print("[green]Headers already exist.[/green]")

    except typer.Exit:
        raise
    except (SystemExit, KeyboardInterrupt):
        raise
    except Exception as e:
        console.print(f"[yellow]Error:[/yellow] {e}")
        if not spreadsheet_id or not re.match(r'^[a-zA-Z0-9_-]+$', spreadsheet_id):
            console.print("[red]Could not connect. Check credentials and spreadsheet sharing.[/red]")
            raise typer.Exit(1)

    # Save config with restrictive permissions
    content = (
        f'google_credentials = "{CREDENTIALS_PATH}"\n'
        f'spreadsheet_id = "{spreadsheet_id}"\n'
        f'poll_interval_sec = 60\n'
    )
    CONFIG_PATH.write_text(content)
    os.chmod(CONFIG_PATH, 0o600)

    # Apply conditional formatting for status badges
    if sheet_title:
        from slugger.gsheet_sync import apply_formatting

        saved_config = load_config()
        console.print("[dim]Applying formatting...[/dim]")
        if apply_formatting(saved_config):
            console.print("[green]Status badges + header styling applied.[/green]")

    console.print()
    if sheet_title:
        console.print(f"[green bold]Linked![/green bold] Spreadsheet: [cyan]{sheet_title}[/cyan]")
    else:
        console.print("[green]Saved.[/green]")
    console.print(f"[dim]Config: {CONFIG_PATH}[/dim]")


@app.command()
def init() -> None:
    """Alias for 'login'."""
    login()


def _parse_spreadsheet_id(raw: str) -> str:
    """Extract spreadsheet ID from a Google Sheets URL or raw ID."""
    raw = raw.strip()
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", raw)
    if match:
        return match.group(1)
    return raw


@app.command()
def log(
    job_id: Optional[str] = typer.Argument(None, help="Job ID (default: latest)"),
    err: bool = typer.Option(False, "--err", "-e", help="Show stderr instead of stdout"),
    follow: bool = typer.Option(True, "--follow/--no-follow", "-f/-F", help="Follow output (tail -f)"),
    lines: int = typer.Option(50, "-n", "--lines", help="Number of lines to show"),
) -> None:
    """Tail a job's output log (like tail -f)."""
    from slugger.models import JobStatus

    if lines < 1:
        lines = 1
    if job_id and not validate_job_id(job_id):
        console.print(f"[red]Invalid job ID:[/red] {job_id}")
        raise typer.Exit(1)

    job = get_job(job_id) if job_id else get_latest_job()
    if job is None:
        console.print("[red]No job found.[/red]")
        raise typer.Exit(1)

    path = job.stderr_path if err else job.stdout_path
    label = "stderr" if err else "stdout"

    if not path:
        console.print(f"[yellow]Job {job.job_id} has no {label} path recorded.[/yellow]")
        console.print("[dim]Hint: only jobs submitted via slugger have log paths.[/dim]")
        raise typer.Exit(1)

    log_file = Path(path).resolve()
    if not log_file.is_file():
        if job.status == JobStatus.PENDING:
            console.print(f"[yellow]Job {job.job_id} is PENDING — log file not created yet.[/yellow]")
        else:
            console.print(f"[red]Log file not found:[/red] {path}")
        raise typer.Exit(1)

    # Use resolved absolute path for tail
    path = str(log_file)

    console.print(
        f"[cyan]{label}[/cyan] [bold]{job.job_id}[/bold] "
        f"[dim]({job.job_name or job.script})[/dim]"
    )
    console.print(f"[dim]{path}[/dim]\n")

    # Use tail -f for running jobs, tail -n for completed
    should_follow = follow and job.status in (JobStatus.RUNNING, JobStatus.PENDING)
    if should_follow:
        os.execvp("tail", ["tail", "-f", "-n", str(lines), path])
    else:
        os.execvp("tail", ["tail", "-n", str(lines), path])


@app.command()
def gpu(
    job_id: Optional[str] = typer.Argument(None, help="Job ID (default: latest running job)"),
    tool: str = typer.Option("nvitop", "--tool", "-t", help="GPU tool: nvitop or nvidia-smi"),
) -> None:
    """Open GPU monitor on a job's compute node via SSH."""
    from slugger.models import JobStatus

    # Find the job
    if job_id:
        if not validate_job_id(job_id):
            console.print(f"[red]Invalid job ID:[/red] {job_id}")
            raise typer.Exit(1)
        job = get_job(job_id)
    else:
        # Find the latest RUNNING job (most useful default)
        from slugger.store import list_jobs

        running = [j for j in list_jobs(limit=10) if j.status == JobStatus.RUNNING]
        job = running[0] if running else get_latest_job()

    if job is None:
        console.print("[red]No job found.[/red]")
        raise typer.Exit(1)

    if not job.node:
        console.print(f"[yellow]Job {job.job_id} has no node assigned yet (status: {job.status.value}).[/yellow]")
        raise typer.Exit(1)

    node = job.node
    # Handle multi-node: take first node (e.g. "node01,node02" → "node01")
    if "," in node:
        node = node.split(",")[0]

    console.print(
        f"[cyan]Connecting to[/cyan] [bold]{node}[/bold] "
        f"[dim](job {job.job_id}: {job.job_name or job.script})[/dim]"
    )

    # Validate tool against allowlist
    allowed_tools = {"nvitop", "nvidia-smi"}
    if tool not in allowed_tools:
        console.print(f"[red]Unknown GPU tool:[/red] {tool}. Use: {', '.join(sorted(allowed_tools))}")
        raise typer.Exit(1)

    # Validate node hostname
    if not re.match(r'^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?$', node):
        console.print(f"[red]Invalid node name:[/red] {node}")
        raise typer.Exit(1)

    # Build SSH command — replace current process
    if tool == "nvitop":
        ssh_cmd = ["ssh", "-t", node, "nvitop"]
    else:
        ssh_cmd = ["ssh", "-t", node, "watch", "-n1", "nvidia-smi"]

    os.execvp("ssh", ssh_cmd)


@app.command()
def monitor(
    refresh: float = typer.Option(5.0, "-r", "--refresh", help="Refresh interval in seconds"),
    limit: int = typer.Option(50, "-n", "--limit", help="Max jobs to display"),
    project: str = typer.Option("", "--project", "-p", help="Project name filter"),
) -> None:
    """Live dashboard for monitoring jobs."""
    from slugger.monitor import run_monitor

    if refresh < 0.5:
        refresh = 0.5
    resolved = _resolve_project(project)
    run_monitor(refresh_sec=refresh, limit=limit, project=resolved)


@poller_app.command("start")
def poller_start() -> None:
    """Start the background poller."""
    from slugger.poller import is_running, start_poller

    if is_running():
        console.print("[yellow]Poller is already running.[/yellow]")
        return

    start_poller()
    console.print("[green]Poller started.[/green]")


@poller_app.command("stop")
def poller_stop() -> None:
    """Stop the background poller."""
    from slugger.poller import stop_poller

    if stop_poller():
        console.print("[green]Poller stopped.[/green]")
    else:
        console.print("[dim]Poller is not running.[/dim]")


@poller_app.command("status")
def poller_status() -> None:
    """Check poller status."""
    from slugger.poller import get_poller_pid

    pid = get_poller_pid()
    if pid:
        console.print(f"[green]Poller is running[/green] (PID {pid})")
    else:
        console.print("[dim]Poller is not running.[/dim]")
