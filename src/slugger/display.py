"""Rich terminal output for slugger."""

from __future__ import annotations

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from slugger.models import Job, JobStatus

console = Console()

STATUS_COLORS: dict[str, str] = {
    "PENDING": "yellow",
    "RUNNING": "blue",
    "COMPLETED": "green",
    "FAILED": "red",
    "TIMEOUT": "magenta",
    "CANCELLED": "dim",
    "UNKNOWN": "dim",
}


def _status_text(status: JobStatus) -> Text:
    color = STATUS_COLORS.get(status.value, "white")
    return Text(status.value, style=f"bold {color}")


def show_submit_result(job: Job) -> None:
    """Display job submission result."""
    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column("key", style="dim")
    table.add_column("value")

    table.add_row("Job ID", Text(job.job_id, style="bold cyan"))
    table.add_row("Script", job.script)
    table.add_row("Status", _status_text(job.status))
    if job.project:
        table.add_row("Project", Text(job.project, style="magenta"))
    if job.job_name:
        table.add_row("Name", job.job_name)
    if job.partition:
        table.add_row("Partition", job.partition)
    if job.gpus is not None:
        table.add_row("GPUs", str(job.gpus))
    if job.sbatch_args:
        table.add_row("Args", " ".join(job.sbatch_args))
    if job.sheet_row:
        table.add_row("Sheet", Text("synced", style="green"))

    console.print(Panel(table, title="[bold]Job Submitted[/bold]", border_style="green"))


def show_job_detail(job: Job) -> None:
    """Display detailed job status."""
    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column("key", style="dim")
    table.add_column("value")

    table.add_row("Job ID", Text(job.job_id, style="bold cyan"))
    table.add_row("Name", job.job_name or "-")
    table.add_row("Status", _status_text(job.status))
    table.add_row("Script", job.script)
    if job.project:
        table.add_row("Project", Text(job.project, style="magenta"))
    if job.partition:
        table.add_row("Partition", job.partition)
    if job.node:
        table.add_row("Node", job.node)
    if job.gpus is not None:
        table.add_row("GPUs", str(job.gpus))
    if job.cpus is not None:
        table.add_row("CPUs", str(job.cpus))
    if job.memory_mb is not None:
        table.add_row("Memory", f"{job.memory_mb} MB")
    if job.submit_time:
        table.add_row("Submitted", job.submit_time)
    if job.start_time:
        table.add_row("Started", job.start_time)
    if job.end_time:
        table.add_row("Ended", job.end_time)
    if job.elapsed:
        table.add_row("Elapsed", job.elapsed)
    if job.exit_code:
        table.add_row("Exit Code", job.exit_code)

    sheet_status = "synced" if job.sheet_row else "not synced"
    sheet_style = "green" if job.sheet_row else "dim"
    table.add_row("Sheet", Text(sheet_status, style=sheet_style))

    console.print(Panel(table, title=f"[bold]Job {job.job_id}[/bold]"))


def show_job_list(jobs: list[Job], show_project: bool = False) -> None:
    """Display job list as a table.

    Args:
        jobs: Jobs to display.
        show_project: Show Project column (useful for --all-projects view).
    """
    if not jobs:
        console.print("[dim]No jobs found.[/dim]")
        return

    table = Table(title="Jobs")
    table.add_column("ID", style="cyan", no_wrap=True)
    if show_project:
        table.add_column("Project", style="magenta")
    table.add_column("Name")
    table.add_column("Status")
    table.add_column("Partition", style="dim")
    table.add_column("GPUs", justify="right")
    table.add_column("Submitted", style="dim")
    table.add_column("Elapsed", style="dim")
    table.add_column("Sheet", justify="center")

    for job in jobs:
        sheet_mark = "[green]v[/green]" if job.sheet_row else "[dim]-[/dim]"
        row: list = [job.job_id]
        if show_project:
            row.append(job.project or "-")
        row.extend([
            job.job_name or "-",
            _status_text(job.status),
            job.partition or "-",
            str(job.gpus) if job.gpus is not None else "-",
            job.submit_time or "-",
            job.elapsed or "-",
            sheet_mark,
        ])
        table.add_row(*row)

    console.print(table)
