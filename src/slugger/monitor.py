"""Live terminal monitor dashboard using Rich Live display."""

from __future__ import annotations

import os
import select
import subprocess
import sys
import termios
import time
import tty
from datetime import datetime

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from slugger.models import Job, JobStatus
from slugger.slurm import validate_job_id
from slugger.store import list_all_jobs

STATUS_ICONS: dict[str, str] = {
    "PENDING": "◯",
    "RUNNING": "●",
    "COMPLETED": "✓",
    "FAILED": "✗",
    "TIMEOUT": "⏱",
    "CANCELLED": "⊘",
    "UNKNOWN": "?",
}

STATUS_COLORS: dict[str, str] = {
    "PENDING": "yellow",
    "RUNNING": "bold blue",
    "COMPLETED": "green",
    "FAILED": "bold red",
    "TIMEOUT": "magenta",
    "CANCELLED": "dim",
    "UNKNOWN": "dim",
}


def _styled_status(status: JobStatus) -> Text:
    icon = STATUS_ICONS.get(status.value, "?")
    color = STATUS_COLORS.get(status.value, "white")
    return Text(f"{icon} {status.value}", style=color)


def _build_summary(jobs: list[Job], project: str = "") -> Panel:
    """Build the top summary bar with job counts."""
    counts: dict[str, int] = {}
    for job in jobs:
        counts[job.status.value] = counts.get(job.status.value, 0) + 1

    parts: list[str] = []
    for status_name, color in [
        ("RUNNING", "bold blue"),
        ("PENDING", "yellow"),
        ("COMPLETED", "green"),
        ("FAILED", "bold red"),
        ("TIMEOUT", "magenta"),
        ("CANCELLED", "dim"),
    ]:
        count = counts.get(status_name, 0)
        if count > 0:
            icon = STATUS_ICONS[status_name]
            parts.append(f"[{color}]{icon} {count} {status_name}[/{color}]")

    total = len(jobs)
    summary_text = "  ".join(parts) if parts else "[dim]No jobs[/dim]"

    title_suffix = f" [dim]\\[{project}][/dim]" if project else ""
    return Panel(
        Text.from_markup(f"  {summary_text}    [dim]│[/dim]  [bold]{total}[/bold] total"),
        title=f"[bold]Slugger Monitor[/bold]{title_suffix}",
        border_style="cyan",
        height=3,
    )


def _build_job_table(jobs: list[Job]) -> Table:
    """Build the main job table."""
    table = Table(
        expand=True,
        show_edge=False,
        pad_edge=False,
        row_styles=["", "dim"],
    )
    table.add_column("ID", style="cyan", no_wrap=True, width=10)
    table.add_column("Name", min_width=15, max_width=30)
    table.add_column("Status", width=14)
    table.add_column("Partition", width=14, style="dim")
    table.add_column("GPU", justify="right", width=4)
    table.add_column("Node", width=16, style="dim")
    table.add_column("Submitted", width=20, style="dim")
    table.add_column("Elapsed", width=12, style="dim")
    table.add_column("Exit", width=5, justify="center")

    for job in jobs:
        exit_style = ""
        exit_text = job.exit_code or ""
        if exit_text and not exit_text.startswith("0"):
            exit_style = "bold red"
        elif exit_text:
            exit_style = "green"

        submit_display = _format_time(job.submit_time)

        table.add_row(
            job.job_id,
            Text(job.job_name or "-", overflow="ellipsis"),
            _styled_status(job.status),
            job.partition or "-",
            str(job.gpus) if job.gpus is not None else "-",
            job.node or "-",
            submit_display,
            job.elapsed or _running_elapsed(job),
            Text(exit_text, style=exit_style) if exit_text else Text("-", style="dim"),
        )

    return table


def _build_active_detail(jobs: list[Job], selected: int = -1) -> Panel:
    """Build a detail panel for currently active (RUNNING/PENDING) jobs."""
    active = [j for j in jobs if j.status in (JobStatus.RUNNING, JobStatus.PENDING)]

    if not active:
        return Panel(
            Text("No active jobs", style="dim", justify="center"),
            title="[bold]Active Jobs[/bold]",
            border_style="dim",
            height=6,
        )

    rows: list[Text] = []
    for i, job in enumerate(active[:10]):
        icon = STATUS_ICONS.get(job.status.value, "?")
        color = STATUS_COLORS.get(job.status.value, "white")
        elapsed = job.elapsed or _running_elapsed(job)
        gpu_str = f"GPU:{job.gpus}" if job.gpus else ""
        node_str = job.node or "waiting"
        is_selected = i == selected

        if is_selected:
            # Reverse video: white background, dark text
            content = f" ▸ {icon} {job.job_id}  {job.job_name or job.script.split('/')[-1]:<20s}  {node_str:<16s}  {gpu_str:<8s}  {elapsed} "
            line = Text(content, style="reverse bold")
        else:
            line = Text()
            line.append("   ")
            line.append(f"{icon} ", style=color)
            line.append(f"{job.job_id} ", style="cyan")
            line.append(f"{job.job_name or job.script.split('/')[-1]:<20s} ", style="white")
            line.append(f"{node_str:<16s} ", style="dim")
            line.append(f"{gpu_str:<8s} ", style="yellow")
            line.append(elapsed, style="dim")
        rows.append(line)

    if len(active) > 10:
        rows.append(Text(f"   ... and {len(active) - 10} more", style="dim"))

    border = "cyan" if selected >= 0 else "blue"
    return Panel(
        Group(*rows),
        title=f"[bold]Active Jobs ({len(active)})[/bold]",
        border_style=border,
    )


def _build_scontrol_panel(detail: str, job_id: str) -> Panel:
    """Build a panel showing scontrol output."""
    if not detail:
        return Panel(
            Text("Press 'd' on an active job to see details", style="dim", justify="center"),
            title="[bold]Job Detail[/bold]",
            border_style="dim",
            height=3,
        )

    text = Text()
    for line in detail.splitlines():
        line = line.strip()
        if not line:
            continue
        # Parse key=value pairs and colorize
        if "=" in line:
            parts = line.split()
            for j, part in enumerate(parts):
                if j > 0:
                    text.append("  ")
                if "=" in part:
                    key, _, val = part.partition("=")
                    text.append(f"{key}=", style="dim")
                    # Highlight important values
                    if key in ("JobState", "State"):
                        style = _state_color(val)
                    elif key in ("NodeList", "BatchHost"):
                        style = "cyan"
                    elif key in ("NumGPUs", "Gres", "TresPerNode"):
                        style = "yellow"
                    elif key in ("JobId", "JobName"):
                        style = "bold"
                    elif key in ("RunTime", "TimeLimit", "Elapsed"):
                        style = "green"
                    elif key in ("ExitCode", "DerivedExitCode"):
                        style = "red" if val != "0:0" else "green"
                    else:
                        style = "white"
                    text.append(val, style=style)
                else:
                    text.append(part)
            text.append("\n")
        else:
            text.append(line + "\n")

    return Panel(
        text,
        title=f"[bold]scontrol show job {job_id}[/bold]",
        border_style="green",
    )


def _state_color(state: str) -> str:
    mapping = {
        "RUNNING": "bold blue",
        "PENDING": "yellow",
        "COMPLETED": "green",
        "FAILED": "bold red",
        "TIMEOUT": "magenta",
        "CANCELLED": "dim",
    }
    return mapping.get(state.upper(), "white")


def _query_scontrol(job_id: str) -> str:
    """Run scontrol show job and return formatted output."""
    if not validate_job_id(job_id):
        return ""
    cmd = ["scontrol", "show", "job", job_id]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        return f"scontrol failed: {result.stderr.strip()}"
    return result.stdout.strip()


def _format_time(iso_str: str | None) -> str:
    """Format ISO time to a compact display string."""
    if not iso_str:
        return "-"
    try:
        dt = datetime.fromisoformat(iso_str)
        now = datetime.now()
        if dt.date() == now.date():
            return dt.strftime("today %H:%M")
        return dt.strftime("%m/%d %H:%M")
    except (ValueError, TypeError):
        return iso_str


def _running_elapsed(job: Job) -> str:
    """Calculate elapsed time for a still-running job."""
    if job.status not in (JobStatus.RUNNING, JobStatus.PENDING):
        return "-"
    if not job.start_time and not job.submit_time:
        return "-"

    ref = job.start_time or job.submit_time
    try:
        start = datetime.fromisoformat(ref)
        delta = datetime.now() - start
        hours, remainder = divmod(int(delta.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours > 0:
            return f"{hours}:{minutes:02d}:{seconds:02d}"
        return f"{minutes}:{seconds:02d}"
    except (ValueError, TypeError):
        return "-"


def _build_footer() -> Text:
    now = datetime.now().strftime("%H:%M:%S")
    footer = Text()
    footer.append(f"  Last refresh: {now}", style="dim")
    footer.append("    ", style="dim")
    footer.append("↑↓", style="bold")
    footer.append(" select  ", style="dim")
    footer.append("d", style="bold")
    footer.append(" detail(toggle)  ", style="dim")
    footer.append("u", style="bold")
    footer.append(" refresh  ", style="dim")
    footer.append("Ctrl+C", style="bold")
    footer.append(" exit", style="dim")
    return footer


def build_dashboard(
    jobs: list[Job],
    project: str = "",
    selected: int = -1,
    scontrol_detail: str = "",
    scontrol_job_id: str = "",
) -> Group:
    """Build the complete dashboard layout."""
    summary = _build_summary(jobs, project=project)
    table = _build_job_table(jobs)
    active_detail = _build_active_detail(jobs, selected=selected)
    footer = _build_footer()

    table_panel = Panel(table, title="[bold]All Jobs[/bold]", border_style="white")

    parts: list[object] = [summary, active_detail]
    if scontrol_detail:
        parts.append(_build_scontrol_panel(scontrol_detail, scontrol_job_id))
    parts.append(table_panel)
    parts.append(footer)

    return Group(*parts)


def _read_key_raw(fd: int, timeout: float) -> str | None:
    """Read a keypress from fd already in cbreak mode.

    Returns: 'UP', 'DOWN', or single character, or None on timeout.
    """
    ready, _, _ = select.select([fd], [], [], timeout)
    if not ready:
        return None

    # Read up to 4 bytes at once — escape sequences arrive as a burst
    buf = os.read(fd, 4)
    if not buf:
        return None

    # Arrow keys: \x1b[A (up), \x1b[B (down)
    if buf[:2] == b"\x1b[":
        code = buf[2:3]
        if code == b"A":
            return "UP"
        if code == b"B":
            return "DOWN"
        return None

    if buf[0:1] == b"\x1b":
        return None  # bare escape or unknown sequence

    return buf[0:1].decode("utf-8", errors="replace")


def _get_active_jobs(jobs: list[Job]) -> list[Job]:
    return [j for j in jobs if j.status in (JobStatus.RUNNING, JobStatus.PENDING)]


def run_monitor(refresh_sec: float = 5.0, limit: int = 50, project: str = "") -> None:
    """Run the live monitor dashboard."""
    console = Console()

    proj_label = f" [{project}]" if project else ""
    console.print(f"[bold cyan]Starting Slugger Monitor{proj_label}...[/bold cyan]")
    console.print("[dim]↑↓ select, d detail, u refresh, Ctrl+C exit[/dim]\n")

    selected: int = -1
    scontrol_detail: str = ""
    scontrol_job_id: str = ""

    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)

    try:
        # Set cbreak ONCE for the entire session
        tty.setcbreak(fd)

        jobs = list_all_jobs(limit=limit, project=project)
        with Live(
            build_dashboard(jobs, project=project, selected=selected,
                            scontrol_detail=scontrol_detail, scontrol_job_id=scontrol_job_id),
            console=console,
            refresh_per_second=30,
            screen=True,
        ) as live:
            while True:
                elapsed = 0.0
                step = 0.03
                force_refresh = False
                while elapsed < refresh_sec:
                    key = _read_key_raw(fd, step)
                    if key is not None:
                        active = _get_active_jobs(jobs)
                        n_active = len(active)

                        if key == "UP" and n_active > 0:
                            if selected <= 0:
                                selected = n_active - 1
                            else:
                                selected -= 1
                            # Immediate visual update without re-fetching jobs
                            live.update(build_dashboard(
                                jobs, project=project, selected=selected,
                                scontrol_detail=scontrol_detail, scontrol_job_id=scontrol_job_id,
                            ))
                            continue
                        elif key == "DOWN" and n_active > 0:
                            if selected < 0 or selected >= n_active - 1:
                                selected = 0
                            else:
                                selected += 1
                            live.update(build_dashboard(
                                jobs, project=project, selected=selected,
                                scontrol_detail=scontrol_detail, scontrol_job_id=scontrol_job_id,
                            ))
                            continue
                        elif len(key) == 1 and key.lower() == "d":
                            if scontrol_detail:
                                # Toggle off
                                scontrol_detail = ""
                                scontrol_job_id = ""
                            elif 0 <= selected < n_active:
                                # Toggle on
                                job_id = active[selected].job_id
                                scontrol_detail = _query_scontrol(job_id)
                                scontrol_job_id = job_id
                            else:
                                continue
                            live.update(build_dashboard(
                                jobs, project=project, selected=selected,
                                scontrol_detail=scontrol_detail, scontrol_job_id=scontrol_job_id,
                            ))
                            continue
                        elif len(key) == 1 and key.lower() == "u":
                            force_refresh = True
                            break

                    elapsed += step

                if force_refresh or elapsed >= refresh_sec:
                    jobs = list_all_jobs(limit=limit, project=project)

                # Clamp selection to current active count
                active = _get_active_jobs(jobs)
                if selected >= len(active):
                    selected = max(len(active) - 1, -1)

                live.update(build_dashboard(
                    jobs, project=project, selected=selected,
                    scontrol_detail=scontrol_detail, scontrol_job_id=scontrol_job_id,
                ))
    except KeyboardInterrupt:
        pass
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        console.print("\n[dim]Monitor stopped.[/dim]")
