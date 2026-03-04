# Slugger

SLURM job manager with Google Sheets logging. `sbatch` drop-in replacement that auto-tracks jobs per project.

## Install

```bash
pip install -e .
```

## Setup

```bash
slugger login    # paste service account JSON path + spreadsheet URL
```

See [Google Sheets Setup Guide](GOOGLE_SHEETS_SETUP.md) for detailed instructions on creating a service account and linking a spreadsheet.

## Commands

### `slugger submit`

```
slugger submit <script> [sbatch_args...] [-p PROJECT]
```

| Arg | Description |
|-----|-------------|
| `script` | Path to batch script |
| `sbatch_args` | Extra sbatch arguments (passthrough) |
| `-p, --project` | Project name (default: cwd folder name) |

- Parses `#SBATCH` directives from script (job-name, gpus, partition, output/error paths, etc.)
- Parses `#SLUGGER` directives for slugger-specific options
- Auto-starts background poller
- Creates row in project's spreadsheet tab

### `slugger list`

```
slugger list [-a] [-n LIMIT] [-p PROJECT] [--all-projects]
```

| Arg | Description |
|-----|-------------|
| `-a, --all` | Include completed/failed jobs |
| `-n, --limit` | Max jobs to show (default: 20) |
| `-p, --project` | Filter by project |
| `--all-projects` | Show all projects (adds Project column) |

### `slugger status`

```
slugger status [JOB_ID]
```

Detailed info for a single job. Default: latest job.

### `slugger monitor`

```
slugger monitor [-r REFRESH] [-n LIMIT] [-p PROJECT]
```

| Arg | Description |
|-----|-------------|
| `-r, --refresh` | Refresh interval in seconds (default: 5) |
| `-n, --limit` | Max jobs displayed (default: 50) |
| `-p, --project` | Filter by project |

Interactive keybindings:

| Key | Action |
|-----|--------|
| `↑` / `↓` | Select active job (highlighted) |
| `d` | Toggle `scontrol show job` detail panel |
| `u` | Force refresh |
| `Ctrl+C` | Exit |

### `slugger log`

```
slugger log [JOB_ID] [-e] [-f/-F] [-n LINES]
```

| Arg | Description |
|-----|-------------|
| `JOB_ID` | Job ID (default: latest job) |
| `-e, --err` | Show stderr instead of stdout |
| `-f/--follow` | Follow output like `tail -f` (default: on for running jobs) |
| `-F/--no-follow` | Don't follow, just print last lines |
| `-n, --lines` | Number of lines to show (default: 50) |

Streams the job's SLURM output log. Auto-detects `#SBATCH --output` / `--error` paths.

### `slugger gpu`

```
slugger gpu [JOB_ID] [-t TOOL]
```

| Arg | Description |
|-----|-------------|
| `JOB_ID` | Job ID (default: latest running job) |
| `-t, --tool` | `nvitop` (default) or `nvidia-smi` |

Run GPU monitor on the job's compute node via `srun --jobid`.

### `slugger login` / `slugger init`

```
slugger login
```

Interactive setup wizard: paste service account JSON path and spreadsheet URL. Creates `~/.slugger/config.toml` and applies header formatting to the spreadsheet.

### `slugger sync`

```
slugger sync [JOB_ID] [-p PROJECT] [-a] [--unsynced]
```

| Arg | Description |
|-----|-------------|
| `JOB_ID` | Sync a single job |
| `-p, --project` | Filter by project |
| `-a, --all` | Sync all jobs (including completed) |
| `--unsynced` | Sync only jobs not yet in the sheet |

No args = sync all active jobs in current project.

```bash
# Push jobs that were submitted before sheet was linked
slugger sync --unsynced

# Re-sync everything
slugger sync --all
```

### `slugger sheet info`

```
slugger sheet info
```

Show connected spreadsheet details: title, URL, worksheet tabs with job counts, and unsynced jobs summary.

### `slugger poller`

```
slugger poller start|stop|status
```

Background daemon that polls SLURM (every 60s) and updates job status + sheets. Auto-starts on submit. Gracefully handles transient SLURM outages.

## Directives

### `#SBATCH` (standard SLURM — auto-parsed)

```bash
#!/bin/bash
#SBATCH --job-name="Train Qwen3-VL-8B (MMDiT-v2; D4S8)"
#SBATCH --nodes=1
#SBATCH --gpus=2
#SBATCH --partition=sjw_alinlab
#SBATCH --output=slurm_out/%j-train.out
#SBATCH --error=slurm_out/%j-train.err
```

Slugger auto-extracts: job-name, gpus, partition, cpus-per-task, output/error paths.

### `#SLUGGER` (slugger-specific)

```bash
#SLUGGER --project MyProject
#SLUGGER --gpus 2 --partition sjw_alinlab --time 48:00:00
```

| Directive | Maps to |
|-----------|---------|
| `--project` | Project name (slugger only, not passed to sbatch) |
| `--gpus N` | `--gres=gpu:N` |
| `--partition X` | `--partition=X` |
| `--time T` | `--time=T` |
| `--cpus N` | `--cpus-per-task=N` |
| `--mem X` | `--mem=X` |
| `--name X` | `--job-name=X` |

## Per-Project Tabs

Jobs are grouped by project in the spreadsheet (one worksheet tab per project).

```bash
# In ~/Isaac-GR00T/ → project = "Isaac-GR00T"
slugger submit train.sh

# In ~/other-project/ → project = "other-project"
slugger submit eval.sh

# Override project name
slugger submit train.sh -p custom-name
```

## Config

`~/.slugger/config.toml`

```toml
google_credentials = "~/.slugger/credentials.json"
spreadsheet_id = "1BxiM..."
poll_interval_sec = 60
```

Env var overrides: `SLUGGER_GOOGLE_CREDENTIALS`, `SLUGGER_SPREADSHEET_ID`, `SLUGGER_POLL_INTERVAL`
