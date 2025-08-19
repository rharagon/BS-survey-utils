# BS survey utils

This repository contains two Python scripts designed to automate and parallelize the processing of Scratch projects using [Litterbox](https://github.com/Niebert/Litterbox), as well as fetching project titles from the Scratch API.

---

## Table of Contents

- [197_BSsurvey_multiproceso_litterbox.py](#197_bssurvey_multiproceso_litterboxpy)
- [198_BSsurvey_get_multi_filenames.py](#198_bssurvey_get_multi_filenamespy)

---

## 197_BSsurvey_multiproceso_litterbox.py

### Overview
A **multiprocessing** script to execute Litterbox in batch mode, reading a list of projects from a CSV file (one project per row). It supports **automatic resumption**, **skipping failed projects**, and **consolidating results** into a single CSV.

### Key Features
- **Automatic Resumption**: Skips projects already in `ok_projects.txt` and retries those in `failed_projects.txt`.
- **Skip Failed Projects**: Optionally ignore projects already marked as failed.
- **No Temporary Files**: Temporary files are always deleted after each run.
- **State Files**: Only `ok_projects.txt`, `failed_projects.txt`, and `last_project_processed.txt` are maintained.
- **CSV per Worker**: Option to write a single CSV per process.
- **Consolidation**: Generate a unified CSV at the end.

### Usage Example
```bash
python 197_BSsurvey_multiproceso_litterbox.py \
  --csv "path/to/projects.csv" \
  --jar "path/to/Litterbox-1.9.2.full.jar" \
  --results-dir "path/to/results" \
  --output-dir "path/to/output" \
  --logs-dir "path/to/logs" \
  --tmp-dir "path/to/tmp" \
  --state-dir "path/to/state" \
  --max-workers 4 --timeout 1620 \
  --single-csv-per-worker --consolidate --auto-resume
```

### Arguments
| Argument | Description |
|----------|-------------|
| `--csv` | Path to the CSV file containing projects. |
| `--jar` | Path to the Litterbox JAR file. |
| `--results-dir` | Base directory for Litterbox results. |
| `--output-dir` | Directory to save output CSVs. |
| `--logs-dir` | Directory for log files. |
| `--tmp-dir` | Directory for temporary files. |
| `--state-dir` | Directory for state files. |
| `--java-bin` | Path to Java binary (if not in PATH). |
| `--timeout` | Timeout per project (in seconds). |
| `--max-workers` | Maximum number of parallel processes. |
| `--retries` | Number of retries for failed projects. |
| `--resume-failed` | (Legacy) Process only projects in `failed_projects.txt`. |
| `--auto-resume` | Skip OK projects and retry failed ones. |
| `--skip-failed` | Skip projects already marked as failed. |
| `--single-csv-per-worker` | Write a single CSV per process. |
| `--consolidate` | Generate a unified CSV at the end. |
| `--dry-run` | Print what would be done without executing Java. |

---

## 198_BSsurvey_get_multi_filenames.py

### Overview
A script to **download Scratch project titles** in parallel, reading project IDs from a CSV file and writing/updating an output CSV. It avoids re-fetching IDs already present in the output file and supports **retry logic with exponential backoff**.

### Key Features
- **Incremental Fetching**: Only fetches IDs not already in the output CSV.
- **Parallel Processing**: Uses multiprocessing for efficiency.
- **Retry Logic**: Implements exponential backoff for failed requests.
- **Rate Limiting**: Respects Scratch API rate limits and includes a courtesy pause between requests.

### Usage Example
```bash
python 198_BSsurvey_get_multi_filenames.py
```
*Note: Adjust `INPUT_CSV` and `OUTPUT_CSV` paths in the script before running.*

### Configuration
| Variable | Description |
|----------|-------------|
| `INPUT_CSV` | Path to the input CSV file containing project IDs. |
| `OUTPUT_CSV` | Path to the output CSV file for titles. |
| `API_URL` | Scratch API endpoint for project metadata. |
| `TIMEOUT` | HTTP request timeout (in seconds). |
| `MAX_RETRIES` | Maximum number of retries per ID. |
| `BACKOFF_BASE` | Base delay for exponential backoff (in seconds). |
| `TOTAL_PAUSE` | Total desired pause between aggregated requests. |
| `MAX_WORKERS_ENV` | Optional environment variable to set the number of workers. |

---

## Requirements
- Python 3.9+
- Java (for Litterbox)
- Required Python packages: `pandas`, `requests`

---

## License
This project is open-source and available under the [Creative Commons Zero v1.0 Universal License](LICENSE).
```
