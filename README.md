# BS survey utils

This repository contains utilities and data developed to support the **registered report**:
**[How Do Code Smells Affect Skill Growth in Scratch Novice Programmers?](https://arxiv.org/abs/2507.17314)**

It includes two Python scripts designed to automate and parallelize the processing of Scratch projects using [Litterbox](https://github.com/Niebert/Litterbox), as well as fetching project titles from the Scratch API.

This work builds upon and complements the following repositories:
- [scratch_anonymous_downloader_multiprocess](https://github.com/rharagon/scratch_anonymous_downloader_multiprocess)
- [DrScratch-by-Console](https://github.com/rharagon/DrScratch-by-Console)

---

## Table of Contents

- [197_BSsurvey_multiproceso_litterbox.py](#197_bssurvey_multiproceso_litterboxpy)
- [198_BSsurvey_get_multi_filenames.py](#198_bssurvey_get_multi_filenamespy)
- [extract_scratch_meta.py](#extract_scratch_metapy)
- [sb3_pick.sh](#sb3_picksh)
- [pick_sb3.ps1](#pick_sb3ps1)
- [wrangling_combining_RR.R](#wrangling_combining_rrr)

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

## extract_scratch_meta.py

### Overview
A tool to **extract metadata** from downloaded Scratch `.sb3` files using the public Scratch API.
It scans a folder, optionally recursively, and writes a CSV containing project titles, authors, dates, and remix lineage.

### Key Features
- **Automatic ID Detection**: Derives the project ID from each filename.
- **Parallel Requests**: Uses threads or processes for faster API calls.
- **Robust Output**: Stores errors alongside metadata for later review.
- **Progress Bar**: Reports progress via `tqdm`.

### Usage Example
```bash
python extract_scratch_meta.py --input path/to/sb3_dir --output meta.csv --workers 4
```

---

## sb3_pick.sh

### Overview
A **Bash** helper that gathers all `.sb3` files under a directory and packs them into a single ZIP archive while reporting counts and sizes.

### Key Features
- **Recursive Search**: Finds every `.sb3` file within the source directory.
- **Portable Archive**: Creates a timestamped ZIP by default.
- **Summary Report**: Prints totals for files, sizes, and elapsed time.

### Usage Example
```bash
./sb3_pick.sh path/to/source [output.zip]
```

---

## pick_sb3.ps1

### Overview
A **PowerShell** script that copies all `.sb3` files from the current directory tree into a destination folder.

### Key Features
- **Destination Check**: Creates the target directory if it does not exist.
- **Verbose Copy**: Logs each file copied for easy tracking.

### Usage Example
```powershell
./pick_sb3.ps1 -Destino C:\ruta\destino
```

---

## wrangling_combining_RR.R

### Overview
An **R** script employing the `tidyverse` to combine titles and analysis results from multiple CSV sources, removing duplicates and aligning column names.

### Key Features
- **Dataset Merging**: Joins Litterbox outputs with DrScratch results.
- **Column Normalisation**: Renames and filters fields for compatibility.
- **Sample method**: Nearest Neighbor Matching
- **Export Helpers**: Writes consolidated datasets back to CSV.

### Usage Example
```r
Rscript wrangling_combining_RR.R
```

---

## Requirements
- Python 3.9+
- Java (for Litterbox)
- R with `tidyverse` package (for data wrangling script)
- Required Python packages: `pandas`, `requests`, `tqdm`

---

## License
This project is open-source and available under the [Creative Commons Zero v1.0 Universal License](LICENSE).
```
