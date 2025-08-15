#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Programa multiproceso para ejecutar Litterbox por lotes leyendo **una lista de proyectos**
desde un CSV (cada fila = un proyecto). Inspirado en el script PowerShell original.

Novedad: opción para **un único CSV por hilo (worker)**, de forma que al finalizar
basta con concatenar N ficheros (uno por proceso) o incluso trabajar con ellos tal cual.
Además, ahora puedes **consolidar** automáticamente todos los CSV en **uno solo** con `--consolidate`.

Características:
- Lee proyectos desde un CSV (si hay cabecera: `project`/`proyecto`; si no, 1ª columna).
- Modo por defecto: 1 proceso por proyecto, CSV por proyecto (`litter_results_<token>.csv`).
- Modo `--single-csv-per-worker`: reparte proyectos en `max-workers` buckets; cada proceso
  escribe a su CSV único (`litter_results_worker_XX.csv`) y va anexando.
- `--consolidate`: al terminar, crea `litter_results_all.csv` uniendo todos los CSV generados
  (por proyecto o por worker, según el modo), sin duplicar cabeceras.
- Compatibilidad con Litterbox: se llama a `--project-list` con un fichero temporal que
  contiene **un único proyecto** por ejecución.
- Timeout configurable (27 min por defecto) por proyecto.
- Directorios configurables para resultados, logs y temporales.
- Archivos de estado incrementales: `last_project_processed.txt`, `ok_projects.txt`,
  `failed_projects.txt`.
- Reintentos opcionales y opción `--resume-failed`.

Uso rápido (CSV por worker + consolidación):
    python multiproceso_litterbox.py \
      --csv "C:/propios/Doc_sin_respaldo_nube/down_sb3/sb3_litter.csv" \
      --jar "C:/ruta/a/Litterbox-1.9.2.full.jar" \
      --results-dir "C:/resultados" \
      --output-dir "C:/lit_results" \
      --logs-dir "C:/logs_litterbox" \
      --tmp-dir "C:/tmp_litterbox" \
      --max-workers 4 --timeout 1620 \
      --single-csv-per-worker --consolidate

Requisitos:
- Python 3.9+
- Java en PATH (o especificar ruta completa en --java-bin)
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple
import logging
from logging.handlers import RotatingFileHandler

LAST_PROCESSED_FILENAME = "last_project_processed.txt"
OK_FILENAME = "ok_projects.txt"
FAILED_FILENAME = "failed_projects.txt"


@dataclass
class JobSpec:
    project: str  # identificador del proyecto
    token: str    # se usa para nombrar archivos temporales y logs


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Ejecuta Litterbox en paralelo para múltiples proyectos desde un CSV.")
    p.add_argument("--csv", required=True,
                   help="Ruta al CSV que contiene los proyectos (1 por fila o columna project/proyecto)")
    p.add_argument("--jar", required=True,
                   help="Ruta al jar de Litterbox, p.ej. C:/.../Litterbox-1.9.2.full.jar")
    p.add_argument("--results-dir", default=str(Path.cwd() / "resultados"),
                   help="Directorio base para --path (por defecto ./resultados)")
    p.add_argument("--output-dir", default=str(Path.cwd() / "lit_results"),
                   help="Directorio donde guardar los CSV de salida (por defecto ./lit_results)")
    p.add_argument("--logs-dir", default=str(Path.cwd() / "logs_litterbox"),
                   help="Directorio para logs (por defecto ./logs_litterbox)")
    p.add_argument("--tmp-dir", default=str(Path.cwd() / "tmp_litterbox"),
                   help="Directorio para ficheros temporales de project-list (uno por proyecto)")
    p.add_argument("--java-bin", default="java", help="Binario de Java si no está en PATH")
    p.add_argument("--timeout", type=int, default=27*60,
                   help="Timeout por proyecto en segundos (defecto 1620 = 27 min)")
    p.add_argument("--max-workers", type=int, default=os.cpu_count() or 4,
                   help="Número máximo de procesos en paralelo")
    p.add_argument("--retries", type=int, default=0, help="Reintentos por proyecto fallido")
    p.add_argument("--resume-failed", action="store_true",
                   help="Si existe failed_projects.txt, reintenta solo esos en lugar del CSV")
    p.add_argument("--single-csv-per-worker", action="store_true",
                   help="Crea un único CSV por proceso/worker, consolidando resultados en caliente")
    p.add_argument("--consolidate", action="store_true",
                   help="Al terminar, genera un único CSV unificado: litter_results_all.csv")
    p.add_argument("--dry-run", action="store_true", help="Imprime lo que haría sin ejecutar Java")
    return p.parse_args()


def setup_logging(logs_dir: Path) -> None:
    logs_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(processName)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    fh = RotatingFileHandler(logs_dir / "run.log", maxBytes=5_000_000, backupCount=5, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    logger.addHandler(fh)


def derive_token_from_project(project: str) -> str:
    name = Path(project).name
    token = name.split("_")[-1] if "_" in name else name
    token = token.replace(" ", "-")
    token = token.replace(".txt", "").replace(".csv", "").replace(".list", "").strip(".-_")
    return token or name


def ensure_dirs(*paths: Path) -> None:
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)


def write_last_processed(base_dir: Path, project: str) -> None:
    (base_dir / LAST_PROCESSED_FILENAME).write_text(str(project), encoding="utf-8")


def append_ok(base_dir: Path, project: str) -> None:
    with (base_dir / OK_FILENAME).open("a", encoding="utf-8") as f:
        f.write(str(project) + "\n")


def append_failed(base_dir: Path, project: str) -> None:
    with (base_dir / FAILED_FILENAME).open("a", encoding="utf-8") as f:
        f.write(str(project) + "\n")


def read_failed_list(base_dir: Path) -> List[str]:
    ff = base_dir / FAILED_FILENAME
    if not ff.exists():
        return []
    return [line.strip() for line in ff.read_text(encoding="utf-8").splitlines() if line.strip()]


def load_jobs_from_csv(csv_path: Path) -> List[JobSpec]:
    jobs: List[JobSpec] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        try:
            first_row = next(reader)
        except StopIteration:
            return jobs

        def add_job(raw: str):
            raw = (raw or "").strip().strip('"')
            if not raw:
                return
            jobs.append(JobSpec(project=raw, token=derive_token_from_project(raw)))

        normalized = [c.strip().lower() for c in first_row]
        header_like = any("project" in c or "proyecto" in c for c in normalized)
        if header_like:
            try:
                idx = next(i for i, c in enumerate(normalized) if ("project" in c) or ("proyecto" in c))
            except StopIteration:
                idx = 0
            for row in reader:
                if not row:
                    continue
                add_job(row[idx])
        else:
            add_job(first_row[0] if first_row else "")
            for row in reader:
                if not row:
                    continue
                add_job(row[0])
    return jobs


def run_litterbox(java_bin: str, jar_path: Path, project: str, results_dir: Path, output_csv: Path,
                  timeout: int, tmp_dir: Path, dry_run: bool = False) -> Tuple[bool, str, Optional[int]]:
    """Ejecuta Litterbox para **un proyecto**, escribiendo a un CSV dado.
    Para compatibilidad con la CLI, generamos un fichero temporal de lista con un solo proyecto
    y lo pasamos por `--project-list`.
    """
    tmp_dir.mkdir(parents=True, exist_ok=True)
    token = derive_token_from_project(project)
    list_file = tmp_dir / f"project_{token}.txt"
    list_file.write_text(str(project) + "\n", encoding="utf-8")

    cmd = [
        java_bin, "-jar", str(jar_path), "check",
        "--project-list", str(list_file),
        "--path", str(results_dir),
        "--output", str(output_csv),
    ]

    if dry_run:
        return True, f"DRY-RUN: {' '.join(cmd)}", 0

    try:
        completed = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout, check=False
        )
        ok = completed.returncode == 0
        msg = completed.stdout.strip() + ("\n" + completed.stderr.strip() if completed.stderr else "")
        return ok, msg, completed.returncode
    except subprocess.TimeoutExpired as ex:
        return False, f"Timeout tras {timeout}s: {ex}", None
    except FileNotFoundError as ex:
        return False, f"No se encontró ejecutable: {ex}", None
    except Exception as ex:
        return False, f"Error inesperado: {ex}", None


# ===== Utilidades CSV =====

def append_csv_rows(src: Path, dst: Path) -> None:
    """Añade el contenido de `src` a `dst` evitando duplicar la cabecera.
    Si `dst` no existe, copia íntegro. Si existe, salta la primera línea de `src`.
    Asume que Litterbox produce CSV con cabecera en la primera línea.
    """
    if not src.exists():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    if not dst.exists():
        dst.write_bytes(src.read_bytes())
        return
    with src.open("rb") as fsrc, dst.open("ab") as fdst:
        first = True
        for line in fsrc:
            if first:
                first = False
                continue
            fdst.write(line)


def consolidate_csvs(output_dir: Path, pattern: str, dest_name: str = "litter_results_all.csv") -> Path:
    dest = output_dir / dest_name
    if dest.exists():
        dest.unlink()
    files = sorted(output_dir.glob(pattern))
    header_written = False
    with dest.open("ab") as fdst:
        for f in files:
            if not f.is_file():
                continue
            with f.open("rb") as fsrc:
                first = True
                for line in fsrc:
                    if first:
                        first = False
                        if not header_written:
                            fdst.write(line)
                            header_written = True
                        # si ya había cabecera, la saltamos
                    else:
                        fdst.write(line)
    return dest


# ===== Modo por proyecto (CSV por proyecto) =====

def process_one_project(spec: JobSpec, args: argparse.Namespace, base_dir: Path) -> Tuple[JobSpec, bool, str]:
    ensure_dirs(Path(args.results_dir), Path(args.output_dir), Path(args.tmp_dir))
    output_csv = Path(args.output_dir) / f"litter_results_{spec.token}.csv"
    ok, msg, rc = run_litterbox(
        args.java_bin, Path(args.jar), spec.project, Path(args.results_dir), output_csv,
        args.timeout, Path(args.tmp_dir), args.dry_run
    )
    if ok:
        try:
            write_last_processed(base_dir, spec.project)
        except Exception:
            pass
    return spec, ok, msg


# ===== Modo CSV por worker =====

def chunk_round_robin(items: Sequence[JobSpec], k: int) -> List[List[JobSpec]]:
    buckets: List[List[JobSpec]] = [[] for _ in range(max(1, k))]
    for i, it in enumerate(items):
        buckets[i % max(1, k)].append(it)
    return buckets


def worker_loop(worker_idx: int, specs: List[JobSpec], args: argparse.Namespace, base_dir: Path) -> Tuple[int, List[str], List[str]]:
    ensure_dirs(Path(args.results_dir), Path(args.output_dir), Path(args.tmp_dir))
    worker_csv = Path(args.output_dir) / f"litter_results_worker_{worker_idx:02d}.csv"
    ok_projects: List[str] = []
    failed_projects: List[str] = []

    for spec in specs:
        tmp_out = Path(args.tmp_dir) / f"tmp_out_{spec.token}.csv"
        ok, msg, rc = run_litterbox(
            args.java_bin, Path(args.jar), spec.project, Path(args.results_dir), tmp_out,
            args.timeout, Path(args.tmp_dir), args.dry_run
        )
        if ok:
            append_csv_rows(tmp_out, worker_csv)
            ok_projects.append(spec.project)
            try:
                write_last_processed(base_dir, spec.project)
            except Exception:
                pass
        else:
            failed_projects.append(spec.project)
        try:
            if tmp_out.exists():
                tmp_out.unlink()
        except Exception:
            pass
    return worker_idx, ok_projects, failed_projects


def main() -> int:
    args = parse_args()

    base_dir = Path.cwd()
    logs_dir = Path(args.logs_dir)
    ensure_dirs(Path(args.results_dir), Path(args.output_dir), Path(args.tmp_dir), logs_dir)
    setup_logging(logs_dir)

    logger = logging.getLogger(__name__)

    # Determinar conjunto de trabajos
    if args.resume_failed:
        failed_projects = read_failed_list(base_dir)
        if not failed_projects:
            logging.info("No hay failed_projects.txt o está vacío; se usará el CSV completo.")
            jobs = load_jobs_from_csv(Path(args.csv))
        else:
            jobs = [JobSpec(project=p, token=derive_token_from_project(p)) for p in failed_projects]
            logging.info("Reanudando únicamente %d proyectos de failed_projects.txt", len(jobs))
    else:
        jobs = load_jobs_from_csv(Path(args.csv))

    if not jobs:
        logging.error("No se encontraron proyectos a partir del CSV/failed_projects.")
        return 2

    logging.info("Se procesarán %d proyectos con hasta %d procesos.", len(jobs), args.max_workers)

    # Limpiar listas de estado para esta ejecución (se regenerarán)
    (base_dir / FAILED_FILENAME).unlink(missing_ok=True)
    (base_dir / OK_FILENAME).unlink(missing_ok=True)

    failures_total: List[str] = []

    if not args.single_csv_per_worker:
        # ===== Modo por proyecto (comportamiento previo) =====
        to_run: List[JobSpec] = jobs
        attempt = 0
        while to_run and attempt <= args.retries:
            attempt += 1
            logging.info("Intento %d de %d | Pendientes: %d", attempt, args.retries + 1, len(to_run))
            current_failures: List[JobSpec] = []

            with ProcessPoolExecutor(max_workers=args.max_workers) as ex:
                futures = [ex.submit(process_one_project, spec, args, base_dir) for spec in to_run]
                for fut in as_completed(futures):
                    spec, ok, msg = fut.result()
                    if ok:
                        logging.info("OK: proyecto %s", spec.project)
                        append_ok(base_dir, spec.project)
                    else:
                        logging.warning("FALLO: proyecto %s\n%s", spec.project, msg)
                        current_failures.append(spec)

            for spec in current_failures:
                try:
                    append_failed(base_dir, spec.project)
                except Exception as ex:
                    logging.error("Error registrando fallo '%s': %s", spec.project, ex)

            failures_total.extend([s.project for s in current_failures])
            to_run = current_failures  # Reintentar solo los que fallaron

        # Consolidación (modo por proyecto): unir todos los litter_results_*.csv en uno solo
        if args.consolidate and not args.dry_run:
            logging.info("Consolidando CSVs por proyecto en un único archivo...")
            consolidate_csvs(Path(args.output_dir), pattern="litter_results_*.csv", dest_name="litter_results_all.csv")
    else:
        # ===== Modo CSV por worker =====
        buckets = chunk_round_robin(jobs, args.max_workers)
        attempt = 0
        remaining_buckets = buckets
        while attempt <= args.retries:
            logging.info("Intento %d de %d en modo per-worker", attempt + 1, args.retries + 1)
            new_buckets: List[List[JobSpec]] = [[] for _ in range(len(remaining_buckets))]
            with ProcessPoolExecutor(max_workers=len(remaining_buckets)) as ex:
                futures = []
                for idx, specs in enumerate(remaining_buckets):
                    futures.append(ex.submit(worker_loop, idx, specs, args, base_dir))
                for fut in as_completed(futures):
                    worker_idx, oks, fails = fut.result()
                    for p in oks:
                        append_ok(base_dir, p)
                    for p in fails:
                        append_failed(base_dir, p)
                    failures_total.extend(fails)
                    new_buckets[worker_idx] = [JobSpec(project=p, token=derive_token_from_project(p)) for p in fails]
            attempt += 1
            remaining_buckets = new_buckets
            if all(len(b) == 0 for b in remaining_buckets):
                break

        # Consolidación (modo per-worker): unir los litter_results_worker_*.csv
        if args.consolidate and not args.dry_run:
            logging.info("Consolidando CSVs de workers en un único archivo...")
            consolidate_csvs(Path(args.output_dir), pattern="litter_results_worker_*.csv", dest_name="litter_results_all.csv")

    # Resumen al final en consola
    ok_file = base_dir / OK_FILENAME
    failed_file = base_dir / FAILED_FILENAME
    print("\nResumen:")
    if ok_file.exists():
        try:
            ok_count = sum(1 for _ in ok_file.open("r", encoding="utf-8"))
            print(f"  OK: {ok_count} proyectos (ver {OK_FILENAME})")
        except Exception:
            pass
    if failed_file.exists():
        try:
            print("  Fallidos:")
            with failed_file.open("r", encoding="utf-8") as f:
                for line in f:
                    print("   -", line.strip())
        except Exception:
            pass
    else:
        print("  Sin fallos. ¡Todos los proyectos se procesaron correctamente!")

    return 0 if not failures_total else 1


if __name__ == "__main__":
    sys.exit(main())
