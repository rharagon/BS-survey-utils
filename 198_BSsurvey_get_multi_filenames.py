#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Descarga títulos de proyectos de Scratch de forma incremental y en paralelo.

• Lee IDs de la columna 'filename' en INPUT_CSV
• Escribe/actualiza OUTPUT_CSV en modo append a medida que obtiene títulos
• Evita IDs ya presentes en OUTPUT_CSV (reanuda automáticamente)
• Multiproceso con control de errores y reintentos con backoff
"""

from pathlib import Path
import pandas as pd
import requests
import csv
import os
import sys
import time
import random
import logging
from typing import Tuple, Optional
from concurrent.futures import ProcessPoolExecutor, as_completed

# ------------------------------------------------------------------
# Ajusta estas rutas a tu entorno:
# INPUT_CSV   = Path("./data/X_TT.csv")
INPUT_CSV   = Path("C:/propios/Doc_sin_respaldo_nube/down_sb3/no_multi/nomulti_sb3.csv")
# OUTPUT_CSV  = Path("./data/X_TT_titles.csv")
OUTPUT_CSV  = Path("C:/propios/Doc_sin_respaldo_nube/down_sb3/no_multi/nomulti_sb3_titles.csv")

API_URL          = "https://api.scratch.mit.edu/projects/{}"
TIMEOUT          = 10         # segundos para timeout HTTP
MAX_RETRIES      = 5          # reintentos por ID
BACKOFF_BASE     = 0.75       # segundos base para backoff exponencial
TOTAL_PAUSE      = 0.25       # “cortesía”: pausa total deseada entre peticiones agregadas
MAX_WORKERS_ENV  = "SCRATCH_MAX_WORKERS"  # variable de entorno opcional para fijar workers
# ------------------------------------------------------------------


def _fetch_title_with_retries(pid: str, local_pause: float) -> Tuple[str, str]:
    """
    Devuelve (pid, title_o_error) con reintentos y backoff.
    No lanza excepciones: siempre devuelve una cadena en el segundo elemento.
    """
    # Pequeña espera inicial con jitter para desincronizar procesos
    time.sleep(local_pause * random.uniform(0.5, 1.5))

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(API_URL.format(pid), timeout=TIMEOUT)
            # Manejo específico de algunos códigos
            if r.status_code == 404:
                return pid, "NOT_FOUND: 404"
            if r.status_code == 429:
                # Demasiadas peticiones: respeta el Retry-After si existe
                retry_after = r.headers.get("Retry-After")
                wait = float(retry_after) if retry_after else BACKOFF_BASE * (2 ** (attempt - 1))
                time.sleep(wait + random.uniform(0, 0.25))
                continue

            r.raise_for_status()
            title = r.json().get("title", "")
            if not isinstance(title, str):
                title = str(title) if title is not None else ""
            return pid, title.strip()
        except requests.exceptions.RequestException as exc:
            # Errores de red/timeout/etc -> backoff y reintento
            if attempt < MAX_RETRIES:
                sleep_for = BACKOFF_BASE * (2 ** (attempt - 1)) + random.uniform(0, 0.25)
                time.sleep(sleep_for)
            else:
                return pid, f"ERROR: {type(exc).__name__}: {exc}"
        except ValueError as exc:
            # JSON mal formado u otros ValueError
            return pid, f"ERROR: ValueError: {exc}"
        except Exception as exc:
            # Cualquier otro error inesperado no aborta el flujo
            return pid, f"ERROR: {type(exc).__name__}: {exc}"

    # No debería alcanzarse
    return pid, "ERROR: Unknown"


def _safe_read_csv(path: Path, dtype=str) -> Optional[pd.DataFrame]:
    try:
        return pd.read_csv(path, dtype=dtype)
    except FileNotFoundError:
        return None
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    except Exception as exc:
        logging.error("No se pudo leer %s: %s", path, exc)
        return None


def _flush_row(writer, fh, row):
    writer.writerow(row)
    fh.flush()
    try:
        os.fsync(fh.fileno())
    except OSError:
        # En algunos FS/entornos puede no ser necesario/posible
        pass


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    # 1 — leer IDs de entrada
    df_in = _safe_read_csv(INPUT_CSV, dtype=str)
    if df_in is None:
        print(f"❌ No se pudo leer el archivo de entrada: {INPUT_CSV}")
        sys.exit(1)
    if "filename" not in df_in.columns:
        print("❌ La columna 'filename' no está en el CSV de entrada.")
        sys.exit(1)

    all_ids = (
        df_in["filename"]
        .dropna()
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .unique()
        .tolist()
    )

    if not all_ids:
        print("Nada que hacer: no se encontraron IDs en el CSV de entrada.")
        return

    # 2 — leer IDs ya procesados
    processed = set()
    df_out = _safe_read_csv(OUTPUT_CSV, dtype=str)
    if df_out is not None and not df_out.empty and "filename" in df_out.columns:
        processed = set(df_out["filename"].dropna().astype(str).str.strip().tolist())

    remaining = [pid for pid in all_ids if pid not in processed]
    if not remaining:
        print("Nada que hacer: todos los IDs ya están en el archivo de salida.")
        return

    total = len(all_ids)
    todo = len(remaining)
    print(f"⏳ Quedan {todo} de {total} IDs por obtener título…")

    # 3 — abrir salida en append y escribir cabecera si hace falta
    header_needed = not OUTPUT_CSV.exists() or (df_out is not None and df_out.empty)
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    fh = open(OUTPUT_CSV, "a", newline="", encoding="utf-8")
    writer = csv.writer(fh)
    if header_needed:
        _flush_row(writer, fh, ["filename", "project_title"])

    # 4 — calcular nº de workers y pausa local por proceso
    #    Permitimos fijarlo por variable de entorno si se desea
    try:
        max_workers_env = int(os.environ.get(MAX_WORKERS_ENV, "0"))
    except ValueError:
        max_workers_env = 0

    cpu_count = os.cpu_count() or 2
    default_workers = max(2, min(32, cpu_count))  # límite razonable
    max_workers = max_workers_env if max_workers_env > 0 else default_workers

    # Para mantener una pausa total aproximada, repartimos entre procesos:
    # (si TOTAL_PAUSE = 0.25 y max_workers=8, cada proceso dormirá ~0.031s)
    local_pause = max(TOTAL_PAUSE / max_workers, 0.0)

    print(f"⚙️  Usando {max_workers} procesos. Pausa local por proceso ≈ {local_pause:.3f}s")

    # 5 — ejecutar en paralelo y escribir a medida que llegan resultados
    done = 0
    errors = 0

    try:
        with ProcessPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_fetch_title_with_retries, pid, local_pause): pid for pid in remaining}

            for fut in as_completed(futures):
                pid = futures[fut]
                try:
                    _pid, title = fut.result()
                except Exception as exc:
                    # No debería ocurrir gracias al manejo interno, pero por si acaso
                    title = f"ERROR: WorkerFailure: {type(exc).__name__}: {exc}"
                    errors += 1
                else:
                    if title.startswith("ERROR"):
                        errors += 1

                _flush_row(writer, fh, [pid, title])
                done += 1
                if done % 50 == 0 or done == todo:
                    print(f"Progreso: {done}/{todo}")

    except KeyboardInterrupt:
        print("\n🧹 Interrumpido por el usuario. Progreso guardado hasta el último registro escrito.")
    except Exception as exc:
        # Cualquier error no esperado en el bucle principal no aborta lo ya escrito
        print(f"\n⚠️  Error no controlado en ejecución: {type(exc).__name__}: {exc}")
        print("El progreso previo permanece guardado en el CSV de salida.")
    finally:
        try:
            fh.close()
        except Exception:
            pass

    print(f"✅ Terminado. Registros procesados: {done}. Con errores: {errors}.")
    print(f"Salida: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
