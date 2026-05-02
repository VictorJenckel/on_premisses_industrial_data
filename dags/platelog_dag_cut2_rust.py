"""
platelog_dag_cut2_rust.py — Airflow orchestration for the cut2 inspection line ETL.

This DAG is the "brain" of the cut2 ingestion pipeline.
It handles all orchestration concerns:
  - Watermark management (byte-offset per hourly file)
  - SSH stat check (detect remote file changes without downloading)
  - rsync transfer (incremental, checksum-verified)
  - Hour-transition window (handles files at XX:00 boundary)
  - Audit log registration
  - Staging file cleanup

The heavy ETL work (CSV parsing, deduplication, upsert) is delegated to
the Rust binary `cut2_etl`, which receives and returns data via CLI args
and JSON stdout. Logs from the binary arrive via stderr.

Architecture role:
  This DAG feeds the bronze (raw) layer of the inspection data lake.
  The structured data it produces flows into:
    - PostgreSQL (partitioned by date, one partition per line per day)
    - AAS telemetry submodel (Digital Twin state sync — in development)
    - Anomaly detection pipeline for glass ribbon drift monitoring
"""

import json
import os
import glob
import subprocess
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# =============================================================================
# CONFIGURATION 
# =============================================================================

# Rust binary path (built via: cargo build --release)
RUST_BINARY = os.environ.get("cut2_ETL_BINARY", "/opt/airflow/bin/cut2_etl")

# Remote machine access (cut2 line host)
MACHINE_USER   = os.environ.get("cut2_MACHINE_USER", "operator")
MACHINE_PASS   = os.environ.get("cut2_MACHINE_PASS")          # required
MACHINE_IP     = os.environ.get("cut2_MACHINE_IP")            # required
REMOTE_DATA_DIR = os.environ.get("cut2_REMOTE_DATA_DIR", "/home/operator/cut2_data/history")

# Local staging path (inside the Airflow container or host volume)
STAGING_DIR  = os.environ.get("cut2_STAGING_DIR", "/opt/airflow/staging/cut2")
DEST_TABLE   = os.environ.get("cut2_DEST_TABLE", "inspection_data_cut2")

# Files within this many minutes of the hour boundary include the previous
# hour's file as well, to prevent data loss at the transition window.
TRANSITION_WINDOW_MINUTES = int(os.environ.get("cut2_TRANSITION_WINDOW_MIN", "10"))

# PostgreSQL connection (Airflow container must reach the DB host)
POSTGRES_CONFIG = {
    "dbname":   os.environ.get("PG_DBNAME", "dbc"),
    "user":     os.environ.get("PG_USER"),     # required
    "password": os.environ.get("PG_PASSWORD"), # required
    "host":     os.environ.get("PG_HOST"),     # required
    "port":     int(os.environ.get("PG_PORT", "5432")),
}

# =============================================================================
# DAG DEFINITION
# =============================================================================

default_args = {
    "owner":            "victor_jenckel",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
}

dag = DAG(
    "platelog_cut2_ingestion",
    default_args=default_args,
    description=(
        "cut2 inspection line ingestion: watermark-based rsync + Rust ETL → PostgreSQL. "
        "Bronze layer of the glass inspection Digital Twin data pipeline."
    ),
    schedule_interval=timedelta(minutes=2),
    catchup=False,
    max_active_runs=1,
    tags=["inspection", "cut2", "bronze", "digital-twin"],
)

# =============================================================================
# ORCHESTRATION HELPERS (Python/Airflow responsibilities — not delegated to Rust)
# =============================================================================

def _get_target_files() -> list[tuple[str, str, str, str]]:
    """
    Returns a list of (remote_path, date_str, hour_str, watermark_key) tuples
    for the current hour, plus the previous hour if within the transition window.

    The remote log structure is: REMOTE_DATA_DIR/YYYY/MM/DD/HH.csv
    """
    now = datetime.now()
    targets = [now]
    if now.minute < TRANSITION_WINDOW_MINUTES:
        targets.append(now - timedelta(hours=1))

    result = []
    for dt in targets:
        date_str      = dt.strftime("%Y-%m-%d")
        hour_str      = dt.strftime("%H")
        remote_path   = (
            f"{REMOTE_DATA_DIR}/"
            f"{dt.strftime('%Y')}/"
            f"{dt.strftime('%m')}/"
            f"{dt.strftime('%d')}/"
            f"{hour_str}.csv"
        )
        watermark_key = f"watermark_cut2_{dt.strftime('%Y%m%d')}_{hour_str}"
        result.append((remote_path, date_str, hour_str, watermark_key))
    return result


def _get_remote_file_size(remote_path: str) -> int | None:
    """
    Returns the remote file size in bytes via SSH stat, without downloading.
    Returns None if the file does not exist or the host is unreachable.
    """
    cmd = [
        "sshpass", "-p", MACHINE_PASS,
        "ssh", "-o", "StrictHostKeyChecking=no",
        f"{MACHINE_USER}@{MACHINE_IP}",
        f"stat -c%s {remote_path}",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        s = result.stdout.strip()
        if s.isdigit():
            return int(s)
    print(f"Cannot stat remote file '{remote_path}': {result.stderr.strip()}")
    return None


def _cleanup_staging():
    """Removes local cut2 CSV files older than 24 hours from the staging directory."""
    cutoff = datetime.now() - timedelta(hours=24)
    for fpath in glob.glob(os.path.join(STAGING_DIR, "cut2_*.csv")):
        try:
            if datetime.fromtimestamp(os.path.getmtime(fpath)) < cutoff:
                os.remove(fpath)
                print(f"Removed stale staging file: {os.path.basename(fpath)}")
        except Exception as exc:
            print(f"Failed to remove '{fpath}': {exc}")


def _register_audit_log(file_name: str, total: int, status: str, message: str = "") -> None:
    """
    Writes ETL execution result to the audit table.
    Failures here do not raise — the audit log must never block the main pipeline.
    """
    try:
        with psycopg2.connect(**POSTGRES_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO etl_log_platelog_cut2
                        (file_name, total_registros, status, mensagem)
                    VALUES (%s, %s, %s, %s);
                    """,
                    (file_name, total, status, message[:500]),
                )
                conn.commit()
        print(f"Audit log: {status} — {file_name} ({total} rows)")
    except Exception as exc:
        print(f"Audit log write failed (non-blocking): {exc}")


# =============================================================================
# TASK 1: DOWNLOAD
# Watermark check via SSH stat → rsync transfer if file changed.
# Watermark is NOT updated here — only after successful processing.
# =============================================================================

def download_cut2_files(**context):
    """
    For each target file (current hour + optionally previous hour):
      1. Fetch remote file size via SSH stat (no download).
      2. Compare against the stored Airflow Variable watermark.
      3. If changed: rsync to local staging, queue for processing.
      4. If unchanged: skip.

    Pushes the list of files to process via XCom for the next task.
    """
    os.makedirs(STAGING_DIR, exist_ok=True)
    _cleanup_staging()

    print(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    targets = _get_target_files()
    print(f"Evaluating: {[r for r, _, _, _ in targets]}")

    to_process = []

    for remote_path, date_str, hour_str, watermark_key in targets:
        print(f"\n--- Checking: {remote_path} ---")

        remote_size = _get_remote_file_size(remote_path)
        if remote_size is None:
            print("File not found or host unreachable. Skipping.")
            continue

        prev_size = int(Variable.get(watermark_key, default_var=0))
        print(f"Watermark: {prev_size} bytes | Remote: {remote_size} bytes")

        if remote_size == prev_size:
            print("No changes detected. Skipping download.")
            continue

        action = "updated" if remote_size > prev_size else "recreated"
        print(f"File {action} ({prev_size} → {remote_size} bytes). Starting rsync...")

        local_name = f"cut2_{date_str}_{hour_str}.csv"
        local_path = os.path.join(STAGING_DIR, local_name)

        rsync = subprocess.run(
            [
                "sshpass", "-p", MACHINE_PASS,
                "rsync", "-az", "--checksum",
                "-e", "ssh -o StrictHostKeyChecking=no",
                f"{MACHINE_USER}@{MACHINE_IP}:{remote_path}",
                local_path,
            ],
            capture_output=True,
            text=True,
        )

        if rsync.returncode != 0:
            print(f"rsync failed for '{remote_path}': {rsync.stderr}")
            continue

        print(f"File transferred: '{local_path}'")
        to_process.append({
            "file_path":     local_path,
            "date_str":      date_str,
            "hour_str":      hour_str,
            "watermark_key": watermark_key,
            "new_size":      remote_size,
        })

    if not to_process:
        print("\nNo new or modified files in this run.")
        return None

    context["ti"].xcom_push(key="files_to_process", value=to_process)
    return [item["file_path"] for item in to_process]


# =============================================================================
# TASK 2: PROCESS
# Delegates transformation + load to the Rust binary.
# Updates watermark only after confirmed success.
# =============================================================================

def _call_rust_etl(file_path: str, date_str: str) -> int:
    """
    Invokes the cut2_etl Rust binary for a single CSV file.
    Returns the number of rows inserted/updated, or raises on failure.

    The binary receives all connection parameters as CLI args (no env vars
    inside Rust to keep the binary stateless and testable in isolation).
    """
    cmd = [
        RUST_BINARY,
        "--arquivo-path", file_path,
        "--data-str",     date_str,
        "--dest-table",   DEST_TABLE,
        "--pg-host",      POSTGRES_CONFIG["host"],
        "--pg-port",      str(POSTGRES_CONFIG["port"]),
        "--pg-user",      POSTGRES_CONFIG["user"],
        "--pg-password",  POSTGRES_CONFIG["password"],
        "--pg-dbname",    POSTGRES_CONFIG["dbname"],
    ]

    print(f"Calling Rust binary: {os.path.basename(file_path)} (date: {date_str})")

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)

    # Rust binary writes its own progress logs to stderr
    if result.stderr:
        for line in result.stderr.strip().splitlines():
            print(f"[rust] {line}")

    if result.returncode != 0:
        raise RuntimeError(
            f"cut2_etl exited with code {result.returncode}: {result.stderr[:500]}"
        )

    try:
        payload = json.loads(result.stdout.strip())
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Invalid JSON from cut2_etl: {result.stdout[:200]} — {exc}"
        ) from exc

    if payload.get("status") != "success":
        raise RuntimeError(f"cut2_etl reported error: {payload.get('error', 'unknown')}")

    return int(payload.get("rows", 0))


def process_cut2_files(**context):
    """
    For each file queued by the download task:
      1. Delegates CSV transformation + upsert to the Rust ETL binary.
      2. Updates the Airflow Variable watermark ONLY on success.
      3. Registers the result in the audit log.
    """
    ti = context["ti"]
    files_to_process = ti.xcom_pull(
        task_ids="download_cut2_files",
        key="files_to_process",
    )

    if not files_to_process:
        print("No changes detected by watermark check. Processing skipped.")
        return 0

    total_rows = 0

    for item in files_to_process:
        file_path     = item["file_path"]
        date_str      = item["date_str"]
        watermark_key = item["watermark_key"]
        new_size      = item["new_size"]
        file_name     = os.path.basename(file_path)

        print(f"\n--- Processing: {file_path} ---")

        if not os.path.exists(file_path):
            print(f"File not found after download: {file_path}")
            _register_audit_log(file_name, 0, "error", "File missing after rsync.")
            continue

        rows   = 0
        status = "success"
        msg    = ""

        try:
            rows = _call_rust_etl(file_path, date_str)
            total_rows += rows

            # Watermark advances only after confirmed successful insert
            Variable.set(watermark_key, new_size)
            print(f"Watermark '{watermark_key}' → {new_size} bytes.")

        except Exception as exc:
            status = "error"
            msg    = str(exc)
            print(f"Failed to process '{file_name}': {exc}")
            raise

        finally:
            _register_audit_log(file_name, rows, status, msg)

    print(f"\nDone. {total_rows} total rows inserted/updated.")
    return total_rows


# =============================================================================
# TASK WIRING
# =============================================================================

download_task = PythonOperator(
    task_id="download_cut2_files",
    python_callable=download_cut2_files,
    provide_context=True,
    dag=dag,
)

process_task = PythonOperator(
    task_id="process_cut2_files",
    python_callable=process_cut2_files,
    provide_context=True,
    dag=dag,
)

download_task >> process_task
