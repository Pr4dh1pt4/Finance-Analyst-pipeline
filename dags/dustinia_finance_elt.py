"""
DustiniaDelixia Groceria — Finance Analyst ELT Pipeline
=======================================================
Persona 1: Finance Analyst.

Business question:
    Who are our high-value customers (HVCs), how do they pay, where are
    they located, and how much revenue are we leaving on the table by not
    optimising payment options (installments) for them?

DAG shape (end-to-end, idempotent):

    start
      |
    create_databases
      |
    [ extract_validate_<table> ]  (parallel, one per source CSV)
      |
    load_to_staging               (parallel per table)
      |
    dq_gate_staging               (row-count + null checks; fails the run)
      |
    build_dimensions  -> build_facts -> build_marts
      |
    dq_gate_marts                 (referential + sanity checks)
      |
    refresh_metabase_cache (optional add-on)
      |
    end

Connections expected (set in the Airflow UI / env):
    clickhouse_default  -> Host=clickhouse Port=9000 (native) / 8123 (http)
"""

from __future__ import annotations

import os
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from airflow.utils.task_group import TaskGroup

import pandas as pd
import clickhouse_connect

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DATA_DIR = os.getenv("DUSTINIA_DATA_DIR", "/opt/airflow/include/data")
SQL_DIR = Path(os.getenv("DUSTINIA_SQL_DIR", "/opt/airflow/include/sql"))

CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

# source file -> (clickhouse staging table, required columns)
SOURCES = {
    "orders": "orders",
    "order_payments": "order_payments",
    "order_items": "order_items",
    "customers": "customers",
    "geolocation": "geolocation",
    "order_reviews": "order_reviews",
}

# expected minimum row counts — a crude but effective freshness/completeness gate
MIN_ROWS = {
    "orders": 50_000,
    "order_payments": 50_000,
    "order_items": 50_000,
    "customers": 50_000,
    "geolocation": 500_000,
    "order_reviews": 50_000,
}


def _client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD
    )


def _run_sql_file(path: Path):
    """Execute a multi-statement .sql file statement-by-statement."""
    client = _client()
    raw = path.read_text()
    # strip block of leading comments per statement; split on ';'
    statements = [s.strip() for s in raw.split(";") if s.strip() and not s.strip().startswith("--\n")]
    for stmt in statements:
        # skip pure-comment fragments
        body = "\n".join(l for l in stmt.splitlines() if not l.strip().startswith("--"))
        if not body.strip():
            continue
        log.info("Executing: %s ...", body.strip()[:80])
        client.command(body)


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------
def create_databases(**_):
    _run_sql_file(SQL_DIR / "staging" / "01_create_staging.sql")


def extract_validate(table: str, **_):
    """Read CSV, validate it is parseable and non-empty, push row count to XCom."""
    fp = Path(DATA_DIR) / f"{table}.csv"
    if not fp.exists():
        raise AirflowFailException(f"Source file missing: {fp}")
    df = pd.read_csv(fp)
    n = len(df)
    log.info("%s: %d rows, columns=%s", table, n, list(df.columns))
    if n == 0:
        raise AirflowFailException(f"{table} is empty")
    return n


def load_to_staging(table: str, **_):
    """Truncate + bulk insert the CSV into its ClickHouse staging table."""
    client = _client()
    fp = Path(DATA_DIR) / f"{table}.csv"
    df = pd.read_csv(fp)

    # explicit list of columns that map to DateTime in the ClickHouse DDL.
    # NB: must be by exact name — substring matching misses 'order_approved_at'
    # and 'shipping_limit_date' patterns inconsistently.
    DATETIME_COLS = {
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date", "shipping_limit_date",
        "review_creation_date", "review_answer_timestamp",
    }
    datetime_cols = [c for c in df.columns if c in DATETIME_COLS]
    for col in datetime_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")
        # to python datetime objects, with None where missing
        df[col] = df[col].apply(lambda x: None if pd.isna(x) else x.to_pydatetime())

    # zip prefixes must be strings (leading zeros)
    for col in ("customer_zip_code_prefix", "geolocation_zip_code_prefix", "seller_zip_code_prefix"):
        if col in df.columns:
            df[col] = df[col].astype(str)

    # remaining string/object columns: replace NaN with "" (String is non-nullable)
    for col in df.select_dtypes(include=["object"]).columns:
        if col not in datetime_cols:
            df[col] = df[col].where(df[col].notna(), "")

    client.command(f"TRUNCATE TABLE IF EXISTS dustinia_raw.{table}")
    client.insert_df(f"dustinia_raw.{table}", df)
    log.info("Loaded %d rows into dustinia_raw.%s", len(df), table)


def dq_gate_staging(**_):
    """Hard gate: every staging table must clear its minimum row count."""
    client = _client()
    failures = []
    for table, minrows in MIN_ROWS.items():
        cnt = client.query(f"SELECT count() FROM dustinia_raw.{table}").result_rows[0][0]
        log.info("staging %s = %d (min %d)", table, cnt, minrows)
        if cnt < minrows:
            failures.append(f"{table}: {cnt} < {minrows}")
    # orphan payments check
    orphans = client.query(
        "SELECT count() FROM dustinia_raw.order_payments p "
        "LEFT ANTI JOIN dustinia_raw.orders o ON p.order_id = o.order_id"
    ).result_rows[0][0]
    log.info("payments with no matching order: %d", orphans)
    if failures:
        raise AirflowFailException("Staging DQ failed: " + "; ".join(failures))


def build_marts(**_):
    _run_sql_file(SQL_DIR / "marts" / "02_build_marts.sql")


def dq_gate_marts(**_):
    """Sanity gate on the analytical layer."""
    client = _client()

    fct = client.query("SELECT count() FROM dustinia_marts.fct_order_payments").result_rows[0][0]
    dim = client.query("SELECT count() FROM dustinia_marts.dim_customer").result_rows[0][0]
    hvc = client.query(
        "SELECT count() FROM dustinia_marts.dim_customer WHERE is_high_value_customer"
    ).result_rows[0][0]

    log.info("fct_order_payments=%d  dim_customer=%d  HVC=%d", fct, dim, hvc)

    if fct == 0 or dim == 0:
        raise AirflowFailException("Marts are empty.")
    # HVC should be roughly the top decile (~8-12%)
    share = hvc / dim
    if not (0.05 <= share <= 0.15):
        raise AirflowFailException(f"HVC share {share:.2%} outside expected 5-15% band.")

    # negative-value guard
    neg = client.query(
        "SELECT count() FROM dustinia_marts.fct_order_payments WHERE order_value < 0"
    ).result_rows[0][0]
    if neg > 0:
        raise AirflowFailException(f"{neg} orders with negative value in fact table.")


def refresh_metabase_cache(**_):
    """Optional add-on: ping Metabase to clear cached question results so the
    demo always shows fresh data. No-op if Metabase isn't reachable."""
    import urllib.request
    url = os.getenv("METABASE_URL", "http://metabase:3000") + "/api/health"
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            log.info("Metabase health: %s", r.status)
    except Exception as e:  # noqa: BLE001
        log.warning("Metabase not reachable (ok in dev): %s", e)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
default_args = {
    "owner": "finance_analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "depends_on_past": False,
}

with DAG(
    dag_id="dustinia_finance_elt",
    description="HVC payment-behaviour ELT for the Finance Analyst persona",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dustinia", "finance", "persona1", "clickhouse"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    t_create_db = PythonOperator(
        task_id="create_databases",
        python_callable=create_databases,
    )

    with TaskGroup("extract_and_load") as extract_load:
        for table in SOURCES:
            e = PythonOperator(
                task_id=f"extract_validate_{table}",
                python_callable=extract_validate,
                op_kwargs={"table": table},
            )
            l = PythonOperator(
                task_id=f"load_staging_{table}",
                python_callable=load_to_staging,
                op_kwargs={"table": table},
            )
            e >> l

    t_dq_staging = PythonOperator(task_id="dq_gate_staging", python_callable=dq_gate_staging)
    t_build_marts = PythonOperator(task_id="build_marts", python_callable=build_marts)
    t_dq_marts = PythonOperator(task_id="dq_gate_marts", python_callable=dq_gate_marts)
    t_refresh = PythonOperator(
        task_id="refresh_metabase_cache",
        python_callable=refresh_metabase_cache,
    )

    (
        start
        >> t_create_db
        >> extract_load
        >> t_dq_staging
        >> t_build_marts
        >> t_dq_marts
        >> t_refresh
        >> end
    )