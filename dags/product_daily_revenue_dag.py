from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

from common.db import get_connection
from common.dummyjson_client import fetch_carts, fetch_products
from common.raw_loader import ensure_raw_tables, upsert_raw_carts, upsert_raw_products

# Retries with backoff only make sense on the extraction tasks: a network
# failure is transient, a dbt failure is a logic error that retrying won't fix.
EXTRACT_TASK_KWARGS = {
    "retries": 3,
    "retry_delay": pendulum.duration(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": pendulum.duration(minutes=10),
}


@dag(
    dag_id="product_daily_revenue_dag",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 0},
    tags=["x3m", "dummyjson"],
)
def product_daily_revenue_dag():
    @task(**EXTRACT_TASK_KWARGS)
    def extract_products():
        return fetch_products()

    @task(**EXTRACT_TASK_KWARGS)
    def extract_carts():
        return fetch_carts()

    @task
    def load_raw(products_payload: dict, carts_payload: dict, ds=None):
        # `ds` is Airflow's logical execution date (data interval), never
        # datetime.now() -- critical for backfill and idempotency to work.
        conn = get_connection()
        try:
            ensure_raw_tables(conn)
            upsert_raw_products(conn, ds, products_payload)
            upsert_raw_carts(conn, ds, carts_payload)
        finally:
            conn.close()

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "/opt/dbt_venv/bin/dbt build "
            "--project-dir /opt/airflow/dbt_project "
            "--profiles-dir /opt/airflow/dbt_project"
        ),
    )

    load_raw(extract_products(), extract_carts()) >> dbt_build


product_daily_revenue_dag()
