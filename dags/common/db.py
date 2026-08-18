import os

import psycopg2


def get_connection():
    return psycopg2.connect(
        host=os.environ["DBT_PG_HOST"],
        port=os.environ.get("DBT_PG_PORT", "5432"),
        user=os.environ["DBT_PG_USER"],
        password=os.environ["DBT_PG_PASSWORD"],
        dbname=os.environ["DBT_TARGET_DB"],
    )
