from psycopg2.extras import Json

_TABLES = {
    "raw_products": "raw.raw_products",
    "raw_carts": "raw.raw_carts",
}


def ensure_raw_tables(conn):
    with conn.cursor() as cur:
        # Parallel backfill runs call this concurrently. "IF NOT EXISTS" alone
        # isn't safe under concurrent DDL (both sessions can pass the
        # existence check before either commits), so serialize with a
        # transaction-scoped advisory lock -- released automatically on commit.
        cur.execute("SELECT pg_advisory_xact_lock(hashtext('ensure_raw_tables'));")
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        for table in _TABLES.values():
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    load_date DATE PRIMARY KEY,
                    data JSONB NOT NULL,
                    extracted_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                """
            )
    conn.commit()


def _upsert(conn, table, load_date, payload):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {table} (load_date, data, extracted_at)
            VALUES (%s, %s, now())
            ON CONFLICT (load_date)
            DO UPDATE SET data = EXCLUDED.data, extracted_at = EXCLUDED.extracted_at;
            """,
            (load_date, Json(payload)),
        )
    conn.commit()


def upsert_raw_products(conn, load_date, payload):
    _upsert(conn, _TABLES["raw_products"], load_date, payload)


def upsert_raw_carts(conn, load_date, payload):
    _upsert(conn, _TABLES["raw_carts"], load_date, payload)
