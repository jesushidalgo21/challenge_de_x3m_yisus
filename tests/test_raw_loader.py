from unittest.mock import MagicMock

from common.raw_loader import ensure_raw_tables, upsert_raw_carts, upsert_raw_products


def _mock_conn():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    return conn, cursor


def test_ensure_raw_tables_locks_and_creates_schema_and_tables():
    conn, cursor = _mock_conn()

    ensure_raw_tables(conn)

    executed_sql = [c.args[0] for c in cursor.execute.call_args_list]
    assert any("pg_advisory_xact_lock" in sql for sql in executed_sql)
    assert any("CREATE SCHEMA IF NOT EXISTS raw" in sql for sql in executed_sql)
    assert any("raw.raw_products" in sql for sql in executed_sql)
    assert any("raw.raw_carts" in sql for sql in executed_sql)
    conn.commit.assert_called_once()


def test_upsert_raw_products_targets_products_table_with_correct_params():
    conn, cursor = _mock_conn()

    upsert_raw_products(conn, "2026-08-15", {"products": [{"id": 1}]})

    sql, params = cursor.execute.call_args.args
    assert "INSERT INTO raw.raw_products" in sql
    assert "ON CONFLICT (load_date)" in sql
    assert params[0] == "2026-08-15"
    conn.commit.assert_called_once()


def test_upsert_raw_carts_targets_carts_table():
    conn, cursor = _mock_conn()

    upsert_raw_carts(conn, "2026-08-15", {"carts": []})

    sql, params = cursor.execute.call_args.args
    assert "INSERT INTO raw.raw_carts" in sql
    conn.commit.assert_called_once()


def test_rerunning_same_load_date_is_idempotent_at_the_sql_level():
    conn, cursor = _mock_conn()

    upsert_raw_products(conn, "2026-08-15", {"products": [{"id": 1}]})
    upsert_raw_products(conn, "2026-08-15", {"products": [{"id": 1}, {"id": 2}]})

    assert cursor.execute.call_count == 2
    first_load_date = cursor.execute.call_args_list[0].args[1][0]
    second_load_date = cursor.execute.call_args_list[1].args[1][0]
    assert first_load_date == second_load_date == "2026-08-15"
    # Same ON CONFLICT (load_date) statement both times -- upsert, not insert-only.
    for c in cursor.execute.call_args_list:
        assert "ON CONFLICT (load_date)" in c.args[0]
