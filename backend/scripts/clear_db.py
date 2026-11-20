import json
import psycopg
import logging
from pathlib import Path

def load_database_config():
    config_path = Path(__file__).resolve().parents[2] / "config.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    return config["database"]

def get_connection():
    return psycopg.connect(
        **load_database_config(),
        connect_timeout=10,
    )

def clear_expired(table, target_field):
    query = psycopg.sql.SQL(
        "DELETE FROM {} WHERE {} <= CURRENT_TIMESTAMP"
    ).format(
        psycopg.sql.Identifier(table),
        psycopg.sql.Identifier(target_field),
    )

    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(query)
            deleted_count = c.rowcount

    return deleted_count

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    tasks = [
        ("scanner_events", "expired_at"),
    ]

    failed = False

    for table, target_field in tasks:
        try:
            count = clear_expired(table, target_field)
        except Exception:
            failed = True
            logging.exception(
                "清理失败 table=%s, filed=%s",
                table,
                target_field
            )

        else:
            logging.info(
                "清理完成 table=%s, filed=%s, count=%d",
                table,
                target_field,
                count
            )
    return 1 if failed else 0

if __name__ == "__main__":
    raise SystemExit(main())
