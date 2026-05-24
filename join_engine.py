import csv
import logging
import os
import sqlite3
from pathlib import Path

# approach: stream both CSVs into a temp sqlite DB in chunks, run the join on disk,
# then stream results back out. never loads a full file into python memory at once.

logger = logging.getLogger(__name__)

CHUNK_SIZE = 50_000  # rows per batch, 50k worked well without hitting memory limits
BASE_DIR = Path(__file__).parent


def join_sqlite(users_path=None, transactions_path=None, output_path=None, job_id="manual"):
    users_path = users_path or str(BASE_DIR / "users.csv")
    transactions_path = transactions_path or str(BASE_DIR / "transactions.csv")
    output_path = output_path or str(BASE_DIR / f"result_{job_id}.csv")
    db_path = str(BASE_DIR / f"_temp_{job_id}.db")

    logger.info("[%s] Starting join...", job_id)

    try:
        conn = sqlite3.connect(db_path)
        _setup_db(conn)
        _load_users(conn, users_path, job_id)
        _load_transactions(conn, transactions_path, job_id)

        logger.info("[%s] Building index on transactions.user_id...", job_id)
        conn.execute("CREATE INDEX idx_tx_user ON transactions(user_id)")
        conn.commit()

        _run_join(conn, output_path, job_id)
    finally:
        conn.close()
        if os.path.exists(db_path):
            os.remove(db_path)


def _setup_db(conn):
    # cap sqlite's in-memory page cache to ~32MB so we stay within the 256MB limit
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-32000")
    conn.execute("PRAGMA temp_store=FILE")  # spill sort/hash work to disk

    conn.execute("""
        CREATE TABLE users (
            user_id     INTEGER PRIMARY KEY,
            name        TEXT NOT NULL,
            signup_date TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE transactions (
            transaction_id INTEGER,
            user_id        INTEGER,
            amount         REAL
        )
    """)
    conn.commit()


def _load_users(conn, path, job_id):
    logger.info("[%s] Streaming users.csv into sqlite...", job_id)
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        batch = []
        for row in reader:
            batch.append((int(row["user_id"]), row["name"], row["signup_date"]))
            if len(batch) >= CHUNK_SIZE:
                conn.executemany("INSERT OR IGNORE INTO users VALUES (?,?,?)", batch)
                conn.commit()
                batch.clear()
        if batch:
            conn.executemany("INSERT OR IGNORE INTO users VALUES (?,?,?)", batch)
            conn.commit()
    logger.info("[%s] users.csv loaded.", job_id)


def _load_transactions(conn, path, job_id):
    logger.info("[%s] Streaming transactions.csv into sqlite...", job_id)
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        batch = []
        for row in reader:
            batch.append((
                int(row["transaction_id"]),
                int(row["user_id"]),
                float(row["amount"]),
            ))
            if len(batch) >= CHUNK_SIZE:
                conn.executemany("INSERT INTO transactions VALUES (?,?,?)", batch)
                conn.commit()
                batch.clear()
        if batch:
            conn.executemany("INSERT INTO transactions VALUES (?,?,?)", batch)
            conn.commit()
    logger.info("[%s] transactions.csv loaded.", job_id)


def _run_join(conn, output_path, job_id):
    logger.info("[%s] Running INNER JOIN, writing to %s...", job_id, output_path)
    cursor = conn.execute("""
        SELECT t.transaction_id, t.user_id, u.name, u.signup_date, t.amount
        FROM transactions t
        INNER JOIN users u ON t.user_id = u.user_id
    """)
    with open(output_path, "w", newline="") as out:
        writer = csv.writer(out)
        writer.writerow(["transaction_id", "user_id", "name", "signup_date", "amount"])
        while True:
            rows = cursor.fetchmany(CHUNK_SIZE)
            if not rows:
                break
            writer.writerows(rows)
    logger.info("[%s] Join complete, result written to %s", job_id, output_path)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    join_sqlite()


if __name__ == "__main__":
    main()