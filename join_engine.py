import csv
import logging
import os
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

CHUNK_SIZE = 50_000
NUM_BUCKETS = 25
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
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-32000")  # ~32MB page cache
    conn.execute("PRAGMA temp_store=FILE")

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


def join_hash_partition(users_path=None, transactions_path=None, output_path=None, job_id="manual"):
    users_path = users_path or str(BASE_DIR / "users.csv")
    transactions_path = transactions_path or str(BASE_DIR / "transactions.csv")
    output_path = output_path or str(BASE_DIR / f"result_{job_id}.csv")
    tmp_dir = BASE_DIR / f"_tmp_{job_id}"
    tmp_dir.mkdir(exist_ok=True)

    logger.info("[%s] Starting hash-partition join (buckets=%d)...", job_id, NUM_BUCKETS)

    try:
        _partition(users_path, transactions_path, tmp_dir, job_id)
        _join_partitions(tmp_dir, output_path, job_id)
    finally:
        for f in tmp_dir.iterdir():
            f.unlink()
        tmp_dir.rmdir()

    logger.info("[%s] Hash-partition join complete, result written to %s", job_id, output_path)


def _partition(users_path, transactions_path, tmp_dir, job_id):
    user_files = [open(tmp_dir / f"u_{i}.csv", "w", newline="") for i in range(NUM_BUCKETS)]
    tx_files   = [open(tmp_dir / f"t_{i}.csv", "w", newline="") for i in range(NUM_BUCKETS)]
    user_writers = [csv.writer(f) for f in user_files]
    tx_writers   = [csv.writer(f) for f in tx_files]

    try:
        logger.info("[%s] Pass 1a — partitioning users.csv...", job_id)
        with open(users_path, newline="") as f:
            for row in csv.DictReader(f):
                uid = int(row["user_id"])
                user_writers[uid % NUM_BUCKETS].writerow([uid, row["name"], row["signup_date"]])

        logger.info("[%s] Pass 1b — partitioning transactions.csv...", job_id)
        with open(transactions_path, newline="") as f:
            for row in csv.DictReader(f):
                uid = int(row["user_id"])
                tx_writers[uid % NUM_BUCKETS].writerow(
                    [int(row["transaction_id"]), uid, row["amount"]]
                )
    finally:
        for fh in user_files + tx_files:
            fh.close()


def _join_partitions(tmp_dir, output_path, job_id):
    with open(output_path, "w", newline="") as out:
        writer = csv.writer(out)
        writer.writerow(["transaction_id", "user_id", "name", "signup_date", "amount"])

        for i in range(NUM_BUCKETS):
            logger.info("[%s] Pass 2 — joining partition %d/%d...", job_id, i + 1, NUM_BUCKETS)

            user_map = {}
            with open(tmp_dir / f"u_{i}.csv", newline="") as f:
                for uid_s, name, signup_date in csv.reader(f):
                    user_map[int(uid_s)] = (name, signup_date)

            with open(tmp_dir / f"t_{i}.csv", newline="") as f:
                for tx_id, uid_s, amount in csv.reader(f):
                    uid = int(uid_s)
                    if uid in user_map:
                        name, signup_date = user_map[uid]
                        writer.writerow([tx_id, uid, name, signup_date, amount])


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    join_sqlite()


if __name__ == "__main__":
    main()