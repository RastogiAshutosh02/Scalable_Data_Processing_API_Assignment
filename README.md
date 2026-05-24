# Scalable Data Processing API

Solutions for two assignments — out-of-core data joins and non-blocking FastAPI endpoints.

---

## Files

| File | What it does |
|---|---|
| `generate_data.py` | Creates synthetic `users.csv` (5M rows) and `transactions.csv` (10M rows) |
| `join_engine.py` | Core SQLite-backed join — streams both CSVs in 50k-row chunks |
| `assignment1_sol1_chunked.py` | Assignment 1, Solution 1 (chunked hash join) |
| `assignment1_sol2_partition.py` | Assignment 1, Solution 2 (hash partition join) |
| `app.py` | FastAPI service with both async approaches |
| `assignment2_sol1_background.py` | Assignment 2, Solution 1 (BackgroundTasks + ThreadPool) |
| `assignment2_sol2_processpool.py` | Assignment 2, Solution 2 (ProcessPoolExecutor) |
| `requirements.txt` | Python dependencies |

---

## Quick start

```bash
pip install -r requirements.txt

# generate test data (~500MB each, takes a couple of minutes)
python generate_data.py

# run the standalone join
python join_engine.py

# start the API
uvicorn app:app --host 0.0.0.0 --port 8000
```

Trigger a job:

```bash
# Solution 1 - BackgroundTasks
curl -X POST http://localhost:8000/trigger-join/background-tasks

# Solution 2 - ProcessPool
curl -X POST http://localhost:8000/trigger-join/process-pool

# check status
curl http://localhost:8000/status/<job_id>
```

---

## Assignment 1 — Out-of-Core Join

Both solutions share `join_engine.py`. The engine streams both CSV files into a temp SQLite database in fixed-size batches, builds an index on `user_id`, runs the INNER JOIN on disk, and streams results back out. Python memory stays flat regardless of input size.

SQLite is configured with WAL mode and a 32MB page cache cap to stay under 256MB.

**Solution 1** — reads both files in 50k-row chunks and inserts into SQLite directly.

**Solution 2** — hash-partitions the input first, then loads each partition separately.

---

## Assignment 2 — Non-Blocking FastAPI

Both endpoints return a `job_id` immediately and run the join in the background.

**Solution 1 — BackgroundTasks + ThreadPoolExecutor**
Pushes the blocking work to a thread pool so the event loop stays free. Simple, no extra infra. GIL limits true parallelism but fine for I/O-heavy work.

**Solution 2 — ProcessPoolExecutor**
Spawns a real OS process per job. Fully GIL-free and crash-isolated. Trade-off: ~100-300ms process startup overhead.
