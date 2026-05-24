# Scalable Data Processing API

Two assignments — out-of-core data joins and a non-blocking FastAPI backend.

---

## Files

| File | Description |
|---|---|
| `generate_data.py` | Generates `users.csv` (5M rows) and `transactions.csv` (10M rows) |
| `join_engine.py` | Both join implementations — SQLite streaming and hash-partition |
| `assignment1_sol1_chunked.py` | Assignment 1, Solution 1 — SQLite-based join |
| `assignment1_sol2_partition.py` | Assignment 1, Solution 2 — hash-partition join |
| `app.py` | FastAPI app with both background approaches |
| `assignment2_sol1_background.py` | Assignment 2, Solution 1 entry point |
| `assignment2_sol2_processpool.py` | Assignment 2, Solution 2 entry point |
| `requirements.txt` | Dependencies |

---

## Quick start

```bash
pip install -r requirements.txt

# generate test data (takes a couple of minutes)
python generate_data.py

# run a standalone join
python assignment1_sol1_chunked.py
python assignment1_sol2_partition.py

# start the API
uvicorn app:app --host 0.0.0.0 --port 8000
```

Trigger a job:

```bash
curl -X POST http://localhost:8000/trigger-join/background-tasks
curl -X POST http://localhost:8000/trigger-join/process-pool

# check status
curl http://localhost:8000/status/<job_id>
```

---

## Assignment 1 — Out-of-Core Join

Both solutions are in `join_engine.py`. Neither reads the full CSV into memory at once.

### Solution 1 — SQLite streaming join

Streams both CSVs into a temporary SQLite database in 50k-row batches, builds an index on `user_id`, runs the INNER JOIN entirely on disk, then streams results back out row by row. Python memory stays roughly flat no matter how big the files are.

SQLite is configured with a 32MB page cache limit and `temp_store=FILE` so any internal sorting spills to disk rather than RAM.

**Pros:** straightforward to implement, single pass over each file, SQLite handles all the join logic  
**Cons:** needs extra disk space for the temp `.db` file, adds a SQLite dependency

### Solution 2 — Hash-partition join

Two passes, no database needed.

- **Pass 1** — read both CSVs and split each row into one of 25 bucket files based on `user_id % 25`. Any two rows with the same `user_id` always end up in the same bucket.
- **Pass 2** — for each bucket, load the user rows into a dict, then stream the transaction rows and write out matches.

Memory at any point is just one bucket's worth of users (~200k rows) plus one transaction row being processed.

**Pros:** no external dependencies, easy to control memory usage by adjusting bucket count  
**Cons:** scans both input files twice, writes intermediate bucket files to disk

---

## Assignment 2 — Non-Blocking FastAPI

Both endpoints return a `job_id` right away and run the join in the background. Job status is stored in memory and queryable via `GET /status/{job_id}`.

### Solution 1 — BackgroundTasks + ThreadPoolExecutor

FastAPI's `BackgroundTasks` schedules the work after the HTTP response goes out. The join runs in a thread pool so the event loop isn't blocked.

**Pros:**
- No extra setup needed — no Celery, no Redis
- Simple flow, job status is in shared memory so reads are fast

**Cons:**
- The GIL means threads can't truly run in parallel for CPU-heavy work
- A thread crash could affect the whole server process
- Jobs are lost on server restart since it's all in-memory

### Solution 2 — ProcessPoolExecutor

Uses `asyncio.create_task` + `run_in_executor` to run each job in a separate OS process, fully bypassing the GIL.

**Pros:**
- True parallelism across CPU cores
- A crashed worker process doesn't bring down the API server

**Cons:**
- Starting a new process adds some overhead per job (~100-300ms)
- The worker can't directly update the parent's job dict, so status gets written from the parent after the future completes
