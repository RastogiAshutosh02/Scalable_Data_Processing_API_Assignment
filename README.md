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

Both solutions live in `join_engine.py` and are run via their respective entry-point scripts. Neither loads a full file into Python memory at once.

### Solution 1 — SQLite streaming join (`assignment1_sol1_chunked.py`)

Streams both CSVs into a temporary on-disk SQLite database in 50k-row batches, builds an index on `transactions.user_id`, then lets SQLite execute the INNER JOIN and streams the cursor back out in chunks. Python never holds more than one batch at a time.

SQLite is tuned to stay inside 256 MB: `PRAGMA cache_size=-32000` (32 MB page cache), `PRAGMA temp_store=FILE` (sort spills go to disk).

**Pros:** Single-pass ingest, SQLite handles all sorting and indexing internally, minimal code.  
**Cons:** Requires disk space for the temp `.db` file (~2× input size); SQLite is a hard dependency.

### Solution 2 — Hash-partition join (`assignment1_sol2_partition.py`)

Two-pass, purely file-based algorithm with no database.

- **Pass 1** — Scan both CSVs once and distribute every row into one of 25 bucket files keyed by `user_id % 25`. Users and transactions that share a `user_id` always land in the same bucket.
- **Pass 2** — For each bucket, load the user rows into a Python dict, then stream the transaction rows and emit matches. Peak memory = one bucket of users (~200k rows ≈ ~20 MB) plus one transaction row.

**Pros:** No external dependencies, memory use is strictly bounded by bucket size, easy to tune `NUM_BUCKETS`.  
**Cons:** Two full scans of both input files; writes intermediate bucket files to disk (≈ input size).

---

## Assignment 2 — Non-Blocking FastAPI

Both endpoints in `app.py` accept `POST` requests, return a `job_id` immediately, and run the join in the background. Job status is tracked in an in-memory dict and queryable via `GET /status/{job_id}`.

### Solution 1 — BackgroundTasks + ThreadPoolExecutor (`/trigger-join/background-tasks`)

FastAPI's `BackgroundTasks` schedules the work after the HTTP response is sent. The blocking join is offloaded to a `ThreadPoolExecutor` so the event loop is never blocked.

**Pros:**
- No extra infrastructure — no Celery, no Redis, nothing to install
- Tasks start immediately after the response; simple to reason about
- Job status lives in shared memory so status reads are instant

**Cons:**
- Python's GIL prevents threads from using multiple CPU cores for compute-heavy work
- A crashing thread can corrupt shared state in the same process
- Jobs are lost if the server restarts (in-memory store only)

### Solution 2 — ProcessPoolExecutor (`/trigger-join/process-pool`)

Uses `asyncio.create_task` + `loop.run_in_executor` to submit the join to a `ProcessPoolExecutor`. Each job runs in a real OS subprocess, completely bypassing the GIL.

**Pros:**
- True CPU parallelism across cores — GIL does not apply
- Process isolation means a crash in the worker cannot take down the API server
- The OS reclaims subprocess memory when the job finishes

**Cons:**
- Forking a new Python process adds ~100–300 ms of startup overhead per job
- The subprocess cannot directly write to the parent's `jobs` dict; status is updated from the parent once the future resolves
- Each subprocess carries its own Python interpreter (~30–50 MB extra RSS)
