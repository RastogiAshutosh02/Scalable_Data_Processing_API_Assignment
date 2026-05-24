import asyncio
import logging
import uuid
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime, timezone

from fastapi import BackgroundTasks, FastAPI, HTTPException

from join import join_sqlite

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Data Join API")

# in-memory job store
jobs = {}

_thread_pool = ThreadPoolExecutor(max_workers=4)
_process_pool = ProcessPoolExecutor(max_workers=2)


def _run_join_worker(job_id):
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    log = logging.getLogger(__name__)
    log.info("[%s] Worker started", job_id)
    join_sqlite(job_id=job_id)
    log.info("[%s] Worker finished - result_%s.csv written", job_id, job_id)


@app.post("/trigger-join/background-tasks")
async def trigger_background_tasks(background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())[:8]
    jobs[job_id] = {"status": "queued", "approach": "background_tasks", "created_at": _now()}
    background_tasks.add_task(_run_in_thread, job_id)
    return {"job_id": job_id, "status": "queued", "approach": "background_tasks"}


async def _run_in_thread(job_id):
    jobs[job_id]["status"] = "running"
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(_thread_pool, _run_join_worker, job_id)
        jobs[job_id].update({"status": "completed", "completed_at": _now()})
        logger.info("[%s] Thread job done", job_id)
    except Exception as e:
        jobs[job_id].update({"status": "failed", "error": str(e)})
        logger.error("[%s] Thread job failed: %s", job_id, e)


@app.post("/trigger-join/process-pool")
async def trigger_process_pool():
    job_id = str(uuid.uuid4())[:8]
    jobs[job_id] = {"status": "queued", "approach": "process_pool", "created_at": _now()}
    asyncio.create_task(_run_in_process(job_id))
    return {"job_id": job_id, "status": "queued", "approach": "process_pool"}


async def _run_in_process(job_id):
    jobs[job_id]["status"] = "running"
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(_process_pool, _run_join_worker, job_id)
        jobs[job_id].update({"status": "completed", "completed_at": _now()})
        logger.info("[%s] Process job done", job_id)
    except Exception as e:
        jobs[job_id].update({"status": "failed", "error": str(e)})
        logger.error("[%s] Process job failed: %s", job_id, e)


@app.get("/status/{job_id}")
async def get_status(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    return {"job_id": job_id, **jobs[job_id]}


@app.get("/jobs")
async def list_jobs():
    return [{"job_id": k, **v} for k, v in jobs.items()]


@app.get("/health")
async def health():
    running = sum(1 for j in jobs.values() if j["status"] == "running")
    return {"status": "ok", "running_jobs": running}


def _now():
    return datetime.now(timezone.utc).isoformat()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)
