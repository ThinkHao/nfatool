from __future__ import annotations

import json
import shutil
from pathlib import Path
import sys
from typing import Optional
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from .config import get_settings, BASE_DIR
from .db import init_db, session_scope
from .models import Task, JobRun
from .schemas import TaskCreate, TaskUpdate, TaskOut, JobRunCreate, JobRunOut
from .security import api_key_auth
from .services.scheduler import (
    create_ad_hoc_job_run,
    create_job_run_from_task,
    load_tasks_into_scheduler,
    schedule_retention_cleanup,
)
from .services.scheduler import scheduler, apply_schedule_for_task_snapshot
from .services.storage import get_job_dir
from .services.logger import get_job_log_path

app = FastAPI(title="NFA 95th Web Service", version="0.1.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# Static files: support dev and frozen (PyInstaller)
def _candidate_static_dirs():
    here = Path(__file__).resolve().parent
    candidates = [
        here / "static",                  # dev
        BASE_DIR / "static",              # alongside exe (optional)
    ]
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        mp = Path(meipass)
        candidates += [
            mp / "static",
            mp / "server" / "static",
        ]
    return [p for p in candidates if p.exists()]

_static_dirs = _candidate_static_dirs()
if _static_dirs:
    app.mount("/static", StaticFiles(directory=str(_static_dirs[0])), name="static")


@app.on_event("startup")
def on_startup():
    settings = get_settings()
    init_db()
    load_tasks_into_scheduler()
    schedule_retention_cleanup()


def _get_next_run_time(task_id: int):
    try:
        if scheduler:
            job = scheduler.get_job(f"task-{task_id}")
            if job and job.next_run_time:
                return job.next_run_time
    except Exception:
        pass
    return None


@app.get("/", response_class=HTMLResponse)
async def root_page():
    # Try multiple locations for index.html
    for d in _candidate_static_dirs():
        index_path = d / "index.html"
        if index_path.exists():
            return index_path.read_text(encoding="utf-8")
    return "<h1>NFA 95th Web Service</h1>"


@app.get("/api/health")
async def health():
    return {"status": "ok"}


@app.get("/api/meta/cp-mapping", dependencies=[Depends(api_key_auth)])
async def cp_mapping():
    # Prefer mapping.json under server/, fallback to project root
    server_dir = Path(__file__).resolve().parent
    meipass = getattr(sys, "_MEIPASS", None)
    candidates = [
        server_dir / "mapping.json",              # server/mapping.json
        BASE_DIR / "mapping.json",                # exe dir/mapping.json
        server_dir.parent / "mapping.json",       # repo root mapping.json (dev fallback)
    ]
    if meipass:
        mp = Path(meipass)
        candidates += [mp / "mapping.json", mp / "server" / "mapping.json"]
    mapping = {}
    for p in candidates:
        if p.exists():
            try:
                mapping = json.loads(p.read_text(encoding="utf-8"))
                break
            except Exception:
                mapping = {}
                break
    return {"mapping": mapping}


@app.get("/api/meta/paths", dependencies=[Depends(api_key_auth)])
async def meta_paths():
    s = get_settings()
    return {
        "storage_dir": s.STORAGE_DIR,
        "log_dir": s.LOG_DIR,
        "sqlite_url": s.SQLITE_URL,
    }


# Tasks CRUD
@app.post("/api/tasks", response_model=TaskOut, dependencies=[Depends(api_key_auth)])
async def create_task(payload: TaskCreate):
    with session_scope() as s:
        if s.query(Task).filter(Task.name == payload.name).first():
            raise HTTPException(status_code=400, detail="Task name already exists")
        t = Task(
            name=payload.name,
            active=payload.active,
            kind=payload.kind,
            schedule_type=payload.schedule_type,
            schedule_expr=payload.schedule_expr,
            schedule_time_of_day=payload.schedule_time_of_day,
            timezone=payload.timezone,
            window_selector=payload.window_selector,
            window_params=json.dumps(payload.window_params or {}, ensure_ascii=False),
            params=json.dumps(payload.params or {}, ensure_ascii=False),
            export_formats=json.dumps(payload.export_formats or ["csv"], ensure_ascii=False),
            output_filename_template=payload.output_filename_template,
        )
        s.add(t)
        s.flush()
        # (Re)schedule if periodic
        try:
            apply_schedule_for_task_snapshot(
                task_id=t.id,
                active=t.active,
                kind=t.kind,
                schedule_type=t.schedule_type,
                schedule_expr=t.schedule_expr,
                schedule_time_of_day=t.schedule_time_of_day,
            )
        except Exception:
            pass
        # Build TaskOut with parsed JSON fields
        return TaskOut(
            id=t.id,
            name=t.name,
            active=t.active,
            kind=t.kind,
            schedule_type=t.schedule_type,
            schedule_expr=t.schedule_expr,
            schedule_time_of_day=t.schedule_time_of_day,
            timezone=t.timezone,
            window_selector=t.window_selector,
            window_params=(json.loads(t.window_params) if t.window_params else None),
            params=(json.loads(t.params) if t.params else {}),
            export_formats=(json.loads(t.export_formats) if t.export_formats else ["csv"]),
            output_filename_template=t.output_filename_template,
            created_at=t.created_at,
            updated_at=t.updated_at,
            next_run_time=_get_next_run_time(t.id),
        )


@app.get("/api/tasks", response_model=list[TaskOut], dependencies=[Depends(api_key_auth)])
async def list_tasks():
    with session_scope() as s:
        rows = s.query(Task).order_by(Task.id.desc()).all()
        out: list[TaskOut] = []
        for t in rows:
            out.append(TaskOut(
                id=t.id,
                name=t.name,
                active=t.active,
                kind=t.kind,
                schedule_type=t.schedule_type,
                schedule_expr=t.schedule_expr,
                schedule_time_of_day=t.schedule_time_of_day,
                timezone=t.timezone,
                window_selector=t.window_selector,
                window_params=(json.loads(t.window_params) if t.window_params else None),
                params=(json.loads(t.params) if t.params else {}),
                export_formats=(json.loads(t.export_formats) if t.export_formats else ["csv"]),
                output_filename_template=t.output_filename_template,
                created_at=t.created_at,
                updated_at=t.updated_at,
                next_run_time=_get_next_run_time(t.id),
            ))
        return out


@app.get("/api/tasks/{task_id}", response_model=TaskOut, dependencies=[Depends(api_key_auth)])
async def get_task(task_id: int):
    with session_scope() as s:
        t = s.get(Task, task_id)
        if not t:
            raise HTTPException(status_code=404, detail="Task not found")
        return TaskOut(
            id=t.id,
            name=t.name,
            active=t.active,
            kind=t.kind,
            schedule_type=t.schedule_type,
            schedule_expr=t.schedule_expr,
            schedule_time_of_day=t.schedule_time_of_day,
            timezone=t.timezone,
            window_selector=t.window_selector,
            window_params=(json.loads(t.window_params) if t.window_params else None),
            params=(json.loads(t.params) if t.params else {}),
            export_formats=(json.loads(t.export_formats) if t.export_formats else ["csv"]),
            output_filename_template=t.output_filename_template,
            created_at=t.created_at,
            updated_at=t.updated_at,
            next_run_time=_get_next_run_time(t.id),
        )


@app.put("/api/tasks/{task_id}", response_model=TaskOut, dependencies=[Depends(api_key_auth)])
async def update_task(task_id: int, payload: TaskUpdate):
    with session_scope() as s:
        t = s.get(Task, task_id)
        if not t:
            raise HTTPException(status_code=404, detail="Task not found")
        data = payload.model_dump(exclude_unset=True)
        if "window_params" in data and data["window_params"] is not None:
            data["window_params"] = json.dumps(data["window_params"], ensure_ascii=False)
        if "params" in data and data["params"] is not None:
            data["params"] = json.dumps(data["params"], ensure_ascii=False)
        if "export_formats" in data and data["export_formats"] is not None:
            data["export_formats"] = json.dumps(data["export_formats"], ensure_ascii=False)
        for k, v in data.items():
            setattr(t, k, v)
        s.add(t)
        # (Re)schedule on update
        try:
            apply_schedule_for_task_snapshot(
                task_id=t.id,
                active=t.active,
                kind=t.kind,
                schedule_type=t.schedule_type,
                schedule_expr=t.schedule_expr,
                schedule_time_of_day=t.schedule_time_of_day,
            )
        except Exception:
            pass
        return TaskOut(
            id=t.id,
            name=t.name,
            active=t.active,
            kind=t.kind,
            schedule_type=t.schedule_type,
            schedule_expr=t.schedule_expr,
            schedule_time_of_day=t.schedule_time_of_day,
            timezone=t.timezone,
            window_selector=t.window_selector,
            window_params=(json.loads(t.window_params) if t.window_params else None),
            params=(json.loads(t.params) if t.params else {}),
            export_formats=(json.loads(t.export_formats) if t.export_formats else ["csv"]),
            output_filename_template=t.output_filename_template,
            created_at=t.created_at,
            updated_at=t.updated_at,
            next_run_time=_get_next_run_time(t.id),
        )


@app.delete("/api/tasks/{task_id}", dependencies=[Depends(api_key_auth)])
async def delete_task(task_id: int):
    with session_scope() as s:
        t = s.get(Task, task_id)
        if not t:
            raise HTTPException(status_code=404, detail="Task not found")
        s.delete(t)
    # Remove scheduled job if exists
    try:
        if scheduler:
            scheduler.remove_job(f"task-{task_id}")
    except Exception:
        pass
    return {"ok": True}


@app.post("/api/tasks/{task_id}/run", dependencies=[Depends(api_key_auth)])
async def trigger_task_run(task_id: int):
    job_id = create_job_run_from_task(task_id)
    return {"job_id": job_id}


@app.post("/api/jobs/run", dependencies=[Depends(api_key_auth)])
async def run_ad_hoc(payload: JobRunCreate):
    settings = get_settings()
    job_id = create_ad_hoc_job_run(payload.model_dump(exclude_unset=True), settings.TIMEZONE)
    return {"job_id": job_id}


@app.get("/api/jobs", response_model=list[JobRunOut], dependencies=[Depends(api_key_auth)])
async def list_jobs(task_id: Optional[int] = Query(default=None)):
    out: list[JobRunOut] = []
    with session_scope() as s:
        q = s.query(JobRun)
        if task_id is not None:
            q = q.filter(JobRun.task_id == task_id)
        # SQLite 不支持 NULLS LAST 语法，这里改为先按 IS NULL 升序，再按时间降序，实现等价效果
        rows = q.order_by((JobRun.started_at.is_(None)).asc(), JobRun.started_at.desc()).limit(200).all()
        for r in rows:
            artifacts = []
            try:
                artifacts = json.loads(r.artifacts) if r.artifacts else []
            except Exception:
                artifacts = []
            row = JobRunOut(
                id=r.id,
                task_id=r.task_id,
                status=r.status,
                started_at=r.started_at,
                finished_at=r.finished_at,
                resolved_window=json.loads(r.resolved_window) if r.resolved_window else None,
                resolved_params=json.loads(r.resolved_params) if r.resolved_params else None,
                artifacts=artifacts,
                log_path=r.log_path,
                error_message=r.error_message,
            )
            out.append(row)
    return out


@app.get("/api/jobs/{job_id}", response_model=JobRunOut, dependencies=[Depends(api_key_auth)])
async def get_job(job_id: str):
    with session_scope() as s:
        r = s.get(JobRun, job_id)
        if not r:
            raise HTTPException(status_code=404, detail="Job not found")
        artifacts = []
        try:
            artifacts = json.loads(r.artifacts) if r.artifacts else []
        except Exception:
            artifacts = []
        return JobRunOut(
            id=r.id,
            task_id=r.task_id,
            status=r.status,
            started_at=r.started_at,
            finished_at=r.finished_at,
            resolved_window=json.loads(r.resolved_window) if r.resolved_window else None,
            resolved_params=json.loads(r.resolved_params) if r.resolved_params else None,
            artifacts=artifacts,
            log_path=r.log_path,
            error_message=r.error_message,
        )


@app.get("/api/jobs/{job_id}/download", dependencies=[Depends(api_key_auth)])
async def download_artifact(job_id: str, file: str):
    from .services.storage import safe_artifact_path
    p = safe_artifact_path(job_id, file)
    if not p.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path=str(p), filename=p.name)


@app.delete("/api/jobs/{job_id}", dependencies=[Depends(api_key_auth)])
async def delete_job(job_id: str):
    # Cancel scheduled run if still pending in scheduler
    try:
        if scheduler:
            scheduler.remove_job(job_id)
    except Exception:
        pass
    # Remove artifacts directory and log file
    try:
        d = get_job_dir(job_id)
        if d.exists():
            shutil.rmtree(d, ignore_errors=True)
    except Exception:
        pass
    try:
        log_path = get_job_log_path(job_id)
        if log_path.exists():
            log_path.unlink(missing_ok=True)
    except Exception:
        pass
    # Remove DB row
    with session_scope() as s:
        r = s.get(JobRun, job_id)
        if r:
            s.delete(r)
    return {"ok": True}
