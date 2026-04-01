from __future__ import annotations

import json
import shutil
import zipfile
from datetime import datetime
from pathlib import Path
import sys
from typing import Optional
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import and_, func, or_
import pymysql

from .config import (
    get_settings,
    BASE_DIR,
    get_data_source_catalog,
    get_data_source_instances,
    list_data_source_instances,
    upsert_runtime_data_source_instance,
    delete_runtime_data_source_instance,
    list_data_source_config_audit,
    rotate_data_source_encryption_key,
    get_data_key_rotation_status,
    set_data_key_rotation_policy,
    auto_rotate_data_source_key,
)
from .db import init_db, session_scope
from .models import Task, JobRun, TaskGroup
from .schemas import (
    TaskCreate, TaskUpdate, TaskOut, JobRunCreate, JobRunOut, TaskPageOut, JobRunPageOut,
    TaskBatchDelete, JobBatchDelete, JobBatchDownload,
    TaskGroupCreate, TaskGroupRename, TaskGroupAssign,
    DataSourceInstancePayload, DataSourceTestPayload, DataSourceRotateKeyPayload, DataSourceRotatePolicyPayload, UpdateApplyPayload,
)
from .security import api_key_auth
from .services.scheduler import (
    create_ad_hoc_job_run,
    create_job_run_from_task,
    load_tasks_into_scheduler,
    schedule_retention_cleanup,
    schedule_config_key_rotation,
)
from .services.scheduler import apply_schedule_for_task_snapshot
from .services import scheduler as scheduler_service
from .services.storage import get_job_dir
from .services.logger import get_job_log_path
from .services.compute95 import _connect_edc_db
from .services.updater import check_update, apply_update, get_update_status

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
    schedule_config_key_rotation()


@app.on_event("shutdown")
def on_shutdown():
    try:
        if scheduler_service.scheduler:
            scheduler_service.scheduler.shutdown(wait=False)
    except Exception:
        pass


def _get_next_run_time(task_id: int):
    try:
        if scheduler_service.scheduler:
            job = scheduler_service.scheduler.get_job(f"task-{task_id}")
            if job and job.next_run_time:
                return job.next_run_time
    except Exception:
        pass
    return None


def _normalize_group_name(value: str | None) -> str | None:
    if value is None:
        return None
    name = str(value).strip()
    return name or None


def _ensure_task_group_exists(s, group_name: str | None) -> None:
    name = _normalize_group_name(group_name)
    if not name:
        return
    row = s.query(TaskGroup).filter(TaskGroup.name == name).first()
    if row:
        return
    s.add(TaskGroup(name=name))


def _backfill_task_groups_from_tasks(s) -> None:
    rows = (
        s.query(Task.group_name)
        .filter(Task.group_name.is_not(None))
        .filter(Task.group_name != "")
        .distinct()
        .all()
    )
    names = [str(r[0]).strip() for r in rows if r and r[0] and str(r[0]).strip()]
    if not names:
        return
    existing = {
        str(x.name)
        for x in s.query(TaskGroup).filter(TaskGroup.name.in_(names)).all()
    }
    for name in names:
        if name not in existing:
            s.add(TaskGroup(name=name))


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

@app.get("/api/meta/data-sources", dependencies=[Depends(api_key_auth)])
async def meta_data_sources():
    return get_data_source_catalog()


def _test_nfa_connection(cfg: dict) -> None:
    db_cfg = {
        "host": cfg.get("host"),
        "port": int(cfg.get("port", 3306)),
        "user": cfg.get("user"),
        "password": cfg.get("password"),
        "db": cfg.get("db"),
        "charset": cfg.get("charset", "utf8mb4"),
    }
    if not (db_cfg["host"] and db_cfg["user"] and db_cfg["password"] and db_cfg["db"]):
        raise ValueError("NFA instance config must include host/port/user/password/db")
    conn = pymysql.connect(
        host=db_cfg["host"],
        port=db_cfg["port"],
        user=db_cfg["user"],
        password=db_cfg["password"],
        db=db_cfg["db"],
        charset=db_cfg["charset"],
        connect_timeout=8,
        read_timeout=10,
        write_timeout=10,
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 AS ok")
            cursor.fetchall()
    finally:
        conn.close()


@app.get("/api/meta/data-sources/instances", dependencies=[Depends(api_key_auth)])
async def list_data_source_instances_api(source_type: str = Query(default="all")):
    st = (source_type or "all").lower()
    if st == "all":
        items = list_data_source_instances("nfa") + list_data_source_instances("edc")
    else:
        items = list_data_source_instances(st)
    items = sorted(items, key=lambda x: (str(x.get("source_type") or ""), str(x.get("instance") or "")))
    return {"items": items}


@app.post("/api/meta/data-sources/instances", dependencies=[Depends(api_key_auth)])
async def save_data_source_instance_api(payload: DataSourceInstancePayload, request: Request):
    try:
        actor = f"{request.client.host if request.client else 'unknown'}"
        upsert_runtime_data_source_instance(payload.source_type, payload.instance, payload.config or {}, actor=actor)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"ok": True}


@app.delete("/api/meta/data-sources/instances", dependencies=[Depends(api_key_auth)])
async def delete_data_source_instance_api(request: Request, source_type: str = Query(...), instance: str = Query(...)):
    actor = f"{request.client.host if request.client else 'unknown'}"
    removed = delete_runtime_data_source_instance(source_type, instance, actor=actor)
    return {"ok": True, "deleted": 1 if removed else 0}


@app.get("/api/meta/data-sources/audit", dependencies=[Depends(api_key_auth)])
async def list_data_source_audit_api(limit: int = Query(default=100, ge=1, le=500)):
    return {"items": list_data_source_config_audit(limit=limit)}


@app.post("/api/meta/data-sources/rotate-key", dependencies=[Depends(api_key_auth)])
async def rotate_data_source_key_api(payload: DataSourceRotateKeyPayload):
    try:
        result = rotate_data_source_encryption_key(payload.old_seed, payload.new_seed)
        return {"ok": True, **result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/meta/data-sources/rotate-policy", dependencies=[Depends(api_key_auth)])
async def get_rotate_policy_api():
    return {"ok": True, **get_data_key_rotation_status()}


@app.post("/api/meta/data-sources/rotate-policy", dependencies=[Depends(api_key_auth)])
async def set_rotate_policy_api(payload: DataSourceRotatePolicyPayload):
    try:
        status = set_data_key_rotation_policy(bool(payload.enabled), int(payload.interval_days))
        return {"ok": True, **status}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/meta/data-sources/rotate-key-auto", dependencies=[Depends(api_key_auth)])
async def rotate_key_auto_api(force: bool = Query(default=True)):
    try:
        result = auto_rotate_data_source_key(force=bool(force))
        return {"ok": True, **result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/meta/update", dependencies=[Depends(api_key_auth)])
async def get_update_info_api():
    try:
        return check_update()
    except Exception as e:
        return {"ok": False, "message": str(e)}


@app.post("/api/meta/update/apply", dependencies=[Depends(api_key_auth)])
async def apply_update_api(payload: UpdateApplyPayload):
    try:
        return apply_update(restart_after_update=bool(payload.restart_after_update))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/meta/update/status", dependencies=[Depends(api_key_auth)])
async def get_update_status_api():
    try:
        return get_update_status()
    except Exception as e:
        return {"ok": False, "status": "unknown", "running": False, "message": str(e)}


@app.post("/api/meta/data-sources/test", dependencies=[Depends(api_key_auth)])
async def test_data_source_connection_api(payload: DataSourceTestPayload):
    source_type = (payload.source_type or "nfa").lower()
    cfg = payload.config
    if cfg is None:
        if not payload.instance:
            raise HTTPException(status_code=400, detail="instance or config is required")
        inst = get_data_source_instances(source_type)
        if payload.instance not in inst:
            raise HTTPException(status_code=404, detail=f"instance not found: {payload.instance}")
        cfg = inst[payload.instance]
    if not isinstance(cfg, dict):
        raise HTTPException(status_code=400, detail="config must be object")

    started = datetime.now()
    try:
        if source_type == "edc":
            conn, tunnel = _connect_edc_db(cfg)
            try:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1 AS ok")
                    cursor.fetchall()
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
                try:
                    if tunnel is not None:
                        tunnel.stop()
                except Exception:
                    pass
        else:
            _test_nfa_connection(cfg)
        elapsed_ms = int((datetime.now() - started).total_seconds() * 1000)
        return {"ok": True, "message": "connection ok", "elapsed_ms": elapsed_ms}
    except Exception as e:
        elapsed_ms = int((datetime.now() - started).total_seconds() * 1000)
        return {"ok": False, "message": str(e), "elapsed_ms": elapsed_ms}


# Tasks CRUD
@app.post("/api/tasks", response_model=TaskOut, dependencies=[Depends(api_key_auth)])
async def create_task(payload: TaskCreate):
    with session_scope() as s:
        if s.query(Task).filter(Task.name == payload.name).first():
            raise HTTPException(status_code=400, detail={"code": "TASK_NAME_DUPLICATE", "message": "任务名称已存在，请更换后重试"})
        t = Task(
            name=payload.name,
            group_name=_normalize_group_name(payload.group_name),
            active=payload.active,
            kind=payload.kind,
            data_source_type=payload.data_source_type or "nfa",
            data_source_instance=payload.data_source_instance or "default",
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
        _ensure_task_group_exists(s, t.group_name)
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
            group_name=t.group_name,
            active=t.active,
            kind=t.kind,
            data_source_type=t.data_source_type or "nfa",
            data_source_instance=t.data_source_instance or "default",
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
            latest_budget_summary=(json.loads(t.latest_budget_summary) if t.latest_budget_summary else None),
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
                group_name=t.group_name,
                active=t.active,
                kind=t.kind,
                data_source_type=t.data_source_type or "nfa",
                data_source_instance=t.data_source_instance or "default",
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
                latest_budget_summary=(json.loads(t.latest_budget_summary) if t.latest_budget_summary else None),
            ))
        return out


@app.get("/api/tasks/groups", dependencies=[Depends(api_key_auth)])
async def list_task_groups():
    with session_scope() as s:
        _backfill_task_groups_from_tasks(s)
        rows = s.query(TaskGroup).order_by(TaskGroup.name.asc()).all()
        items = [str(r.name) for r in rows if r and r.name]
        return {"items": items}


@app.post("/api/tasks/groups", dependencies=[Depends(api_key_auth)])
async def create_task_group(payload: TaskGroupCreate):
    name = _normalize_group_name(payload.name)
    if not name:
        raise HTTPException(status_code=400, detail="group name is required")
    with session_scope() as s:
        dup = s.query(TaskGroup).filter(TaskGroup.name == name).first()
        if dup:
            raise HTTPException(status_code=409, detail={"code": "TASK_GROUP_DUPLICATE", "message": "分组名称已存在"})
        s.add(TaskGroup(name=name))
    return {"ok": True, "name": name}


@app.patch("/api/tasks/groups/rename", dependencies=[Depends(api_key_auth)])
async def rename_task_group(payload: TaskGroupRename):
    old_name = _normalize_group_name(payload.old_name)
    new_name = _normalize_group_name(payload.new_name)
    if not old_name or not new_name:
        raise HTTPException(status_code=400, detail="old_name and new_name are required")
    if old_name == new_name:
        return {"ok": True, "name": new_name, "updated_tasks": 0, "merged": False}
    with session_scope() as s:
        _backfill_task_groups_from_tasks(s)
        old_group = s.query(TaskGroup).filter(TaskGroup.name == old_name).first()
        old_count = s.query(Task).filter(Task.group_name == old_name).count()
        if not old_group and old_count == 0:
            raise HTTPException(status_code=404, detail="group not found")
        target_group = s.query(TaskGroup).filter(TaskGroup.name == new_name).first()
        merged = False
        if target_group and not payload.merge:
            raise HTTPException(status_code=409, detail={"code": "TASK_GROUP_DUPLICATE", "message": "目标分组已存在"})
        if not target_group:
            s.add(TaskGroup(name=new_name))
        else:
            merged = True
        updated = s.query(Task).filter(Task.group_name == old_name).update({"group_name": new_name}, synchronize_session=False)
        if old_group:
            s.delete(old_group)
    return {"ok": True, "name": new_name, "updated_tasks": int(updated or 0), "merged": merged}


@app.delete("/api/tasks/groups", dependencies=[Depends(api_key_auth)])
async def delete_task_group(name: str = Query(...)):
    group_name = _normalize_group_name(name)
    if not group_name:
        raise HTTPException(status_code=400, detail="name is required")
    with session_scope() as s:
        _backfill_task_groups_from_tasks(s)
        row = s.query(TaskGroup).filter(TaskGroup.name == group_name).first()
        moved = s.query(Task).filter(Task.group_name == group_name).update({"group_name": None}, synchronize_session=False)
        if row:
            s.delete(row)
        if not row and int(moved or 0) == 0:
            raise HTTPException(status_code=404, detail="group not found")
    return {"ok": True, "moved_tasks": int(moved or 0)}


@app.get("/api/jobs/page", response_model=JobRunPageOut, dependencies=[Depends(api_key_auth)])
async def list_jobs_page(
    task_id: Optional[int] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=200),
    status: Optional[str] = Query(default=None),
    month: Optional[str] = Query(default=None),  # YYYY-MM
    task_kind: Optional[str] = Query(default="all"),  # all|periodic|one_off
    sort_by: Optional[str] = Query(default="started_at"),
    sort_order: Optional[str] = Query(default="desc")
):
    with session_scope() as s:
        base = s.query(JobRun).outerjoin(Task, JobRun.task_id == Task.id)
        if task_id is not None:
            base = base.filter(JobRun.task_id == task_id)
        if status:
            try:
                base = base.filter(JobRun.status == status)
            except Exception:
                pass
        tk = (task_kind or "all").lower()
        if tk in {"periodic", "one_off"}:
            if tk == "one_off":
                base = base.filter(or_(Task.kind == "one_off", JobRun.task_id.is_(None)))
            else:
                base = base.filter(Task.kind == "periodic")
        if month:
            try:
                m = datetime.strptime(month, "%Y-%m")
                if m.month == 12:
                    m2 = datetime(m.year + 1, 1, 1)
                else:
                    m2 = datetime(m.year, m.month + 1, 1)
                ts_col = func.coalesce(JobRun.finished_at, JobRun.started_at)
                base = base.filter(and_(ts_col >= m, ts_col < m2))
            except Exception:
                pass
        total = base.count()
        # sorting
        sort_field = (sort_by or "started_at").lower()
        order = (sort_order or "desc").lower()
        if sort_field == "finished_at":
            primary = JobRun.finished_at
        elif sort_field == "id":
            primary = JobRun.id
        elif sort_field == "status":
            primary = JobRun.status
        else:
            primary = JobRun.started_at
        if order == "asc":
            order_clause = primary.asc()
        else:
            order_clause = primary.desc()
        # keep IS NULL ordering first for started_at to be consistent
        if sort_field == "started_at":
            rows = base.order_by((JobRun.started_at.is_(None)).asc(), order_clause).offset((page - 1) * page_size).limit(page_size).all()
        else:
            rows = base.order_by(order_clause).offset((page - 1) * page_size).limit(page_size).all()
        task_ids = [r.task_id for r in rows if r.task_id is not None]
        task_kind_map: dict[int, str] = {}
        if task_ids:
            trows = s.query(Task).filter(Task.id.in_(task_ids)).all()
            task_kind_map = {t.id: (t.kind or "one_off") for t in trows}
        items: list[JobRunOut] = []
        for r in rows:
            artifacts = []
            try:
                artifacts = json.loads(r.artifacts) if r.artifacts else []
            except Exception:
                artifacts = []
            items.append(JobRunOut(
                id=r.id,
                task_id=r.task_id,
                task_kind=("one_off" if not r.task_id else task_kind_map.get(r.task_id)),
                status=r.status,
                progress_pct=(r.progress_pct or 0),
                progress_stage=r.progress_stage,
                progress_events=(json.loads(r.progress_events) if r.progress_events else []),
                started_at=r.started_at,
                finished_at=r.finished_at,
                resolved_window=json.loads(r.resolved_window) if r.resolved_window else None,
                resolved_params=json.loads(r.resolved_params) if r.resolved_params else None,
                artifacts=artifacts,
                log_path=r.log_path,
                error_message=r.error_message,
            ))
        return JobRunPageOut(items=items, total=total, page=page, page_size=page_size)

@app.get("/api/tasks/page", response_model=TaskPageOut, dependencies=[Depends(api_key_auth)])
async def list_tasks_page(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=200),
    q: Optional[str] = Query(default=None),
    task_kind: Optional[str] = Query(default="all"),  # all|periodic|one_off
    task_group: Optional[str] = Query(default=None),
    sort_by: Optional[str] = Query(default="id"),
    sort_order: Optional[str] = Query(default="desc")
):
    with session_scope() as s:
        query = s.query(Task)
        # filter by name contains
        if q:
            try:
                query = query.filter(Task.name.like(f"%{q}%"))
            except Exception:
                pass
        tk = (task_kind or "all").lower()
        if tk in {"periodic", "one_off"}:
            query = query.filter(Task.kind == tk)
        if task_group:
            tg = str(task_group).strip()
            if tg:
                query = query.filter(Task.group_name == tg)
        total = query.count()
        # sorting
        sort_field = (sort_by or "id").lower()
        order = (sort_order or "desc").lower()
        order_clause = Task.id.desc()
        if sort_field == "id":
            order_clause = Task.id.desc() if order != "asc" else Task.id.asc()
        elif sort_field == "name":
            order_clause = Task.name.desc() if order != "asc" else Task.name.asc()
        elif sort_field == "created_at":
            order_clause = Task.created_at.desc() if order != "asc" else Task.created_at.asc()
        elif sort_field == "updated_at":
            order_clause = Task.updated_at.desc() if order != "asc" else Task.updated_at.asc()
        rows = query.order_by(order_clause).offset((page - 1) * page_size).limit(page_size).all()
        items: list[TaskOut] = []
        for t in rows:
            items.append(TaskOut(
                id=t.id,
                name=t.name,
                group_name=t.group_name,
                active=t.active,
                kind=t.kind,
                data_source_type=t.data_source_type or "nfa",
                data_source_instance=t.data_source_instance or "default",
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
                latest_budget_summary=(json.loads(t.latest_budget_summary) if t.latest_budget_summary else None),
            ))
        return TaskPageOut(items=items, total=total, page=page, page_size=page_size)


@app.get("/api/tasks/{task_id}", response_model=TaskOut, dependencies=[Depends(api_key_auth)])
async def get_task(task_id: int):
    with session_scope() as s:
        t = s.get(Task, task_id)
        if not t:
            raise HTTPException(status_code=404, detail="Task not found")
        return TaskOut(
            id=t.id,
            name=t.name,
            group_name=t.group_name,
            active=t.active,
            kind=t.kind,
            data_source_type=t.data_source_type or "nfa",
            data_source_instance=t.data_source_instance or "default",
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
            latest_budget_summary=(json.loads(t.latest_budget_summary) if t.latest_budget_summary else None),
        )


@app.put("/api/tasks/{task_id}", response_model=TaskOut, dependencies=[Depends(api_key_auth)])
async def update_task(task_id: int, payload: TaskUpdate):
    with session_scope() as s:
        t = s.get(Task, task_id)
        if not t:
            raise HTTPException(status_code=404, detail="Task not found")
        data = payload.model_dump(exclude_unset=True)
        if "name" in data and data["name"]:
            dup = s.query(Task).filter(Task.name == data["name"], Task.id != task_id).first()
            if dup:
                raise HTTPException(status_code=400, detail={"code": "TASK_NAME_DUPLICATE", "message": "任务名称已存在，请更换后重试"})
        if "window_params" in data and data["window_params"] is not None:
            data["window_params"] = json.dumps(data["window_params"], ensure_ascii=False)
        if "params" in data and data["params"] is not None:
            data["params"] = json.dumps(data["params"], ensure_ascii=False)
        if "export_formats" in data and data["export_formats"] is not None:
            data["export_formats"] = json.dumps(data["export_formats"], ensure_ascii=False)
        if "group_name" in data:
            data["group_name"] = _normalize_group_name(data["group_name"])
            _ensure_task_group_exists(s, data["group_name"])
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
            group_name=t.group_name,
            active=t.active,
            kind=t.kind,
            data_source_type=t.data_source_type or "nfa",
            data_source_instance=t.data_source_instance or "default",
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
            latest_budget_summary=(json.loads(t.latest_budget_summary) if t.latest_budget_summary else None),
        )


@app.patch("/api/tasks/{task_id}/group", dependencies=[Depends(api_key_auth)])
async def patch_task_group(task_id: int, payload: TaskGroupAssign):
    with session_scope() as s:
        t = s.get(Task, task_id)
        if not t:
            raise HTTPException(status_code=404, detail="Task not found")
        group_name = _normalize_group_name(payload.group_name)
        _ensure_task_group_exists(s, group_name)
        t.group_name = group_name
        s.add(t)
    return {"ok": True, "task_id": task_id, "group_name": group_name}


@app.delete("/api/tasks/{task_id}", dependencies=[Depends(api_key_auth)])
async def delete_task(task_id: int):
    with session_scope() as s:
        t = s.get(Task, task_id)
        if not t:
            raise HTTPException(status_code=404, detail="Task not found")
        s.delete(t)
    # Remove scheduled job if exists
    try:
        if scheduler_service.scheduler:
            scheduler_service.scheduler.remove_job(f"task-{task_id}")
    except Exception:
        pass
    return {"ok": True}


@app.post("/api/tasks/batch-delete", dependencies=[Depends(api_key_auth)])
async def batch_delete_tasks(payload: TaskBatchDelete):
    ids = [int(x) for x in (payload.ids or []) if str(x).isdigit()]
    if not ids:
        return {"ok": True, "deleted": 0}
    deleted = 0
    with session_scope() as s:
        rows = s.query(Task).filter(Task.id.in_(ids)).all()
        for t in rows:
            s.delete(t)
            deleted += 1
    try:
        if scheduler:
            for tid in ids:
                try:
                    scheduler.remove_job(f"task-{tid}")
                except Exception:
                    pass
    except Exception:
        pass
    return {"ok": True, "deleted": deleted}


@app.post("/api/tasks/{task_id}/run", dependencies=[Depends(api_key_auth)])
async def trigger_task_run(task_id: int):
    try:
        job_id = create_job_run_from_task(task_id)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
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
        task_ids = [r.task_id for r in rows if r.task_id is not None]
        task_kind_map: dict[int, str] = {}
        if task_ids:
            trows = s.query(Task).filter(Task.id.in_(task_ids)).all()
            task_kind_map = {t.id: (t.kind or "one_off") for t in trows}
        for r in rows:
            artifacts = []
            try:
                artifacts = json.loads(r.artifacts) if r.artifacts else []
            except Exception:
                artifacts = []
            row = JobRunOut(
                id=r.id,
                task_id=r.task_id,
                task_kind=("one_off" if not r.task_id else task_kind_map.get(r.task_id)),
                status=r.status,
                progress_pct=(r.progress_pct or 0),
                progress_stage=r.progress_stage,
                progress_events=(json.loads(r.progress_events) if r.progress_events else []),
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
        tk = "one_off"
        if r.task_id:
            t = s.get(Task, r.task_id)
            tk = (t.kind if t else None)
        artifacts = []
        try:
            artifacts = json.loads(r.artifacts) if r.artifacts else []
        except Exception:
            artifacts = []
        return JobRunOut(
            id=r.id,
            task_id=r.task_id,
            task_kind=tk,
            status=r.status,
            progress_pct=(r.progress_pct or 0),
            progress_stage=r.progress_stage,
            progress_events=(json.loads(r.progress_events) if r.progress_events else []),
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
        if scheduler_service.scheduler:
            scheduler_service.scheduler.remove_job(job_id)
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


@app.post("/api/jobs/batch-delete", dependencies=[Depends(api_key_auth)])
async def batch_delete_jobs(payload: JobBatchDelete):
    ids = [str(x).strip() for x in (payload.ids or []) if str(x).strip()]
    if not ids:
        return {"ok": True, "deleted": 0}
    deleted = 0
    for jid in ids:
        try:
            if scheduler_service.scheduler:
                try:
                    scheduler_service.scheduler.remove_job(jid)
                except Exception:
                    pass
            d = get_job_dir(jid)
            if d.exists():
                shutil.rmtree(d, ignore_errors=True)
            log_path = get_job_log_path(jid)
            if log_path.exists():
                log_path.unlink(missing_ok=True)
            with session_scope() as s:
                r = s.get(JobRun, jid)
                if r:
                    s.delete(r)
                    deleted += 1
        except Exception:
            pass
    return {"ok": True, "deleted": deleted}


def _resolve_batch_download_files(payload: JobBatchDownload) -> tuple[list[tuple[str, str, Path]], str, str]:
    fmt = (payload.file_format or "csv").lower()
    if fmt not in {"all", "csv", "xlsx"}:
        fmt = "csv"
    status = (payload.status or "succeeded").lower()
    task_kind = (payload.task_kind or "all").lower()

    rows: list[JobRun] = []
    with session_scope() as s:
        q = s.query(JobRun).outerjoin(Task, JobRun.task_id == Task.id)
        if payload.run_ids:
            ids = [str(x).strip() for x in payload.run_ids if str(x).strip()]
            q = q.filter(JobRun.id.in_(ids))
        else:
            if status != "all":
                q = q.filter(JobRun.status == status)
            if task_kind in {"periodic", "one_off"}:
                if task_kind == "one_off":
                    q = q.filter(or_(Task.kind == "one_off", JobRun.task_id.is_(None)))
                else:
                    q = q.filter(Task.kind == "periodic")
            if payload.month:
                try:
                    m = datetime.strptime(payload.month, "%Y-%m")
                    if m.month == 12:
                        m2 = datetime(m.year + 1, 1, 1)
                    else:
                        m2 = datetime(m.year, m.month + 1, 1)
                    ts_col = func.coalesce(JobRun.finished_at, JobRun.started_at)
                    q = q.filter(and_(ts_col >= m, ts_col < m2))
                except Exception:
                    pass
        rows = q.order_by(JobRun.started_at.desc()).all()

    files: list[tuple[str, str, Path]] = []
    for r in rows:
        if r.status != "succeeded":
            continue
        artifacts = []
        try:
            artifacts = json.loads(r.artifacts) if r.artifacts else []
        except Exception:
            artifacts = []
        for a in artifacts:
            fn = str(a.get("filename") or "").strip()
            if not fn:
                continue
            suffix = Path(fn).suffix.lower().lstrip(".")
            if fmt != "all" and suffix != fmt:
                continue
            p = get_job_dir(r.id) / fn
            if p.exists() and p.is_file():
                files.append((str(r.id), fn, p))
    return files, task_kind, fmt


@app.post("/api/jobs/batch-download/preview", dependencies=[Depends(api_key_auth)])
async def batch_download_preview(payload: JobBatchDownload):
    files, _, _ = _resolve_batch_download_files(payload)
    run_ids = {run_id for run_id, _, _ in files}
    return {"matched_runs": len(run_ids), "matched_files": len(files)}


@app.post("/api/jobs/batch-download", dependencies=[Depends(api_key_auth)])
async def batch_download_jobs(payload: JobBatchDownload):
    files, task_kind, fmt = _resolve_batch_download_files(payload)

    if not files:
        raise HTTPException(status_code=404, detail="No artifacts matched current filters")

    storage_dir = Path(get_settings().STORAGE_DIR)
    dl_dir = storage_dir / "downloads"
    dl_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_name = f"artifacts_{payload.month or 'all'}_{task_kind}_{fmt}_{ts}.zip"
    zip_path = dl_dir / zip_name
    used_names: set[str] = set()
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for run_id, filename, p in files:
            arc = filename
            if arc in used_names:
                short_run_id = str(run_id)[:8]
                arc = f"{short_run_id}_{filename}"
                if arc in used_names:
                    stem = Path(filename).stem
                    suffix = Path(filename).suffix
                    idx = 2
                    while True:
                        candidate = f"{short_run_id}_{stem}_{idx}{suffix}"
                        if candidate not in used_names:
                            arc = candidate
                            break
                        idx += 1
            used_names.add(arc)
            zf.write(p, arcname=arc)
    return FileResponse(path=str(zip_path), filename=zip_name, media_type="application/zip")

