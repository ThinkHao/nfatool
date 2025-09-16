from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from ..config import get_settings
from ..db import session_scope
from ..models import JobRun, Task
from .logger import create_job_logger, get_job_log_path
from .storage import get_job_dir, list_artifacts
from .time_windows import resolve_time_window
from .compute95 import compute_and_export


scheduler: Optional[AsyncIOScheduler] = None
_semaphore: Optional[asyncio.Semaphore] = None


def _ensure_scheduler():
    global scheduler
    if scheduler is None:
        scheduler = AsyncIOScheduler()
        scheduler.start()


def _ensure_semaphore():
    global _semaphore
    if _semaphore is None:
        _semaphore = asyncio.Semaphore(get_settings().CONCURRENCY_LIMIT)


async def _run_compute_placeholder(job_id: str, resolved_params: dict) -> list[dict]:
    """Placeholder compute: writes a small CSV artifact to demonstrate pipeline.
    Replace with actual compute95 integration in later steps.
    """
    await asyncio.sleep(0.1)
    d = get_job_dir(job_id)
    csv_path = d / "result_placeholder.csv"
    csv_path.write_text("id,name,value\n1,placeholder,95.0\n", encoding="utf-8")
    return [{"filename": csv_path.name, "size": csv_path.stat().st_size, "path": str(csv_path)}]


async def _execute_job(job_id: str):
    settings = get_settings()
    _ensure_semaphore()
    logger = create_job_logger(job_id)
    with session_scope() as s:
        run: JobRun = s.get(JobRun, job_id)
        if not run:
            return
        run.status = "running"
        run.started_at = datetime.utcnow()
        s.add(run)
    async with _semaphore:  # concurrency control
        try:
            logger.info("Job started: %s", job_id)
            # Load snapshots
            with session_scope() as s:
                run: JobRun = s.get(JobRun, job_id)
                params = json.loads(run.resolved_params or '{}')
                window = json.loads(run.resolved_window or '{}')
                export_formats = None
                output_filename_template = None
                if run.task_id:
                    t: Task = s.get(Task, run.task_id)
                    try:
                        export_formats = json.loads(t.export_formats) if t.export_formats else None
                    except Exception:
                        export_formats = None
                    output_filename_template = t.output_filename_template
                else:
                    # ad-hoc: allow export options via resolved_params
                    export_formats = params.get('export_formats') or export_formats
                    output_filename_template = params.get('output_filename_template') or output_filename_template

            # Run real compute and export in thread to avoid blocking event loop
            loop = asyncio.get_running_loop()
            artifacts = await loop.run_in_executor(None, lambda: compute_and_export(job_id, window, params, export_formats, output_filename_template))
            # Update DB
            with session_scope() as s:
                run: JobRun = s.get(JobRun, job_id)
                run.status = "succeeded"
                run.finished_at = datetime.utcnow()
                run.artifacts = json.dumps(artifacts, ensure_ascii=False)
                run.log_path = str(get_job_log_path(job_id))
                s.add(run)
            logger.info("Job succeeded: %s", job_id)
        except Exception as e:  # noqa
            with session_scope() as s:
                run: JobRun = s.get(JobRun, job_id)
                run.status = "failed"
                run.finished_at = datetime.utcnow()
                run.error_message = str(e)
                run.log_path = str(get_job_log_path(job_id))
                s.add(run)
            logger.exception("Job failed: %s", job_id)


def create_job_run_from_task(task_id: int) -> str:
    """Create a job run row for the task, resolve window and params snapshot, then schedule execution."""
    settings = get_settings()
    with session_scope() as s:
        task: Task = s.get(Task, task_id)
        if not task:
            raise ValueError("Task not found")
        job_id = str(uuid.uuid4())
        # resolve window
        import json as _json
        tz = task.timezone or settings.TIMEZONE
        window_params = _json.loads(task.window_params) if task.window_params else None
        start, end, label = resolve_time_window(task.window_selector, window_params, tz)
        params = _json.loads(task.params or '{}')
        # snapshots
        resolved_window = {"start_time": start, "end_time": end, "label": label}
        resolved_params = dict(params)
        run = JobRun(
            id=job_id,
            task_id=task.id,
            status="pending",
            resolved_window=_json.dumps(resolved_window, ensure_ascii=False),
            resolved_params=_json.dumps(resolved_params, ensure_ascii=False),
        )
        s.add(run)
    _ensure_scheduler()
    # Schedule coroutine directly; AsyncIOScheduler will run it on its event loop
    scheduler.add_job(_execute_job, args=[job_id], id=job_id, replace_existing=True)
    return job_id


def create_ad_hoc_job_run(payload: dict, default_tz: str) -> str:
    job_id = str(uuid.uuid4())
    import json as _json
    # window
    selector = payload.get("window_selector") or "custom"
    wparams = payload.get("window_params") or {}
    start, end, label = resolve_time_window(selector, wparams, default_tz)
    resolved_window = {"start_time": start, "end_time": end, "label": label}
    resolved_params = payload.get("params") or {}
    # allow export options in ad-hoc
    if payload.get("export_formats"):
        resolved_params["export_formats"] = payload.get("export_formats")
    if payload.get("output_filename_template"):
        resolved_params["output_filename_template"] = payload.get("output_filename_template")
    with session_scope() as s:
        run = JobRun(
            id=job_id,
            task_id=None,
            status="pending",
            resolved_window=_json.dumps(resolved_window, ensure_ascii=False),
            resolved_params=_json.dumps(resolved_params, ensure_ascii=False),
        )
        s.add(run)
    _ensure_scheduler()
    # Schedule coroutine directly; AsyncIOScheduler will run it on its event loop
    scheduler.add_job(_execute_job, args=[job_id], id=job_id, replace_existing=True)
    return job_id


def load_tasks_into_scheduler():
    settings = get_settings()
    _ensure_scheduler()
    with session_scope() as s:
        tasks = s.query(Task).filter(Task.active == True, Task.kind == 'periodic').all()  # noqa: E712
        for t in tasks:
            # Register triggers based on schedule
            job_id = f"task-{t.id}"
            try:
                if t.schedule_type == "cron" and t.schedule_expr:
                    scheduler.add_job(lambda tid=t.id: create_job_run_from_task(tid),
                                      trigger=CronTrigger.from_crontab(t.schedule_expr), id=job_id, replace_existing=True)
                elif t.schedule_type == "interval" and t.schedule_expr:
                    seconds = int(t.schedule_expr)
                    scheduler.add_job(lambda tid=t.id: create_job_run_from_task(tid),
                                      trigger=IntervalTrigger(seconds=seconds), id=job_id, replace_existing=True)
                elif t.schedule_type == "weekly_preset" and t.schedule_time_of_day:
                    hh, mm, ss = (t.schedule_time_of_day.split(":") + ["0", "0"])[:3]
                    scheduler.add_job(lambda tid=t.id: create_job_run_from_task(tid),
                                      trigger=CronTrigger(day_of_week="mon", hour=int(hh), minute=int(mm), second=int(ss)),
                                      id=job_id, replace_existing=True)
            except Exception:
                # Ignore bad schedules for now
                pass


def schedule_retention_cleanup():
    settings = get_settings()
    _ensure_scheduler()

    def _cleanup():
        cutoff = datetime.utcnow() - timedelta(days=settings.RETENTION_DAYS)
        from ..models import JobRun
        import shutil
        from ..config import get_settings as _gs
        from .storage import get_job_dir
        with session_scope() as s:
            old_runs = s.query(JobRun).filter(JobRun.finished_at != None, JobRun.finished_at < cutoff).all()  # noqa: E711
            for r in old_runs:
                # remove files
                d = get_job_dir(r.id)
                if d.exists():
                    shutil.rmtree(d, ignore_errors=True)
                # remove row
                s.delete(r)

    scheduler.add_job(_cleanup, trigger=CronTrigger(hour=3, minute=30), id="retention-cleanup", replace_existing=True)


def apply_schedule_for_task_snapshot(task_id: int, active: bool, kind: str | None,
                                     schedule_type: str | None, schedule_expr: str | None,
                                     schedule_time_of_day: str | None) -> None:
    """(Re)register or remove a single task's schedule based on provided snapshot fields."""
    _ensure_scheduler()
    job_id = f"task-{task_id}"
    # Remove existing
    try:
        scheduler.remove_job(job_id)
    except Exception:
        pass
    # Add back if periodic and active with valid schedule
    if not active or kind != 'periodic':
        return
    try:
        if schedule_type == 'cron' and schedule_expr:
            scheduler.add_job(lambda tid=task_id: create_job_run_from_task(tid),
                              trigger=CronTrigger.from_crontab(schedule_expr), id=job_id, replace_existing=True)
        elif schedule_type == 'interval' and schedule_expr:
            seconds = int(schedule_expr)
            scheduler.add_job(lambda tid=task_id: create_job_run_from_task(tid),
                              trigger=IntervalTrigger(seconds=seconds), id=job_id, replace_existing=True)
        elif schedule_type == 'weekly_preset' and schedule_time_of_day:
            hh, mm, ss = (schedule_time_of_day.split(":") + ["0", "0"])[:3]
            scheduler.add_job(lambda tid=task_id: create_job_run_from_task(tid),
                              trigger=CronTrigger(day_of_week='mon', hour=int(hh), minute=int(mm), second=int(ss)),
                              id=job_id, replace_existing=True)
    except Exception:
        # ignore bad schedules
        pass
