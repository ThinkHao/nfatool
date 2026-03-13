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

from ..config import get_settings, auto_rotate_data_source_key
from ..db import session_scope
from ..models import JobRun, Task
from .logger import create_job_logger, get_job_log_path
from .storage import get_job_dir, list_artifacts
from .time_windows import resolve_time_window
from .compute95 import compute_and_export


scheduler: Optional[AsyncIOScheduler] = None
_semaphore: Optional[asyncio.Semaphore] = None


def _is_transient_error(err: Exception) -> bool:
    text = str(err or "").lower()
    markers = [
        "could not establish session to ssh gateway",
        "connection refused",
        "can't connect to mysql server",
        "lost connection",
        "timed out",
        "timeout",
        "temporarily unavailable",
        "connection reset",
        "too many connections",
    ]
    return any(m in text for m in markers)


def _append_progress_event(run: JobRun, pct: int, stage: str) -> None:
    now = datetime.utcnow().isoformat(timespec="seconds")
    try:
        events = json.loads(run.progress_events) if run.progress_events else []
    except Exception:
        events = []
    if not isinstance(events, list):
        events = []
    last = events[-1] if events else {}
    last_pct = int(last.get("pct", -1)) if isinstance(last, dict) else -1
    last_stage = str(last.get("stage", "")) if isinstance(last, dict) else ""
    # Limit event noise: append only on stage change or >=10% progress step.
    if stage != last_stage or abs(int(pct) - last_pct) >= 10:
        events.append({"time": now, "pct": int(pct), "stage": str(stage)})
        run.progress_events = json.dumps(events[-30:], ensure_ascii=False)

def _extract_terminal_reason(artifacts: list[dict] | None) -> Optional[str]:
    if not isinstance(artifacts, list):
        return None
    for item in artifacts:
        if not isinstance(item, dict):
            continue
        reason = item.get("terminal_reason")
        if reason:
            return str(reason)
    return None


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
        run.progress_pct = 1
        run.progress_stage = "任务已启动"
        _append_progress_event(run, 1, "任务已启动")
        run.started_at = datetime.utcnow()
        s.add(run)
    async with _semaphore:  # concurrency control
        try:
            logger.info("Job started: %s", job_id)
            with session_scope() as s:
                run: JobRun = s.get(JobRun, job_id)
                if run:
                    run.progress_pct = 5
                    run.progress_stage = "读取任务快照"
                    _append_progress_event(run, 5, "读取任务快照")
                    s.add(run)
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

            def _progress_cb(pct: int, stage: str):
                with session_scope() as s2:
                    rr: JobRun = s2.get(JobRun, job_id)
                    if not rr:
                        return
                    rr.progress_pct = max(1, min(99, int(pct)))
                    rr.progress_stage = str(stage)
                    _append_progress_event(rr, rr.progress_pct, rr.progress_stage)
                    s2.add(rr)

            retry_max = int(params.get("retry_max_attempts", 2))
            retry_delay = int(params.get("retry_delay_seconds", 8))
            if retry_max < 1:
                retry_max = 1
            if retry_delay < 0:
                retry_delay = 0
            # Run real compute and export in thread to avoid blocking event loop
            loop = asyncio.get_running_loop()
            artifacts = None
            last_err: Exception | None = None
            for attempt in range(1, retry_max + 1):
                try:
                    stage = f"执行计算 (attempt {attempt}/{retry_max})"
                    _progress_cb(10, stage)
                    artifacts = await loop.run_in_executor(
                        None,
                        lambda: compute_and_export(job_id, window, params, export_formats, output_filename_template, _progress_cb)
                    )
                    last_err = None
                    break
                except Exception as e:
                    last_err = e
                    if attempt >= retry_max or not _is_transient_error(e):
                        break
                    _progress_cb(10, f"临时错误，{retry_delay}s 后重试: {e}")
                    logger.warning("Job %s transient error on attempt %s/%s: %s", job_id, attempt, retry_max, e)
                    if retry_delay > 0:
                        await asyncio.sleep(retry_delay)
            if last_err is not None:
                raise last_err
            # Update DB
            terminal_reason = _extract_terminal_reason(artifacts)
            with session_scope() as s:
                run: JobRun = s.get(JobRun, job_id)
                run.status = "succeeded"
                run.progress_pct = 100
                run.progress_stage = "执行完成（无匹配数据）" if terminal_reason else "执行完成"
                _append_progress_event(run, 100, run.progress_stage)
                run.finished_at = datetime.utcnow()
                run.artifacts = json.dumps(artifacts, ensure_ascii=False)
                run.error_message = terminal_reason if terminal_reason else None
                run.log_path = str(get_job_log_path(job_id))
                s.add(run)
            # Sync optional budget summary to parent task for quick dashboard view.
            try:
                from ..models import Task as _Task
                summary_path = get_job_dir(job_id) / "budget_summary.json"
                if summary_path.exists():
                    summary_text = summary_path.read_text(encoding="utf-8")
                    with session_scope() as s:
                        run2: JobRun = s.get(JobRun, job_id)
                        if run2 and run2.task_id:
                            t: _Task = s.get(_Task, run2.task_id)
                            if t:
                                t.latest_budget_summary = summary_text
                                s.add(t)
            except Exception:
                pass
            logger.info("Job succeeded: %s", job_id)
        except asyncio.CancelledError:
            with session_scope() as s:
                run: JobRun = s.get(JobRun, job_id)
                if run:
                    run.status = "failed"
                    run.progress_pct = 100
                    run.progress_stage = "执行中止"
                    _append_progress_event(run, 100, "执行中止")
                    run.finished_at = datetime.utcnow()
                    run.error_message = "任务被取消：服务正在关闭"
                    run.log_path = str(get_job_log_path(job_id))
                    s.add(run)
            logger.warning("Job cancelled during shutdown: %s", job_id)
            return
        except Exception as e:  # noqa
            with session_scope() as s:
                run: JobRun = s.get(JobRun, job_id)
                run.status = "failed"
                run.progress_pct = 100
                run.progress_stage = "执行失败"
                _append_progress_event(run, 100, "执行失败")
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
        prevent_overlap = bool(params.get("prevent_overlap", True))
        if prevent_overlap:
            active_run = (
                s.query(JobRun)
                .filter(JobRun.task_id == task.id, JobRun.status.in_(["pending", "running"]))
                .order_by(JobRun.started_at.desc())
                .first()
            )
            if active_run:
                raise ValueError(f"Task {task.id} already has active run: {active_run.id}")
        # snapshots
        resolved_window = {"start_time": start, "end_time": end, "label": label}
        resolved_params = dict(params)
        resolved_params.setdefault("data_source_type", task.data_source_type or "nfa")
        resolved_params.setdefault("data_source_instance", task.data_source_instance or "default")
        run = JobRun(
            id=job_id,
            task_id=task.id,
            status="pending",
            progress_pct=0,
            progress_stage="等待执行",
            progress_events=json.dumps([{"time": datetime.utcnow().isoformat(timespec="seconds"), "pct": 0, "stage": "等待执行"}], ensure_ascii=False),
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
    if payload.get("data_source_type"):
        resolved_params["data_source_type"] = payload.get("data_source_type")
    if payload.get("data_source_instance"):
        resolved_params["data_source_instance"] = payload.get("data_source_instance")
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
            progress_pct=0,
            progress_stage="等待执行",
            progress_events=_json.dumps([{"time": datetime.utcnow().isoformat(timespec="seconds"), "pct": 0, "stage": "等待执行"}], ensure_ascii=False),
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


def schedule_config_key_rotation():
    _ensure_scheduler()

    def _rotate():
        try:
            auto_rotate_data_source_key(force=False)
        except Exception:
            pass

    # Check once daily; actual rotate decision is based on configured interval days.
    scheduler.add_job(_rotate, trigger=CronTrigger(hour=4, minute=10), id="config-key-rotation", replace_existing=True)


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



