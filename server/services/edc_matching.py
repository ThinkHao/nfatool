from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from fnmatch import fnmatchcase
from typing import Any

from ..config import get_data_source_instances
from ..db import session_scope
from ..models import EdcMatchSnapshot, EdcMatchSnapshotItem, JobRun, Task


EDC_MATCH_MAX_OBJECTS = 5000


def parse_edc_names(value: str | None) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in re.split(r"[,\n]", str(value or "")):
        token = raw.strip()
        if token and token not in seen:
            seen.add(token)
            out.append(token)
    return out


def safe_identifier(name: str, field_name: str) -> str:
    if not isinstance(name, str) or not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
        raise ValueError(f"invalid SQL identifier for {field_name}")
    return name


def effective_pattern(token: str, mode: str) -> tuple[str, str]:
    if "*" in token or "?" in token:
        return token.replace("*", "%").replace("?", "_"), "LIKE"
    if str(mode or "prefix").lower() == "exact":
        return token, "="
    return f"{token}%", "LIKE"


def build_name_predicate(name_col: str, expression: str, mode: str) -> tuple[str, list[str]]:
    tokens = parse_edc_names(expression)
    if not tokens:
        raise ValueError("edc_name is required in params when data_source_type=edc")
    eq: list[str] = []
    likes: list[tuple[str, str]] = []
    for token in tokens:
        pattern, op = effective_pattern(token, mode)
        if op == "=":
            eq.append(pattern)
        else:
            likes.append((f"{name_col} LIKE %s", pattern))
    parts: list[str] = []
    args: list[str] = []
    if len(eq) == 1:
        parts.append(f"{name_col} = %s")
        args.extend(eq)
    elif eq:
        parts.append(f"{name_col} IN ({', '.join(['%s'] * len(eq))})")
        args.extend(eq)
    parts.extend(x[0] for x in likes)
    args.extend(x[1] for x in likes)
    return parts[0] if len(parts) == 1 else "(" + " OR ".join(parts) + ")", args


def _match_rule(name: str, tokens: list[str], mode: str) -> tuple[str | None, str | None, str | None]:
    for token in tokens:
        if "*" in token or "?" in token:
            if fnmatchcase(name, token):
                pattern, op = effective_pattern(token, mode)
                return token, op, pattern
        elif str(mode or "prefix").lower() == "exact":
            if name == token:
                return token, "=", token
        elif name.startswith(token):
            return token, "LIKE", f"{token}%"
    return None, None, None


def resolve_edc_match(conn: Any, cfg: dict[str, Any], expression: str, mode: str,
                      start_time: str, end_time: str, exclude_like: str | None) -> list[dict[str, Any]]:
    table = safe_identifier(str(cfg.get("table", "edc_data")), "table")
    time_col = safe_identifier(str(cfg.get("time_column", "create_time")), "time_column")
    name_col = safe_identifier(str(cfg.get("name_column", "edc_name")), "name_column")
    fragment, args = build_name_predicate(name_col, expression, mode)
    where = [fragment, f"{time_col} >= %s", f"{time_col} <= %s", f"{name_col} IS NOT NULL"]
    sql_args: list[Any] = [*args, start_time, end_time]
    if exclude_like:
        where.append(f"{name_col} NOT LIKE %s")
        sql_args.append(exclude_like)
    sql = f"SELECT DISTINCT {name_col} AS edc_name FROM {table} WHERE {' AND '.join(where)} ORDER BY {name_col}"
    with conn.cursor() as cursor:
        cursor.execute(sql, tuple(sql_args))
        rows = list(cursor.fetchall() or [])
    tokens = parse_edc_names(expression)
    out: list[dict[str, Any]] = []
    for row in rows:
        name = str(row.get("edc_name") or "").strip()
        if not name:
            continue
        matched_by, op, pattern = _match_rule(name, tokens, mode)
        out.append({"edc_name": name, "matched_by": matched_by, "match_operator": op, "effective_pattern": pattern})
    if len(out) > EDC_MATCH_MAX_OBJECTS:
        raise ValueError(f"EDC 实际匹配对象过多：{len(out)} > {EDC_MATCH_MAX_OBJECTS}，请缩小匹配条件")
    return out


def _fingerprint(cfg: dict[str, Any]) -> str:
    safe = {k: v for k, v in cfg.items() if k not in {"password", "ssh_password", "ssh_pkey", "ssh_pkey_password"}}
    return hashlib.sha256(json.dumps(safe, ensure_ascii=False, sort_keys=True, default=str).encode()).hexdigest()


def snapshot_to_dict(snapshot: EdcMatchSnapshot | None) -> dict[str, Any] | None:
    if not snapshot:
        return None
    return {
        "id": snapshot.id,
        "job_run_id": snapshot.job_run_id,
        "task_id": snapshot.task_id,
        "task_name": snapshot.task_name_snapshot,
        "data_source_instance": snapshot.data_source_instance,
        "window_start": snapshot.window_start,
        "window_end": snapshot.window_end,
        "edc_name_expression": snapshot.edc_name_expression,
        "match_mode": snapshot.match_mode,
        "exclude_like": snapshot.exclude_like,
        "matched_count": snapshot.matched_count,
        "match_hash": snapshot.match_hash,
        "status": snapshot.status,
        "error_message": snapshot.error_message,
        "resolved_at": snapshot.resolved_at,
        "items": [
            {"edc_name": x.edc_name, "matched_by": x.matched_by, "match_operator": x.match_operator, "effective_pattern": x.effective_pattern}
            for x in (snapshot.items or [])
        ],
    }


def get_snapshot(job_id: str) -> dict[str, Any] | None:
    with session_scope() as session:
        row = session.query(EdcMatchSnapshot).filter(EdcMatchSnapshot.job_run_id == job_id).first()
        return snapshot_to_dict(row)


def get_snapshot_names(job_id: str) -> list[str] | None:
    snapshot = get_snapshot(job_id)
    if snapshot is None:
        return None
    return [str(x["edc_name"]) for x in snapshot.get("items", [])]


def save_snapshot(job_id: str, resolved_window: dict[str, Any], params: dict[str, Any], items: list[dict[str, Any]],
                  cfg: dict[str, Any], task_id: int | None = None, task_name: str | None = None) -> dict[str, Any]:
    names = sorted({str(x.get("edc_name") or "") for x in items if str(x.get("edc_name") or "")})
    match_hash = hashlib.sha256("\n".join(names).encode("utf-8")).hexdigest()
    expression = str(params.get("edc_name") or "").strip()
    mode = str(params.get("edc_match_mode") or cfg.get("wildcard_mode") or "prefix")
    exclude_like = params.get("edc_exclude_like", cfg.get("exclude_like", "%-backup"))
    with session_scope() as session:
        existing = session.query(EdcMatchSnapshot).filter(EdcMatchSnapshot.job_run_id == job_id).first()
        if existing:
            return snapshot_to_dict(existing) or {}
        row = EdcMatchSnapshot(
            job_run_id=job_id, task_id=task_id, task_name_snapshot=task_name,
            data_source_instance=str(params.get("data_source_instance") or "default"),
            window_start=str(resolved_window.get("start_time") or ""), window_end=str(resolved_window.get("end_time") or ""),
            edc_name_expression=expression, match_mode=mode, exclude_like=exclude_like,
            matched_count=len(names), match_hash=match_hash, status="resolved", source_config_fingerprint=_fingerprint(cfg),
        )
        row.items = [EdcMatchSnapshotItem(snapshot=row, edc_name=name,
                                          matched_by=next((x.get("matched_by") for x in items if x.get("edc_name") == name), None),
                                          match_operator=next((x.get("match_operator") for x in items if x.get("edc_name") == name), None),
                                          effective_pattern=next((x.get("effective_pattern") for x in items if x.get("edc_name") == name), None)) for name in names]
        session.add(row)
        session.flush()
        return snapshot_to_dict(row) or {}


def source_config(instance: str, params: dict[str, Any]) -> dict[str, Any] | None:
    direct = params.get("db_config")
    if isinstance(direct, dict):
        return direct
    instances = get_data_source_instances("edc")
    if instance in instances:
        return instances[instance]
    if instance == "default" and len(instances) == 1:
        return next(iter(instances.values()))
    return None
