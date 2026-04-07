from __future__ import annotations

import json
import re
import logging
from pathlib import Path as _Path
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List
import time

import pandas as pd
import pymysql
import paramiko

# Reuse existing script functions (vendored under server/ext)
from ..ext import calculate_95th_percentile as c95

from ..config import get_data_source_instances
from .exporter import export_csv, export_xlsx
from .storage import get_job_dir, safe_artifact_path
from .unit_conversion import mbps_to_raw, raw_to_mbps

logger = logging.getLogger(__name__)


def _as_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    s = str(v or "").strip().lower()
    return s in {"1", "true", "yes", "y", "on"}


def _connect_edc_db(cfg: Dict[str, Any]) -> tuple[Any, Any]:
    db_cfg = {
        "host": cfg.get("host"),
        "port": int(cfg.get("port", 3306)),
        "user": cfg.get("user"),
        "password": cfg.get("password"),
        "db": cfg.get("db"),
        "charset": cfg.get("charset", "utf8mb4"),
    }
    if not (db_cfg["host"] and db_cfg["user"] and db_cfg["password"] and db_cfg["db"]):
        raise ValueError("EDC instance config must include host/port/user/password/db")

    connect_timeout = int(cfg.get("connect_timeout", 10))
    read_timeout = int(cfg.get("read_timeout", 120))
    write_timeout = int(cfg.get("write_timeout", 120))

    ssh_enabled = _as_bool(cfg.get("ssh_enabled"))
    if not ssh_enabled:
        conn = pymysql.connect(
            host=db_cfg["host"],
            port=db_cfg["port"],
            user=db_cfg["user"],
            password=db_cfg["password"],
            db=db_cfg["db"],
            charset=db_cfg["charset"],
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            write_timeout=write_timeout,
            cursorclass=pymysql.cursors.DictCursor,
        )
        return conn, None

    try:
        from sshtunnel import SSHTunnelForwarder  # type: ignore
    except Exception as e:
        raise ValueError(
            "EDC instance enabled ssh tunnel, but package `sshtunnel` is not installed. "
            "Please install with: pip install sshtunnel"
        ) from e

    ssh_host = str(cfg.get("ssh_host") or "").strip()
    ssh_port = int(cfg.get("ssh_port", 22))
    ssh_user = str(cfg.get("ssh_user") or "").strip()
    if not (ssh_host and ssh_user):
        raise ValueError("EDC ssh config must include ssh_host/ssh_port/ssh_user when ssh_enabled=true")

    ssh_password = cfg.get("ssh_password")
    ssh_pkey = cfg.get("ssh_pkey")
    ssh_pkey_password = cfg.get("ssh_pkey_password")
    ssh_allow_agent = _as_bool(cfg.get("ssh_allow_agent", True))
    if isinstance(ssh_pkey, str):
        k = ssh_pkey.strip()
        # Common misconfiguration: putting "ssh-rsa AAAA..." public key text in ssh_pkey.
        if k.startswith(("ssh-rsa ", "ssh-ed25519 ", "ecdsa-sha2-")):
            raise ValueError(
                "ssh_pkey must be a PRIVATE key file path, not a public key string. "
                "Use ssh_pkey like C:/Users/<you>/.ssh/id_rsa, or remove ssh_pkey and rely on ssh_allow_agent=true."
            )
        if k and not _Path(k).exists():
            raise ValueError(f"ssh_pkey file not found: {k}")
    if not ssh_password and not ssh_pkey and not ssh_allow_agent:
        raise ValueError(
            "EDC ssh config must provide ssh_password/ssh_pkey, "
            "or set ssh_allow_agent=true when ssh_enabled=true"
        )

    remote_host = str(cfg.get("ssh_remote_host") or db_cfg["host"])
    remote_port = int(cfg.get("ssh_remote_port", db_cfg["port"]))
    local_host = str(cfg.get("ssh_local_host") or "127.0.0.1")
    local_port = int(cfg.get("ssh_local_port", 0))
    legacy_rsa = _as_bool(cfg.get("ssh_legacy_rsa", False))

    retries = int(cfg.get("ssh_connect_retries", 2))
    if retries < 1:
        retries = 1
    retry_delay_ms = int(cfg.get("ssh_retry_delay_ms", 700))
    if retry_delay_ms < 0:
        retry_delay_ms = 0

    forwarder = None
    last_error: Exception | None = None
    for i in range(retries):
        forwarder = SSHTunnelForwarder(
            (ssh_host, ssh_port),
            ssh_username=ssh_user,
            ssh_password=ssh_password,
            ssh_pkey=ssh_pkey,
            ssh_private_key_password=ssh_pkey_password,
            allow_agent=ssh_allow_agent,
            remote_bind_address=(remote_host, remote_port),
            local_bind_address=(local_host, local_port),
            set_keepalive=30.0,
        )
        try:
            if legacy_rsa:
                # For old SSH servers (e.g. OpenSSH_6.x), prefer legacy ssh-rsa
                # to avoid RSA SHA2 signature incompatibility.
                orig_pubkeys = tuple(getattr(paramiko.Transport, "_preferred_pubkeys", ()))
                try:
                    pref = list(orig_pubkeys) if orig_pubkeys else []
                    pref = [x for x in pref if x != "ssh-rsa"]
                    paramiko.Transport._preferred_pubkeys = tuple(["ssh-rsa"] + pref)
                    forwarder.start()
                finally:
                    if orig_pubkeys:
                        paramiko.Transport._preferred_pubkeys = orig_pubkeys
            else:
                forwarder.start()
            last_error = None
            break
        except Exception as e:
            last_error = e
            try:
                forwarder.stop()
            except Exception:
                pass
            if i < retries - 1 and retry_delay_ms > 0:
                time.sleep(retry_delay_ms / 1000.0)

    if last_error is not None:
        auth_mode = "password" if ssh_password else ("private-key-file" if ssh_pkey else ("ssh-agent/default-key" if ssh_allow_agent else "none"))
        raise ValueError(
            f"SSH gateway connection failed after {retries} attempt(s): {ssh_user}@{ssh_host}:{ssh_port} (auth={auth_mode}). "
            f"Please verify network reachability, SSH credentials, and server auth policy. detail={last_error}"
        ) from last_error

    conn = pymysql.connect(
        host=forwarder.local_bind_host or local_host,
        port=int(forwarder.local_bind_port),
        user=db_cfg["user"],
        password=db_cfg["password"],
        db=db_cfg["db"],
        charset=db_cfg["charset"],
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
        write_timeout=write_timeout,
        cursorclass=pymysql.cursors.DictCursor,
    )
    return conn, forwarder


def _render_template(template: str, params: Dict[str, Any], window_label: str, end_date: str | None) -> str:
    province = params.get("province", "province")
    cp = params.get("cp", "cp")
    direction = params.get("direction", "both")
    edc_name = params.get("edc_name", "edc")
    source = params.get("data_source_type", "nfa")
    instance = params.get("data_source_instance", "default")
    context = {
        "province": province,
        "cp": cp,
        "direction": direction,
        "edc": edc_name,
        "source": source,
        "instance": instance,
        "window": window_label,
        "date": end_date or "",
    }
    out = template
    for k, v in context.items():
        out = out.replace("{" + k + "}", str(v))
    return out


def _build_base_filename(params: Dict[str, Any], window_label: str, output_filename_template: str | None, end_date: str | None) -> str:
    if output_filename_template:
        return _render_template(output_filename_template, params, window_label, end_date)
    source = str(params.get("data_source_type") or "nfa").lower()
    if source == "edc":
        edc_name = params.get("edc_name", "edc")
        instance = params.get("data_source_instance", "default")
        return f"{edc_name}-{instance}-{window_label}"
    province = params.get("province", "province")
    cp = params.get("cp", "cp")
    direction = params.get("direction", "both")
    return f"{province}-{cp}-{direction}-{window_label}"


def _normalize_source_type(source_type: str | None) -> str:
    s = (source_type or "nfa").strip().lower()
    return s if s in {"nfa", "edc"} else "nfa"


def _load_source_config(source_type: str, instance: str | None, params: Dict[str, Any]) -> Dict[str, Any] | None:
    inst = (instance or "default").strip() or "default"
    cfg = params.get("db_config")
    if isinstance(cfg, dict):
        return cfg
    instances = get_data_source_instances(source_type)
    if inst in instances:
        return instances[inst]
    if inst == "default" and len(instances) == 1:
        return list(instances.values())[0]
    return None


def _safe_identifier(name: str, field_name: str) -> str:
    if not isinstance(name, str) or not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
        raise ValueError(f"invalid SQL identifier for {field_name}")
    return name


def _edc_like_pattern(edc_name: str, wildcard_mode: str) -> tuple[str, str]:
    if "*" in edc_name or "?" in edc_name:
        # glob-like wildcards:
        # * => any length, ? => single character
        return edc_name.replace("*", "%").replace("?", "_"), "LIKE"
    wm = (wildcard_mode or "prefix").lower()
    if wm == "exact":
        return edc_name, "="
    return f"{edc_name}%", "LIKE"


def _query_edc_window(
    conn,
    table_name: str,
    time_col: str,
    name_col: str,
    value_col: str,
    edc_name: str,
    start_s: str,
    end_s: str,
    wildcard_mode: str,
    exclude_like: str | None,
) -> list[dict]:
    pattern, op = _edc_like_pattern(edc_name, wildcard_mode)
    where = [f"{name_col} {op} %s", f"{time_col} >= %s", f"{time_col} <= %s"]
    args: list[Any] = [pattern, start_s, end_s]
    if exclude_like:
        where.append(f"{name_col} NOT LIKE %s")
        args.append(exclude_like)
    sql = f"""
        SELECT {time_col} AS create_time, SUM({value_col}) AS total_service_size
        FROM {table_name}
        WHERE {" AND ".join(where)}
        GROUP BY {time_col}
        ORDER BY {time_col}
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, tuple(args))
        return list(cursor.fetchall() or [])


def _pick_daily_95_from_rows(rows: list[dict], rank_index: int) -> tuple[float, int]:
    if not rows:
        return 0.0, 0
    vals = sorted((float(r.get("total_service_size") or 0.0) for r in rows), reverse=True)
    idx = min(max(0, rank_index), len(vals) - 1)
    return vals[idx], len(vals)


def _normalize_nfa_targets(targets: List[Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for it in targets or []:
        if isinstance(it, dict):
            ipg = it.get("ipgroup_id")
            uid = it.get("nfa_uuid")
            h = it.get("hash_uuid")
        elif isinstance(it, (list, tuple)) and len(it) >= 2:
            ipg = it[0]
            uid = it[1]
            h = it[2] if len(it) >= 3 else None
        else:
            continue
        if ipg is None or uid is None:
            continue
        out.append({"ipgroup_id": ipg, "nfa_uuid": uid, "hash_uuid": h})
    return out


def _infer_query_path(targets: List[Any]) -> str:
    norm = _normalize_nfa_targets(targets)
    if not norm:
        return "none"
    total = len(norm)
    with_hash = sum(1 for t in norm if t.get("hash_uuid"))
    if with_hash == 0:
        return "pair_fallback"
    if with_hash == total:
        return "hash_uuid"
    return "hash_uuid+pair_fallback"


def _classify_query_error(exc: Exception) -> str:
    text = str(exc or "").lower()
    timeout_markers = ("timeout", "timed out", "read timeout", "lock wait timeout")
    return "QUERY_TIMEOUT" if any(m in text for m in timeout_markers) else "QUERY_FAILED"


def _nfa_has_any_raw_points(
    conn,
    targets: List[Any],
    start_time: str,
    end_time: str,
    sample_pairs: int = 8,
) -> Dict[str, Any]:
    """Best-effort probe for raw 5-minute points in window.

    This is used only as a guard against false "no data" caused by query failures
    in batched aggregation paths.
    """
    norm = _normalize_nfa_targets(targets)
    if not norm:
        return {"has_points": False, "query_path": "none", "query_elapsed_ms": 0}

    picked = norm[: max(1, int(sample_pairs))]
    with_hash = [x for x in picked if x.get("hash_uuid")]
    without_hash = [x for x in picked if not x.get("hash_uuid")]
    used_hash = False
    used_pair = False
    t0 = time.monotonic()

    if with_hash:
        placeholders = ", ".join(["%s"] * len(with_hash))
        sql = f"""
            SELECT 1
            FROM nfa_ip_group_speed_logs_5m
            WHERE create_time BETWEEN %s AND %s
              AND hash_uuid IN ({placeholders})
            LIMIT 1
        """
        params: List[Any] = [start_time, end_time] + [x["hash_uuid"] for x in with_hash]
        with conn.cursor() as cursor:
            cursor.execute(sql, tuple(params))
            row = cursor.fetchone()
        used_hash = True
        if row:
            return {
                "has_points": True,
                "query_path": "hash_uuid" if not without_hash else "hash_uuid+pair_fallback",
                "query_elapsed_ms": int((time.monotonic() - t0) * 1000),
            }

    if without_hash:
        placeholders = ", ".join(["(%s, %s)"] * len(without_hash))
        sql = f"""
            SELECT 1
            FROM nfa_ip_group_speed_logs_5m
            WHERE create_time BETWEEN %s AND %s
              AND (ipgroup_id, nfa_uuid) IN ({placeholders})
            LIMIT 1
        """
        params = [start_time, end_time]
        for item in without_hash:
            params.extend([item["ipgroup_id"], item["nfa_uuid"]])
        with conn.cursor() as cursor:
            cursor.execute(sql, tuple(params))
            row = cursor.fetchone()
        used_pair = True
        if row:
            return {
                "has_points": True,
                "query_path": "pair_fallback" if not used_hash else "hash_uuid+pair_fallback",
                "query_elapsed_ms": int((time.monotonic() - t0) * 1000),
            }

    query_path = "none"
    if used_hash and used_pair:
        query_path = "hash_uuid+pair_fallback"
    elif used_hash:
        query_path = "hash_uuid"
    elif used_pair:
        query_path = "pair_fallback"
    return {
        "has_points": False,
        "query_path": query_path,
        "query_elapsed_ms": int((time.monotonic() - t0) * 1000),
    }


def _compute_edc_and_export(
    job_id: str,
    resolved_window: Dict[str, Any],
    params: Dict[str, Any],
    export_formats: List[str],
    output_filename_template: str | None,
    total_days: int,
    progress_cb: Callable[[int, str], None] | None = None,
) -> List[Dict[str, Any]]:
    start_time = resolved_window["start_time"]
    end_time = resolved_window["end_time"]
    window_label = resolved_window.get("label") or f"{start_time.split(' ')[0]}-{end_time.split(' ')[0]}"
    end_date = (end_time.split(' ')[0] if isinstance(end_time, str) and ' ' in end_time else str(end_time))
    base_name = _build_base_filename(params, window_label, output_filename_template, end_date)
    sortby = params.get("sortby")
    sort_order = params.get("sort_order", "desc")

    edc_name = str(params.get("edc_name") or "").strip()
    if not edc_name:
        raise ValueError("edc_name is required in params when data_source_type=edc")

    source_instance = str(params.get("data_source_instance") or "default")
    cfg = _load_source_config("edc", source_instance, params)
    if not cfg:
        raise ValueError(f"EDC data source instance not found: {source_instance}")

    table_name = _safe_identifier(str(cfg.get("table", "edc_data")), "table")
    time_col = _safe_identifier(str(cfg.get("time_column", "create_time")), "time_column")
    name_col = _safe_identifier(str(cfg.get("name_column", "edc_name")), "name_column")
    value_col = _safe_identifier(str(cfg.get("value_column", "service_size")), "value_column")
    wildcard_mode = str(cfg.get("wildcard_mode", "prefix"))
    if params.get("edc_match_mode"):
        wildcard_mode = str(params.get("edc_match_mode"))
    exclude_like = cfg.get("exclude_like", "%-backup")
    if "edc_exclude_like" in params:
        exclude_like = params.get("edc_exclude_like")
    rank_index = int(params.get("edc_rank_index", cfg.get("daily_rank_index", 14)))
    export_daily = bool(params.get("export_daily", False))
    monthly_aggregate = bool(params.get("monthly_aggregate", False))
    settlement_mode = str(params.get("settlement_mode") or "range_95")
    unit_base = int(params.get("unit_base", 1024))
    if unit_base not in (1000, 1024):
        unit_base = 1024
    # EDC 5-minute points: raw -> Mbps uses *8/300 (NFA keeps *8/60 in its own branch).
    edc_divisor = 300.0

    _progress(progress_cb, 6, "EDC: 正在建立数据库连接")
    conn, tunnel = _connect_edc_db(cfg)
    try:
        _progress(progress_cb, 8, "EDC: 已连接数据源，开始按天计算")
        st = pd.to_datetime(start_time)
        et = pd.to_datetime(end_time)
        daily_rows: List[Dict[str, Any]] = []
        cur = st.normalize()
        while cur <= et.normalize():
            day_start = max(st, cur)
            day_end = min(et, cur + timedelta(days=1) - timedelta(seconds=1))
            rows = _query_edc_window(
                conn, table_name, time_col, name_col, value_col, edc_name,
                f"{day_start:%Y-%m-%d %H:%M:%S}", f"{day_end:%Y-%m-%d %H:%M:%S}",
                wildcard_mode, exclude_like,
            )
            raw95, points = _pick_daily_95_from_rows(rows, rank_index)
            daily_rows.append({
                "date": f"{cur:%Y-%m-%d}",
                "edc_name": edc_name,
                "data_source_instance": source_instance,
                "daily_95th_percentile_raw": raw95,
                "daily_95th_percentile_mbps": raw_to_mbps(float(raw95), unit_base, edc_divisor),
                "data_points_daily": points,
            })
            done_days = len(daily_rows)
            total_days_span = max(1, int((et.normalize() - st.normalize()).days + 1))
            pct = 8 + int(done_days * 72 / total_days_span)
            _progress(progress_cb, pct, f"EDC: 按天计算进度 {done_days}/{total_days_span}")
            cur += timedelta(days=1)

        # Prepare raw settlement baselines for optional budget summary.
        daily_avg_raw = 0.0
        if daily_rows:
            daily_avg_raw = float(pd.Series([float(r.get("daily_95th_percentile_raw") or 0.0) for r in daily_rows]).sum()) / float(total_days)
        full_rows = _query_edc_window(
            conn, table_name, time_col, name_col, value_col, edc_name,
            f"{st:%Y-%m-%d %H:%M:%S}", f"{et:%Y-%m-%d %H:%M:%S}",
            wildcard_mode, exclude_like,
        )
        if not full_rows:
            reason = f"EDC 无匹配数据：模式={edc_name}，时间范围={st:%Y-%m-%d}~{et:%Y-%m-%d}"
            _progress(progress_cb, 92, "EDC: 无匹配数据，正在结束任务")
            return _make_terminal_no_data_artifacts(
                job_id,
                base_name,
                reason,
                source_type="edc",
                source_instance=source_instance,
                resolved_window=resolved_window,
                key_params={
                    "edc_name": edc_name,
                    "wildcard_mode": wildcard_mode,
                    "exclude_like": exclude_like,
                    "rank_index": rank_index,
                },
                counters={"daily_days": len(daily_rows), "full_rows": 0},
            )
        range_raw, _range_points = _pick_daily_95_from_rows(full_rows, rank_index)

        # Optional "data budget" summary: convert raw settlement by custom formula.
        if bool(params.get("data_budget_enabled", False)):
            try:
                mul = float(params.get("data_budget_mul", 8))
            except Exception:
                mul = 8.0
            try:
                div = float(params.get("data_budget_div", 300))
            except Exception:
                div = 300.0
            if div == 0:
                div = 300.0
            ym = f"{et:%Y-%m}"

            def _step1(raw_v: float) -> float:
                return float(raw_v) * float(mul) / float(div)

            def _conv(step1_v: float, base: int) -> float:
                return float(step1_v) / float(base) / float(base)

            daily_step1 = _step1(daily_avg_raw)
            range_step1 = _step1(range_raw)

            effective_pattern, match_op = _edc_like_pattern(edc_name, wildcard_mode)
            budget_summary = {
                "year_month": ym,
                "formula": f"raw*{mul:g}/{div:g}/base/base",
                "source_table": table_name,
                "source_time_column": time_col,
                "source_name_column": name_col,
                "source_value_column": value_col,
                "match_mode": wildcard_mode,
                "match_operator": match_op,
                "effective_pattern": effective_pattern,
                "exclude_like": exclude_like,
                "raw_daily_95_avg": float(daily_avg_raw),
                "raw_range_95": float(range_raw),
                "raw_daily_95_avg_step1": float(daily_step1),
                "raw_range_95_step1": float(range_step1),
                "daily_95_avg_1000": _conv(daily_step1, 1000),
                "range_95_1000": _conv(range_step1, 1000),
                "daily_95_avg_1024": _conv(daily_step1, 1024),
                "range_95_1024": _conv(range_step1, 1024),
                "daily_days": int(len(daily_rows)),
                "range_points": int(_range_points),
                "updated_at": datetime.utcnow().isoformat(timespec="seconds"),
            }
            try:
                summary_path = get_job_dir(job_id) / "budget_summary.json"
                summary_path.write_text(json.dumps(budget_summary, ensure_ascii=False), encoding="utf-8")
            except Exception:
                pass

        if monthly_aggregate:
            def _month_ranges(start_s: str, end_s: str) -> list[dict]:
                s = pd.to_datetime(start_s)
                e = pd.to_datetime(end_s)
                cur = pd.Timestamp(year=s.year, month=s.month, day=1)
                out: list[dict] = []
                while cur <= e:
                    if cur.month == 12:
                        next_first = pd.Timestamp(year=cur.year + 1, month=1, day=1)
                    else:
                        next_first = pd.Timestamp(year=cur.year, month=cur.month + 1, day=1)
                    seg_start = max(cur, s)
                    seg_end = min(next_first - pd.Timedelta(seconds=1), e)
                    out.append({
                        "label": f"{cur.year}-{cur.month:02d}",
                        "start": f"{seg_start:%Y-%m-%d %H:%M:%S}",
                        "end": f"{seg_end:%Y-%m-%d %H:%M:%S}",
                    })
                    cur = next_first
                return out

            rows_all: list[dict] = []
            month_ranges = _month_ranges(start_time, end_time)
            total_months = max(1, len(month_ranges))
            for i, rg in enumerate(month_ranges, start=1):
                _progress(progress_cb, 80 + int(i * 10 / total_months), f"EDC: 按月聚合 {rg['label']} ({i}/{total_months})")
                month_daily = [r for r in daily_rows if str(r.get("date", "")).startswith(rg["label"])]
                if settlement_mode == "daily_95_avg":
                    if month_daily:
                        month_mbps = float(pd.Series([float(x.get("daily_95th_percentile_mbps") or 0.0) for x in month_daily]).sum()) / float(len(month_daily))
                    else:
                        month_mbps = 0.0
                    month_raw = mbps_to_raw(float(month_mbps), unit_base, edc_divisor)
                    points = len(month_daily)
                else:
                    month_full_rows = _query_edc_window(
                        conn, table_name, time_col, name_col, value_col, edc_name,
                        rg["start"], rg["end"], wildcard_mode, exclude_like
                    )
                    month_raw, points = _pick_daily_95_from_rows(month_full_rows, rank_index)
                    month_mbps = raw_to_mbps(float(month_raw), unit_base, edc_divisor)
                rows_all.append({
                    "month": rg["label"],
                    "edc_name": edc_name,
                    "data_source_instance": source_instance,
                    "95th_percentile_raw": float(month_raw),
                    "95th_percentile_mbps": float(month_mbps),
                    "settlement_mode": settlement_mode,
                    "data_points": int(points),
                })

            dfm = pd.DataFrame(rows_all)
            if sortby and sortby in dfm.columns:
                dfm = dfm.sort_values(by=sortby, ascending=(sort_order == "asc"))
            elif "month" in dfm.columns:
                dfm = dfm.sort_values(by="month", ascending=True, kind="stable")
            _progress(progress_cb, 92, "EDC: 正在导出按月产物")
            return _export_df(
                dfm,
                job_id,
                f"{base_name}-monthly",
                export_formats,
                empty_terminal={
                    "reason": f"EDC 按月聚合结果为空：模式={edc_name}，窗口={window_label}",
                    "source_type": "edc",
                    "source_instance": source_instance,
                    "resolved_window": resolved_window,
                    "key_params": {"edc_name": edc_name, "monthly_aggregate": True},
                    "counters": {"months": len(month_ranges)},
                },
                flow_context={"source_type": "edc", "unit_base": unit_base},
            )

        if export_daily:
            df = pd.DataFrame(daily_rows)
            if sortby and sortby in df.columns:
                df = df.sort_values(by=sortby, ascending=(sort_order == "asc"))
            _progress(progress_cb, 92, "EDC: 正在导出产物")
            return _export_df(
                df,
                job_id,
                base_name,
                export_formats,
                empty_terminal={
                    "reason": f"EDC 每日导出结果为空：模式={edc_name}，窗口={window_label}",
                    "source_type": "edc",
                    "source_instance": source_instance,
                    "resolved_window": resolved_window,
                    "key_params": {"edc_name": edc_name, "export_daily": True},
                    "counters": {"daily_days": len(daily_rows)},
                },
                flow_context={"source_type": "edc", "unit_base": unit_base},
            )

        if settlement_mode == "daily_95_avg":
            daily_series = pd.Series([float(r["daily_95th_percentile_mbps"]) for r in daily_rows])
            value_mbps = float(daily_series.sum()) / float(total_days) if not daily_series.empty else 0.0
            value_raw = mbps_to_raw(float(value_mbps), unit_base, edc_divisor)
        else:
            value_raw, points = _pick_daily_95_from_rows(full_rows, rank_index)
            value_mbps = raw_to_mbps(float(value_raw), unit_base, edc_divisor)

        if settlement_mode == "daily_95_avg":
            points = len(daily_rows)
        df = pd.DataFrame([{
            "edc_name": edc_name,
            "data_source_instance": source_instance,
            "95th_percentile_raw": float(value_raw),
            "95th_percentile_mbps": float(value_mbps),
            "settlement_mode": settlement_mode,
            "data_points": int(points),
        }])
        if sortby and sortby in df.columns:
            df = df.sort_values(by=sortby, ascending=(sort_order == "asc"))
        _progress(progress_cb, 92, "EDC: 正在导出产物")
        return _export_df(
            df,
            job_id,
            base_name,
            export_formats,
            empty_terminal={
                "reason": f"EDC 汇总结果为空：模式={edc_name}，窗口={window_label}",
                "source_type": "edc",
                "source_instance": source_instance,
                "resolved_window": resolved_window,
                "key_params": {"edc_name": edc_name, "settlement_mode": settlement_mode},
                "counters": {"full_rows": len(full_rows)},
            },
            flow_context={"source_type": "edc", "unit_base": unit_base},
        )
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


def _to_dataframe(results: List[Dict[str, Any]]) -> pd.DataFrame:
    if not results:
        return pd.DataFrame()
    return pd.DataFrame(results)


def _progress(progress_cb: Callable[[int, str], None] | None, pct: int, stage: str) -> None:
    if not progress_cb:
        return
    try:
        progress_cb(max(0, min(100, int(pct))), str(stage))
    except Exception:
        pass


def _build_terminal_diagnostics(
    *,
    terminal_code: str,
    reason: str,
    source_type: str | None,
    source_instance: str | None,
    resolved_window: Dict[str, Any] | None = None,
    key_params: Dict[str, Any] | None = None,
    counters: Dict[str, Any] | None = None,
    extras: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "terminal_code": str(terminal_code or "NO_MATCH"),
        "terminal_reason": str(reason or ""),
        "generated_at": datetime.utcnow().isoformat(timespec="seconds"),
        "source_type": str(source_type or "nfa"),
        "source_instance": str(source_instance or "default"),
    }
    if isinstance(resolved_window, dict):
        payload["window"] = {
            "start_time": resolved_window.get("start_time"),
            "end_time": resolved_window.get("end_time"),
            "label": resolved_window.get("label"),
        }
    if isinstance(key_params, dict):
        payload["key_params"] = key_params
    if isinstance(counters, dict):
        payload["counters"] = counters
    if isinstance(extras, dict):
        for k, v in extras.items():
            if v is not None:
                payload[k] = v
    return payload


def _make_terminal_artifacts(
    job_id: str,
    filename_noext: str,
    reason: str,
    *,
    terminal_code: str,
    diagnostics: Dict[str, Any],
) -> List[Dict[str, Any]]:
    if terminal_code == "NO_MATCH":
        suffix = "no_data"
    elif terminal_code == "EMPTY_RESULT":
        suffix = "empty_result"
    else:
        suffix = "query_failed"
    txt = safe_artifact_path(job_id, f"{filename_noext}-{suffix}.txt")
    txt.write_text(f"{reason}\n", encoding="utf-8")
    diag = safe_artifact_path(job_id, f"{filename_noext}-diagnostics.json")
    diag.write_text(json.dumps(diagnostics, ensure_ascii=False, indent=2), encoding="utf-8")
    return [
        {
            "filename": txt.name,
            "size": txt.stat().st_size,
            "path": str(txt),
            "terminal_code": terminal_code,
            "terminal_reason": reason,
        },
        {
            "filename": diag.name,
            "size": diag.stat().st_size,
            "path": str(diag),
            "artifact_kind": "diagnostics",
            "terminal_code": terminal_code,
            "terminal_reason": reason,
        },
    ]


def _normalize_flow_columns_for_export(
    df: pd.DataFrame,
    flow_context: Dict[str, Any] | None = None,
) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    ctx = flow_context or {}
    source_type = str(ctx.get("source_type") or "nfa").strip().lower()
    unit_base = int(ctx.get("unit_base") or 1024)
    if unit_base not in (1000, 1024):
        unit_base = 1024
    seconds_per_point = 300.0 if source_type == "edc" else 60.0

    def _fmt_3(v: Any) -> str:
        try:
            if pd.isna(v):
                return ""
        except Exception:
            pass
        try:
            return f"{float(v):.3f}"
        except Exception:
            return ""

    out = df.copy()
    flow_col_pairs = (
        ("daily_95th_percentile_mbps", "daily_95th_percentile_raw"),
        ("95th_percentile_mbps", "95th_percentile_raw"),
    )
    for mbps_col, raw_col in flow_col_pairs:
        if mbps_col in out.columns and raw_col not in out.columns:
            mbps_series = pd.to_numeric(out[mbps_col], errors="coerce")
            out[raw_col] = mbps_series.map(
                lambda x: mbps_to_raw(float(x), unit_base, seconds_per_point) if pd.notna(x) else None
            )
        if mbps_col in out.columns:
            out[mbps_col] = out[mbps_col].map(_fmt_3)
        if raw_col in out.columns:
            out[raw_col] = out[raw_col].map(_fmt_3)

    cols = list(out.columns)
    for mbps_col, raw_col in flow_col_pairs:
        if mbps_col in cols and raw_col in cols:
            cols = [c for c in cols if c != raw_col]
            cols.insert(cols.index(mbps_col), raw_col)
    out = out.loc[:, cols]
    return out


def _export_df(
    df: pd.DataFrame,
    job_id: str,
    filename_noext: str,
    export_formats: List[str],
    *,
    empty_terminal: Dict[str, Any] | None = None,
    flow_context: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    artifacts: List[Dict[str, Any]] = []
    if df is None:
        return artifacts
    # 确保导出包含 saler_group/saler 列（即使为空）
    try:
        for _col in ("saler_group", "saler"):
            if _col not in df.columns:
                df[_col] = ""
    except Exception:
        # 容错，不阻断导出
        pass
    if df.empty:
        # 输出一个空CSV占位，至少包含列
        csv_path = safe_artifact_path(job_id, f"{filename_noext}.csv")
        export_csv(df, csv_path)
        artifacts.append({"filename": csv_path.name, "size": csv_path.stat().st_size, "path": str(csv_path)})
        reason = str((empty_terminal or {}).get("reason") or f"结果为空：{filename_noext}")
        source_type = str((empty_terminal or {}).get("source_type") or "nfa")
        source_instance = str((empty_terminal or {}).get("source_instance") or "default")
        diagnostics = _build_terminal_diagnostics(
            terminal_code=str((empty_terminal or {}).get("terminal_code") or "EMPTY_RESULT"),
            reason=reason,
            source_type=source_type,
            source_instance=source_instance,
            resolved_window=(empty_terminal or {}).get("resolved_window"),
            key_params=(empty_terminal or {}).get("key_params"),
            counters=(empty_terminal or {}).get("counters"),
            extras=(empty_terminal or {}).get("diagnostic_extras"),
        )
        artifacts.extend(
            _make_terminal_artifacts(
                job_id,
                filename_noext,
                reason,
                terminal_code=str((empty_terminal or {}).get("terminal_code") or "EMPTY_RESULT"),
                diagnostics=diagnostics,
            )
        )
        return artifacts
    df = _normalize_flow_columns_for_export(df, flow_context=flow_context)
    if "csv" in export_formats:
        p = safe_artifact_path(job_id, f"{filename_noext}.csv")
        export_csv(df, p)
        artifacts.append({"filename": p.name, "size": p.stat().st_size, "path": str(p)})
    if "xlsx" in export_formats:
        p = safe_artifact_path(job_id, f"{filename_noext}.xlsx")
        export_xlsx(df, p)
        artifacts.append({"filename": p.name, "size": p.stat().st_size, "path": str(p)})
    return artifacts

def _make_terminal_no_data_artifacts(
    job_id: str,
    filename_noext: str,
    reason: str,
    *,
    source_type: str,
    source_instance: str,
    resolved_window: Dict[str, Any] | None,
    key_params: Dict[str, Any] | None = None,
    counters: Dict[str, Any] | None = None,
    extras: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    diagnostics = _build_terminal_diagnostics(
        terminal_code="NO_MATCH",
        reason=reason,
        source_type=source_type,
        source_instance=source_instance,
        resolved_window=resolved_window,
        key_params=key_params,
        counters=counters,
        extras=extras,
    )
    return _make_terminal_artifacts(
        job_id,
        filename_noext,
        reason,
        terminal_code="NO_MATCH",
        diagnostics=diagnostics,
    )


def _make_terminal_query_failure_artifacts(
    job_id: str,
    filename_noext: str,
    reason: str,
    *,
    source_type: str,
    source_instance: str,
    resolved_window: Dict[str, Any] | None,
    key_params: Dict[str, Any] | None = None,
    counters: Dict[str, Any] | None = None,
    terminal_code: str = "QUERY_FAILED",
    extras: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    diagnostics = _build_terminal_diagnostics(
        terminal_code=terminal_code,
        reason=reason,
        source_type=source_type,
        source_instance=source_instance,
        resolved_window=resolved_window,
        key_params=key_params,
        counters=counters,
        extras=extras,
    )
    return _make_terminal_artifacts(
        job_id,
        filename_noext,
        reason,
        terminal_code=terminal_code,
        diagnostics=diagnostics,
    )

def compute_and_export(
    job_id: str,
    resolved_window: Dict[str, Any],
    params: Dict[str, Any],
    export_formats: List[str] | None,
    output_filename_template: str | None,
    progress_cb: Callable[[int, str], None] | None = None,
) -> List[Dict[str, Any]]:
    """Run 95th percentile computation using the existing script functions and export artifacts.

    params accepts the same keys as the original CLI script, except start/end time which come from resolved_window.
    """
    export_formats = export_formats or ["csv"]

    # Resolve times
    start_time = resolved_window["start_time"]
    end_time = resolved_window["end_time"]
    window_label = resolved_window.get("label") or f"{start_time.split(' ')[0]}-{end_time.split(' ')[0]}"
    end_date = (end_time.split(' ')[0] if isinstance(end_time, str) and ' ' in end_time else str(end_time))
    # 选定天数（含首尾，按日期粒度）
    try:
        _sd = pd.to_datetime(start_time).date()
        _ed = pd.to_datetime(end_time).date()
        total_days = max(1, (_ed - _sd).days + 1)
    except Exception:
        total_days = 1

    source_type = _normalize_source_type(params.get("data_source_type"))
    source_instance = str(params.get("data_source_instance") or "default")
    params["data_source_type"] = source_type
    params["data_source_instance"] = source_instance
    _progress(progress_cb, 3, f"初始化: 数据源 {source_type}/{source_instance}")
    if source_type == "edc":
        return _compute_edc_and_export(job_id, resolved_window, params, export_formats, output_filename_template, total_days, progress_cb)

    # Required params
    province = params.get("province")
    cp = params.get("cp")
    if not province or not cp:
        raise ValueError("province and cp are required in params")

    # Optional params & defaults
    direction = params.get("direction", "both")
    school = params.get("school")
    export_daily = bool(params.get("export_daily", False))
    exclude_school = params.get("exclude_school")
    sortby = params.get("sortby")
    sort_order = params.get("sort_order", "desc")
    aggregate_all = bool(params.get("aggregate_all", False))
    batch_size = int(params.get("batch_size", 200))

    # New params
    try:
        unit_base = int(params.get("unit_base", 1024))
    except Exception:
        unit_base = 1024
    if unit_base not in (1000, 1024):
        unit_base = 1024
    settlement_mode = params.get("settlement_mode")  # None -> preserve prior behavior
    combine_v4_v6 = bool(params.get("combine_v4_v6", False))
    merge_key = params.get("merge_key")
    monthly_aggregate = bool(params.get("monthly_aggregate", False))

    # Prefer selected NFA instance config; fallback to db_config.ini if provided in params
    db_cfg = None
    selected_cfg = _load_source_config("nfa", source_instance, params)
    if selected_cfg:
        db_cfg = {
            'host': selected_cfg.get('host'),
            'port': int(selected_cfg.get('port', 3306)),
            'user': selected_cfg.get('user'),
            'password': selected_cfg.get('password'),
            'db': selected_cfg.get('db'),
            'charset': selected_cfg.get('charset', 'utf8mb4'),
            'connect_timeout': int(selected_cfg.get('connect_timeout', 10)),
            'read_timeout': int(selected_cfg.get('read_timeout', 120)),
            'write_timeout': int(selected_cfg.get('write_timeout', 120)),
        }
    if not db_cfg:
        # use ini path relative to server/ directory
        ini_arg = params.get("config") or "db_config.ini"
        base_dir = Path(__file__).resolve().parents[1]  # server/
        ini_path = Path(ini_arg)
        if not ini_path.is_absolute():
            ini_path = base_dir / ini_arg
        try:
            db_cfg = c95.load_db_config(str(ini_path))
        except SystemExit as e:
            raise ValueError(
                f"NFA 数据源配置不可用：{ini_path}。"
                f"请在“数据源配置管理”中配置 NFA 实例，或补全该配置文件后重试。"
            ) from e

    try:
        conn = c95.connect_to_db(db_cfg)
    except SystemExit as e:
        raise ValueError(
            "NFA 数据库连接失败。请检查 NFA 数据源配置（host/port/user/password/db）后重试。"
        ) from e

    artifacts: List[Dict[str, Any]] = []
    try:
        diag_context: Dict[str, Any] = {}

        def _nfa_empty_terminal(reason: str, counters: Dict[str, Any] | None = None) -> Dict[str, Any]:
            return {
                "reason": reason,
                "source_type": "nfa",
                "source_instance": source_instance,
                "resolved_window": resolved_window,
                "key_params": {
                    "province": province,
                    "cp": cp,
                    "school": school,
                    "direction": direction,
                    "settlement_mode": settlement_mode,
                    "monthly_aggregate": monthly_aggregate,
                    "aggregate_all": aggregate_all,
                    "batch_size": batch_size,
                    "exclude_school": exclude_school,
                    "combine_v4_v6": combine_v4_v6,
                    "merge_key": merge_key,
                },
                "counters": counters or {},
                "diagnostic_extras": dict(diag_context),
            }

        _progress(progress_cb, 8, "NFA: 已连接数据源，加载匹配对象")
        schools = c95.get_schools_by_province_and_cp(conn, province, cp, school)
        if not schools:
            reason = f"NFA 无匹配对象：省份={province}，CP={cp}" + (f"，院校={school}" if school else "")
            artifacts.extend(
                _make_terminal_no_data_artifacts(
                    job_id,
                    _build_base_filename(params, window_label, output_filename_template, end_date),
                    reason,
                    source_type="nfa",
                    source_instance=source_instance,
                    resolved_window=resolved_window,
                    key_params={"province": province, "cp": cp, "school": school},
                    counters={"matched_schools": 0},
                    extras=dict(diag_context),
                )
            )
            return artifacts

        base_name = _build_base_filename(params, window_label, output_filename_template, end_date)
        _progress(progress_cb, 9, "NFA: 预筛选窗口内有流量对象")
        pairs_all = [(s["ipgroup_id"], s["nfa_uuid"]) for s in schools]
        query_path = _infer_query_path(schools)
        diag_context["query_path"] = query_path
        probe_batch = max(100, min(1000, int(batch_size)))
        if hasattr(c95, "filter_pairs_with_data"):
            t_prefilter = time.monotonic()
            active_pairs = c95.filter_pairs_with_data(conn, schools, start_time, end_time, batch_size=probe_batch)
            prefilter_elapsed_ms = int((time.monotonic() - t_prefilter) * 1000)
            logger.info(
                "NFA prefilter done: query_path=%s elapsed_ms=%s pairs_total=%s pairs_active=%s",
                query_path,
                prefilter_elapsed_ms,
                len(pairs_all),
                len(active_pairs) if isinstance(active_pairs, set) else "unknown",
            )
            diag_context["prefilter_elapsed_ms"] = prefilter_elapsed_ms
        else:
            active_pairs = None
            prefilter_elapsed_ms = None
            _progress(progress_cb, 9, "NFA: 预筛函数缺失，跳过预筛")
        if active_pairs is not None:
            schools = [s for s in schools if (s.get("ipgroup_id"), s.get("nfa_uuid")) in active_pairs]
            if not schools:
                reason = f"NFA 无匹配流量数据：省份={province}，CP={cp}，窗口={window_label}"
                return _make_terminal_no_data_artifacts(
                    job_id,
                    base_name,
                    reason,
                    source_type="nfa",
                    source_instance=source_instance,
                    resolved_window=resolved_window,
                    key_params={"province": province, "cp": cp, "school": school, "direction": direction},
                    counters={"pairs_total": len(pairs_all), "pairs_active": 0},
                    extras=dict(diag_context),
                )
            _progress(progress_cb, 11, f"NFA: 窗口内有数据对象 {len(schools)}/{len(pairs_all)}")

        # Helper: split [start_time, end_time] into calendar months
        def _month_ranges(start_s: str, end_s: str):
            out = []
            s = pd.to_datetime(start_s)
            e = pd.to_datetime(end_s)
            cur = pd.Timestamp(year=s.year, month=s.month, day=1, hour=0, minute=0, second=0)
            # ensure first month covers from start_s if mid-month
            while cur <= e:
                if cur.month == 12:
                    next_first = pd.Timestamp(year=cur.year + 1, month=1, day=1)
                else:
                    next_first = pd.Timestamp(year=cur.year, month=cur.month + 1, day=1)
                seg_start = max(cur, s)
                seg_end = min(next_first - pd.Timedelta(seconds=1), e)
                out.append({
                    'label': f"{cur.year}-{cur.month:02d}",
                    'start': f"{seg_start:%Y-%m-%d %H:%M:%S}",
                    'end': f"{seg_end:%Y-%m-%d %H:%M:%S}",
                })
                cur = next_first
            return out

        if monthly_aggregate:
            # 忽略 export_daily，改为输出按自然月聚合的行
            base_name = _build_base_filename(params, window_label, output_filename_template, end_date)
            rows_all: list[dict] = []

            month_ranges = _month_ranges(start_time, end_time)
            total_months = max(1, len(month_ranges))
            t_monthly_agg = time.monotonic()
            for i, rg in enumerate(month_ranges, start=1):
                st = pd.to_datetime(rg['start'])
                et = pd.to_datetime(rg['end'])
                _progress(progress_cb, 12 + int(i * 68 / total_months), f"NFA: 按月聚合 {rg['label']} ({i}/{total_months})")

                if aggregate_all:
                    if settlement_mode == 'daily_95_avg':
                        # 先算该月“每日95”，再按该月天数求平均
                        rows_daily = c95.aggregate_all_and_compute(conn, schools, st, et, direction, True, unit_base=unit_base)
                        df_daily = _to_dataframe(rows_daily)
                        try:
                            _days = max(1, int(df_daily.shape[0]))
                        except Exception:
                            _days = 1
                        if not df_daily.empty and 'daily_95th_percentile_mbps' in df_daily.columns:
                            avg_val = float(df_daily['daily_95th_percentile_mbps'].sum()) / float(_days)
                        else:
                            avg_val = 0.0
                        month_rows = [{
                            'school_id': '',
                            'ipgroup_name': '全部院校汇总',
                            'ipgroup_id': '',
                            'nfa_uuid': '',
                            'saler_group': '',
                            'saler': '',
                            '95th_percentile_mbps': avg_val,
                            'direction': direction,
                        }]
                    else:
                        # 该月范围内在时间点上汇总后计算 95
                        month_rows = c95.aggregate_all_and_compute(conn, schools, st, et, direction, False, unit_base=unit_base)
                else:
                    month_rows = c95.process_schools_batched(
                        conn, schools,
                        st, et,
                        direction, False,
                        batch_size=batch_size, unit_base=unit_base, combine_v4_v6=combine_v4_v6, merge_key=merge_key
                    )

                if month_rows:
                    for it in month_rows:
                        it['month'] = rg['label']
                    rows_all.extend(month_rows)
            monthly_elapsed_ms = int((time.monotonic() - t_monthly_agg) * 1000)
            diag_context["query_elapsed_ms"] = monthly_elapsed_ms
            logger.info(
                "NFA monthly aggregate done: query_path=%s elapsed_ms=%s schools=%s months=%s rows=%s",
                query_path,
                monthly_elapsed_ms,
                len(schools),
                len(month_ranges),
                len(rows_all),
            )

            dfm = _to_dataframe(rows_all)
            sort_keys = []
            if not dfm.empty:
                if 'month' in dfm.columns:
                    sort_keys.append('month')
                if 'cp' in dfm.columns:
                    sort_keys.append('cp')
                # 名称列兼容
                name_col = 'school_name' if 'school_name' in dfm.columns else ('ipgroup_name' if 'ipgroup_name' in dfm.columns else None)
                if name_col:
                    sort_keys.append(name_col)
            if sort_keys:
                dfm = dfm.sort_values(by=sort_keys, kind='stable')
            if dfm.empty:
                try:
                    t_probe = time.monotonic()
                    probe = _nfa_has_any_raw_points(conn, schools, start_time, end_time)
                    probe_elapsed_ms = int((time.monotonic() - t_probe) * 1000)
                    diag_context["probe_status"] = "found_raw_points" if probe.get("has_points") else "no_raw_points"
                    diag_context["probe_elapsed_ms"] = probe_elapsed_ms
                    if probe.get("query_path"):
                        diag_context["query_path"] = str(probe.get("query_path"))
                    logger.info(
                        "NFA empty probe done: has_points=%s query_path=%s elapsed_ms=%s",
                        bool(probe.get("has_points")),
                        probe.get("query_path"),
                        probe_elapsed_ms,
                    )
                    if probe.get("has_points"):
                        raise ValueError(
                            f"NFA 查询存在原始点位，但按月聚合结果为空。可能是批量查询超时或连接不稳定。"
                            f"建议缩小时间范围或降低 batch_size 后重试。窗口={window_label}"
                        )
                except ValueError:
                    raise
                except Exception as e:
                    terminal_code = _classify_query_error(e)
                    reason = (
                        f"NFA 空结果探测失败：{e}。"
                        f"可能是数据库查询超时或连接异常，窗口={window_label}"
                    )
                    return _make_terminal_query_failure_artifacts(
                        job_id,
                        f"{base_name}-monthly",
                        reason,
                        source_type="nfa",
                        source_instance=source_instance,
                        resolved_window=resolved_window,
                        key_params={
                            "province": province,
                            "cp": cp,
                            "school": school,
                            "direction": direction,
                            "monthly_aggregate": monthly_aggregate,
                            "aggregate_all": aggregate_all,
                        },
                        counters={"matched_schools": len(schools), "months": len(month_ranges)},
                        terminal_code=terminal_code,
                        extras={
                            **diag_context,
                            "probe_status": "probe_failed",
                            "exception_type": type(e).__name__,
                        },
                    )
            _progress(progress_cb, 92, "NFA: 正在导出按月产物")
            artifacts += _export_df(
                dfm,
                job_id,
                f"{base_name}-monthly",
                export_formats,
                empty_terminal=_nfa_empty_terminal(
                    f"NFA 按月聚合结果为空：省份={province}，CP={cp}，窗口={window_label}",
                    {"matched_schools": len(schools), "months": len(month_ranges)},
                ),
                flow_context={"source_type": "nfa", "unit_base": unit_base},
            )
            return artifacts

        if exclude_school:
            exclude_set = {x.strip() for x in exclude_school.split(',') if x.strip()}
            excluded_schools = [s for s in schools if s.get('school_name') in exclude_set]
            remaining_schools = [s for s in schools if s.get('school_name') not in exclude_set]
            base_name = _build_base_filename(params, window_label, output_filename_template, end_date)

            # 1) 排除组（逐校）
            if excluded_schools:
                # 根据 settlement_mode 决定输出
                if settlement_mode == 'daily_95_avg':
                    # 计算每日95，再按学校平均（使用 batched 以支持 V4/V6 合并）
                    rows_daily = c95.process_schools_batched(
                        conn, excluded_schools,
                        pd.to_datetime(start_time), pd.to_datetime(end_time),
                        direction, True, batch_size=batch_size, unit_base=unit_base, combine_v4_v6=combine_v4_v6, merge_key=merge_key
                    )
                    df_daily = _to_dataframe(rows_daily)
                    if export_daily:
                        df_excluded = df_daily
                    else:
                        if not df_daily.empty and 'daily_95th_percentile_mbps' in df_daily.columns:
                            group_cols = ['school_id','ipgroup_name','ipgroup_id','nfa_uuid']
                            for _c in ('saler_group','saler'):
                                if _c in df_daily.columns:
                                    group_cols.append(_c)
                            tmp = df_daily.groupby(group_cols, as_index=False)['daily_95th_percentile_mbps'].sum()
                            tmp['95th_percentile_mbps'] = tmp['daily_95th_percentile_mbps'] / float(total_days)
                            df_excluded = tmp.drop(columns=['daily_95th_percentile_mbps'])
                        else:
                            df_excluded = pd.DataFrame()
                else:
                    # 逐校（或逐校按天）- 使用 batched 版本以支持 V4/V6 合并
                    results_excluded = c95.process_schools_batched(
                        conn, excluded_schools,
                        pd.to_datetime(start_time), pd.to_datetime(end_time),
                        direction, export_daily, batch_size=batch_size, unit_base=unit_base, combine_v4_v6=combine_v4_v6, merge_key=merge_key
                    )
                    df_excluded = _to_dataframe(results_excluded)
                # 排序
                if sortby and sortby in df_excluded.columns:
                    df_excluded = df_excluded.sort_values(by=sortby, ascending=(sort_order == 'asc'))
                _progress(progress_cb, 76, "NFA: 正在导出排除组产物")
                artifacts += _export_df(
                    df_excluded,
                    job_id,
                    f"{base_name}_excluded",
                    export_formats,
                    empty_terminal=_nfa_empty_terminal(
                        f"NFA 排除组结果为空：省份={province}，CP={cp}，窗口={window_label}",
                        {"excluded_schools": len(excluded_schools)},
                    ),
                    flow_context={"source_type": "nfa", "unit_base": unit_base},
                )

            # 2) 剩余组（整体汇总）
            if remaining_schools:
                # 名单导出
                from collections import Counter
                name_list = [ (s.get('ipgroup_name') or s.get('school_name') or '').strip() for s in remaining_schools ]
                name_list = [n for n in name_list if n]
                name_counter = Counter(name_list)
                names_txt = safe_artifact_path(job_id, f"{base_name}_remaining_names.txt")
                with names_txt.open('w', encoding='utf-8-sig') as f:
                    for n, c in sorted(name_counter.items(), key=lambda x: x[0]):
                        f.write(f"{n} x{c}\n" if c > 1 else f"{n}\n")
                artifacts.append({"filename": names_txt.name, "size": names_txt.stat().st_size, "path": str(names_txt)})

                # 数据聚合
                pairs = [(s['ipgroup_id'], s['nfa_uuid']) for s in remaining_schools]
                df_agg = c95.aggregate_speed_data_for_pairs_db(conn, pairs, pd.to_datetime(start_time), pd.to_datetime(end_time))
                if df_agg.empty:
                    df_agg = c95.aggregate_speed_data_for_schools(conn, remaining_schools, pd.to_datetime(start_time), pd.to_datetime(end_time))
                if not df_agg.empty:
                    if settlement_mode == 'daily_95_avg':
                        # 先按天计算95；导出每日则逐天输出；否则对日95取平均
                        df_agg['recv_mbps'] = df_agg['recv'] * 8 / 60 / float(unit_base) / float(unit_base)
                        df_agg['send_mbps'] = df_agg['send'] * 8 / 60 / float(unit_base) / float(unit_base)
                        df_agg['date'] = df_agg['create_time'].dt.date
                        if export_daily:
                            rows: List[Dict[str, Any]] = []
                            for date_obj, g in df_agg.groupby('date'):
                                if direction == 'recv':
                                    series = g['recv_mbps']
                                elif direction == 'send':
                                    series = g['send_mbps']
                                else:
                                    series = g['recv_mbps'] + g['send_mbps']
                                val = float(c95.calculate_95th_from_series(series))
                                rows.append({
                                    'school_id': '',
                                    'ipgroup_name': '剩余院校汇总',
                                    'ipgroup_id': '',
                                    'nfa_uuid': '',
                                    'saler_group': '',
                                    'saler': '',
                                    'date': f"{date_obj:%Y-%m-%d}",
                                    'daily_95th_percentile_mbps': val,
                                    'direction': direction,
                                    'data_points_daily': int(series.shape[0])
                                })
                            df_remaining = pd.DataFrame(rows)
                        else:
                            vals = []
                            for date_obj, g in df_agg.groupby('date'):
                                if direction == 'recv':
                                    series = g['recv_mbps']
                                elif direction == 'send':
                                    series = g['send_mbps']
                                else:
                                    series = g['recv_mbps'] + g['send_mbps']
                                vals.append(float(c95.calculate_95th_from_series(series)))
                            avg_val = (float(pd.Series(vals).sum())/float(total_days)) if vals else 0.0
                            df_remaining = pd.DataFrame([{
                                'school_id': '',
                                'ipgroup_name': '剩余院校汇总',
                                'ipgroup_id': '',
                                'nfa_uuid': '',
                                'saler_group': '',
                                'saler': '',
                                '95th_percentile_mbps': avg_val,
                                'direction': direction
                            }])
                    elif export_daily:
                        df_agg['date'] = df_agg['create_time'].dt.date
                        rows: List[Dict[str, Any]] = []
                        for date_obj, group in df_agg.groupby('date'):
                            val = c95.calculate_95th_percentile(group.to_dict('records'), direction, unit_base=unit_base)
                            rows.append({
                                'school_id': '',
                                'ipgroup_name': '剩余院校汇总',
                                'ipgroup_id': '',
                                'nfa_uuid': '',
                                'saler_group': '',
                                'saler': '',
                                'date': f"{date_obj:%Y-%m-%d}",
                                'daily_95th_percentile_mbps': val,
                                'direction': direction,
                                'data_points_daily': len(group)
                            })
                        df_remaining = pd.DataFrame(rows)
                    else:
                        val = c95.calculate_95th_percentile(df_agg.to_dict('records'), direction, unit_base=unit_base)
                        df_remaining = pd.DataFrame([{
                            'school_id': '',
                            'ipgroup_name': '剩余院校汇总',
                            'ipgroup_id': '',
                            'nfa_uuid': '',
                            'saler_group': '',
                            'saler': '',
                            '95th_percentile_mbps': val,
                            'data_points': len(df_agg),
                            'direction': direction
                        }])
                    # 排序
                    if sortby and sortby in df_remaining.columns:
                        df_remaining = df_remaining.sort_values(by=sortby, ascending=(sort_order == 'asc'))
                    _progress(progress_cb, 90, "NFA: 正在导出剩余组产物")
                    artifacts += _export_df(
                        df_remaining,
                        job_id,
                        f"{base_name}_remaining",
                        export_formats,
                        empty_terminal=_nfa_empty_terminal(
                            f"NFA 剩余组结果为空：省份={province}，CP={cp}，窗口={window_label}",
                            {"remaining_schools": len(remaining_schools)},
                        ),
                        flow_context={"source_type": "nfa", "unit_base": unit_base},
                    )
            return artifacts
        else:
            base_name = _build_base_filename(params, window_label, output_filename_template, end_date)
            if aggregate_all:
                # 全部院校在时间点上汇总后再计算
                if settlement_mode == 'daily_95_avg':
                    # 先拿到每日95列表
                    rows_daily = c95.aggregate_all_and_compute(
                        conn, schools,
                        pd.to_datetime(start_time), pd.to_datetime(end_time),
                        direction, True, unit_base=unit_base
                    )
                    df_daily = _to_dataframe(rows_daily)
                    if export_daily:
                        df = df_daily
                    else:
                        if not df_daily.empty and 'daily_95th_percentile_mbps' in df_daily.columns:
                            avg_val = float(df_daily['daily_95th_percentile_mbps'].sum())/float(total_days)
                        else:
                            avg_val = 0.0
                        df = pd.DataFrame([{
                            'school_id': '',
                            'ipgroup_name': '全部院校汇总',
                            'ipgroup_id': '',
                            'nfa_uuid': '',
                            '95th_percentile_mbps': avg_val,
                            'direction': direction
                        }])
                else:
                    rows = c95.aggregate_all_and_compute(
                        conn, schools,
                        pd.to_datetime(start_time), pd.to_datetime(end_time),
                        direction, export_daily, unit_base=unit_base
                    )
                    df = _to_dataframe(rows)
                if sortby and sortby in df.columns:
                    df = df.sort_values(by=sortby, ascending=(sort_order == 'asc'))
                _progress(progress_cb, 92, "NFA: 正在导出产物")
                artifacts += _export_df(
                    df,
                    job_id,
                    base_name,
                    export_formats,
                    empty_terminal=_nfa_empty_terminal(
                        f"NFA 汇总结果为空：省份={province}，CP={cp}，窗口={window_label}",
                        {"matched_schools": len(schools), "aggregate_all": True},
                    ),
                    flow_context={"source_type": "nfa", "unit_base": unit_base},
                )
                return artifacts
            else:
                # 逐校（或逐校按天）- 批量拉取 + 内存分组
                if settlement_mode == 'daily_95_avg':
                    # 先每日95，再根据 export_daily 决定是否求平均
                    t_rows = time.monotonic()
                    rows_daily = c95.process_schools_batched(
                        conn, schools,
                        pd.to_datetime(start_time), pd.to_datetime(end_time),
                        direction, True, batch_size=batch_size, unit_base=unit_base, combine_v4_v6=combine_v4_v6, merge_key=merge_key
                    )
                    query_elapsed_ms = int((time.monotonic() - t_rows) * 1000)
                    diag_context["query_elapsed_ms"] = query_elapsed_ms
                    logger.info(
                        "NFA batched query done: query_path=%s elapsed_ms=%s schools=%s rows=%s export_daily=%s",
                        query_path,
                        query_elapsed_ms,
                        len(schools),
                        len(rows_daily or []),
                        bool(export_daily),
                    )
                    # 不再回退到逐院校单查：空结果即表示该窗口无匹配流量数据
                    df_daily = _to_dataframe(rows_daily)
                    if export_daily:
                        df = df_daily
                    else:
                        if not df_daily.empty and 'daily_95th_percentile_mbps' in df_daily.columns:
                            group_cols = ['school_id','ipgroup_name','ipgroup_id','nfa_uuid']
                            for _c in ('saler_group','saler'):
                                if _c in df_daily.columns:
                                    group_cols.append(_c)
                            tmp = df_daily.groupby(group_cols, as_index=False)['daily_95th_percentile_mbps'].sum()
                            tmp['95th_percentile_mbps'] = tmp['daily_95th_percentile_mbps'] / float(total_days)
                            df = tmp.drop(columns=['daily_95th_percentile_mbps'])
                        else:
                            df = pd.DataFrame()
                else:
                    t_rows = time.monotonic()
                    rows = c95.process_schools_batched(
                        conn, schools,
                        pd.to_datetime(start_time), pd.to_datetime(end_time),
                        direction, export_daily, batch_size=batch_size, unit_base=unit_base, combine_v4_v6=combine_v4_v6, merge_key=merge_key
                    )
                    # 不再回退到原逐院校方法：避免无数据场景下逐校空查导致长时间运行
                    df = _to_dataframe(rows)
                    query_elapsed_ms = int((time.monotonic() - t_rows) * 1000)
                    diag_context["query_elapsed_ms"] = query_elapsed_ms
                    logger.info(
                        "NFA batched query done: query_path=%s elapsed_ms=%s schools=%s rows=%s export_daily=%s",
                        query_path,
                        query_elapsed_ms,
                        len(schools),
                        len(rows or []),
                        bool(export_daily),
                    )
                if df is None or df.empty:
                    try:
                        t_probe = time.monotonic()
                        probe = _nfa_has_any_raw_points(conn, schools, start_time, end_time)
                        probe_elapsed_ms = int((time.monotonic() - t_probe) * 1000)
                        diag_context["probe_status"] = "found_raw_points" if probe.get("has_points") else "no_raw_points"
                        diag_context["probe_elapsed_ms"] = probe_elapsed_ms
                        if probe.get("query_path"):
                            diag_context["query_path"] = str(probe.get("query_path"))
                        logger.info(
                            "NFA empty probe done: has_points=%s query_path=%s elapsed_ms=%s",
                            bool(probe.get("has_points")),
                            probe.get("query_path"),
                            probe_elapsed_ms,
                        )
                        if probe.get("has_points"):
                            raise ValueError(
                                f"NFA 查询存在原始点位，但聚合结果为空。可能是批量查询超时或连接不稳定。"
                                f"建议缩小时间范围或降低 batch_size 后重试。窗口={window_label}"
                            )
                    except ValueError:
                        raise
                    except Exception as e:
                        terminal_code = _classify_query_error(e)
                        reason = (
                            f"NFA 空结果探测失败：{e}。"
                            f"可能是数据库查询超时或连接异常，窗口={window_label}"
                        )
                        return _make_terminal_query_failure_artifacts(
                            job_id,
                            base_name,
                            reason,
                            source_type="nfa",
                            source_instance=source_instance,
                            resolved_window=resolved_window,
                            key_params={
                                "province": province,
                                "cp": cp,
                                "school": school,
                                "direction": direction,
                                "monthly_aggregate": monthly_aggregate,
                                "aggregate_all": aggregate_all,
                            },
                            counters={"matched_schools": len(schools), "pairs_total": len(pairs_all)},
                            terminal_code=terminal_code,
                            extras={
                                **diag_context,
                                "probe_status": "probe_failed",
                                "exception_type": type(e).__name__,
                            },
                        )
                    reason = f"NFA 无匹配流量数据：省份={province}，CP={cp}，窗口={window_label}"
                    return _make_terminal_no_data_artifacts(
                        job_id,
                        base_name,
                        reason,
                        source_type="nfa",
                        source_instance=source_instance,
                        resolved_window=resolved_window,
                        key_params={"province": province, "cp": cp, "school": school, "direction": direction},
                        counters={"matched_schools": len(schools), "pairs_total": len(pairs_all)},
                        extras=dict(diag_context),
                    )
                if sortby and sortby in df.columns:
                    df = df.sort_values(by=sortby, ascending=(sort_order == 'asc'))
                _progress(progress_cb, 92, "NFA: 正在导出产物")
                artifacts += _export_df(
                    df,
                    job_id,
                    base_name,
                    export_formats,
                    empty_terminal=_nfa_empty_terminal(
                        f"NFA 产物结果为空：省份={province}，CP={cp}，窗口={window_label}",
                        {"matched_schools": len(schools)},
                    ),
                    flow_context={"source_type": "nfa", "unit_base": unit_base},
                )
                return artifacts
    finally:
        try:
            conn.close()
        except Exception:
            pass
