from __future__ import annotations

import json
import re
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

        if export_daily:
            df = pd.DataFrame(daily_rows)
            if sortby and sortby in df.columns:
                df = df.sort_values(by=sortby, ascending=(sort_order == "asc"))
            _progress(progress_cb, 92, "EDC: 正在导出产物")
            return _export_df(df, job_id, base_name, export_formats)

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
        return _export_df(df, job_id, base_name, export_formats)
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


def _export_df(df: pd.DataFrame, job_id: str, filename_noext: str, export_formats: List[str]) -> List[Dict[str, Any]]:
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
        return artifacts
    if "csv" in export_formats:
        p = safe_artifact_path(job_id, f"{filename_noext}.csv")
        export_csv(df, p)
        artifacts.append({"filename": p.name, "size": p.stat().st_size, "path": str(p)})
    if "xlsx" in export_formats:
        p = safe_artifact_path(job_id, f"{filename_noext}.xlsx")
        export_xlsx(df, p)
        artifacts.append({"filename": p.name, "size": p.stat().st_size, "path": str(p)})
    return artifacts


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
        }
    if not db_cfg:
        # use ini path relative to server/ directory
        ini_arg = params.get("config") or "db_config.ini"
        base_dir = Path(__file__).resolve().parents[1]  # server/
        ini_path = Path(ini_arg)
        if not ini_path.is_absolute():
            ini_path = base_dir / ini_arg
        db_cfg = c95.load_db_config(str(ini_path))

    conn = c95.connect_to_db(db_cfg)

    artifacts: List[Dict[str, Any]] = []
    try:
        _progress(progress_cb, 8, "NFA: 已连接数据源，加载匹配对象")
        schools = c95.get_schools_by_province_and_cp(conn, province, cp, school)
        if not schools:
            # 无数据时输出一份说明文件
            txt = safe_artifact_path(job_id, f"{_build_base_filename(params, window_label, output_filename_template, end_date)}-no_data.txt")
            txt.write_text("No schools matched the filter.", encoding="utf-8")
            artifacts.append({"filename": txt.name, "size": txt.stat().st_size, "path": str(txt)})
            return artifacts

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

            dfm = _to_dataframe(rows_all)
            if not dfm.empty:
                sort_keys = []
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
            _progress(progress_cb, 92, "NFA: 正在导出按月产物")
            artifacts += _export_df(dfm, job_id, f"{base_name}-monthly", export_formats)
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
                artifacts += _export_df(df_excluded, job_id, f"{base_name}_excluded", export_formats)

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
                    artifacts += _export_df(df_remaining, job_id, f"{base_name}_remaining", export_formats)
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
                artifacts += _export_df(df, job_id, base_name, export_formats)
                return artifacts
            else:
                # 逐校（或逐校按天）- 批量拉取 + 内存分组
                if settlement_mode == 'daily_95_avg':
                    # 先每日95，再根据 export_daily 决定是否求平均
                    rows_daily = c95.process_schools_batched(
                        conn, schools,
                        pd.to_datetime(start_time), pd.to_datetime(end_time),
                        direction, True, batch_size=batch_size, unit_base=unit_base, combine_v4_v6=combine_v4_v6, merge_key=merge_key
                    )
                    if not rows_daily:
                        rows_daily = c95.process_schools(
                            conn, schools,
                            pd.to_datetime(start_time), pd.to_datetime(end_time),
                            direction, True, unit_base=unit_base
                        )
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
                    rows = c95.process_schools_batched(
                        conn, schools,
                        pd.to_datetime(start_time), pd.to_datetime(end_time),
                        direction, export_daily, batch_size=batch_size, unit_base=unit_base, combine_v4_v6=combine_v4_v6, merge_key=merge_key
                    )
                    if not rows:
                        # 回退到原方法
                        rows = c95.process_schools(
                            conn, schools,
                            pd.to_datetime(start_time), pd.to_datetime(end_time),
                            direction, export_daily, unit_base=unit_base
                        )
                    df = _to_dataframe(rows)
                if sortby and sortby in df.columns:
                    df = df.sort_values(by=sortby, ascending=(sort_order == 'asc'))
                _progress(progress_cb, 92, "NFA: 正在导出产物")
                artifacts += _export_df(df, job_id, base_name, export_formats)
                return artifacts
    finally:
        try:
            conn.close()
        except Exception:
            pass
