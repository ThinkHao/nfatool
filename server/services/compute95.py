from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

# Reuse existing script functions (vendored under server/ext)
from ..ext import calculate_95th_percentile as c95

from ..config import get_settings
from .exporter import export_csv, export_xlsx
from .storage import safe_artifact_path


def _render_template(template: str, params: Dict[str, Any], window_label: str, end_date: str | None) -> str:
    province = params.get("province", "province")
    cp = params.get("cp", "cp")
    direction = params.get("direction", "both")
    context = {
        "province": province,
        "cp": cp,
        "direction": direction,
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
    province = params.get("province", "province")
    cp = params.get("cp", "cp")
    direction = params.get("direction", "both")
    return f"{province}-{cp}-{direction}-{window_label}"


def _to_dataframe(results: List[Dict[str, Any]]) -> pd.DataFrame:
    if not results:
        return pd.DataFrame()
    return pd.DataFrame(results)


def _export_df(df: pd.DataFrame, job_id: str, filename_noext: str, export_formats: List[str]) -> List[Dict[str, Any]]:
    artifacts: List[Dict[str, Any]] = []
    if df is None:
        return artifacts
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


def compute_and_export(job_id: str, resolved_window: Dict[str, Any], params: Dict[str, Any], export_formats: List[str] | None, output_filename_template: str | None) -> List[Dict[str, Any]]:
    """Run 95th percentile computation using the existing script functions and export artifacts.

    params accepts the same keys as the original CLI script, except start/end time which come from resolved_window.
    """
    settings = get_settings()

    export_formats = export_formats or ["csv"]

    # Resolve times
    start_time = resolved_window["start_time"]
    end_time = resolved_window["end_time"]
    window_label = resolved_window.get("label") or f"{start_time.split(' ')[0]}-{end_time.split(' ')[0]}"
    end_date = (end_time.split(' ')[0] if isinstance(end_time, str) and ' ' in end_time else str(end_time))

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

    # Prefer env MySQL settings; fallback to db_config.ini if provided in params
    db_cfg = None
    if settings.MYSQL_HOST and settings.MYSQL_USER and settings.MYSQL_PASSWORD and settings.MYSQL_DB:
        db_cfg = {
            'host': settings.MYSQL_HOST,
            'port': settings.MYSQL_PORT or 3306,
            'user': settings.MYSQL_USER,
            'password': settings.MYSQL_PASSWORD,
            'db': settings.MYSQL_DB,
            'charset': settings.MYSQL_CHARSET or 'utf8mb4',
        }
    else:
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
        schools = c95.get_schools_by_province_and_cp(conn, province, cp, school)
        if not schools:
            # 无数据时输出一份说明文件
            txt = safe_artifact_path(job_id, f"{_build_base_filename(params, window_label, output_filename_template, end_date)}-no_data.txt")
            txt.write_text("No schools matched the filter.", encoding="utf-8")
            artifacts.append({"filename": txt.name, "size": txt.stat().st_size, "path": str(txt)})
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
                    # 计算每日95，再按学校平均
                    rows_daily = c95.process_schools(
                        conn, excluded_schools,
                        pd.to_datetime(start_time), pd.to_datetime(end_time),
                        direction, True, unit_base=unit_base
                    )
                    df_daily = _to_dataframe(rows_daily)
                    if export_daily:
                        df_excluded = df_daily
                    else:
                        if not df_daily.empty and 'daily_95th_percentile_mbps' in df_daily.columns:
                            group_cols = ['school_id','ipgroup_name','ipgroup_id','nfa_uuid']
                            df_excluded = (
                                df_daily.groupby(group_cols, as_index=False)['daily_95th_percentile_mbps']
                                        .mean()
                                        .rename(columns={'daily_95th_percentile_mbps': '95th_percentile_mbps'})
                            )
                        else:
                            df_excluded = pd.DataFrame()
                else:
                    # 保持原有逐校行为（也可切换为 batched 版本以提升性能）
                    results_excluded = c95.process_schools(
                        conn, excluded_schools,
                        pd.to_datetime(start_time), pd.to_datetime(end_time),
                        direction, export_daily, unit_base=unit_base
                    )
                    df_excluded = _to_dataframe(results_excluded)
                # 排序
                if sortby and sortby in df_excluded.columns:
                    df_excluded = df_excluded.sort_values(by=sortby, ascending=(sort_order == 'asc'))
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
                            avg_val = float(pd.Series(vals).mean()) if vals else 0.0
                            df_remaining = pd.DataFrame([{
                                'school_id': '',
                                'ipgroup_name': '剩余院校汇总',
                                'ipgroup_id': '',
                                'nfa_uuid': '',
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
                            '95th_percentile_mbps': val,
                            'data_points': len(df_agg),
                            'direction': direction
                        }])
                    # 排序
                    if sortby and sortby in df_remaining.columns:
                        df_remaining = df_remaining.sort_values(by=sortby, ascending=(sort_order == 'asc'))
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
                            avg_val = float(df_daily['daily_95th_percentile_mbps'].mean())
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
                artifacts += _export_df(df, job_id, base_name, export_formats)
                return artifacts
            else:
                # 逐校（或逐校按天）- 批量拉取 + 内存分组
                if settlement_mode == 'daily_95_avg':
                    # 先每日95，再根据 export_daily 决定是否求平均
                    rows_daily = c95.process_schools_batched(
                        conn, schools,
                        pd.to_datetime(start_time), pd.to_datetime(end_time),
                        direction, True, batch_size=batch_size, unit_base=unit_base
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
                            df = (
                                df_daily.groupby(group_cols, as_index=False)['daily_95th_percentile_mbps']
                                       .mean()
                                       .rename(columns={'daily_95th_percentile_mbps': '95th_percentile_mbps'})
                            )
                        else:
                            df = pd.DataFrame()
                else:
                    rows = c95.process_schools_batched(
                        conn, schools,
                        pd.to_datetime(start_time), pd.to_datetime(end_time),
                        direction, export_daily, batch_size=batch_size, unit_base=unit_base
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
                artifacts += _export_df(df, job_id, base_name, export_formats)
                return artifacts
    finally:
        try:
            conn.close()
        except Exception:
            pass
