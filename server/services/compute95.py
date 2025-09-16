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
                results_excluded = c95.process_schools(conn, excluded_schools, pd.to_datetime(start_time), pd.to_datetime(end_time), direction, export_daily)
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
                    if export_daily:
                        df_agg['date'] = df_agg['create_time'].dt.date
                        rows: List[Dict[str, Any]] = []
                        for date_obj, group in df_agg.groupby('date'):
                            val = c95.calculate_95th_percentile(group.to_dict('records'), direction)
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
                        val = c95.calculate_95th_percentile(df_agg.to_dict('records'), direction)
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
            # 单组逐校
            results = c95.process_schools(conn, schools, pd.to_datetime(start_time), pd.to_datetime(end_time), direction, export_daily)
            df = _to_dataframe(results)
            if sortby and sortby in df.columns:
                df = df.sort_values(by=sortby, ascending=(sort_order == 'asc'))
            base_name = _build_base_filename(params, window_label, output_filename_template, end_date)
            artifacts += _export_df(df, job_id, base_name, export_formats)
            return artifacts
    finally:
        try:
            conn.close()
        except Exception:
            pass
