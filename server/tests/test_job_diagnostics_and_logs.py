from __future__ import annotations

import asyncio
import importlib
import json
import sys
import types
from pathlib import Path

import pandas as pd
import pytest
try:
    from fastapi.testclient import TestClient
except ModuleNotFoundError:  # pragma: no cover
    TestClient = None

# Some CI/dev environments may not have DB connector deps installed.
if "pymysql" not in sys.modules:
    pymysql_stub = types.SimpleNamespace()
    pymysql_stub.cursors = types.SimpleNamespace(DictCursor=object)
    pymysql_stub.connect = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("pymysql stub"))  # pragma: no cover
    sys.modules["pymysql"] = pymysql_stub
if "paramiko" not in sys.modules:
    paramiko_stub = types.SimpleNamespace(Transport=types.SimpleNamespace(_preferred_pubkeys=()))
    sys.modules["paramiko"] = paramiko_stub

from server.services import compute95
from server.services.compute95 import _export_df, _make_terminal_no_data_artifacts, _nfa_raw_dataframe
from server.services.unit_conversion import mbps_to_raw
from server.ext.calculate_95th_percentile import save_results


def _reload_runtime(monkeypatch, tmp_path: Path):
    db_path = tmp_path / "app.db"
    storage_dir = tmp_path / "storage"
    log_dir = tmp_path / "logs"
    monkeypatch.setenv("SQLITE_URL", f"sqlite:///{db_path.as_posix()}")
    monkeypatch.setenv("STORAGE_DIR", str(storage_dir))
    monkeypatch.setenv("LOG_DIR", str(log_dir))

    import server.config as config
    import server.db as db
    import server.main as main
    import server.models as models

    config = importlib.reload(config)
    config.get_settings.cache_clear()
    db = importlib.reload(db)
    models = importlib.reload(models)
    main = importlib.reload(main)
    db.init_db()
    return db, models, main


def test_no_data_artifacts_include_diagnostics(monkeypatch, tmp_path):
    import server.config as config

    monkeypatch.setenv("STORAGE_DIR", str(tmp_path / "storage"))
    config.get_settings.cache_clear()

    artifacts = _make_terminal_no_data_artifacts(
        "job-1",
        "demo",
        "NFA 无匹配流量数据",
        source_type="nfa",
        source_instance="default",
        resolved_window={"start_time": "2026-03-01 00:00:00", "end_time": "2026-03-31 23:59:59", "label": "2026-03"},
        key_params={"province": "广东省", "cp": "ali"},
        counters={"matched_schools": 0},
    )

    names = {x["filename"] for x in artifacts}
    assert "demo-no_data.txt" in names
    assert "demo-diagnostics.json" in names

    diag = next(x for x in artifacts if x["filename"].endswith("-diagnostics.json"))
    payload = json.loads(Path(diag["path"]).read_text(encoding="utf-8"))
    assert payload["terminal_code"] == "NO_MATCH"
    assert payload["source_type"] == "nfa"
    assert payload["key_params"]["cp"] == "ali"


def test_export_df_empty_adds_empty_result_diagnostics(monkeypatch, tmp_path):
    import server.config as config

    monkeypatch.setenv("STORAGE_DIR", str(tmp_path / "storage"))
    config.get_settings.cache_clear()

    df = pd.DataFrame(columns=["school_id", "ipgroup_name"])
    artifacts = _export_df(
        df,
        "job-2",
        "empty-demo",
        ["csv"],
        empty_terminal={
            "reason": "结果为空：测试",
            "source_type": "nfa",
            "source_instance": "default",
            "resolved_window": {"start_time": "2026-03-01 00:00:00", "end_time": "2026-03-31 23:59:59", "label": "2026-03"},
            "key_params": {"province": "广东省", "cp": "ali"},
            "counters": {"matched_schools": 10},
        },
    )

    names = {x["filename"] for x in artifacts}
    assert "empty-demo.csv" in names
    assert "empty-demo-empty_result.txt" in names
    assert "empty-demo-diagnostics.json" in names
    marker = next(x for x in artifacts if x["filename"].endswith("-empty_result.txt"))
    assert marker["terminal_code"] == "EMPTY_RESULT"


def test_export_df_raw_adds_both_source_unit_conversions(monkeypatch, tmp_path):
    import server.config as config

    monkeypatch.setenv("STORAGE_DIR", str(tmp_path / "storage"))
    config.get_settings.cache_clear()

    df = pd.DataFrame([{"create_time": "2026-01-01 00:00:00", "recv": 60_000_000, "send": 30_000_000}])
    artifacts = _export_df(
        df,
        "job-raw",
        "nfa-raw",
        ["csv"],
        flow_context={"source_type": "nfa", "unit_base": 1024, "raw_export": True},
    )
    csv_path = next(x["path"] for x in artifacts if x["filename"].endswith(".csv"))
    out_df = pd.read_csv(csv_path)

    assert out_df.loc[0, "recv"] == 60_000_000
    assert out_df.loc[0, "recv_mbps_1000"] == pytest.approx(8.0)
    assert out_df.loc[0, "recv_mbps_1024"] == pytest.approx(7.62939453125)
    assert "send_mbps_1000" in out_df.columns
    assert "send_mbps_1024" in out_df.columns


def test_nfa_raw_dataframe_preserves_rows_and_merges_metadata(monkeypatch):
    raw = pd.DataFrame([
        {"ipgroup_id": 1, "nfa_uuid": "u1", "create_time": "2026-01-01 00:00:00", "recv": 10, "send": 2},
        {"ipgroup_id": 2, "nfa_uuid": "u2", "create_time": "2026-01-01 00:00:00", "recv": 20, "send": 3},
    ])
    monkeypatch.setattr(compute95.c95, "fetch_speed_data_for_pairs_raw", lambda *args, **kwargs: raw.copy())
    schools = [
        {"ipgroup_id": 1, "nfa_uuid": "u1", "hash_uuid": "h1", "ipgroup_name": "A_V4", "school_name": "A"},
        {"ipgroup_id": 2, "nfa_uuid": "u2", "hash_uuid": "h2", "ipgroup_name": "A_V6", "school_name": "A"},
    ]

    out = _nfa_raw_dataframe(
        object(), schools, "2026-01-01 00:00:00", "2026-01-01 00:05:00",
        batch_size=200, combine_v4_v6=False, merge_key=None,
    )
    assert len(out) == 2
    assert set(out["ipgroup_name"]) == {"A_V4", "A_V6"}
    assert set(out["hash_uuid"]) == {"h1", "h2"}

    merged = _nfa_raw_dataframe(
        object(), schools, "2026-01-01 00:00:00", "2026-01-01 00:05:00",
        batch_size=200, combine_v4_v6=True, merge_key="ipgroup_name_base",
    )
    assert len(merged) == 1
    assert merged.loc[0, "ipgroup_name"] == "A"
    assert merged.loc[0, "recv"] == 30
    assert merged.loc[0, "send"] == 5


def test_export_df_nfa_daily_formats_flow_and_adds_raw_column(monkeypatch, tmp_path):
    import server.config as config

    monkeypatch.setenv("STORAGE_DIR", str(tmp_path / "storage"))
    config.get_settings.cache_clear()

    df = pd.DataFrame([
        {"ipgroup_name": "A", "daily_95th_percentile_mbps": 9329.581960042318},
    ])
    artifacts = _export_df(
        df,
        "job-nfa-daily",
        "nfa-daily",
        ["csv"],
        flow_context={"source_type": "nfa", "unit_base": 1024},
    )
    csv_path = next(x["path"] for x in artifacts if x["filename"].endswith(".csv"))
    out_df = pd.read_csv(csv_path, dtype=str)

    assert "daily_95th_percentile_mbps" in out_df.columns
    assert "daily_95th_percentile_raw" in out_df.columns
    assert out_df.columns.get_loc("daily_95th_percentile_raw") < out_df.columns.get_loc("daily_95th_percentile_mbps")
    assert out_df.loc[0, "daily_95th_percentile_mbps"] == "9329.582"
    expected_raw = f"{mbps_to_raw(9329.581960042318, 1024, 60):.3f}"
    assert out_df.loc[0, "daily_95th_percentile_raw"] == expected_raw


def test_export_df_nfa_period_formats_flow_and_adds_raw_column(monkeypatch, tmp_path):
    import server.config as config

    monkeypatch.setenv("STORAGE_DIR", str(tmp_path / "storage"))
    config.get_settings.cache_clear()

    df = pd.DataFrame([
        {"ipgroup_name": "A", "95th_percentile_mbps": 18847.703043619793},
    ])
    artifacts = _export_df(
        df,
        "job-nfa-period",
        "nfa-period",
        ["csv"],
        flow_context={"source_type": "nfa", "unit_base": 1024},
    )
    csv_path = next(x["path"] for x in artifacts if x["filename"].endswith(".csv"))
    out_df = pd.read_csv(csv_path, dtype=str)

    assert "95th_percentile_mbps" in out_df.columns
    assert "95th_percentile_raw" in out_df.columns
    assert out_df.columns.get_loc("95th_percentile_raw") < out_df.columns.get_loc("95th_percentile_mbps")
    assert out_df.loc[0, "95th_percentile_mbps"] == "18847.703"
    expected_raw = f"{mbps_to_raw(18847.703043619793, 1024, 60):.3f}"
    assert out_df.loc[0, "95th_percentile_raw"] == expected_raw


def test_export_df_edc_keeps_existing_raw_value_and_only_formats(monkeypatch, tmp_path):
    import server.config as config

    monkeypatch.setenv("STORAGE_DIR", str(tmp_path / "storage"))
    config.get_settings.cache_clear()

    df = pd.DataFrame([
        {
            "edc_name": "x",
            "daily_95th_percentile_mbps": 1.0,
            "daily_95th_percentile_raw": 999.0,
        },
    ])
    artifacts = _export_df(
        df,
        "job-edc",
        "edc-daily",
        ["csv"],
        flow_context={"source_type": "edc", "unit_base": 1024},
    )
    csv_path = next(x["path"] for x in artifacts if x["filename"].endswith(".csv"))
    out_df = pd.read_csv(csv_path, dtype=str)

    assert out_df.loc[0, "daily_95th_percentile_mbps"] == "1.000"
    assert out_df.loc[0, "daily_95th_percentile_raw"] == "999.000"


def test_export_df_flow_format_tolerates_empty_and_invalid_values(monkeypatch, tmp_path):
    import server.config as config

    monkeypatch.setenv("STORAGE_DIR", str(tmp_path / "storage"))
    config.get_settings.cache_clear()

    df = pd.DataFrame([
        {"95th_percentile_mbps": None},
        {"95th_percentile_mbps": "abc"},
        {"95th_percentile_mbps": float("nan")},
        {"95th_percentile_mbps": 1.2},
    ])
    artifacts = _export_df(
        df,
        "job-invalid",
        "invalid-flow",
        ["csv"],
        flow_context={"source_type": "nfa", "unit_base": 1024},
    )
    csv_path = next(x["path"] for x in artifacts if x["filename"].endswith(".csv"))
    out_df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)

    assert out_df.loc[0, "95th_percentile_mbps"] == ""
    assert out_df.loc[1, "95th_percentile_mbps"] == ""
    assert out_df.loc[2, "95th_percentile_mbps"] == ""
    assert out_df.loc[3, "95th_percentile_mbps"] == "1.200"

    assert out_df.loc[0, "95th_percentile_raw"] == ""
    assert out_df.loc[1, "95th_percentile_raw"] == ""
    assert out_df.loc[2, "95th_percentile_raw"] == ""
    assert out_df.loc[3, "95th_percentile_raw"] != ""


def test_save_results_default_sort_groups_v4_v6_by_school_name(tmp_path):
    output_path = tmp_path / "grouped.csv"
    results = [
        {"ipgroup_name": "Beta_V6", "95th_percentile_mbps": 4.0},
        {"ipgroup_name": "Alpha_V6", "95th_percentile_mbps": 3.0},
        {"ipgroup_name": "Beta_V4", "95th_percentile_mbps": 2.0},
        {"ipgroup_name": "Alpha_V4", "95th_percentile_mbps": 1.0},
    ]

    save_results(
        results,
        str(output_path),
        is_daily=False,
        direction="recv",
        start_time="2026-01-01 00:00:00",
        end_time="2026-01-31 23:59:59",
    )

    out_df = pd.read_csv(output_path, dtype=str)
    assert out_df["ipgroup_name"].tolist() == [
        "Alpha_V4",
        "Alpha_V6",
        "Beta_V4",
        "Beta_V6",
    ]


def test_export_df_default_sort_groups_nfa_daily_v4_v6_rows(monkeypatch, tmp_path):
    import server.config as config

    monkeypatch.setenv("STORAGE_DIR", str(tmp_path / "storage"))
    config.get_settings.cache_clear()

    df = pd.DataFrame([
        {"school_icipgroup_name": "南京理工大学_B站", "ipgroup_name": "南京理工大学_B站_V6", "date": "2025-12-01", "daily_95th_percentile_mbps": 1.0},
        {"school_icipgroup_name": "南京理工大学_B站", "ipgroup_name": "南京理工大学_B站_V4", "date": "2025-12-01", "daily_95th_percentile_mbps": 2.0},
        {"school_icipgroup_name": "南京理工大学_B站", "ipgroup_name": "南京理工大学_B站_V6", "date": "2025-12-02", "daily_95th_percentile_mbps": 3.0},
        {"school_icipgroup_name": "南京理工大学_B站", "ipgroup_name": "南京理工大学_B站_V4", "date": "2025-12-02", "daily_95th_percentile_mbps": 4.0},
    ])

    artifacts = _export_df(
        df,
        "job-sort-daily",
        "nfa-daily-sort",
        ["csv"],
        flow_context={"source_type": "nfa", "unit_base": 1024, "combine_v4_v6": False},
    )
    csv_path = next(x["path"] for x in artifacts if x["filename"].endswith(".csv"))
    out_df = pd.read_csv(csv_path, dtype=str)

    assert out_df["ipgroup_name"].tolist() == [
        "南京理工大学_B站_V4",
        "南京理工大学_B站_V4",
        "南京理工大学_B站_V6",
        "南京理工大学_B站_V6",
    ]
    assert out_df["date"].tolist() == [
        "2025-12-01",
        "2025-12-02",
        "2025-12-01",
        "2025-12-02",
    ]


def test_job_log_tail_api(monkeypatch, tmp_path):
    if TestClient is None:
        pytest.skip("fastapi is not installed")
    db, models, main = _reload_runtime(monkeypatch, tmp_path)
    from server.services.logger import get_job_log_path

    client = TestClient(main.app)
    try:
        job_id = "job-log-tail"
        with db.session_scope() as s:
            s.add(models.JobRun(
                id=job_id,
                task_id=None,
                status="succeeded",
                progress_pct=100,
                progress_stage="done",
                resolved_window="{}",
                resolved_params="{}",
                artifacts="[]",
            ))

        p = get_job_log_path(job_id)
        p.write_text("line1\nline2\nline3\n", encoding="utf-8")

        res = client.get(f"/api/jobs/{job_id}/log-tail?lines=2")
        assert res.status_code == 200
        data = res.json()
        assert "line2" in data["content"]
        assert "line3" in data["content"]
        assert "line1" not in data["content"]

        res_dl = client.get(f"/api/jobs/{job_id}/log")
        assert res_dl.status_code == 200
        assert "line1" in res_dl.text

        res_missing = client.get("/api/jobs/job-not-exists/log-tail")
        assert res_missing.status_code == 404
    finally:
        client.close()


class _FakeNfaConn:
    def close(self):
        return None


def _nfa_params(monthly_aggregate: bool = True) -> dict:
    return {
        "province": "山东省",
        "cp": "bilibili",
        "school": "长清大学城",
        "direction": "recv",
        "monthly_aggregate": monthly_aggregate,
        "aggregate_all": False,
        "batch_size": 200,
        "data_source_type": "nfa",
        "data_source_instance": "default",
        "db_config": {
            "host": "fake",
            "port": 3306,
            "user": "fake",
            "password": "fake",
            "db": "fake",
        },
    }


def _window() -> dict:
    return {
        "start_time": "2026-02-01 00:00:00",
        "end_time": "2026-03-31 23:59:59",
        "label": "2026-02-01-2026-03-31",
    }


def test_monthly_probe_failure_returns_query_timeout(monkeypatch, tmp_path):
    import server.config as config

    monkeypatch.setenv("STORAGE_DIR", str(tmp_path / "storage"))
    config.get_settings.cache_clear()

    captured = {"first_target_type": ""}
    monkeypatch.setattr(compute95.c95, "connect_to_db", lambda cfg: _FakeNfaConn())
    monkeypatch.setattr(
        compute95.c95,
        "get_schools_by_province_and_cp",
        lambda conn, province, cp, school: [
            {"ipgroup_id": 2500196, "nfa_uuid": "u1", "hash_uuid": "h1", "school_name": "长清大学城"}
        ],
    )

    def _fake_filter(conn, targets, start, end, batch_size=300):
        captured["first_target_type"] = type(targets[0]).__name__ if targets else ""
        return {(2500196, "u1")}

    monkeypatch.setattr(compute95.c95, "filter_pairs_with_data", _fake_filter)
    monkeypatch.setattr(compute95.c95, "process_schools_batched", lambda *args, **kwargs: [])

    def _raise_timeout(*args, **kwargs):
        raise TimeoutError("query timeout")

    monkeypatch.setattr(compute95, "_nfa_has_any_raw_points", _raise_timeout)

    artifacts = compute95.compute_and_export(
        "job-timeout",
        _window(),
        _nfa_params(monthly_aggregate=True),
        ["csv"],
        None,
    )

    assert captured["first_target_type"] == "dict"
    marker = next(x for x in artifacts if x.get("terminal_code"))
    assert marker["terminal_code"] == "QUERY_TIMEOUT"
    diag = next(x for x in artifacts if x["filename"].endswith("-diagnostics.json"))
    payload = json.loads(Path(diag["path"]).read_text(encoding="utf-8"))
    assert payload["terminal_code"] == "QUERY_TIMEOUT"
    assert payload["probe_status"] == "probe_failed"
    assert payload["exception_type"] == "TimeoutError"
    assert payload["query_path"] == "hash_uuid"


def test_monthly_query_failure_skips_probe_and_preserves_root_cause(monkeypatch, tmp_path):
    import server.config as config

    monkeypatch.setenv("STORAGE_DIR", str(tmp_path / "storage"))
    config.get_settings.cache_clear()

    probe_called = {"value": False}

    monkeypatch.setattr(compute95.c95, "connect_to_db", lambda cfg: _FakeNfaConn())
    monkeypatch.setattr(
        compute95.c95,
        "get_schools_by_province_and_cp",
        lambda conn, province, cp, school: [
            {"ipgroup_id": 2500196, "nfa_uuid": "u1", "hash_uuid": "h1", "school_name": "长清大学城"}
        ],
    )
    monkeypatch.setattr(compute95.c95, "filter_pairs_with_data", lambda *args, **kwargs: {(2500196, "u1")})
    monkeypatch.setattr(
        compute95.c95,
        "process_schools_batched",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            compute95.c95.NfaBatchQueryError("monthly_compute", "hash_uuid", TimeoutError("query timeout"))
        ),
    )

    def _probe_should_not_run(*args, **kwargs):
        probe_called["value"] = True
        raise AssertionError("probe should not run after batch query failure")

    monkeypatch.setattr(compute95, "_nfa_has_any_raw_points", _probe_should_not_run)

    artifacts = compute95.compute_and_export(
        "job-monthly-query-failed",
        _window(),
        _nfa_params(monthly_aggregate=True),
        ["csv"],
        None,
    )

    assert probe_called["value"] is False
    marker = next(x for x in artifacts if x.get("terminal_code"))
    assert marker["terminal_code"] == "QUERY_TIMEOUT"
    diag = next(x for x in artifacts if x["filename"].endswith("-diagnostics.json"))
    payload = json.loads(Path(diag["path"]).read_text(encoding="utf-8"))
    assert payload["terminal_code"] == "QUERY_TIMEOUT"
    assert payload["failure_stage"] == "monthly_compute"
    assert payload["failure_query_path"] == "hash_uuid"
    assert payload["exception_type"] == "TimeoutError"
    assert "probe_status" not in payload


def test_period_query_failure_skips_probe_and_preserves_root_cause(monkeypatch, tmp_path):
    import server.config as config

    monkeypatch.setenv("STORAGE_DIR", str(tmp_path / "storage"))
    config.get_settings.cache_clear()

    probe_called = {"value": False}

    monkeypatch.setattr(compute95.c95, "connect_to_db", lambda cfg: _FakeNfaConn())
    monkeypatch.setattr(
        compute95.c95,
        "get_schools_by_province_and_cp",
        lambda conn, province, cp, school: [
            {"ipgroup_id": 2500196, "nfa_uuid": "u1", "hash_uuid": "h1", "school_name": "长清大学城"}
        ],
    )
    monkeypatch.setattr(compute95.c95, "filter_pairs_with_data", lambda *args, **kwargs: {(2500196, "u1")})
    monkeypatch.setattr(
        compute95.c95,
        "process_schools_batched",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            compute95.c95.NfaBatchQueryError("raw_fetch", "hash_uuid", RuntimeError("interface gone"))
        ),
    )

    def _probe_should_not_run(*args, **kwargs):
        probe_called["value"] = True
        raise AssertionError("probe should not run after batch query failure")

    monkeypatch.setattr(compute95, "_nfa_has_any_raw_points", _probe_should_not_run)

    artifacts = compute95.compute_and_export(
        "job-period-query-failed",
        _window(),
        _nfa_params(monthly_aggregate=False),
        ["csv"],
        None,
    )

    assert probe_called["value"] is False
    marker = next(x for x in artifacts if x.get("terminal_code"))
    assert marker["terminal_code"] == "QUERY_FAILED"
    diag = next(x for x in artifacts if x["filename"].endswith("-diagnostics.json"))
    payload = json.loads(Path(diag["path"]).read_text(encoding="utf-8"))
    assert payload["terminal_code"] == "QUERY_FAILED"
    assert payload["failure_stage"] == "raw_fetch"
    assert payload["failure_query_path"] == "hash_uuid"
    assert payload["exception_type"] == "RuntimeError"
    assert "probe_status" not in payload


def test_monthly_true_empty_keeps_empty_result(monkeypatch, tmp_path):
    import server.config as config

    monkeypatch.setenv("STORAGE_DIR", str(tmp_path / "storage"))
    config.get_settings.cache_clear()

    monkeypatch.setattr(compute95.c95, "connect_to_db", lambda cfg: _FakeNfaConn())
    monkeypatch.setattr(
        compute95.c95,
        "get_schools_by_province_and_cp",
        lambda conn, province, cp, school: [
            {"ipgroup_id": 2500196, "nfa_uuid": "u1", "hash_uuid": "h1", "school_name": "长清大学城"}
        ],
    )
    monkeypatch.setattr(compute95.c95, "filter_pairs_with_data", lambda *args, **kwargs: {(2500196, "u1")})
    monkeypatch.setattr(compute95.c95, "process_schools_batched", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        compute95,
        "_nfa_has_any_raw_points",
        lambda *args, **kwargs: {"has_points": False, "query_path": "hash_uuid", "query_elapsed_ms": 12},
    )

    artifacts = compute95.compute_and_export(
        "job-empty",
        _window(),
        _nfa_params(monthly_aggregate=True),
        ["csv"],
        None,
    )

    marker = next(x for x in artifacts if x.get("terminal_code"))
    assert marker["terminal_code"] == "EMPTY_RESULT"
    diag = next(x for x in artifacts if x["filename"].endswith("-diagnostics.json"))
    payload = json.loads(Path(diag["path"]).read_text(encoding="utf-8"))
    assert payload["terminal_code"] == "EMPTY_RESULT"
    assert payload["query_path"] == "hash_uuid"
    assert payload["probe_status"] == "no_raw_points"


def test_scheduler_marks_query_terminal_code_as_failed(monkeypatch, tmp_path):
    if TestClient is None:
        pytest.skip("fastapi is not installed")
    db, models, _main = _reload_runtime(monkeypatch, tmp_path)
    import server.services.scheduler as scheduler
    scheduler = importlib.reload(scheduler)

    job_id = "job-query-terminal-failed"
    with db.session_scope() as s:
        s.add(models.JobRun(
            id=job_id,
            task_id=None,
            status="pending",
            progress_pct=0,
            progress_stage="等待执行",
            resolved_window=json.dumps(_window(), ensure_ascii=False),
            resolved_params=json.dumps({"data_source_type": "nfa", "data_source_instance": "default"}, ensure_ascii=False),
            artifacts="[]",
        ))

    monkeypatch.setattr(
        scheduler,
        "compute_and_export",
        lambda *args, **kwargs: [
            {
                "filename": "x-query_failed.txt",
                "size": 1,
                "path": str(tmp_path / "x-query_failed.txt"),
                "terminal_code": "QUERY_TIMEOUT",
                "terminal_reason": "数据库查询超时",
            }
        ],
    )

    asyncio.run(scheduler._execute_job(job_id))

    with db.session_scope() as s:
        run = s.get(models.JobRun, job_id)
        assert run is not None
        assert run.status == "failed"
        assert run.error_message == "数据库查询超时"
