from __future__ import annotations

from datetime import datetime
import sys
import types

import pandas as pd

if "pymysql" not in sys.modules:
    pymysql_stub = types.SimpleNamespace()
    pymysql_stub.cursors = types.SimpleNamespace(DictCursor=object)
    sys.modules["pymysql"] = pymysql_stub

from server.ext import calculate_95th_percentile as c95


def _fake_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ipgroup_id": 101,
                "nfa_uuid": "u-v4",
                "create_time": datetime(2026, 1, 1, 0, 0, 0),
                "recv": 6000,
                "send": 3000,
            },
            {
                "ipgroup_id": 102,
                "nfa_uuid": "u-v6",
                "create_time": datetime(2026, 1, 1, 0, 0, 0),
                "recv": 12000,
                "send": 6000,
            },
        ]
    )


def _schools() -> list[dict]:
    return [
        {
            "ipgroup_id": 101,
            "nfa_uuid": "u-v4",
            "ipgroup_name": "foo_V4",
            "school_name": "foo",
            "cp": "bilibili",
            "school_id": "s1",
            "saler_group": "g",
            "saler": "a",
        },
        {
            "ipgroup_id": 102,
            "nfa_uuid": "u-v6",
            "ipgroup_name": "foo_V6",
            "school_name": "foo",
            "cp": "bilibili",
            "school_id": "s1",
            "saler_group": "g",
            "saler": "a",
        },
    ]


def test_process_schools_batched_does_not_merge_when_switch_off(monkeypatch):
    monkeypatch.setattr(c95, "fetch_speed_data_for_pairs_raw", lambda *args, **kwargs: _fake_df())

    out = c95.process_schools_batched(
        connection=None,
        schools=_schools(),
        start_time="2026-01-01 00:00:00",
        end_time="2026-01-01 00:05:00",
        direction="recv",
        export_daily=False,
        combine_v4_v6=False,
        merge_key="ipgroup_name_base",
    )

    assert len(out) == 2
    assert {r["ipgroup_name"] for r in out} == {"foo_V4", "foo_V6"}


def test_process_schools_batched_merges_when_switch_on(monkeypatch):
    monkeypatch.setattr(c95, "fetch_speed_data_for_pairs_raw", lambda *args, **kwargs: _fake_df())

    out = c95.process_schools_batched(
        connection=None,
        schools=_schools(),
        start_time="2026-01-01 00:00:00",
        end_time="2026-01-01 00:05:00",
        direction="recv",
        export_daily=False,
        combine_v4_v6=True,
        merge_key="ipgroup_name_base",
    )

    assert len(out) == 1
    assert out[0]["ipgroup_name"] == "foo"
