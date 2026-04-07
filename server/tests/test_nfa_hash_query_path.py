from __future__ import annotations

from datetime import datetime
import sys
import types

if "pymysql" not in sys.modules:
    pymysql_stub = types.SimpleNamespace()
    pymysql_stub.cursors = types.SimpleNamespace(DictCursor=object)
    sys.modules["pymysql"] = pymysql_stub
from server.ext import calculate_95th_percentile as c95


class _FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=()):
        s = " ".join(str(sql).split())
        self.conn.sqls.append(s)
        self.conn.params.append(tuple(params) if params is not None else tuple())

        if "SELECT create_time, recv, send FROM nfa_ip_group_speed_logs_5m WHERE hash_uuid" in s:
            self._rows = list(self.conn.speed_rows_hash)
            return
        if "SELECT create_time, recv, send FROM nfa_ip_group_speed_logs_5m WHERE ipgroup_id" in s:
            self._rows = list(self.conn.speed_rows_pair)
            return

        if "SELECT ipgroup_id, nfa_uuid, create_time, recv, send" in s and "hash_uuid IN" in s:
            self._rows = list(self.conn.hash_raw_rows)
            return
        if "SELECT ipgroup_id, nfa_uuid, create_time, recv, send" in s and "(ipgroup_id, nfa_uuid) IN" in s:
            self._rows = list(self.conn.pair_raw_rows)
            return

        if "SELECT DISTINCT ipgroup_id, nfa_uuid" in s and "hash_uuid IN" in s:
            self._rows = list(self.conn.hash_filter_rows)
            return
        if "SELECT DISTINCT ipgroup_id, nfa_uuid" in s and "(ipgroup_id, nfa_uuid) IN" in s:
            self._rows = list(self.conn.pair_filter_rows)
            return

        if "GROUP BY create_time" in s and "hash_uuid IN" in s:
            self._rows = list(self.conn.hash_agg_rows)
            return
        if "GROUP BY create_time" in s and "(ipgroup_id, nfa_uuid) IN" in s:
            self._rows = list(self.conn.pair_agg_rows)
            return

        self._rows = []

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeConn:
    def __init__(self):
        self.sqls = []
        self.params = []
        self.speed_rows_hash = []
        self.speed_rows_pair = []
        self.hash_raw_rows = []
        self.pair_raw_rows = []
        self.hash_filter_rows = []
        self.pair_filter_rows = []
        self.hash_agg_rows = []
        self.pair_agg_rows = []

    def cursor(self):
        return _FakeCursor(self)


def test_get_speed_data_prefers_hash_uuid_query():
    conn = _FakeConn()
    conn.speed_rows_hash = [{"create_time": datetime(2026, 1, 1, 0, 0), "recv": 1, "send": 2}]

    rows = c95.get_speed_data(conn, 1, "u1", "2026-01-01 00:00:00", "2026-01-01 00:05:00", hash_uuid="h1")

    assert len(rows) == 1
    assert any("hash_uuid = %s" in s for s in conn.sqls)
    assert not any("ipgroup_id = %s AND nfa_uuid = %s" in s for s in conn.sqls)


def test_fetch_speed_data_for_pairs_raw_hash_path():
    conn = _FakeConn()
    conn.hash_raw_rows = [
        {"ipgroup_id": 1, "nfa_uuid": "u1", "create_time": datetime(2026, 1, 1, 0, 0), "recv": 10, "send": 2},
        {"ipgroup_id": 1, "nfa_uuid": "u1", "create_time": datetime(2026, 1, 1, 0, 5), "recv": 12, "send": 3},
    ]

    df = c95.fetch_speed_data_for_pairs_raw(
        conn,
        [{"ipgroup_id": 1, "nfa_uuid": "u1", "hash_uuid": "h1"}],
        "2026-01-01 00:00:00",
        "2026-01-01 00:10:00",
        batch_size=200,
    )

    assert not df.empty
    assert any("hash_uuid IN" in s for s in conn.sqls)
    assert not any("(ipgroup_id, nfa_uuid) IN" in s for s in conn.sqls)


def test_filter_pairs_with_data_mixed_hash_and_pair_fallback():
    conn = _FakeConn()
    conn.hash_filter_rows = [{"ipgroup_id": 1, "nfa_uuid": "u1"}]
    conn.pair_filter_rows = [{"ipgroup_id": 2, "nfa_uuid": "u2"}]

    matched = c95.filter_pairs_with_data(
        conn,
        [
            {"ipgroup_id": 1, "nfa_uuid": "u1", "hash_uuid": "h1"},
            {"ipgroup_id": 2, "nfa_uuid": "u2", "hash_uuid": None},
        ],
        "2026-01-01 00:00:00",
        "2026-01-02 00:00:00",
        batch_size=100,
    )

    assert matched == {(1, "u1"), (2, "u2")}
    assert any("hash_uuid IN" in s for s in conn.sqls)
    assert any("(ipgroup_id, nfa_uuid) IN" in s for s in conn.sqls)


def test_aggregate_speed_data_for_pairs_db_mixed_paths_are_merged():
    conn = _FakeConn()
    conn.hash_agg_rows = [
        {"create_time": datetime(2026, 1, 1, 0, 0), "recv": 100, "send": 10},
    ]
    conn.pair_agg_rows = [
        {"create_time": datetime(2026, 1, 1, 0, 0), "recv": 50, "send": 5},
        {"create_time": datetime(2026, 1, 1, 0, 5), "recv": 20, "send": 2},
    ]

    df = c95.aggregate_speed_data_for_pairs_db(
        conn,
        [
            {"ipgroup_id": 1, "nfa_uuid": "u1", "hash_uuid": "h1"},
            {"ipgroup_id": 2, "nfa_uuid": "u2", "hash_uuid": None},
        ],
        "2026-01-01 00:00:00",
        "2026-01-01 00:10:00",
    )

    assert not df.empty
    row0 = df[df["create_time"] == datetime(2026, 1, 1, 0, 0)].iloc[0]
    assert float(row0["recv"]) == 150.0
    assert float(row0["send"]) == 15.0
    assert any("hash_uuid IN" in s for s in conn.sqls)
    assert any("(ipgroup_id, nfa_uuid) IN" in s for s in conn.sqls)
