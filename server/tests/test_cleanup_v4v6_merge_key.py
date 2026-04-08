from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from server.scripts.cleanup_v4v6_merge_key import cleanup_tasks_merge_key


def _init_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE tasks (id INTEGER PRIMARY KEY, name TEXT, params TEXT)")
        cur.executemany(
            "INSERT INTO tasks (id, name, params) VALUES (?, ?, ?)",
            [
                (
                    1,
                    "keep-enabled",
                    json.dumps({"combine_v4_v6": True, "merge_key": "ipgroup_name_base"}, ensure_ascii=False),
                ),
                (
                    2,
                    "clean-disabled",
                    json.dumps({"combine_v4_v6": False, "merge_key": "school_name"}, ensure_ascii=False),
                ),
                (
                    3,
                    "keep-empty",
                    json.dumps({"combine_v4_v6": False, "merge_key": ""}, ensure_ascii=False),
                ),
            ],
        )
        conn.commit()
    finally:
        conn.close()


def test_cleanup_tasks_merge_key_apply(tmp_path):
    db_path = tmp_path / "app.db"
    _init_db(db_path)

    report = cleanup_tasks_merge_key(db_path, apply=True)
    assert report["matched_ids"] == [2]
    assert report["updated_count"] == 1

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT params FROM tasks WHERE id=2")
        params = json.loads(cur.fetchone()[0])
        assert params["combine_v4_v6"] is False
        assert params.get("merge_key", "") == ""
    finally:
        conn.close()
