from __future__ import annotations

import importlib
from pathlib import Path

import pytest

TestClient = pytest.importorskip("fastapi.testclient").TestClient


def _load_app_with_temp_db(tmp_path: Path, monkeypatch):
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
    client = TestClient(main.app)
    return client


def test_group_create_and_assign_api(tmp_path, monkeypatch):
    client = _load_app_with_temp_db(tmp_path, monkeypatch)
    try:
        r = client.post("/api/tasks/groups", json={"name": "G1"})
        assert r.status_code == 200

        r = client.get("/api/tasks/groups")
        assert r.status_code == 200
        assert "G1" in (r.json().get("items") or [])

        payload = {
            "name": "task-1",
            "group_name": "G1",
            "kind": "one_off",
            "params": {},
        }
        r = client.post("/api/tasks", json=payload)
        assert r.status_code == 200
        tid = int(r.json()["id"])

        r = client.patch(f"/api/tasks/{tid}/group", json={"group_name": ""})
        assert r.status_code == 200
        assert r.json().get("group_name") is None
    finally:
        client.close()


def test_group_rename_merge_and_delete_api(tmp_path, monkeypatch):
    client = _load_app_with_temp_db(tmp_path, monkeypatch)
    try:
        assert client.post("/api/tasks/groups", json={"name": "A"}).status_code == 200
        assert client.post("/api/tasks/groups", json={"name": "B"}).status_code == 200

        payload = {
            "name": "task-a",
            "group_name": "A",
            "kind": "one_off",
            "params": {},
        }
        r = client.post("/api/tasks", json=payload)
        assert r.status_code == 200
        tid = int(r.json()["id"])

        r = client.patch("/api/tasks/groups/rename", json={"old_name": "A", "new_name": "B", "merge": False})
        assert r.status_code == 409

        r = client.patch("/api/tasks/groups/rename", json={"old_name": "A", "new_name": "B", "merge": True})
        assert r.status_code == 200
        assert int(r.json().get("updated_tasks") or 0) >= 1

        r = client.get("/api/tasks/page?task_group=B")
        assert r.status_code == 200
        ids = [int(x["id"]) for x in (r.json().get("items") or [])]
        assert tid in ids

        r = client.delete("/api/tasks/groups?name=B")
        assert r.status_code == 200

        r = client.get(f"/api/tasks/{tid}")
        assert r.status_code == 200
        assert r.json().get("group_name") is None
    finally:
        client.close()
