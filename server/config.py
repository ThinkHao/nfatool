from __future__ import annotations

import json
import sqlite3
import base64
import hashlib
import secrets
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

from pydantic_settings import BaseSettings, SettingsConfigDict
import sys


def _app_base_dir() -> Path:
    # If packaged by PyInstaller --onefile, use the executable directory
    if getattr(sys, 'frozen', False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


BASE_DIR = _app_base_dir()
_RUNTIME_CONFIG_KEY_OVERRIDE: Optional[str] = None


class Settings(BaseSettings):
    # Security
    API_KEY: Optional[str] = None  # If None, auth is disabled

    # General
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    TIMEZONE: str = "Asia/Shanghai"
    RETENTION_DAYS: int = 30
    CONCURRENCY_LIMIT: int = 3

    # Storage
    STORAGE_DIR: Optional[str] = None
    LOG_DIR: Optional[str] = None

    # SQLite
    SQLITE_URL: Optional[str] = None

    # MySQL for compute95 (optional, for future integration)
    MYSQL_HOST: Optional[str] = None
    MYSQL_PORT: Optional[int] = 3306
    MYSQL_USER: Optional[str] = None
    MYSQL_PASSWORD: Optional[str] = None
    MYSQL_DB: Optional[str] = None
    MYSQL_CHARSET: str = "utf8mb4"

    # Multi-instance configs (JSON object). Example:
    # {"prod":{"host":"127.0.0.1","port":3306,"user":"u","password":"p","db":"d","charset":"utf8mb4"}}
    NFA_INSTANCES_JSON: Optional[str] = None
    EDC_INSTANCES_JSON: Optional[str] = None
    CONFIG_ENCRYPTION_KEY: Optional[str] = None
    CONFIG_AUTO_ROTATE_ENABLED: bool = True
    CONFIG_AUTO_ROTATE_DAYS: int = 30
    APP_VERSION: str = "0.1.0"
    GITHUB_REPO: Optional[str] = None  # owner/repo
    UPDATE_ASSET_LINUX: str = "nfa95"
    UPDATE_ASSET_WINDOWS: str = "nfa95.exe"
    UPDATE_CA_BUNDLE: Optional[str] = None  # custom CA bundle path for updater HTTPS
    UPDATE_SKIP_TLS_VERIFY: bool = False    # unsafe, only for temporary troubleshooting

    model_config = SettingsConfigDict(env_file=str(BASE_DIR / ".env"), env_file_encoding="utf-8", case_sensitive=False)

    def finalize(self):
        base_dir = BASE_DIR
        # Resolve storage dir
        if self.STORAGE_DIR:
            storage_dir = Path(self.STORAGE_DIR)
            if not storage_dir.is_absolute():
                storage_dir = base_dir / storage_dir
        else:
            storage_dir = base_dir / "storage"
        # Resolve log dir
        if self.LOG_DIR:
            log_dir = Path(self.LOG_DIR)
            if not log_dir.is_absolute():
                log_dir = base_dir / log_dir
        else:
            log_dir = base_dir / "logs"
        storage_dir.mkdir(parents=True, exist_ok=True)
        log_dir.mkdir(parents=True, exist_ok=True)
        self.STORAGE_DIR = str(storage_dir)
        self.LOG_DIR = str(log_dir)
        # Default SQLite URL
        if not self.SQLITE_URL:
            db_path = storage_dir / "app.db"
            self.SQLITE_URL = f"sqlite:///{db_path.as_posix()}"


@lru_cache()
def get_settings() -> Settings:
    s = Settings()
    s.finalize()
    return s


def _parse_instances_json(raw: Optional[str]) -> dict[str, dict[str, Any]]:
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for key, val in payload.items():
        if isinstance(key, str) and isinstance(val, dict):
            out[key] = val
    return out


def _sqlite_db_path() -> Path:
    s = get_settings()
    raw = str(s.SQLITE_URL or "")
    prefix = "sqlite:///"
    if raw.startswith(prefix):
        p = raw[len(prefix):]
        return Path(p)
    return Path(s.STORAGE_DIR) / "app.db"


def _legacy_runtime_instances_file() -> Path:
    s = get_settings()
    return Path(s.STORAGE_DIR) / "data_source_instances.json"


def _build_fernet_for_seed(seed_text: str):
    from cryptography.fernet import Fernet
    seed = (seed_text or "").encode("utf-8")
    digest = hashlib.sha256(seed).digest()
    key = base64.urlsafe_b64encode(digest)
    return Fernet(key)


def _effective_config_seed() -> str:
    if _RUNTIME_CONFIG_KEY_OVERRIDE:
        return _RUNTIME_CONFIG_KEY_OVERRIDE
    managed = _load_managed_data_key()
    if managed:
        return managed
    s = get_settings()
    return s.CONFIG_ENCRYPTION_KEY or s.API_KEY or "nfatool-dev-insecure-key"


def _build_fernet():
    return _build_fernet_for_seed(_effective_config_seed())


def _decrypt_config_with_seeds(token: str, seeds: list[str]) -> dict[str, Any]:
    if not token:
        return {}
    tried: set[str] = set()
    for s in seeds:
        ss = str(s or "")
        if not ss or ss in tried:
            continue
        tried.add(ss)
        try:
            f = _build_fernet_for_seed(ss)
            text = f.decrypt(token.encode("utf-8")).decode("utf-8")
            obj = json.loads(text)
            if isinstance(obj, dict):
                return obj
        except Exception:
            continue
    return {}


def _encrypt_config(payload: dict[str, Any]) -> str:
    f = _build_fernet()
    text = json.dumps(payload or {}, ensure_ascii=False)
    return f.encrypt(text.encode("utf-8")).decode("utf-8")


def _decrypt_config(token: str) -> dict[str, Any]:
    if not token:
        return {}
    seeds = [
        _RUNTIME_CONFIG_KEY_OVERRIDE or "",
        _load_managed_data_key() or "",
        _root_seed(),
    ]
    return _decrypt_config_with_seeds(token, seeds)


def _ensure_data_source_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS data_source_configs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source_type VARCHAR(20) NOT NULL,
          instance VARCHAR(100) NOT NULL,
          config_enc TEXT NOT NULL,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
          updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_data_source_configs_st_inst ON data_source_configs (source_type, instance)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS data_source_config_audit (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          action VARCHAR(20) NOT NULL,
          source_type VARCHAR(20) NOT NULL,
          instance VARCHAR(100) NOT NULL,
          actor VARCHAR(200),
          detail TEXT,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_data_source_audit_created_at ON data_source_config_audit (created_at)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS data_source_key_state (
          id INTEGER PRIMARY KEY CHECK (id = 1),
          data_key_enc TEXT,
          auto_rotate_enabled INTEGER DEFAULT 1,
          auto_rotate_days INTEGER DEFAULT 30,
          last_rotated_at DATETIME,
          updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute("INSERT OR IGNORE INTO data_source_key_state (id, auto_rotate_enabled, auto_rotate_days) VALUES (1, 1, 30)")


def _record_config_audit(conn: sqlite3.Connection, action: str, source_type: str, instance: str, actor: str | None, detail: dict[str, Any] | None = None) -> None:
    conn.execute(
        "INSERT INTO data_source_config_audit (action, source_type, instance, actor, detail) VALUES (?, ?, ?, ?, ?)",
        (
            action,
            source_type,
            instance,
            (actor or "")[:200],
            json.dumps(detail or {}, ensure_ascii=False),
        ),
    )


def _root_seed() -> str:
    s = get_settings()
    return s.CONFIG_ENCRYPTION_KEY or s.API_KEY or "nfatool-dev-insecure-key"


def _load_managed_data_key() -> str | None:
    db_path = _sqlite_db_path()
    if not db_path.exists():
        return None
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        try:
            row = conn.execute("SELECT data_key_enc FROM data_source_key_state WHERE id=1").fetchone()
        except Exception:
            return None
        if not row:
            return None
        enc = str(row["data_key_enc"] or "").strip()
        if not enc:
            return None
        try:
            f = _build_fernet_for_seed(_root_seed())
            return f.decrypt(enc.encode("utf-8")).decode("utf-8")
        except Exception:
            return None
    finally:
        conn.close()


def _save_managed_data_key(new_key: str) -> None:
    db_path = _sqlite_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        _ensure_data_source_table(conn)
        f = _build_fernet_for_seed(_root_seed())
        enc = f.encrypt(str(new_key).encode("utf-8")).decode("utf-8")
        conn.execute(
            """
            UPDATE data_source_key_state
               SET data_key_enc=?, last_rotated_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP
             WHERE id=1
            """,
            (enc,),
        )
        conn.commit()
    finally:
        conn.close()


def _load_runtime_instances_all() -> dict[str, dict[str, dict[str, Any]]]:
    out: dict[str, dict[str, dict[str, Any]]] = {"nfa": {}, "edc": {}}
    db_path = _sqlite_db_path()
    if not db_path.exists():
        return out
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        try:
            _ensure_data_source_table(conn)
            managed_key = _load_managed_data_key() or ""
            seeds = [
                _RUNTIME_CONFIG_KEY_OVERRIDE or "",
                managed_key,
                _root_seed(),
            ]
            rows = conn.execute("SELECT source_type, instance, config_enc FROM data_source_configs").fetchall()
            for r in rows:
                st = str(r["source_type"] or "").lower()
                name = str(r["instance"] or "").strip()
                if st in {"nfa", "edc"} and name:
                    out[st][name] = _decrypt_config_with_seeds(str(r["config_enc"] or ""), seeds)
            # One-time best-effort import from legacy JSON runtime file.
            if not out["nfa"] and not out["edc"]:
                lp = _legacy_runtime_instances_file()
                if lp.exists():
                    try:
                        legacy = json.loads(lp.read_text(encoding="utf-8"))
                        if isinstance(legacy, dict):
                            for st in ("nfa", "edc"):
                                part = legacy.get(st)
                                if isinstance(part, dict):
                                    for name, cfg in part.items():
                                        if isinstance(name, str) and isinstance(cfg, dict):
                                            _upsert_runtime_instance(st, name, cfg)
                                            out[st][name] = cfg
                    except Exception:
                        pass
        finally:
            conn.close()
    except Exception:
        return {"nfa": {}, "edc": {}}
    return out


def _upsert_runtime_instance(source_type: str, instance: str, config: dict[str, Any], actor: str | None = None) -> None:
    db_path = _sqlite_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        _ensure_data_source_table(conn)
        enc = _encrypt_config(config)
        now = "CURRENT_TIMESTAMP"
        # Compatible with older SQLite versions (no UPSERT syntax support):
        # update first, then insert if no row was affected.
        cur = conn.execute(
            "UPDATE data_source_configs SET config_enc=?, updated_at=CURRENT_TIMESTAMP WHERE source_type=? AND instance=?",
            (enc, source_type, instance),
        )
        if int(cur.rowcount or 0) <= 0:
            conn.execute(
                "INSERT INTO data_source_configs (source_type, instance, config_enc, created_at, updated_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                (source_type, instance, enc),
            )
        _record_config_audit(
            conn,
            action="upsert",
            source_type=source_type,
            instance=instance,
            actor=actor,
            detail={
                "keys": sorted(list(config.keys())),
                "updated_at": datetime.utcnow().isoformat(timespec="seconds"),
            },
        )
        conn.commit()
    finally:
        conn.close()


def _delete_runtime_instance(source_type: str, instance: str, actor: str | None = None) -> bool:
    db_path = _sqlite_db_path()
    if not db_path.exists():
        return False
    conn = sqlite3.connect(str(db_path))
    try:
        _ensure_data_source_table(conn)
        cur = conn.execute("DELETE FROM data_source_configs WHERE source_type=? AND instance=?", (source_type, instance))
        if int(cur.rowcount or 0) > 0:
            _record_config_audit(
                conn,
                action="delete",
                source_type=source_type,
                instance=instance,
                actor=actor,
                detail={"updated_at": datetime.utcnow().isoformat(timespec="seconds")},
            )
        conn.commit()
        return int(cur.rowcount or 0) > 0
    finally:
        conn.close()


def _env_instances(source_type: str) -> dict[str, dict[str, Any]]:
    s = get_settings()
    st = (source_type or "").lower()
    if st == "edc":
        return _parse_instances_json(s.EDC_INSTANCES_JSON)
    out = _parse_instances_json(s.NFA_INSTANCES_JSON)
    if s.MYSQL_HOST and s.MYSQL_USER and s.MYSQL_PASSWORD and s.MYSQL_DB:
        out.setdefault("default", {
            "host": s.MYSQL_HOST,
            "port": s.MYSQL_PORT or 3306,
            "user": s.MYSQL_USER,
            "password": s.MYSQL_PASSWORD,
            "db": s.MYSQL_DB,
            "charset": s.MYSQL_CHARSET or "utf8mb4",
        })
    return out


def _runtime_instances(source_type: str) -> dict[str, dict[str, Any]]:
    st = (source_type or "").lower()
    all_data = _load_runtime_instances_all()
    return dict(all_data.get(st, {}))


def get_data_source_catalog() -> dict[str, list[str]]:
    nfa_instances = get_data_source_instances("nfa")
    edc_instances = get_data_source_instances("edc")

    return {
        "nfa": sorted(nfa_instances.keys()),
        "edc": sorted(edc_instances.keys()),
    }


def get_data_source_instances(source_type: str) -> dict[str, dict[str, Any]]:
    st = (source_type or "").lower()
    if st not in {"nfa", "edc"}:
        st = "nfa"
    out = _env_instances(st)
    out.update(_runtime_instances(st))  # runtime overrides env
    return out


def list_data_source_instances(source_type: str) -> list[dict[str, Any]]:
    st = (source_type or "").lower()
    if st not in {"nfa", "edc"}:
        st = "nfa"
    env_map = _env_instances(st)
    runtime_map = _runtime_instances(st)
    merged = get_data_source_instances(st)
    rows: list[dict[str, Any]] = []
    for name in sorted(merged.keys()):
        in_env = name in env_map
        in_runtime = name in runtime_map
        origin = "runtime" if in_runtime and not in_env else ("env+runtime" if in_env and in_runtime else "env")
        rows.append({
            "instance": name,
            "source_type": st,
            "origin": origin,
            "config": merged[name],
        })
    return rows


def upsert_runtime_data_source_instance(source_type: str, instance: str, config: dict[str, Any], actor: str | None = None) -> None:
    st = (source_type or "").lower()
    if st not in {"nfa", "edc"}:
        raise ValueError("source_type must be nfa or edc")
    name = str(instance or "").strip()
    if not name:
        raise ValueError("instance is required")
    if not isinstance(config, dict):
        raise ValueError("config must be object")
    _upsert_runtime_instance(st, name, config, actor=actor)


def delete_runtime_data_source_instance(source_type: str, instance: str, actor: str | None = None) -> bool:
    st = (source_type or "").lower()
    if st not in {"nfa", "edc"}:
        st = "nfa"
    name = str(instance or "").strip()
    if not name:
        return False
    return _delete_runtime_instance(st, name, actor=actor)


def list_data_source_config_audit(limit: int = 100) -> list[dict[str, Any]]:
    db_path = _sqlite_db_path()
    if not db_path.exists():
        return []
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        try:
            _ensure_data_source_table(conn)
        except Exception:
            # Read-only db/session may block DDL; continue best-effort if table already exists.
            pass
        try:
            rows = conn.execute(
                "SELECT id, action, source_type, instance, actor, detail, created_at FROM data_source_config_audit ORDER BY id DESC LIMIT ?",
                (max(1, min(int(limit), 500)),),
            ).fetchall()
        except Exception:
            return []
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append({
                "id": int(r["id"]),
                "action": r["action"],
                "source_type": r["source_type"],
                "instance": r["instance"],
                "actor": r["actor"],
                "detail": (json.loads(r["detail"]) if r["detail"] else {}),
                "created_at": r["created_at"],
            })
        return out
    finally:
        conn.close()


def rotate_data_source_encryption_key(old_seed: str, new_seed: str) -> dict[str, int]:
    old_seed = str(old_seed or "")
    new_seed = str(new_seed or "")
    if not old_seed or not new_seed:
        raise ValueError("old_seed and new_seed are required")
    if old_seed == new_seed:
        raise ValueError("new_seed must be different from old_seed")
    db_path = _sqlite_db_path()
    if not db_path.exists():
        return {"rotated": 0}
    f_old = _build_fernet_for_seed(old_seed)
    f_new = _build_fernet_for_seed(new_seed)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        _ensure_data_source_table(conn)
        rows = conn.execute("SELECT id, source_type, instance, config_enc FROM data_source_configs").fetchall()
        rotated = 0
        for r in rows:
            raw = str(r["config_enc"] or "")
            try:
                plain = f_old.decrypt(raw.encode("utf-8"))
            except Exception as e:
                raise ValueError(f"failed to decrypt config for {r['source_type']}/{r['instance']}; old_seed may be incorrect") from e
            new_enc = f_new.encrypt(plain).decode("utf-8")
            conn.execute("UPDATE data_source_configs SET config_enc=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", (new_enc, int(r["id"])))
            rotated += 1
        _record_config_audit(
            conn,
            action="rotate_key_global",
            source_type="all",
            instance="*",
            actor="system",
            detail={
                "rotated_at": datetime.utcnow().isoformat(timespec="seconds"),
                "rotated_count": rotated,
            },
        )
        conn.commit()
        # Allow runtime immediate read with new key before restart.
        global _RUNTIME_CONFIG_KEY_OVERRIDE
        _RUNTIME_CONFIG_KEY_OVERRIDE = new_seed
        try:
            _save_managed_data_key(new_seed)
        except Exception:
            pass
        return {"rotated": rotated}
    finally:
        conn.close()


def get_data_key_rotation_status() -> dict[str, Any]:
    s = get_settings()
    db_path = _sqlite_db_path()
    default_enabled = bool(s.CONFIG_AUTO_ROTATE_ENABLED)
    default_days = max(1, int(s.CONFIG_AUTO_ROTATE_DAYS or 30))
    out = {
        "enabled": default_enabled,
        "interval_days": default_days,
        "last_rotated_at": None,
        "has_managed_key": False,
    }
    if not db_path.exists():
        return out
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        try:
            _ensure_data_source_table(conn)
        except Exception:
            return out
        row = conn.execute(
            "SELECT auto_rotate_enabled, auto_rotate_days, last_rotated_at, data_key_enc FROM data_source_key_state WHERE id=1"
        ).fetchone()
        if not row:
            return out
        out["enabled"] = bool(int(row["auto_rotate_enabled"] or 0))
        out["interval_days"] = max(1, int(row["auto_rotate_days"] or default_days))
        out["last_rotated_at"] = row["last_rotated_at"]
        out["has_managed_key"] = bool(str(row["data_key_enc"] or "").strip())
        return out
    finally:
        conn.close()


def set_data_key_rotation_policy(enabled: bool, interval_days: int) -> dict[str, Any]:
    db_path = _sqlite_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    interval = max(1, int(interval_days or 30))
    conn = sqlite3.connect(str(db_path))
    try:
        _ensure_data_source_table(conn)
        conn.execute(
            "UPDATE data_source_key_state SET auto_rotate_enabled=?, auto_rotate_days=?, updated_at=CURRENT_TIMESTAMP WHERE id=1",
            (1 if enabled else 0, interval),
        )
        conn.commit()
    finally:
        conn.close()
    return get_data_key_rotation_status()


def auto_rotate_data_source_key(force: bool = False) -> dict[str, Any]:
    status = get_data_key_rotation_status()
    if not force and not status.get("enabled", True):
        return {"rotated": False, "reason": "disabled"}
    last_rot = status.get("last_rotated_at")
    interval = max(1, int(status.get("interval_days", 30)))
    if not force and last_rot:
        try:
            dt = datetime.fromisoformat(str(last_rot).replace("Z", ""))
            days = (datetime.utcnow() - dt).days
            if days < interval:
                return {"rotated": False, "reason": f"not_due:{days}/{interval}"}
        except Exception:
            pass
    old_seed = _effective_config_seed()
    new_seed = secrets.token_urlsafe(32)
    res = rotate_data_source_encryption_key(old_seed, new_seed)
    return {"rotated": True, "count": int(res.get("rotated", 0))}
