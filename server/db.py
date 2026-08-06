from __future__ import annotations

from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase, Session

from .config import get_settings


class Base(DeclarativeBase):
    pass


def get_engine():
    settings = get_settings()
    return create_engine(settings.SQLITE_URL, echo=False, future=True)


_engine = get_engine()
SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False, expire_on_commit=False, future=True)


def init_db():
    from . import models  # noqa: F401 ensure models imported
    Base.metadata.create_all(_engine)
    _migrate_sqlite()


def _migrate_sqlite():
    """Lightweight migrations for SQLite: add missing columns if needed."""
    try:
        if _engine.url.get_backend_name() == 'sqlite':
            with _engine.connect() as conn:
                # Ensure 'kind' column exists in tasks
                res = conn.exec_driver_sql("PRAGMA table_info('tasks')")
                cols = [row[1] for row in res.fetchall()]
                if 'kind' not in cols:
                    conn.exec_driver_sql("ALTER TABLE tasks ADD COLUMN kind VARCHAR(20) DEFAULT 'one_off'")
                if 'group_name' not in cols:
                    conn.exec_driver_sql("ALTER TABLE tasks ADD COLUMN group_name VARCHAR(100)")
                if 'data_source_type' not in cols:
                    conn.exec_driver_sql("ALTER TABLE tasks ADD COLUMN data_source_type VARCHAR(20) DEFAULT 'nfa'")
                if 'data_source_instance' not in cols:
                    conn.exec_driver_sql("ALTER TABLE tasks ADD COLUMN data_source_instance VARCHAR(100)")
                if 'latest_budget_summary' not in cols:
                    conn.exec_driver_sql("ALTER TABLE tasks ADD COLUMN latest_budget_summary TEXT")
                conn.exec_driver_sql("""
                    CREATE TABLE IF NOT EXISTS task_groups (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name VARCHAR(100) NOT NULL UNIQUE,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                res2 = conn.exec_driver_sql("PRAGMA table_info('job_runs')")
                job_cols = [row[1] for row in res2.fetchall()]
                if 'progress_pct' not in job_cols:
                    conn.exec_driver_sql("ALTER TABLE job_runs ADD COLUMN progress_pct INTEGER DEFAULT 0")
                if 'progress_stage' not in job_cols:
                    conn.exec_driver_sql("ALTER TABLE job_runs ADD COLUMN progress_stage VARCHAR(200)")
                if 'progress_events' not in job_cols:
                    conn.exec_driver_sql("ALTER TABLE job_runs ADD COLUMN progress_events TEXT")
                conn.exec_driver_sql("""
                    CREATE TABLE IF NOT EXISTS data_source_configs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        source_type VARCHAR(20) NOT NULL,
                        instance VARCHAR(100) NOT NULL,
                        config_enc TEXT NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                conn.exec_driver_sql("""
                    CREATE TABLE IF NOT EXISTS data_source_config_audit (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        action VARCHAR(20) NOT NULL,
                        source_type VARCHAR(20) NOT NULL,
                        instance VARCHAR(100) NOT NULL,
                        actor VARCHAR(200),
                        detail TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                conn.exec_driver_sql("""
                    CREATE TABLE IF NOT EXISTS data_source_key_state (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        data_key_enc TEXT,
                        auto_rotate_enabled INTEGER DEFAULT 1,
                        auto_rotate_days INTEGER DEFAULT 30,
                        last_rotated_at DATETIME,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                conn.exec_driver_sql("INSERT OR IGNORE INTO data_source_key_state (id, auto_rotate_enabled, auto_rotate_days) VALUES (1, 1, 30)")
                # Create helpful indexes (idempotent)
                conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_job_runs_task_id ON job_runs (task_id)")
                conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_job_runs_started_at ON job_runs (started_at)")
                conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_tasks_name ON tasks (name)")
                conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_tasks_group_name ON tasks (group_name)")
                conn.exec_driver_sql("CREATE UNIQUE INDEX IF NOT EXISTS idx_task_groups_name ON task_groups (name)")
                conn.exec_driver_sql("CREATE UNIQUE INDEX IF NOT EXISTS idx_data_source_configs_st_inst ON data_source_configs (source_type, instance)")
                conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_data_source_audit_created_at ON data_source_config_audit (created_at)")
                conn.exec_driver_sql("""
                    CREATE TABLE IF NOT EXISTS edc_match_snapshots (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        job_run_id VARCHAR(36) NOT NULL UNIQUE,
                        task_id INTEGER,
                        task_name_snapshot VARCHAR(200),
                        data_source_instance VARCHAR(100) NOT NULL,
                        window_start VARCHAR(32) NOT NULL,
                        window_end VARCHAR(32) NOT NULL,
                        edc_name_expression TEXT NOT NULL,
                        match_mode VARCHAR(20) NOT NULL DEFAULT 'prefix',
                        exclude_like VARCHAR(255),
                        matched_count INTEGER NOT NULL DEFAULT 0,
                        match_hash VARCHAR(64),
                        status VARCHAR(20) NOT NULL DEFAULT 'resolved',
                        error_message TEXT,
                        source_config_fingerprint VARCHAR(64),
                        resolved_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY(job_run_id) REFERENCES job_runs(id)
                    )
                """)
                conn.exec_driver_sql("""
                    CREATE TABLE IF NOT EXISTS edc_match_snapshot_items (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        snapshot_id INTEGER NOT NULL,
                        edc_name VARCHAR(255) NOT NULL,
                        matched_by VARCHAR(255),
                        match_operator VARCHAR(10),
                        effective_pattern VARCHAR(255),
                        FOREIGN KEY(snapshot_id) REFERENCES edc_match_snapshots(id) ON DELETE CASCADE,
                        UNIQUE(snapshot_id, edc_name)
                    )
                """)
                conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_edc_snapshot_window ON edc_match_snapshots (window_start, window_end)")
                conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_edc_snapshot_instance ON edc_match_snapshots (data_source_instance)")
                conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_edc_snapshot_items_name ON edc_match_snapshot_items (edc_name)")
                # Backfill task_groups from historical task.group_name values.
                conn.exec_driver_sql("""
                    INSERT OR IGNORE INTO task_groups (name, created_at, updated_at)
                    SELECT TRIM(group_name), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    FROM tasks
                    WHERE group_name IS NOT NULL AND TRIM(group_name) != ''
                """)
    except Exception:
        # Best-effort; ignore migration errors to avoid blocking startup
        pass


@contextmanager
def session_scope() -> Session:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
