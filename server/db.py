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
                if 'data_source_type' not in cols:
                    conn.exec_driver_sql("ALTER TABLE tasks ADD COLUMN data_source_type VARCHAR(20) DEFAULT 'nfa'")
                if 'data_source_instance' not in cols:
                    conn.exec_driver_sql("ALTER TABLE tasks ADD COLUMN data_source_instance VARCHAR(100)")
                if 'latest_budget_summary' not in cols:
                    conn.exec_driver_sql("ALTER TABLE tasks ADD COLUMN latest_budget_summary TEXT")
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
                conn.exec_driver_sql("CREATE UNIQUE INDEX IF NOT EXISTS idx_data_source_configs_st_inst ON data_source_configs (source_type, instance)")
                conn.exec_driver_sql("CREATE INDEX IF NOT EXISTS idx_data_source_audit_created_at ON data_source_config_audit (created_at)")
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
