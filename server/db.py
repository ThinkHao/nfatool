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
