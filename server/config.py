from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict
import sys


def _app_base_dir() -> Path:
    # If packaged by PyInstaller --onefile, use the executable directory
    if getattr(sys, 'frozen', False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


BASE_DIR = _app_base_dir()


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
