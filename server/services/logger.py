from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from ..config import get_settings


def get_job_log_path(job_id: str) -> Path:
    settings = get_settings()
    log_dir = Path(settings.LOG_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / f"job_{job_id}.log"


def create_job_logger(job_id: str) -> logging.Logger:
    logger = logging.getLogger(f"job.{job_id}")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        # File handler
        log_path = get_job_log_path(job_id)
        file_handler = RotatingFileHandler(log_path, maxBytes=2_000_000, backupCount=2, encoding="utf-8")
        fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)
        # Console handler (so that running in server/ shows realtime logs)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(fmt)
        logger.addHandler(console_handler)
    return logger
