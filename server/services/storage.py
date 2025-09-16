from __future__ import annotations

import os
from pathlib import Path
from typing import List, Dict

from ..config import get_settings


def get_job_dir(job_id: str) -> Path:
    settings = get_settings()
    base = Path(settings.STORAGE_DIR)
    d = base / "results" / job_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def list_artifacts(job_id: str) -> List[Dict]:
    d = get_job_dir(job_id)
    out: List[Dict] = []
    for p in d.iterdir():
        if p.is_file():
            out.append({
                "filename": p.name,
                "size": p.stat().st_size,
                "path": str(p),
            })
    return out


def safe_artifact_path(job_id: str, filename: str) -> Path:
    d = get_job_dir(job_id)
    # avoid path traversal
    safe_name = os.path.basename(filename)
    return d / safe_name
