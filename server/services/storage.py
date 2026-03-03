from __future__ import annotations

import os
import re
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
    safe_name = os.path.basename(filename or "")
    # normalize invalid filename chars (Windows-safe)
    safe_name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", safe_name)
    safe_name = safe_name.rstrip(" .")
    if not safe_name:
        safe_name = "artifact"
    # avoid reserved DOS device names
    stem, ext = os.path.splitext(safe_name)
    if stem.upper() in {"CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9"}:
        safe_name = f"_{stem}{ext}"
    return d / safe_name
