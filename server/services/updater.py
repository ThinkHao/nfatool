from __future__ import annotations

import json
import os
import platform
import re
import tempfile
import threading
import urllib.request
from pathlib import Path
from typing import Any

from ..config import get_settings


def _norm_ver(v: str) -> tuple[int, ...]:
    s = str(v or "").strip()
    if s.startswith("v"):
        s = s[1:]
    nums = re.findall(r"\d+", s)
    if not nums:
        return (0,)
    return tuple(int(x) for x in nums[:4])


def _is_newer(latest: str, current: str) -> bool:
    return _norm_ver(latest) > _norm_ver(current)


def _github_json(url: str) -> dict[str, Any]:
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "nfa95-updater",
        },
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        raw = resp.read().decode("utf-8", errors="ignore")
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}


def _pick_asset_name() -> str:
    s = get_settings()
    if platform.system().lower().startswith("win"):
        return s.UPDATE_ASSET_WINDOWS or "nfa95.exe"
    return s.UPDATE_ASSET_LINUX or "nfa95"


def check_update() -> dict[str, Any]:
    s = get_settings()
    current = s.APP_VERSION or "0.0.0"
    repo = str(s.GITHUB_REPO or "").strip()
    if not repo:
        return {"ok": False, "message": "GITHUB_REPO is not configured", "current_version": current}
    data = _github_json(f"https://api.github.com/repos/{repo}/releases/latest")
    tag = str(data.get("tag_name") or "").strip()
    assets = data.get("assets") or []
    if not isinstance(assets, list):
        assets = []
    asset_name = _pick_asset_name()
    asset = next((a for a in assets if str(a.get("name") or "") == asset_name), None)
    return {
        "ok": True,
        "repo": repo,
        "current_version": current,
        "latest_version": tag or "",
        "update_available": bool(tag and _is_newer(tag, current)),
        "published_at": data.get("published_at"),
        "asset_name": asset_name,
        "asset_url": (asset or {}).get("browser_download_url"),
        "release_url": data.get("html_url"),
    }


def _download_file(url: str, dst: Path) -> None:
    req = urllib.request.Request(url, headers={"User-Agent": "nfa95-updater"})
    with urllib.request.urlopen(req, timeout=30) as resp, dst.open("wb") as f:
        while True:
            chunk = resp.read(1024 * 1024)
            if not chunk:
                break
            f.write(chunk)


def apply_update(restart_after_update: bool = True) -> dict[str, Any]:
    info = check_update()
    if not info.get("ok"):
        raise ValueError(str(info.get("message") or "update check failed"))
    if not info.get("update_available"):
        return {"ok": True, "updated": False, "message": "already latest", **info}
    url = str(info.get("asset_url") or "").strip()
    if not url:
        raise ValueError(f"release asset not found: {info.get('asset_name')}")

    # Only support self-update for packaged executable.
    target = Path(os.path.realpath(os.sys.executable))
    if not target.exists():
        raise ValueError("executable path not found")

    tmp_dir = Path(tempfile.gettempdir())
    tmp_file = tmp_dir / f"{target.name}.download"
    bak_file = target.with_suffix(target.suffix + ".bak")
    _download_file(url, tmp_file)
    os.chmod(tmp_file, 0o755)
    # keep a backup and atomically replace
    try:
        if target.exists():
            if bak_file.exists():
                bak_file.unlink(missing_ok=True)
            os.replace(target, bak_file)
        os.replace(tmp_file, target)
    finally:
        if tmp_file.exists():
            tmp_file.unlink(missing_ok=True)

    if restart_after_update:
        def _reexec():
            try:
                os.execv(str(target), [str(target)])
            except Exception:
                pass
        threading.Timer(1.0, _reexec).start()
    return {
        "ok": True,
        "updated": True,
        "message": "updated binary successfully",
        "restarted": bool(restart_after_update),
        **info,
    }
