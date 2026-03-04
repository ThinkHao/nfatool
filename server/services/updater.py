from __future__ import annotations

import json
import os
import platform
import re
import ssl
import subprocess
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
    ctx = _ssl_context()
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "nfa95-updater",
        },
    )
    with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
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
    try:
        data = _github_json(f"https://api.github.com/repos/{repo}/releases/latest")
    except Exception as e:
        msg = str(e)
        if "CERTIFICATE_VERIFY_FAILED" in msg or "certificate verify failed" in msg.lower():
            msg += " | hint: configure UPDATE_CA_BUNDLE or install CA certificates (temporary workaround: UPDATE_SKIP_TLS_VERIFY=true)"
        return {"ok": False, "repo": repo, "current_version": current, "message": msg}
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
    ctx = _ssl_context()
    req = urllib.request.Request(url, headers={"User-Agent": "nfa95-updater"})
    with urllib.request.urlopen(req, timeout=30, context=ctx) as resp, dst.open("wb") as f:
        while True:
            chunk = resp.read(1024 * 1024)
            if not chunk:
                break
            f.write(chunk)


def _resolve_runner_log(settings, target: Path) -> Path:
    raw = str(settings.UPDATE_RUNNER_LOG or "").strip()
    if raw:
        p = Path(raw)
        if not p.is_absolute():
            p = target.parent / p
        return p
    return target.parent / "logs" / "update-runner.log"


def _apply_update_external(info: dict[str, Any], restart_after_update: bool) -> dict[str, Any]:
    s = get_settings()
    script_raw = str(s.UPDATE_EXTERNAL_SCRIPT or "").strip()
    if not script_raw:
        raise ValueError("external updater script is not configured")
    script = Path(script_raw)
    if not script.exists():
        raise ValueError(f"external updater script not found: {script}")

    target = Path(os.path.realpath(os.sys.executable))
    if not target.exists():
        raise ValueError("executable path not found")

    latest_version = str(info.get("latest_version") or "").strip() or "latest"
    asset_url = str(info.get("asset_url") or "").strip()
    if not asset_url:
        raise ValueError(f"release asset not found: {info.get('asset_name')}")

    cmd = [
        str(script),
        "--asset-url", asset_url,
        "--version", latest_version,
        "--target", str(target),
        "--service", str(s.UPDATE_SERVICE_NAME or "nfa95.service"),
        "--health-url", str(s.UPDATE_HEALTHCHECK_URL or "http://127.0.0.1:8000/api/health"),
        "--health-timeout", str(int(s.UPDATE_HEALTHCHECK_TIMEOUT_SEC or 45)),
    ]
    if s.UPDATE_CA_BUNDLE:
        cmd.extend(["--ca-bundle", str(s.UPDATE_CA_BUNDLE)])
    if bool(s.UPDATE_SKIP_TLS_VERIFY):
        cmd.append("--insecure")
    if not restart_after_update:
        cmd.append("--no-restart")

    log_path = _resolve_runner_log(s, target)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as lf:
        lf.write(f"\n[updater] trigger external update: version={latest_version}, target={target}\n")
        lf.flush()
        subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=lf,
            stderr=lf,
            start_new_session=True,
            cwd=str(target.parent),
        )

    return {
        "ok": True,
        "updated": True,
        "restarted": bool(restart_after_update),
        "mode": "external-script",
        "runner_script": str(script),
        "runner_log": str(log_path),
        "message": "update started by external runner",
        **info,
    }


def _ssl_context() -> ssl.SSLContext:
    s = get_settings()
    if bool(s.UPDATE_SKIP_TLS_VERIFY):
        return ssl._create_unverified_context()
    # 1) custom CA bundle from env
    if s.UPDATE_CA_BUNDLE:
        p = Path(str(s.UPDATE_CA_BUNDLE))
        if p.exists():
            return ssl.create_default_context(cafile=str(p))
    # 2) certifi bundle (works well on minimal Linux distros)
    try:
        import certifi  # type: ignore
        return ssl.create_default_context(cafile=certifi.where())
    except Exception:
        pass
    # 3) system default CA store
    return ssl.create_default_context()


def apply_update(restart_after_update: bool = True) -> dict[str, Any]:
    info = check_update()
    if not info.get("ok"):
        raise ValueError(str(info.get("message") or "update check failed"))
    if not info.get("update_available"):
        return {"ok": True, "updated": False, "message": "already latest", **info}

    s = get_settings()
    if platform.system().lower().startswith("linux") and str(s.UPDATE_EXTERNAL_SCRIPT or "").strip():
        return _apply_update_external(info, restart_after_update)

    # Only support self-update for packaged executable.
    url = str(info.get("asset_url") or "").strip()
    if not url:
        raise ValueError(f"release asset not found: {info.get('asset_name')}")
    target = Path(os.path.realpath(os.sys.executable))
    if not target.exists():
        raise ValueError("executable path not found")

    # Download to the same directory as target to avoid cross-device rename issues.
    tmp_file = target.parent / f".{target.name}.download"
    bak_file = target.with_suffix(target.suffix + ".bak")
    _download_file(url, tmp_file)
    os.chmod(tmp_file, 0o755)
    # Keep a backup and atomically replace with rollback on failure.
    moved_to_backup = False
    try:
        if target.exists():
            if bak_file.exists():
                bak_file.unlink(missing_ok=True)
            os.replace(target, bak_file)
            moved_to_backup = True
        try:
            os.replace(tmp_file, target)
        except Exception as e:
            if moved_to_backup and bak_file.exists() and not target.exists():
                try:
                    os.replace(bak_file, target)
                except Exception:
                    pass
            raise ValueError(f"failed to replace executable: {e}") from e
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
