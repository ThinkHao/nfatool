from __future__ import annotations

import json
import os
import platform
import re
import ssl
import subprocess
import threading
from datetime import datetime, timezone
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from ..config import get_settings

_APPLY_GUARD = threading.Lock()


def _resolve_update_target() -> Path:
    # Prefer argv[0] so updates keep using the stable launch path (for example
    # /home/nfa95/nfa95 symlink), instead of resolving into nested release dirs.
    argv0 = str((getattr(os.sys, "argv", None) or [""])[0] or "").strip()
    candidates: list[Path] = []
    if argv0:
        candidates.append(Path(os.path.abspath(argv0)))
    exe = str(getattr(os.sys, "executable", "") or "").strip()
    if exe:
        candidates.append(Path(os.path.abspath(exe)))
    for p in candidates:
        try:
            if p.exists():
                return p
        except Exception:
            continue
    if candidates:
        return candidates[0]
    return Path(os.path.abspath("nfa95"))


def _resolve_current_version_from_state() -> str | None:
    try:
        s = get_settings()
        target = _resolve_update_target()
        state_path = _resolve_runner_state(s, target)
        state = _read_runner_state(state_path)
        if str(state.get("status") or "").strip().lower() != "succeeded":
            return None
        ver = str(state.get("version") or "").strip()
        return ver or None
    except Exception:
        return None


def _resolve_current_version_from_target_path() -> str | None:
    try:
        target = _resolve_update_target()
        chain = [
            str(target),
            str(target.resolve(strict=False)),
        ]
        for raw in chain:
            # Match ".../releases/<tag>-<timestamp>/..."
            m = re.search(r"(?:^|[\\/])releases[\\/](v[^\\/]+?)-\d{14}(?:[\\/]|$)", raw)
            if m:
                ver = str(m.group(1) or "").strip()
                if ver:
                    return ver
    except Exception:
        return None
    return None


def _resolve_current_version() -> tuple[str, str]:
    s = get_settings()
    by_state = _resolve_current_version_from_state()
    if by_state:
        return by_state, "state_file"
    by_path = _resolve_current_version_from_target_path()
    if by_path:
        return by_path, "target_path"
    return str(s.APP_VERSION or "0.0.0"), "env_app_version"


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


def _fetch_json(url: str, headers: dict[str, str] | None = None) -> dict[str, Any]:
    ctx = _ssl_context()
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
        raw = resp.read().decode("utf-8", errors="ignore")
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}


def _github_json(url: str) -> dict[str, Any]:
    return _fetch_json(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "nfa95-updater",
        },
    )


def _append_query(url: str, params: dict[str, str]) -> str:
    if not params:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}{urllib.parse.urlencode(params)}"


def _gitee_json(url: str, token: str | None = None) -> dict[str, Any]:
    headers = {"User-Agent": "nfa95-updater"}
    target_url = url
    if token:
        target_url = _append_query(url, {"access_token": token})
    return _fetch_json(target_url, headers=headers)


def _pick_asset_name() -> str:
    s = get_settings()
    if platform.system().lower().startswith("win"):
        return s.UPDATE_ASSET_WINDOWS or "nfa95.exe"
    return s.UPDATE_ASSET_LINUX or "nfa95"


def _parse_source_priority(raw: str) -> list[str]:
    names = [str(x or "").strip().lower() for x in str(raw or "").split(",")]
    allowed = {"gitee", "github"}
    out: list[str] = []
    for name in names:
        if name in allowed and name not in out:
            out.append(name)
    if not out:
        return ["gitee", "github"]
    return out


def _pick_asset(assets: Any, asset_name: str) -> dict[str, Any] | None:
    if not isinstance(assets, list):
        return None
    for item in assets:
        if not isinstance(item, dict):
            continue
        if str(item.get("name") or "").strip() == asset_name:
            return item
    return None


def _asset_download_url(asset: dict[str, Any] | None) -> str | None:
    if not isinstance(asset, dict):
        return None
    keys = ("browser_download_url", "download_url", "url")
    for key in keys:
        url = str(asset.get(key) or "").strip()
        if url:
            return url
    return None


def _check_update_from_github(current: str, repo: str, asset_name: str) -> dict[str, Any]:
    data = _github_json(f"https://api.github.com/repos/{repo}/releases/latest")
    tag = str(data.get("tag_name") or "").strip()
    asset = _pick_asset(data.get("assets") or [], asset_name)
    return {
        "ok": True,
        "source": "github",
        "repo": repo,
        "current_version": current,
        "latest_version": tag or "",
        "update_available": bool(tag and _is_newer(tag, current)),
        "published_at": data.get("published_at"),
        "asset_name": asset_name,
        "asset_url": _asset_download_url(asset),
        "release_url": data.get("html_url"),
    }


def _check_update_from_gitee(current: str, repo: str, asset_name: str, token: str | None) -> dict[str, Any]:
    data = _gitee_json(f"https://gitee.com/api/v5/repos/{repo}/releases/latest", token=token)
    tag = str(data.get("tag_name") or "").strip()
    asset = _pick_asset(data.get("assets") or [], asset_name)
    return {
        "ok": True,
        "source": "gitee",
        "repo": repo,
        "current_version": current,
        "latest_version": tag or "",
        "update_available": bool(tag and _is_newer(tag, current)),
        "published_at": data.get("published_at") or data.get("created_at"),
        "asset_name": asset_name,
        "asset_url": _asset_download_url(asset),
        "release_url": data.get("html_url"),
    }


def check_update() -> dict[str, Any]:
    s = get_settings()
    current, current_source = _resolve_current_version()
    asset_name = _pick_asset_name()
    gitee_repo = str((getattr(s, "GITEE_REPO", None) or s.GITHUB_REPO or "")).strip()
    github_repo = str(s.GITHUB_REPO or "").strip()
    gitee_token = str(getattr(s, "GITEE_TOKEN", None) or "").strip() or None
    source_priority = _parse_source_priority(str(getattr(s, "UPDATE_SOURCE_PRIORITY", "gitee,github")))

    errors: list[str] = []
    for source in source_priority:
        try:
            if source == "gitee":
                if not gitee_repo:
                    raise ValueError("GITEE_REPO/GITHUB_REPO is not configured")
                out = _check_update_from_gitee(current, gitee_repo, asset_name, gitee_token)
                out["current_version_source"] = current_source
                return out
            if source == "github":
                if not github_repo:
                    raise ValueError("GITHUB_REPO is not configured")
                out = _check_update_from_github(current, github_repo, asset_name)
                out["current_version_source"] = current_source
                return out
        except Exception as e:
            msg = str(e)
            if "CERTIFICATE_VERIFY_FAILED" in msg or "certificate verify failed" in msg.lower():
                msg += " | hint: configure UPDATE_CA_BUNDLE or install CA certificates (temporary workaround: UPDATE_SKIP_TLS_VERIFY=true)"
            errors.append(f"{source}: {msg}")

    message = "all update sources failed"
    if errors:
        message = f"{message} | {'; '.join(errors)}"
    return {
        "ok": False,
        "current_version": current,
        "current_version_source": current_source,
        "asset_name": asset_name,
        "message": message,
        "errors": errors,
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


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _resolve_runner_state(settings, target: Path) -> Path:
    raw = str(getattr(settings, "UPDATE_STATE_FILE", "") or "").strip()
    if raw:
        p = Path(raw)
        if not p.is_absolute():
            p = target.parent / p
        return p
    return target.parent / "logs" / "update-runner.state"


def _write_runner_state(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = dict(payload or {})
    data.setdefault("updated_at", _now_iso())
    running = bool(data.get("running", False))
    lines = [
        f"status={str(data.get('status') or '').strip()}",
        f"running={'1' if running else '0'}",
        f"step={str(data.get('step') or '').strip()}",
        f"version={str(data.get('version') or '').strip()}",
        f"target={str(data.get('target') or '').strip()}",
        f"updated_at={str(data.get('updated_at') or '').strip()}",
    ]
    for key in ("started_at", "finished_at", "message", "last_error", "runner_log"):
        val = str(data.get(key) or "").replace("\r", " ").replace("\n", " ").replace("=", ":").strip()
        if val:
            lines.append(f"{key}={val}")
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _read_runner_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    out: dict[str, Any] = {}
    try:
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = str(line or "").strip()
            if not line or "=" not in line:
                continue
            k, v = line.split("=", 1)
            out[str(k).strip()] = str(v).strip()
    except Exception:
        return {}
    if not out:
        return {}
    status = str(out.get("status") or "").strip().lower()
    running_raw = str(out.get("running") or "").strip().lower()
    running = running_raw in {"1", "true", "yes", "y"}
    return {
        "status": status or ("running" if running else "unknown"),
        "running": running or status in {"queued", "running"},
        "step": str(out.get("step") or "").strip() or None,
        "version": str(out.get("version") or "").strip() or None,
        "target": str(out.get("target") or "").strip() or None,
        "updated_at": str(out.get("updated_at") or "").strip() or None,
        "started_at": str(out.get("started_at") or "").strip() or None,
        "finished_at": str(out.get("finished_at") or "").strip() or None,
        "message": str(out.get("message") or "").strip() or None,
        "last_error": str(out.get("last_error") or "").strip() or None,
        "runner_log": str(out.get("runner_log") or "").strip() or None,
    }


def _is_state_stale_running(state: dict[str, Any], stale_sec: int) -> bool:
    if not bool(state.get("running")):
        return False
    raw = str(state.get("updated_at") or "").strip()
    if not raw:
        return False
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return False
    age = (datetime.now(timezone.utc) - dt).total_seconds()
    return age > max(60, int(stale_sec or 7200))


def get_update_status() -> dict[str, Any]:
    s = get_settings()
    target = _resolve_update_target()
    state_path = _resolve_runner_state(s, target)
    log_path = _resolve_runner_log(s, target)
    st = _read_runner_state(state_path)
    if not st:
        return {
            "ok": True,
            "status": "idle",
            "running": False,
            "state_file": str(state_path),
            "runner_log": str(log_path),
            "message": "no update task yet",
        }
    st["ok"] = True
    st["state_file"] = str(state_path)
    st["runner_log"] = st.get("runner_log") or str(log_path)
    return st


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

    target = _resolve_update_target()
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
    state_path = _resolve_runner_state(s, target)
    cmd.extend(["--state-file", str(state_path)])
    cmd.extend(["--download-max-time", str(max(60, int(getattr(s, "UPDATE_DOWNLOAD_MAX_TIME_SEC", 300) or 300)))])
    cmd.extend(["--download-retry", str(max(0, int(getattr(s, "UPDATE_DOWNLOAD_RETRY", 6) or 6)))])
    cmd.extend(["--download-retry-delay", str(max(0, int(getattr(s, "UPDATE_DOWNLOAD_RETRY_DELAY_SEC", 3) or 3)))])
    cmd.extend(["--download-low-speed-time", str(max(0, int(getattr(s, "UPDATE_DOWNLOAD_LOW_SPEED_TIME_SEC", 30) or 30)))])
    cmd.extend(["--download-low-speed-limit", str(max(0, int(getattr(s, "UPDATE_DOWNLOAD_LOW_SPEED_LIMIT_BPS", 10240) or 10240)))])
    if s.UPDATE_CA_BUNDLE:
        cmd.extend(["--ca-bundle", str(s.UPDATE_CA_BUNDLE)])
    if bool(s.UPDATE_SKIP_TLS_VERIFY):
        cmd.append("--insecure")
    if not restart_after_update:
        cmd.append("--no-restart")

    log_path = _resolve_runner_log(s, target)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    _write_runner_state(state_path, {
        "status": "queued",
        "running": True,
        "step": "triggered",
        "version": latest_version,
        "target": str(target),
        "started_at": _now_iso(),
        "message": "update triggered",
        "runner_log": str(log_path),
    })
    with log_path.open("a", encoding="utf-8") as lf:
        lf.write(f"\n[updater] trigger external update: version={latest_version}, target={target}\n")
        lf.flush()
        try:
            subprocess.Popen(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=lf,
                stderr=lf,
                start_new_session=True,
                cwd=str(target.parent),
            )
        except Exception as e:
            _write_runner_state(state_path, {
                "status": "failed",
                "running": False,
                "step": "trigger",
                "version": latest_version,
                "target": str(target),
                "finished_at": _now_iso(),
                "message": "failed to start external updater",
                "last_error": str(e),
                "runner_log": str(log_path),
            })
            raise

    return {
        "ok": True,
        "updated": True,
        "restarted": bool(restart_after_update),
        "mode": "external-script",
        "runner_script": str(script),
        "runner_log": str(log_path),
        "state_file": str(state_path),
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
    with _APPLY_GUARD:
        info = check_update()
        if not info.get("ok"):
            raise ValueError(str(info.get("message") or "update check failed"))
        if not info.get("update_available"):
            return {"ok": True, "updated": False, "message": "already latest", **info}

        s = get_settings()
        if platform.system().lower().startswith("linux") and str(s.UPDATE_EXTERNAL_SCRIPT or "").strip():
            state = get_update_status()
            if _is_state_stale_running(state, int(getattr(s, "UPDATE_STATUS_STALE_SEC", 7200) or 7200)):
                target = _resolve_update_target()
                state_path = _resolve_runner_state(s, target)
                _write_runner_state(state_path, {
                    "status": "failed",
                    "running": False,
                    "step": "stale",
                    "version": str(state.get("version") or info.get("latest_version") or ""),
                    "target": str(target),
                    "started_at": str(state.get("started_at") or ""),
                    "finished_at": _now_iso(),
                    "message": "stale running state cleared",
                    "last_error": "previous updater state was stale; treated as failed",
                })
                state = get_update_status()
            if bool(state.get("running")):
                raise ValueError("已有升级任务正在执行，请稍后查看升级状态")
            return _apply_update_external(info, restart_after_update)

        # Only support self-update for packaged executable.
        url = str(info.get("asset_url") or "").strip()
        if not url:
            raise ValueError(f"release asset not found: {info.get('asset_name')}")
        target = _resolve_update_target()
        if not target.exists():
            raise ValueError("executable path not found")
        state_path = _resolve_runner_state(s, target)

    # Download to the same directory as target to avoid cross-device rename issues.
        tmp_file = target.parent / f".{target.name}.download"
        bak_file = target.with_suffix(target.suffix + ".bak")
        _write_runner_state(state_path, {
            "status": "running",
            "running": True,
            "step": "downloading",
            "version": str(info.get("latest_version") or ""),
            "target": str(target),
            "started_at": _now_iso(),
            "message": "downloading update binary",
        })
        try:
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

            _write_runner_state(state_path, {
                "status": "succeeded",
                "running": False,
                "step": "done",
                "version": str(info.get("latest_version") or ""),
                "target": str(target),
                "finished_at": _now_iso(),
                "message": "updated binary successfully",
            })
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
        except Exception as e:
            _write_runner_state(state_path, {
                "status": "failed",
                "running": False,
                "step": "self-update",
                "version": str(info.get("latest_version") or ""),
                "target": str(target),
                "finished_at": _now_iso(),
                "message": "self update failed",
                "last_error": str(e),
            })
            raise
