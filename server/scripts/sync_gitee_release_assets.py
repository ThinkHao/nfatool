#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


def _request_json(url: str, method: str = "GET", data: dict[str, str] | None = None, headers: dict[str, str] | None = None) -> dict[str, Any]:
    body = None
    req_headers = dict(headers or {})
    if data is not None:
        body = urllib.parse.urlencode(data).encode("utf-8")
        req_headers.setdefault("Content-Type", "application/x-www-form-urlencoded")
    req = urllib.request.Request(url=url, data=body, method=method, headers=req_headers)
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8", errors="ignore")
    obj = json.loads(raw or "{}")
    if isinstance(obj, dict):
        return obj
    return {}


def _request_json_or_none(url: str, method: str = "GET", data: dict[str, str] | None = None, headers: dict[str, str] | None = None) -> dict[str, Any] | None:
    try:
        return _request_json(url=url, method=method, data=data, headers=headers)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def _download_file(url: str, dst: Path) -> None:
    req = urllib.request.Request(url=url, headers={"User-Agent": "nfa95-release-sync"})
    with urllib.request.urlopen(req, timeout=60) as resp, dst.open("wb") as f:
        while True:
            chunk = resp.read(1024 * 1024)
            if not chunk:
                break
            f.write(chunk)


def _ensure_curl() -> None:
    try:
        subprocess.run(["curl", "--version"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as e:
        raise RuntimeError("curl is required in PATH") from e


def _upload_file_with_curl(api_base: str, release_id: str, token: str, file_path: Path) -> None:
    cmd = [
        "curl",
        "--connect-timeout",
        "20",
        "--max-time",
        "600",
        "--retry",
        "2",
        "--retry-delay",
        "2",
        "--retry-all-errors",
        "-sS",
        "-X",
        "POST",
        f"{api_base}/releases/{release_id}/attach_files",
        "-H",
        "Expect:",
        "-F",
        f"access_token={token}",
        "-F",
        f"file=@{file_path}",
    ]
    subprocess.run(cmd, check=True)


def _main() -> int:
    parser = argparse.ArgumentParser(description="Sync release assets from GitHub release to Gitee release.")
    parser.add_argument("--tag", required=True, help="release tag, e.g. v2026.04.02-gitee-sync-nonblock-fix1")
    parser.add_argument("--github-repo", default=os.getenv("GITHUB_REPO", "ThinkHao/nfatool"), help="GitHub repo owner/name")
    parser.add_argument("--gitee-repo", default=os.getenv("GITEE_REPO", ""), help="Gitee repo owner/name (defaults to github-repo)")
    parser.add_argument("--token-env", default="GITEE_TOKEN", help="env var name for gitee token")
    parser.add_argument("--asset", action="append", default=[], help="asset name to sync (repeatable)")
    args = parser.parse_args()

    token = str(os.getenv(args.token_env, "")).strip()
    if not token:
        print(f"[ERR] env {args.token_env} is empty", file=sys.stderr)
        return 2

    github_repo = str(args.github_repo).strip()
    gitee_repo = str(args.gitee_repo).strip() or github_repo
    if "/" not in github_repo or "/" not in gitee_repo:
        print("[ERR] repo format must be owner/name", file=sys.stderr)
        return 2
    gitee_owner, gitee_name = gitee_repo.split("/", 1)
    tag = str(args.tag).strip()
    wanted_assets = args.asset or ["nfa95", "nfa95.exe", ".env.example"]

    _ensure_curl()

    print(f"[INFO] github repo: {github_repo}, tag: {tag}")
    gh_release = _request_json(
        f"https://api.github.com/repos/{github_repo}/releases/tags/{urllib.parse.quote(tag)}",
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "nfa95-release-sync",
        },
    )
    gh_assets_raw = gh_release.get("assets") or []
    gh_assets: dict[str, str] = {}
    if isinstance(gh_assets_raw, list):
        for a in gh_assets_raw:
            if not isinstance(a, dict):
                continue
            name = str(a.get("name") or "").strip()
            url = str(a.get("browser_download_url") or "").strip()
            if name and url:
                gh_assets[name] = url

    with tempfile.TemporaryDirectory(prefix="nfa95-gitee-sync-") as tmp:
        tmpdir = Path(tmp)
        local_files: list[Path] = []
        for name in wanted_assets:
            url = gh_assets.get(name)
            if not url:
                print(f"[WARN] asset not found in github release: {name}")
                continue
            dst = tmpdir / name
            print(f"[INFO] downloading {name} ...")
            _download_file(url, dst)
            local_files.append(dst)

        if not local_files:
            print("[ERR] no assets resolved from github release", file=sys.stderr)
            return 3

        api_base = f"https://gitee.com/api/v5/repos/{gitee_owner}/{gitee_name}"
        print(f"[INFO] gitee repo: {gitee_repo}")
        gitee_release = _request_json_or_none(
            f"{api_base}/releases/tags/{urllib.parse.quote(tag)}?access_token={urllib.parse.quote(token)}"
        )
        if not gitee_release or not str(gitee_release.get("id") or "").strip():
            print("[INFO] gitee release not found, creating...")
            gitee_release = _request_json(
                f"{api_base}/releases",
                method="POST",
                data={
                    "access_token": token,
                    "tag_name": tag,
                    "target_commitish": "main",
                    "name": tag,
                    "body": f"Synced from GitHub release tag {tag}",
                },
            )

        release_id = str(gitee_release.get("id") or "").strip()
        if not release_id:
            print("[ERR] failed to resolve gitee release id", file=sys.stderr)
            return 4
        print(f"[INFO] gitee release id: {release_id}")

        attach_list_raw = _request_json(
            f"{api_base}/releases/{release_id}?access_token={urllib.parse.quote(token)}"
        ).get("assets") or []
        attach_map: dict[str, str] = {}
        if isinstance(attach_list_raw, list):
            for a in attach_list_raw:
                if not isinstance(a, dict):
                    continue
                name = str(a.get("name") or "").strip()
                aid = str(a.get("id") or "").strip()
                if name and aid:
                    attach_map[name] = aid

        for file_path in local_files:
            name = file_path.name
            old_id = attach_map.get(name)
            if old_id:
                print(f"[INFO] deleting old gitee asset: {name} ({old_id})")
                _request_json_or_none(
                    f"{api_base}/releases/{release_id}/attach_files/{old_id}?access_token={urllib.parse.quote(token)}",
                    method="DELETE",
                )
            print(f"[INFO] uploading to gitee: {name}")
            _upload_file_with_curl(api_base=api_base, release_id=release_id, token=token, file_path=file_path)

    print("[OK] gitee release asset sync completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())

