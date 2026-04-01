from __future__ import annotations

from types import SimpleNamespace

import pytest

from server.services import updater


def _settings(**overrides):
    base = {
        "APP_VERSION": "0.1.0",
        "GITHUB_REPO": "ThinkHao/nfatool",
        "GITEE_REPO": None,
        "GITEE_TOKEN": None,
        "UPDATE_SOURCE_PRIORITY": "gitee,github",
        "UPDATE_ASSET_LINUX": "nfa95",
        "UPDATE_ASSET_WINDOWS": "nfa95.exe",
        "UPDATE_EXTERNAL_SCRIPT": None,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_check_update_prefers_gitee(monkeypatch):
    monkeypatch.setattr(updater, "get_settings", lambda: _settings())
    monkeypatch.setattr(updater, "_pick_asset_name", lambda: "nfa95")
    monkeypatch.setattr(
        updater,
        "_check_update_from_gitee",
        lambda current, repo, asset_name, token: {
            "ok": True,
            "source": "gitee",
            "repo": repo,
            "current_version": current,
            "latest_version": "v0.2.0",
            "update_available": True,
            "published_at": "2026-04-01T00:00:00Z",
            "asset_name": asset_name,
            "asset_url": "https://gitee.com/download/nfa95",
            "release_url": "https://gitee.com/release/v0.2.0",
        },
    )
    monkeypatch.setattr(updater, "_check_update_from_github", lambda *_: pytest.fail("should not call github when gitee succeeds"))

    out = updater.check_update()
    assert out["ok"] is True
    assert out["source"] == "gitee"
    assert out["latest_version"] == "v0.2.0"


def test_check_update_fallback_to_github(monkeypatch):
    monkeypatch.setattr(updater, "get_settings", lambda: _settings())
    monkeypatch.setattr(updater, "_pick_asset_name", lambda: "nfa95")

    def _raise(*_args, **_kwargs):
        raise RuntimeError("gitee unavailable")

    monkeypatch.setattr(updater, "_check_update_from_gitee", _raise)
    monkeypatch.setattr(
        updater,
        "_check_update_from_github",
        lambda current, repo, asset_name: {
            "ok": True,
            "source": "github",
            "repo": repo,
            "current_version": current,
            "latest_version": "v0.2.1",
            "update_available": True,
            "published_at": "2026-04-01T00:00:00Z",
            "asset_name": asset_name,
            "asset_url": "https://github.com/download/nfa95",
            "release_url": "https://github.com/release/v0.2.1",
        },
    )

    out = updater.check_update()
    assert out["ok"] is True
    assert out["source"] == "github"
    assert out["latest_version"] == "v0.2.1"


def test_check_update_returns_aggregated_error_when_all_sources_fail(monkeypatch):
    monkeypatch.setattr(updater, "get_settings", lambda: _settings())
    monkeypatch.setattr(updater, "_pick_asset_name", lambda: "nfa95")
    monkeypatch.setattr(updater, "_check_update_from_gitee", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("gitee fail")))
    monkeypatch.setattr(updater, "_check_update_from_github", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("github fail")))

    out = updater.check_update()
    assert out["ok"] is False
    assert "all update sources failed" in out["message"]
    assert len(out["errors"]) == 2
    assert out["errors"][0].startswith("gitee:")
    assert out["errors"][1].startswith("github:")


def test_apply_update_requires_asset_url(monkeypatch):
    monkeypatch.setattr(updater, "get_settings", lambda: _settings())
    monkeypatch.setattr(
        updater,
        "check_update",
        lambda: {
            "ok": True,
            "source": "gitee",
            "latest_version": "v0.2.2",
            "update_available": True,
            "asset_name": "nfa95",
            "asset_url": "",
        },
    )

    with pytest.raises(ValueError, match="release asset not found"):
        updater.apply_update()
