from __future__ import annotations

from pathlib import Path

from server import config


def test_app_base_dir_prefers_argv0_when_frozen(monkeypatch):
    monkeypatch.setattr(config.sys, "frozen", True, raising=False)
    monkeypatch.setattr(config.sys, "argv", ["/home/nfa95/nfa95"], raising=False)
    monkeypatch.setattr(config.sys, "executable", "/home/nfa95/releases/v1/nfa95", raising=False)

    out = config._app_base_dir()
    assert out.as_posix().endswith("/home/nfa95")


def test_app_base_dir_falls_back_to_executable_when_argv0_empty(monkeypatch):
    monkeypatch.setattr(config.sys, "frozen", True, raising=False)
    monkeypatch.setattr(config.sys, "argv", [""], raising=False)
    monkeypatch.setattr(config.sys, "executable", "/home/nfa95/releases/v1/nfa95", raising=False)

    out = config._app_base_dir()
    assert out.as_posix().endswith("/home/nfa95/releases/v1")
