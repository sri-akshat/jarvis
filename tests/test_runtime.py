from __future__ import annotations

from pathlib import Path

from jarvis.cli.runtime import configure_runtime
from jarvis.config import load_settings


def test_load_settings_uses_env(monkeypatch, tmp_path):
    db_path = tmp_path / "env.db"
    monkeypatch.setenv("JARVIS_DATABASE", str(db_path))
    config = load_settings()
    assert config.database_path == db_path


def test_configure_runtime_overrides(monkeypatch, tmp_path):
    db_path = tmp_path / "runtime.db"
    config = configure_runtime(str(db_path), "debug")
    assert config.database_path == db_path
