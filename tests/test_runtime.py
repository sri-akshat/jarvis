from __future__ import annotations

from pathlib import Path

from jarvis.cli.runtime import configure_runtime
from jarvis.config import load_settings


def test_load_settings_uses_env(monkeypatch, tmp_path):
    db_path = tmp_path / "env.db"
    monkeypatch.setenv("JARVIS_DATABASE", str(db_path))
    monkeypatch.delenv("JARVIS_TASK_QUEUE", raising=False)
    monkeypatch.delenv("JARVIS_STRUCTURED_LOGGING", raising=False)
    config = load_settings()
    assert config.database_path == db_path
    assert config.task_queue_url is None
    assert config.structured_logging is False


def test_configure_runtime_overrides(monkeypatch, tmp_path):
    db_path = tmp_path / "runtime.db"
    config = configure_runtime(str(db_path), "debug", structured=True)
    assert config.database_path == db_path


def test_load_settings_reads_redis_queue(monkeypatch):
    redis_url = "redis://localhost:6379/0"
    monkeypatch.setenv("JARVIS_TASK_QUEUE", redis_url)
    config = load_settings()
    assert config.task_queue_url == redis_url


def test_load_settings_from_ini(monkeypatch, tmp_path):
    config_file = tmp_path / "jarvis.ini"
    config_file.write_text(
        """
        [database]
        path = data/custom.db

        [queue]
        task_queue_url = redis://example:6379/1

        [logging]
        level = WARNING
        structured = true
        """
    )
    monkeypatch.setenv("JARVIS_CONFIG_FILE", str(config_file))
    monkeypatch.delenv("JARVIS_DATABASE", raising=False)
    monkeypatch.delenv("JARVIS_TASK_QUEUE", raising=False)
    config = load_settings()
    assert config.database_path == Path("data/custom.db")
    assert config.task_queue_url == "redis://example:6379/1"
    assert config.log_level == "WARNING"
    assert config.structured_logging is True
