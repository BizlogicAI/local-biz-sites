"""Tests for the scheduler module."""

from __future__ import annotations

import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from scraper.config import AutonomousConfig, PipelineConfig, RetryConfig, ScheduleConfig
from scraper.pipeline import SearchSpec
from scraper.scheduler import PipelineScheduler


def _make_config(tmp_path: Path) -> AutonomousConfig:
    return AutonomousConfig(
        schedule=ScheduleConfig(interval_hours=1.0, run_on_start=False),
        retry=RetryConfig(max_attempts=1),
        state_file=tmp_path / "state.json",
        logs_dir=tmp_path / "logs",
        pipeline=PipelineConfig(
            rate_limit_delay_seconds=0.0,
            api=MagicMock(google_maps_api_key="fake-key"),
        ),
    )


class TestPipelineScheduler:
    def test_creates_scheduler(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        specs = [SearchSpec(category="plumber", location="Denver, CO")]
        scheduler = PipelineScheduler(config, specs)
        assert scheduler is not None

    def test_stop_without_start(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        specs = [SearchSpec(category="plumber", location="Denver, CO")]
        scheduler = PipelineScheduler(config, specs)
        scheduler.stop()

    def test_run_now_executes_pipeline(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        specs = [SearchSpec(category="plumber", location="Denver, CO")]
        scheduler = PipelineScheduler(config, specs)

        with patch.object(scheduler._runner, "run_once") as mock_run:
            mock_run.return_value = MagicMock(status="success")
            scheduler.run_now()
            mock_run.assert_called_once_with(specs)

    def test_is_running(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        specs = [SearchSpec(category="plumber", location="Denver, CO")]
        scheduler = PipelineScheduler(config, specs)
        assert scheduler.is_running is False
