"""Tests for autonomous pipeline orchestrator."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from scraper.autonomous import AutonomousRunner, RunOutcome
from scraper.config import AutonomousConfig, PipelineConfig, RetryConfig, ScheduleConfig
from scraper.pipeline import PipelineResult, SearchSpec
from scraper.state_manager import RunStatus, StateManager


def _make_config(tmp_path: Path) -> AutonomousConfig:
    return AutonomousConfig(
        schedule=ScheduleConfig(interval_hours=24.0),
        retry=RetryConfig(max_attempts=2, backoff_base_seconds=0.01),
        state_file=tmp_path / "state.json",
        logs_dir=tmp_path / "logs",
        pipeline=PipelineConfig(
            rate_limit_delay_seconds=0.0,
            api=MagicMock(google_maps_api_key="fake-key"),
        ),
    )


class TestAutonomousRunner:
    def test_run_once_success(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        specs = [SearchSpec(category="plumber", location="Denver, CO")]

        mock_result = PipelineResult(
            discovered=5, analyzed=3, demos_generated=2, errors=()
        )

        with patch("scraper.autonomous.Pipeline") as mock_pipeline_cls:
            mock_pipeline_cls.return_value.run.return_value = mock_result
            runner = AutonomousRunner(config)
            outcome = runner.run_once(specs)

        assert outcome.status == RunStatus.SUCCESS
        assert outcome.result.discovered == 5

    def test_run_once_with_errors_is_partial(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        specs = [SearchSpec(category="plumber", location="Denver, CO")]

        mock_result = PipelineResult(
            discovered=5,
            analyzed=3,
            demos_generated=2,
            errors=("one error",),
        )

        with patch("scraper.autonomous.Pipeline") as mock_pipeline_cls:
            mock_pipeline_cls.return_value.run.return_value = mock_result
            runner = AutonomousRunner(config)
            outcome = runner.run_once(specs)

        assert outcome.status == RunStatus.PARTIAL

    def test_run_once_failure_caught(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        specs = [SearchSpec(category="plumber", location="Denver, CO")]

        with patch("scraper.autonomous.Pipeline") as mock_pipeline_cls:
            mock_pipeline_cls.return_value.run.side_effect = RuntimeError("boom")
            runner = AutonomousRunner(config)
            outcome = runner.run_once(specs)

        assert outcome.status == RunStatus.FAILED
        assert "boom" in outcome.error

    def test_run_once_persists_state(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        specs = [SearchSpec(category="plumber", location="Denver, CO")]

        mock_result = PipelineResult(discovered=5, errors=())

        with patch("scraper.autonomous.Pipeline") as mock_pipeline_cls:
            mock_pipeline_cls.return_value.run.return_value = mock_result
            runner = AutonomousRunner(config)
            runner.run_once(specs)

        loaded = StateManager.load(config.state_file)
        assert len(loaded.run_history) == 1

    def test_run_once_configures_logging(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        specs = [SearchSpec(category="plumber", location="Denver, CO")]

        mock_result = PipelineResult(discovered=0, errors=())

        with patch("scraper.autonomous.Pipeline") as mock_pipeline_cls:
            mock_pipeline_cls.return_value.run.return_value = mock_result
            runner = AutonomousRunner(config)
            runner.run_once(specs)

        assert config.logs_dir.exists()
