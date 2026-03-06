"""Tests for health check module."""

from __future__ import annotations

from pathlib import Path

import pytest

from scraper.health import HealthReport, get_health_report
from scraper.state_manager import RunRecord, RunStatus, StateManager


class TestGetHealthReport:
    def test_no_runs_returns_idle(self, tmp_path: Path) -> None:
        state_file = tmp_path / "state.json"
        report = get_health_report(state_file)
        assert report.status == "idle"
        assert report.last_run_at == ""
        assert report.total_runs == 0

    def test_report_from_successful_runs(self, tmp_path: Path) -> None:
        state_file = tmp_path / "state.json"
        records = (
            RunRecord(
                started_at="2026-03-01T10:00:00Z",
                completed_at="2026-03-01T10:05:00Z",
                status=RunStatus.SUCCESS,
                discovered=10,
                demos_generated=5,
            ),
            RunRecord(
                started_at="2026-03-02T10:00:00Z",
                completed_at="2026-03-02T10:05:00Z",
                status=RunStatus.SUCCESS,
                discovered=8,
                demos_generated=3,
            ),
        )
        sm = StateManager(run_history=records, state_file=state_file)
        sm.save()

        report = get_health_report(state_file)
        assert report.status == "healthy"
        assert report.last_run_at == "2026-03-02T10:00:00Z"
        assert report.total_runs == 2
        assert report.total_discovered == 18
        assert report.total_demos == 8
        assert report.error_rate == 0.0

    def test_report_with_failures(self, tmp_path: Path) -> None:
        state_file = tmp_path / "state.json"
        records = (
            RunRecord(started_at="2026-03-01T10:00:00Z", status=RunStatus.SUCCESS),
            RunRecord(started_at="2026-03-02T10:00:00Z", status=RunStatus.FAILED),
            RunRecord(started_at="2026-03-03T10:00:00Z", status=RunStatus.FAILED),
        )
        sm = StateManager(run_history=records, state_file=state_file)
        sm.save()

        report = get_health_report(state_file)
        assert report.status == "degraded"
        assert report.error_rate == pytest.approx(2 / 3)

    def test_report_last_run_failed(self, tmp_path: Path) -> None:
        state_file = tmp_path / "state.json"
        records = (
            RunRecord(started_at="2026-03-01T10:00:00Z", status=RunStatus.FAILED),
        )
        sm = StateManager(run_history=records, state_file=state_file)
        sm.save()

        report = get_health_report(state_file)
        assert report.status == "degraded"


class TestHealthReport:
    def test_frozen(self) -> None:
        report = HealthReport(
            status="healthy",
            last_run_at="",
            total_runs=0,
            total_discovered=0,
            total_demos=0,
            error_rate=0.0,
        )
        with pytest.raises(AttributeError):
            report.status = "bad"  # type: ignore[misc]
