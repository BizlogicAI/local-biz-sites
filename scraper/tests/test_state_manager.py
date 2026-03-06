"""Tests for autonomous state manager."""

from __future__ import annotations

from pathlib import Path

import pytest

from scraper.state_manager import (
    RunRecord,
    StateManager,
    RunStatus,
)


class TestRunRecord:
    def test_frozen(self) -> None:
        record = RunRecord(
            started_at="2026-03-06T10:00:00Z",
            status=RunStatus.SUCCESS,
        )
        with pytest.raises(AttributeError):
            record.status = RunStatus.FAILED  # type: ignore[misc]

    def test_defaults(self) -> None:
        record = RunRecord(
            started_at="2026-03-06T10:00:00Z",
            status=RunStatus.SUCCESS,
        )
        assert record.discovered == 0
        assert record.analyzed == 0
        assert record.demos_generated == 0
        assert record.errors == ()
        assert record.completed_at == ""


class TestStateManager:
    def test_load_empty_creates_fresh_state(self, tmp_path: Path) -> None:
        state_file = tmp_path / "state.json"
        sm = StateManager.load(state_file)
        assert sm.processed_ids == frozenset()
        assert sm.run_history == ()
        assert sm.failed_ids == {}

    def test_save_and_load_roundtrip(self, tmp_path: Path) -> None:
        state_file = tmp_path / "state.json"
        record = RunRecord(
            started_at="2026-03-06T10:00:00Z",
            completed_at="2026-03-06T10:05:00Z",
            status=RunStatus.SUCCESS,
            discovered=10,
            analyzed=8,
            demos_generated=5,
            errors=("one error",),
        )
        sm = StateManager(
            processed_ids=frozenset({"lead-001", "lead-002"}),
            run_history=(record,),
            failed_ids={"lead-999": "API timeout"},
            state_file=state_file,
        )
        sm.save()

        loaded = StateManager.load(state_file)
        assert loaded.processed_ids == frozenset({"lead-001", "lead-002"})
        assert len(loaded.run_history) == 1
        assert loaded.run_history[0].discovered == 10
        assert loaded.failed_ids == {"lead-999": "API timeout"}

    def test_is_processed(self, tmp_path: Path) -> None:
        sm = StateManager(
            processed_ids=frozenset({"lead-001"}),
            state_file=tmp_path / "state.json",
        )
        assert sm.is_processed("lead-001") is True
        assert sm.is_processed("lead-999") is False

    def test_mark_processed_returns_new_instance(self, tmp_path: Path) -> None:
        sm = StateManager(
            processed_ids=frozenset(),
            state_file=tmp_path / "state.json",
        )
        new_sm = sm.mark_processed("lead-001")
        assert new_sm is not sm
        assert "lead-001" in new_sm.processed_ids
        assert "lead-001" not in sm.processed_ids

    def test_mark_failed_returns_new_instance(self, tmp_path: Path) -> None:
        sm = StateManager(
            processed_ids=frozenset(),
            state_file=tmp_path / "state.json",
        )
        new_sm = sm.mark_failed("lead-999", "timeout")
        assert new_sm is not sm
        assert new_sm.failed_ids == {"lead-999": "timeout"}
        assert sm.failed_ids == {}

    def test_add_run_record(self, tmp_path: Path) -> None:
        sm = StateManager(state_file=tmp_path / "state.json")
        record = RunRecord(
            started_at="2026-03-06T10:00:00Z",
            status=RunStatus.SUCCESS,
        )
        new_sm = sm.add_run(record)
        assert len(new_sm.run_history) == 1
        assert len(sm.run_history) == 0

    def test_run_history_capped(self, tmp_path: Path) -> None:
        records = tuple(
            RunRecord(started_at=f"2026-03-{i:02d}T10:00:00Z", status=RunStatus.SUCCESS)
            for i in range(1, 35)
        )
        sm = StateManager(
            run_history=records,
            state_file=tmp_path / "state.json",
            max_history=30,
        )
        new_record = RunRecord(
            started_at="2026-04-01T10:00:00Z", status=RunStatus.SUCCESS
        )
        new_sm = sm.add_run(new_record)
        assert len(new_sm.run_history) <= 30
        assert new_sm.run_history[-1].started_at == "2026-04-01T10:00:00Z"

    def test_last_run_none_when_empty(self, tmp_path: Path) -> None:
        sm = StateManager(state_file=tmp_path / "state.json")
        assert sm.last_run is None

    def test_last_run_returns_most_recent(self, tmp_path: Path) -> None:
        records = (
            RunRecord(started_at="2026-03-01T10:00:00Z", status=RunStatus.SUCCESS),
            RunRecord(started_at="2026-03-02T10:00:00Z", status=RunStatus.FAILED),
        )
        sm = StateManager(run_history=records, state_file=tmp_path / "state.json")
        assert sm.last_run is not None
        assert sm.last_run.started_at == "2026-03-02T10:00:00Z"

    def test_load_corrupt_file_returns_fresh(self, tmp_path: Path) -> None:
        state_file = tmp_path / "state.json"
        state_file.write_text("not valid json{{{")
        sm = StateManager.load(state_file)
        assert sm.processed_ids == frozenset()
        assert sm.run_history == ()
