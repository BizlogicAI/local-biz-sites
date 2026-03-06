"""State persistence for autonomous pipeline operation."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from scraper.config import STATE_FILE

logger = logging.getLogger(__name__)


class RunStatus(StrEnum):
    """Status of a pipeline run."""

    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"


@dataclass(frozen=True)
class RunRecord:
    """Immutable record of a single pipeline run."""

    started_at: str
    status: RunStatus
    completed_at: str = ""
    discovered: int = 0
    analyzed: int = 0
    demos_generated: int = 0
    errors: tuple[str, ...] = ()


def _record_to_dict(record: RunRecord) -> dict[str, Any]:
    return {
        "started_at": record.started_at,
        "completed_at": record.completed_at,
        "status": record.status.value,
        "discovered": record.discovered,
        "analyzed": record.analyzed,
        "demos_generated": record.demos_generated,
        "errors": list(record.errors),
    }


def _dict_to_record(data: dict[str, Any]) -> RunRecord:
    return RunRecord(
        started_at=data.get("started_at", ""),
        completed_at=data.get("completed_at", ""),
        status=RunStatus(data.get("status", "failed")),
        discovered=data.get("discovered", 0),
        analyzed=data.get("analyzed", 0),
        demos_generated=data.get("demos_generated", 0),
        errors=tuple(data.get("errors", ())),
    )


class StateManager:
    """Track pipeline run history and processed lead IDs.

    All mutations return new StateManager instances (immutable pattern).
    """

    def __init__(
        self,
        *,
        processed_ids: frozenset[str] = frozenset(),
        run_history: tuple[RunRecord, ...] = (),
        failed_ids: dict[str, str] | None = None,
        state_file: Path = STATE_FILE,
        max_history: int = 30,
    ) -> None:
        self._processed_ids = processed_ids
        self._run_history = run_history
        self._failed_ids = dict(failed_ids) if failed_ids else {}
        self._state_file = state_file
        self._max_history = max_history

    @property
    def processed_ids(self) -> frozenset[str]:
        return self._processed_ids

    @property
    def run_history(self) -> tuple[RunRecord, ...]:
        return self._run_history

    @property
    def failed_ids(self) -> dict[str, str]:
        return dict(self._failed_ids)

    @property
    def last_run(self) -> RunRecord | None:
        if not self._run_history:
            return None
        return self._run_history[-1]

    def is_processed(self, lead_id: str) -> bool:
        return lead_id in self._processed_ids

    def mark_processed(self, lead_id: str) -> StateManager:
        return StateManager(
            processed_ids=self._processed_ids | {lead_id},
            run_history=self._run_history,
            failed_ids=self._failed_ids,
            state_file=self._state_file,
            max_history=self._max_history,
        )

    def mark_failed(self, lead_id: str, error: str) -> StateManager:
        new_failed = {**self._failed_ids, lead_id: error}
        return StateManager(
            processed_ids=self._processed_ids,
            run_history=self._run_history,
            failed_ids=new_failed,
            state_file=self._state_file,
            max_history=self._max_history,
        )

    def add_run(self, record: RunRecord) -> StateManager:
        history = (*self._run_history, record)
        if len(history) > self._max_history:
            history = history[-self._max_history :]
        return StateManager(
            processed_ids=self._processed_ids,
            run_history=history,
            failed_ids=self._failed_ids,
            state_file=self._state_file,
            max_history=self._max_history,
        )

    def save(self) -> None:
        data = {
            "processed_ids": sorted(self._processed_ids),
            "failed_ids": self._failed_ids,
            "run_history": [_record_to_dict(r) for r in self._run_history],
        }
        self._state_file.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._state_file.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        tmp_path.replace(self._state_file)

    @classmethod
    def load(cls, state_file: Path = STATE_FILE) -> StateManager:
        if not state_file.exists():
            return cls(state_file=state_file)
        try:
            raw = json.loads(state_file.read_text(encoding="utf-8"))
            return cls(
                processed_ids=frozenset(raw.get("processed_ids", [])),
                run_history=tuple(
                    _dict_to_record(r) for r in raw.get("run_history", [])
                ),
                failed_ids=raw.get("failed_ids", {}),
                state_file=state_file,
            )
        except (json.JSONDecodeError, KeyError, ValueError):
            logger.warning("Corrupt state file %s, starting fresh", state_file)
            return cls(state_file=state_file)
