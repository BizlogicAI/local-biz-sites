"""Health check and operational status reporting."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from scraper.config import STATE_FILE
from scraper.state_manager import RunStatus, StateManager


@dataclass(frozen=True)
class HealthReport:
    """Immutable snapshot of pipeline health."""

    status: str
    last_run_at: str
    total_runs: int
    total_discovered: int
    total_demos: int
    error_rate: float


def get_health_report(state_file: Path = STATE_FILE) -> HealthReport:
    """Generate a health report from persisted state.

    Args:
        state_file: Path to the state JSON file.

    Returns:
        HealthReport with aggregated metrics.
    """
    state = StateManager.load(state_file)
    history = state.run_history

    if not history:
        return HealthReport(
            status="idle",
            last_run_at="",
            total_runs=0,
            total_discovered=0,
            total_demos=0,
            error_rate=0.0,
        )

    total_runs = len(history)
    failed_runs = sum(1 for r in history if r.status == RunStatus.FAILED)
    error_rate = failed_runs / total_runs if total_runs > 0 else 0.0

    total_discovered = sum(r.discovered for r in history)
    total_demos = sum(r.demos_generated for r in history)

    last_run = history[-1]
    status = (
        "healthy"
        if last_run.status != RunStatus.FAILED and error_rate < 0.5
        else "degraded"
    )

    return HealthReport(
        status=status,
        last_run_at=last_run.started_at,
        total_runs=total_runs,
        total_discovered=total_discovered,
        total_demos=total_demos,
        error_rate=error_rate,
    )
