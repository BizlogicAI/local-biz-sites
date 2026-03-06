"""Autonomous pipeline orchestrator with logging and state persistence."""

from __future__ import annotations

import logging
import logging.handlers
from dataclasses import dataclass
from datetime import datetime, timezone

from scraper.config import AutonomousConfig, LEADS_DIR
from scraper.lead_manager import LeadManager
from scraper.pipeline import Pipeline, PipelineResult, SearchSpec
from scraper.state_manager import RunRecord, RunStatus, StateManager

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunOutcome:
    """Immutable result of an autonomous pipeline run."""

    status: RunStatus
    result: PipelineResult
    error: str = ""


def _setup_logging(config: AutonomousConfig) -> None:
    """Configure file-based logging with rotation."""
    config.logs_dir.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger("scraper")
    if root_logger.handlers:
        return

    root_logger.setLevel(logging.DEBUG)

    file_handler = logging.handlers.TimedRotatingFileHandler(
        config.logs_dir / "pipeline.log",
        when="D",
        backupCount=7,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s")
    )
    root_logger.addHandler(file_handler)

    error_handler = logging.handlers.TimedRotatingFileHandler(
        config.logs_dir / "errors.log",
        when="D",
        backupCount=7,
        encoding="utf-8",
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(
        logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s")
    )
    root_logger.addHandler(error_handler)


class AutonomousRunner:
    """Run the pipeline with state tracking and logging."""

    def __init__(self, config: AutonomousConfig) -> None:
        self._config = config

    def run_once(self, search_specs: list[SearchSpec]) -> RunOutcome:
        """Execute a single pipeline run with state persistence.

        Args:
            search_specs: Categories and locations to search.

        Returns:
            RunOutcome with status, result, and any error.
        """
        _setup_logging(self._config)
        started_at = datetime.now(timezone.utc).isoformat()
        state = StateManager.load(self._config.state_file)

        try:
            leads_csv = LEADS_DIR / "leads.csv"
            manager = LeadManager.load_from_csv(leads_csv)
            pipeline = Pipeline(config=self._config.pipeline, lead_manager=manager)
            result = pipeline.run(search_specs)

            status = RunStatus.SUCCESS if not result.errors else RunStatus.PARTIAL
            outcome = RunOutcome(status=status, result=result)

        except Exception as exc:
            logger.exception("Pipeline run failed")
            result = PipelineResult()
            outcome = RunOutcome(
                status=RunStatus.FAILED,
                result=result,
                error=str(exc),
            )

        completed_at = datetime.now(timezone.utc).isoformat()
        record = RunRecord(
            started_at=started_at,
            completed_at=completed_at,
            status=outcome.status,
            discovered=outcome.result.discovered,
            analyzed=outcome.result.analyzed,
            demos_generated=outcome.result.demos_generated,
            errors=outcome.result.errors,
        )
        state = state.add_run(record)
        state.save()

        logger.info(
            "Run complete: status=%s discovered=%d analyzed=%d demos=%d",
            outcome.status,
            outcome.result.discovered,
            outcome.result.analyzed,
            outcome.result.demos_generated,
        )

        return outcome
