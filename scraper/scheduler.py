"""Schedule pipeline runs at configurable intervals."""

from __future__ import annotations

import logging
import signal
import threading
from types import FrameType

from scraper.autonomous import AutonomousRunner
from scraper.config import AutonomousConfig
from scraper.pipeline import SearchSpec

logger = logging.getLogger(__name__)


class PipelineScheduler:
    """Run the pipeline on a configurable interval with graceful shutdown."""

    def __init__(
        self,
        config: AutonomousConfig,
        search_specs: list[SearchSpec],
    ) -> None:
        self._config = config
        self._specs = search_specs
        self._runner = AutonomousRunner(config)
        self._stop_event = threading.Event()
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running

    def run_now(self) -> None:
        """Execute a single pipeline run immediately."""
        logger.info("Starting manual pipeline run")
        self._runner.run_once(self._specs)

    def start(self) -> None:
        """Start the scheduled loop. Blocks until stop() is called or interrupted."""
        self._running = True
        self._install_signal_handlers()
        interval_seconds = self._config.schedule.interval_hours * 3600

        logger.info(
            "Scheduler started: interval=%.1fh, run_on_start=%s",
            self._config.schedule.interval_hours,
            self._config.schedule.run_on_start,
        )

        if self._config.schedule.run_on_start:
            self._safe_run()

        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=interval_seconds)
            if not self._stop_event.is_set():
                self._safe_run()

        self._running = False
        logger.info("Scheduler stopped")

    def stop(self) -> None:
        """Signal the scheduler to stop after the current run completes."""
        logger.info("Shutdown requested")
        self._stop_event.set()

    def _safe_run(self) -> None:
        """Execute a pipeline run, catching all exceptions."""
        try:
            self._runner.run_once(self._specs)
        except Exception:
            logger.exception("Unhandled error during scheduled run")

    def _install_signal_handlers(self) -> None:
        """Install SIGINT/SIGTERM handlers for graceful shutdown."""

        def handler(signum: int, frame: FrameType | None) -> None:
            logger.info("Received signal %d, shutting down", signum)
            self.stop()

        try:
            signal.signal(signal.SIGINT, handler)
            signal.signal(signal.SIGTERM, handler)
        except (OSError, ValueError) as exc:
            logger.warning("Could not install signal handlers: %s", exc)
