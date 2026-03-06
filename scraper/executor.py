"""Execute pipeline stages with retry logic and error recovery."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import TypeVar

from scraper.config import RetryConfig

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ExecutionError(Exception):
    """Raised when execution fails after all retry attempts."""


def execute_with_retry(
    func: Callable[[], T],
    *,
    retry_config: RetryConfig,
    retryable_errors: tuple[type[Exception], ...] = (),
) -> T:
    """Execute a callable with exponential backoff retry.

    Args:
        func: Zero-arg callable to execute.
        retry_config: Retry behavior configuration.
        retryable_errors: Exception types that trigger a retry.

    Returns:
        The return value of func.

    Raises:
        ExecutionError: If all retry attempts are exhausted.
        Exception: If a non-retryable error occurs.
    """
    last_error: Exception | None = None

    for attempt in range(1, retry_config.max_attempts + 1):
        try:
            return func()
        except retryable_errors as exc:
            last_error = exc
            if attempt >= retry_config.max_attempts:
                break
            wait = min(
                retry_config.backoff_base_seconds**attempt,
                retry_config.max_backoff_seconds,
            )
            logger.warning(
                "Attempt %d/%d failed: %s. Retrying in %.1fs",
                attempt,
                retry_config.max_attempts,
                exc,
                wait,
            )
            time.sleep(wait)

    raise ExecutionError(
        f"Failed after {retry_config.max_attempts} attempts: {last_error}"
    )
