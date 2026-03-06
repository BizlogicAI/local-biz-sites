"""Tests for executor with retry logic."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from scraper.config import RetryConfig
from scraper.executor import ExecutionError, execute_with_retry


class TestExecuteWithRetry:
    def test_succeeds_first_attempt(self) -> None:
        func = MagicMock(return_value="ok")
        result = execute_with_retry(func, retry_config=RetryConfig(max_attempts=3))
        assert result == "ok"
        assert func.call_count == 1

    def test_retries_on_transient_error(self) -> None:
        func = MagicMock(side_effect=[ConnectionError("fail"), "ok"])
        with patch("scraper.executor.time.sleep"):
            result = execute_with_retry(
                func,
                retry_config=RetryConfig(max_attempts=3),
                retryable_errors=(ConnectionError,),
            )
        assert result == "ok"
        assert func.call_count == 2

    def test_raises_after_max_attempts(self) -> None:
        func = MagicMock(side_effect=ConnectionError("fail"))
        with patch("scraper.executor.time.sleep"):
            with pytest.raises(ExecutionError, match="after 3 attempts"):
                execute_with_retry(
                    func,
                    retry_config=RetryConfig(max_attempts=3),
                    retryable_errors=(ConnectionError,),
                )
        assert func.call_count == 3

    def test_non_retryable_error_raises_immediately(self) -> None:
        func = MagicMock(side_effect=ValueError("bad input"))
        with pytest.raises(ValueError, match="bad input"):
            execute_with_retry(
                func,
                retry_config=RetryConfig(max_attempts=3),
                retryable_errors=(ConnectionError,),
            )
        assert func.call_count == 1

    def test_backoff_increases(self) -> None:
        func = MagicMock(side_effect=[ConnectionError("1"), ConnectionError("2"), "ok"])
        sleep_calls: list[float] = []
        with patch(
            "scraper.executor.time.sleep", side_effect=lambda s: sleep_calls.append(s)
        ):
            execute_with_retry(
                func,
                retry_config=RetryConfig(
                    max_attempts=3,
                    backoff_base_seconds=2.0,
                    max_backoff_seconds=60.0,
                ),
                retryable_errors=(ConnectionError,),
            )
        assert len(sleep_calls) == 2
        assert sleep_calls[0] == 2.0
        assert sleep_calls[1] == 4.0

    def test_backoff_capped_at_max(self) -> None:
        func = MagicMock(side_effect=[ConnectionError("1"), ConnectionError("2"), "ok"])
        sleep_calls: list[float] = []
        with patch(
            "scraper.executor.time.sleep", side_effect=lambda s: sleep_calls.append(s)
        ):
            execute_with_retry(
                func,
                retry_config=RetryConfig(
                    max_attempts=3,
                    backoff_base_seconds=50.0,
                    max_backoff_seconds=60.0,
                ),
                retryable_errors=(ConnectionError,),
            )
        assert sleep_calls[1] <= 60.0

    def test_default_retryable_errors_empty(self) -> None:
        func = MagicMock(side_effect=RuntimeError("nope"))
        with pytest.raises(RuntimeError):
            execute_with_retry(func, retry_config=RetryConfig(max_attempts=3))
        assert func.call_count == 1
