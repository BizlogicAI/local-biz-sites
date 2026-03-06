"""Tests for web presence analysis and quality scoring."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from scraper.config import (
    RECOMMENDATION_GENERATE_DEMO,
    RECOMMENDATION_SKIP,
    RECOMMENDATION_UPGRADE,
)
from scraper.web_analyzer import (
    AnalysisResult,
    WebAnalyzer,
    WebAnalyzerError,
    _score_html,
)


# --- Sample HTML ---

GOOD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Best Plumbing Co</title>
    <link rel="stylesheet" href="styles.css">
</head>
<body>
    <nav>
        <a href="/">Home</a>
        <a href="/about">About</a>
        <a href="/contact">Contact</a>
    </nav>
    <header>
        <h1>Best Plumbing Co</h1>
        <p>Professional plumbing services in Denver</p>
        <a href="/contact" class="btn">Get a Free Quote</a>
    </header>
    <section>
        <h2>Our Services</h2>
        <p>We offer residential and commercial plumbing.</p>
    </section>
    <footer>
        <p>&copy; 2025 Best Plumbing Co</p>
    </footer>
</body>
</html>
"""

BAD_HTML = """
<html>
<body>
<p>Welcome to our website</p>
<p>Call us at 555-1234</p>
</body>
</html>
"""

MEDIOCRE_HTML = """
<!DOCTYPE html>
<html>
<head>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Some Business</title>
</head>
<body>
    <h1>Some Business</h1>
    <p>We do stuff.</p>
    <p>Contact us today.</p>
</body>
</html>
"""

EMPTY_HTML = ""


# --- _score_html tests ---


class TestScoreHtml:
    def test_good_html_scores_high(self) -> None:
        score = _score_html(GOOD_HTML)
        assert score >= 60

    def test_bad_html_scores_low(self) -> None:
        score = _score_html(BAD_HTML)
        assert score < 30

    def test_mediocre_html_scores_mid(self) -> None:
        score = _score_html(MEDIOCRE_HTML)
        assert 20 <= score <= 65

    def test_empty_html_scores_zero(self) -> None:
        score = _score_html(EMPTY_HTML)
        assert score == 0

    def test_score_clamped_to_100(self) -> None:
        score = _score_html(GOOD_HTML)
        assert 0 <= score <= 100

    def test_viewport_meta_adds_points(self) -> None:
        with_viewport = '<meta name="viewport" content="width=device-width">'
        without_viewport = "<html><body>Hello</body></html>"
        assert _score_html(with_viewport) > _score_html(without_viewport)

    def test_nav_element_adds_points(self) -> None:
        with_nav = "<html><body><nav><a href='/'>Home</a></nav></body></html>"
        without_nav = "<html><body><p>Hello</p></body></html>"
        assert _score_html(with_nav) > _score_html(without_nav)


# --- AnalysisResult tests ---


class TestAnalysisResult:
    def test_result_is_immutable(self) -> None:
        result = AnalysisResult(
            url="https://example.com",
            site_exists=True,
            quality_score=50,
            recommendation=RECOMMENDATION_UPGRADE,
        )
        with pytest.raises(AttributeError):
            result.quality_score = 99  # type: ignore[misc]

    def test_result_fields(self) -> None:
        result = AnalysisResult(
            url="https://example.com",
            site_exists=True,
            quality_score=75,
            recommendation=RECOMMENDATION_SKIP,
            load_time_ms=450,
        )
        assert result.url == "https://example.com"
        assert result.site_exists is True
        assert result.quality_score == 75
        assert result.recommendation == RECOMMENDATION_SKIP
        assert result.load_time_ms == 450


# --- WebAnalyzer tests ---


class TestWebAnalyzer:
    @patch("scraper.web_analyzer.httpx.Client")
    def test_analyze_existing_site(self, mock_client_cls: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = GOOD_HTML
        mock_response.elapsed = MagicMock()
        mock_response.elapsed.total_seconds.return_value = 0.5
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = mock_response
        mock_client_cls.return_value = mock_client

        analyzer = WebAnalyzer()
        result = analyzer.analyze("https://example.com")

        assert result.site_exists is True
        assert result.quality_score >= 60
        assert result.recommendation in {RECOMMENDATION_UPGRADE, RECOMMENDATION_SKIP}
        assert result.load_time_ms == 500

    @patch("scraper.web_analyzer.httpx.Client")
    def test_analyze_bad_site(self, mock_client_cls: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = BAD_HTML
        mock_response.elapsed = MagicMock()
        mock_response.elapsed.total_seconds.return_value = 0.3
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = mock_response
        mock_client_cls.return_value = mock_client

        analyzer = WebAnalyzer()
        result = analyzer.analyze("https://badsite.com")

        assert result.site_exists is True
        assert result.quality_score < 30
        assert result.recommendation == RECOMMENDATION_GENERATE_DEMO

    @patch("scraper.web_analyzer.httpx.Client")
    def test_analyze_nonexistent_site(self, mock_client_cls: MagicMock) -> None:
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.side_effect = httpx.ConnectError("Connection refused")
        mock_client_cls.return_value = mock_client

        analyzer = WebAnalyzer()
        result = analyzer.analyze("https://doesnotexist.invalid")

        assert result.site_exists is False
        assert result.quality_score == 0
        assert result.recommendation == RECOMMENDATION_GENERATE_DEMO

    @patch("scraper.web_analyzer.httpx.Client")
    def test_analyze_timeout(self, mock_client_cls: MagicMock) -> None:
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.side_effect = httpx.TimeoutException("Timed out")
        mock_client_cls.return_value = mock_client

        analyzer = WebAnalyzer()
        result = analyzer.analyze("https://slow-site.com")

        assert result.site_exists is False
        assert result.quality_score == 0
        assert result.recommendation == RECOMMENDATION_GENERATE_DEMO

    @patch("scraper.web_analyzer.httpx.Client")
    def test_analyze_http_error_status(self, mock_client_cls: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "404", request=MagicMock(), response=mock_response
        )

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = mock_response
        mock_client_cls.return_value = mock_client

        analyzer = WebAnalyzer()
        result = analyzer.analyze("https://404-site.com")

        assert result.site_exists is False
        assert result.quality_score == 0

    @patch("scraper.web_analyzer.httpx.Client")
    def test_analyze_empty_url_raises(self, mock_client_cls: MagicMock) -> None:
        analyzer = WebAnalyzer()
        with pytest.raises(WebAnalyzerError, match="URL cannot be empty"):
            analyzer.analyze("")

    @patch("scraper.web_analyzer.httpx.Client")
    def test_analyze_batch(self, mock_client_cls: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = GOOD_HTML
        mock_response.elapsed = MagicMock()
        mock_response.elapsed.total_seconds.return_value = 0.2
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = mock_response
        mock_client_cls.return_value = mock_client

        analyzer = WebAnalyzer()
        urls = ["https://site1.com", "https://site2.com", "https://site3.com"]
        results = analyzer.analyze_batch(urls)

        assert len(results) == 3
        assert all(r.site_exists for r in results)

    def test_recommendation_thresholds(self) -> None:
        analyzer = WebAnalyzer()
        assert analyzer._get_recommendation(10) == RECOMMENDATION_GENERATE_DEMO
        assert analyzer._get_recommendation(29) == RECOMMENDATION_GENERATE_DEMO
        assert analyzer._get_recommendation(30) == RECOMMENDATION_UPGRADE
        assert analyzer._get_recommendation(59) == RECOMMENDATION_UPGRADE
        assert analyzer._get_recommendation(60) == RECOMMENDATION_SKIP
        assert analyzer._get_recommendation(100) == RECOMMENDATION_SKIP
