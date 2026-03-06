"""Web presence analysis and quality scoring."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

import httpx

from scraper.config import (
    QUALITY_THRESHOLD_DEMO,
    QUALITY_THRESHOLD_UPGRADE,
    RECOMMENDATION_GENERATE_DEMO,
    RECOMMENDATION_SKIP,
    RECOMMENDATION_UPGRADE,
)

logger = logging.getLogger(__name__)


class WebAnalyzerError(Exception):
    """Raised when web analysis fails."""


@dataclass(frozen=True)
class AnalysisResult:
    """Immutable result of analyzing a website."""

    url: str
    site_exists: bool
    quality_score: int
    recommendation: str
    load_time_ms: int = 0


def _score_html(html: str) -> int:
    """Score HTML quality based on heuristics (0-100).

    Scoring criteria:
    - Has DOCTYPE declaration (+10)
    - Has viewport meta tag for mobile responsiveness (+20)
    - Has <title> tag (+10)
    - Has <nav> element (+10)
    - Has <header> or <footer> elements (+10)
    - Has heading hierarchy (h1, h2) (+10)
    - Has call-to-action links/buttons (+10)
    - Has semantic HTML (section, article, aside) (+10)
    - Has external stylesheet (+5)
    - Has lang attribute on html tag (+5)
    """
    if not html.strip():
        return 0

    score = 0
    html_lower = html.lower()

    if "<!doctype html>" in html_lower:
        score += 10

    if re.search(r'<meta\s+name=["\']viewport["\']', html_lower):
        score += 20

    if re.search(r"<title>.+</title>", html_lower):
        score += 10

    if "<nav" in html_lower:
        score += 10

    if "<header" in html_lower or "<footer" in html_lower:
        score += 10

    if "<h1" in html_lower and "<h2" in html_lower:
        score += 10

    if re.search(r'class=["\'][^"\']*btn[^"\']*["\']', html_lower) or re.search(
        r"<button", html_lower
    ):
        score += 10

    if any(tag in html_lower for tag in ("<section", "<article", "<aside")):
        score += 10

    if re.search(r'<link[^>]+rel=["\']stylesheet["\']', html_lower):
        score += 5

    if re.search(r'<html[^>]+lang=["\']', html_lower):
        score += 5

    return min(score, 100)


class WebAnalyzer:
    """Analyze web presence and score website quality."""

    def __init__(self, timeout_seconds: int = 15) -> None:
        self._timeout = timeout_seconds

    def _get_recommendation(self, score: int) -> str:
        """Map quality score to a recommendation."""
        if score < QUALITY_THRESHOLD_DEMO:
            return RECOMMENDATION_GENERATE_DEMO
        if score < QUALITY_THRESHOLD_UPGRADE:
            return RECOMMENDATION_UPGRADE
        return RECOMMENDATION_SKIP

    def analyze(self, url: str) -> AnalysisResult:
        """Analyze a single website URL.

        Args:
            url: The website URL to analyze.

        Returns:
            AnalysisResult with site existence, quality score, and recommendation.

        Raises:
            WebAnalyzerError: If the URL is empty.
        """
        if not url.strip():
            raise WebAnalyzerError("URL cannot be empty")

        with httpx.Client(timeout=self._timeout, follow_redirects=True) as client:
            try:
                response = client.get(url)
                response.raise_for_status()
            except (httpx.HTTPError, httpx.TimeoutException):
                logger.info("Site unreachable: %s", url)
                return AnalysisResult(
                    url=url,
                    site_exists=False,
                    quality_score=0,
                    recommendation=RECOMMENDATION_GENERATE_DEMO,
                )

            load_time_ms = int(response.elapsed.total_seconds() * 1000)
            html = response.text
            score = _score_html(html)
            recommendation = self._get_recommendation(score)

            logger.info(
                "Analyzed %s: score=%d, recommendation=%s", url, score, recommendation
            )

            return AnalysisResult(
                url=url,
                site_exists=True,
                quality_score=score,
                recommendation=recommendation,
                load_time_ms=load_time_ms,
            )

    def analyze_batch(self, urls: list[str]) -> list[AnalysisResult]:
        """Analyze multiple URLs sequentially.

        Args:
            urls: List of website URLs to analyze.

        Returns:
            List of AnalysisResult objects.
        """
        return [self.analyze(url) for url in urls]
