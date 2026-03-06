"""Tests for pipeline orchestration module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from scraper.config import (
    PipelineConfig,
)
from scraper.demo_generator import Demo, DemoGeneratorError
from scraper.discovery import DiscoveryError
from scraper.lead_manager import Lead, LeadManager
from scraper.pipeline import (
    Pipeline,
    PipelineResult,
    SearchSpec,
    _deduplicate,
    _filter_for_demos,
    _normalize,
)
from scraper.web_analyzer import AnalysisResult


# --- SearchSpec & PipelineResult ---


class TestDataClasses:
    def test_search_spec_frozen(self) -> None:
        spec = SearchSpec(category="plumber", location="Denver, CO")
        with pytest.raises(AttributeError):
            spec.category = "other"  # type: ignore[misc]

    def test_pipeline_result_frozen(self) -> None:
        result = PipelineResult(
            discovered=5, duplicates_skipped=1, analyzed=3, demos_generated=2, errors=()
        )
        with pytest.raises(AttributeError):
            result.discovered = 10  # type: ignore[misc]

    def test_pipeline_result_fields(self) -> None:
        result = PipelineResult(
            discovered=10,
            duplicates_skipped=2,
            analyzed=5,
            demos_generated=3,
            errors=(),
        )
        assert result.discovered == 10
        assert result.duplicates_skipped == 2
        assert result.analyzed == 5
        assert result.demos_generated == 3
        assert result.errors == ()


# --- Deduplication ---


class TestDeduplicate:
    def test_removes_exact_duplicates(self) -> None:
        existing = (Lead(name="Joe Plumbing", location="Denver, CO"),)
        new_leads = [
            Lead(name="Joe Plumbing", location="Denver, CO"),
            Lead(name="New Biz", location="Denver, CO"),
        ]
        unique, skipped = _deduplicate(new_leads, existing)
        assert len(unique) == 1
        assert unique[0].name == "New Biz"
        assert skipped == 1

    def test_case_insensitive(self) -> None:
        existing = (Lead(name="JOE PLUMBING", location="DENVER, CO"),)
        new_leads = [Lead(name="joe plumbing", location="denver, co")]
        unique, skipped = _deduplicate(new_leads, existing)
        assert len(unique) == 0
        assert skipped == 1

    def test_whitespace_normalized(self) -> None:
        existing = (Lead(name="  Joe Plumbing  ", location=" Denver, CO "),)
        new_leads = [Lead(name="Joe Plumbing", location="Denver, CO")]
        unique, skipped = _deduplicate(new_leads, existing)
        assert len(unique) == 0

    def test_no_duplicates(self) -> None:
        existing = (Lead(name="Existing Biz", location="LA, CA"),)
        new_leads = [Lead(name="New Biz", location="Denver, CO")]
        unique, skipped = _deduplicate(new_leads, existing)
        assert len(unique) == 1
        assert skipped == 0

    def test_empty_existing(self) -> None:
        unique, skipped = _deduplicate(
            [Lead(name="New Biz", location="Denver, CO")], ()
        )
        assert len(unique) == 1
        assert skipped == 0


class TestNormalize:
    def test_strips_and_lowercases(self) -> None:
        assert _normalize("  Hello World  ") == "hello world"

    def test_empty_string(self) -> None:
        assert _normalize("") == ""


# --- Filtering ---


class TestFilterForDemos:
    def test_no_website_qualifies(self) -> None:
        leads = (Lead(name="No Site", has_website=False, quality_score=-1),)
        filtered = _filter_for_demos(leads)
        assert len(filtered) == 1

    def test_low_score_qualifies(self) -> None:
        leads = (Lead(name="Bad Site", has_website=True, quality_score=10),)
        filtered = _filter_for_demos(leads)
        assert len(filtered) == 1

    def test_high_score_excluded(self) -> None:
        leads = (Lead(name="Good Site", has_website=True, quality_score=80),)
        filtered = _filter_for_demos(leads)
        assert len(filtered) == 0

    def test_unanalyzed_with_website_excluded(self) -> None:
        leads = (Lead(name="Unknown", has_website=True, quality_score=-1),)
        filtered = _filter_for_demos(leads)
        assert len(filtered) == 0

    def test_mixed_leads(self) -> None:
        leads = (
            Lead(name="No Site", has_website=False),
            Lead(name="Bad Site", has_website=True, quality_score=15),
            Lead(name="Good Site", has_website=True, quality_score=90),
            Lead(name="Pending", has_website=True, quality_score=-1),
        )
        filtered = _filter_for_demos(leads)
        assert len(filtered) == 2


# --- Pipeline ---


class TestPipeline:
    def _make_pipeline(self, tmp_path: Path) -> Pipeline:
        config = PipelineConfig(
            batch_size=10,
            rate_limit_delay_seconds=0.0,
            api=MagicMock(google_maps_api_key="fake-key"),
        )
        manager = LeadManager(csv_path=tmp_path / "leads.csv")
        return Pipeline(
            config=config, lead_manager=manager, templates_dir=tmp_path / "templates"
        )

    @patch("scraper.pipeline.BusinessDiscovery")
    def test_discover_stage(self, mock_disc_cls: MagicMock, tmp_path: Path) -> None:
        mock_disc = mock_disc_cls.return_value
        mock_disc.search.return_value = [
            Lead(name="Found Biz", category="plumber", location="Denver, CO"),
        ]

        pipeline = self._make_pipeline(tmp_path)
        specs = [SearchSpec(category="plumber", location="Denver, CO")]
        result = pipeline.run(specs)

        assert result.discovered == 1
        mock_disc.search.assert_called_once()

    @patch("scraper.pipeline.BusinessDiscovery")
    def test_discover_error_continues(
        self, mock_disc_cls: MagicMock, tmp_path: Path
    ) -> None:
        mock_disc = mock_disc_cls.return_value
        mock_disc.search.side_effect = DiscoveryError("API fail")

        pipeline = self._make_pipeline(tmp_path)
        result = pipeline.run([SearchSpec(category="plumber", location="Denver, CO")])

        assert result.discovered == 0
        assert len(result.errors) == 1

    @patch("scraper.pipeline.DemoGenerator")
    @patch("scraper.pipeline.WebAnalyzer")
    @patch("scraper.pipeline.BusinessDiscovery")
    def test_full_pipeline_no_website(
        self,
        mock_disc_cls: MagicMock,
        mock_analyzer_cls: MagicMock,
        mock_gen_cls: MagicMock,
        tmp_path: Path,
    ) -> None:
        mock_disc_cls.return_value.search.return_value = [
            Lead(
                name="No Web Biz",
                category="plumber",
                location="Denver, CO",
                has_website=False,
            ),
        ]
        mock_gen_cls.return_value.generate.return_value = Demo(
            slug="no-web-biz",
            demo_path="templates/no-web-biz/index.html",
            category="plumber",
        )

        pipeline = self._make_pipeline(tmp_path)
        result = pipeline.run([SearchSpec(category="plumber", location="Denver, CO")])

        assert result.discovered == 1
        assert result.demos_generated == 1
        mock_gen_cls.return_value.generate.assert_called_once()

    @patch("scraper.pipeline.DemoGenerator")
    @patch("scraper.pipeline.WebAnalyzer")
    @patch("scraper.pipeline.BusinessDiscovery")
    def test_full_pipeline_with_analysis(
        self,
        mock_disc_cls: MagicMock,
        mock_analyzer_cls: MagicMock,
        mock_gen_cls: MagicMock,
        tmp_path: Path,
    ) -> None:
        mock_disc_cls.return_value.search.return_value = [
            Lead(
                name="Bad Site Biz",
                category="plumber",
                location="Denver, CO",
                has_website=True,
                website="http://bad.example.com",
            ),
        ]
        mock_analyzer_cls.return_value.analyze.return_value = AnalysisResult(
            url="http://bad.example.com",
            site_exists=True,
            quality_score=15,
            recommendation="generate_demo",
        )
        mock_gen_cls.return_value.generate.return_value = Demo(
            slug="bad-site-biz",
            demo_path="templates/bad-site-biz/index.html",
            category="plumber",
        )

        pipeline = self._make_pipeline(tmp_path)
        result = pipeline.run([SearchSpec(category="plumber", location="Denver, CO")])

        assert result.discovered == 1
        assert result.analyzed == 1
        assert result.demos_generated == 1

    @patch("scraper.pipeline.DemoGenerator")
    @patch("scraper.pipeline.WebAnalyzer")
    @patch("scraper.pipeline.BusinessDiscovery")
    def test_good_site_skipped(
        self,
        mock_disc_cls: MagicMock,
        mock_analyzer_cls: MagicMock,
        mock_gen_cls: MagicMock,
        tmp_path: Path,
    ) -> None:
        mock_disc_cls.return_value.search.return_value = [
            Lead(
                name="Good Site Biz",
                category="plumber",
                location="Denver, CO",
                has_website=True,
                website="http://good.example.com",
            ),
        ]
        mock_analyzer_cls.return_value.analyze.return_value = AnalysisResult(
            url="http://good.example.com",
            site_exists=True,
            quality_score=85,
            recommendation="skip",
        )

        pipeline = self._make_pipeline(tmp_path)
        result = pipeline.run([SearchSpec(category="plumber", location="Denver, CO")])

        assert result.discovered == 1
        assert result.analyzed == 1
        assert result.demos_generated == 0
        mock_gen_cls.return_value.generate.assert_not_called()

    @patch("scraper.pipeline.BusinessDiscovery")
    def test_deduplication_in_pipeline(
        self, mock_disc_cls: MagicMock, tmp_path: Path
    ) -> None:
        existing_lead = Lead(
            name="Existing Biz", location="Denver, CO", category="plumber"
        )
        manager = LeadManager(leads=(existing_lead,), csv_path=tmp_path / "leads.csv")
        config = PipelineConfig(
            rate_limit_delay_seconds=0.0,
            api=MagicMock(google_maps_api_key="fake-key"),
        )
        pipeline = Pipeline(
            config=config, lead_manager=manager, templates_dir=tmp_path / "templates"
        )

        mock_disc_cls.return_value.search.return_value = [
            Lead(name="Existing Biz", location="Denver, CO", category="plumber"),
            Lead(name="Brand New Biz", location="Denver, CO", category="plumber"),
        ]

        result = pipeline.run([SearchSpec(category="plumber", location="Denver, CO")])
        assert result.discovered == 2
        assert result.duplicates_skipped == 1

    @patch("scraper.pipeline.DemoGenerator")
    @patch("scraper.pipeline.BusinessDiscovery")
    def test_demo_error_continues(
        self,
        mock_disc_cls: MagicMock,
        mock_gen_cls: MagicMock,
        tmp_path: Path,
    ) -> None:
        mock_disc_cls.return_value.search.return_value = [
            Lead(
                name="Fail Biz",
                category="plumber",
                location="Denver, CO",
                has_website=False,
            ),
            Lead(
                name="Ok Biz",
                category="plumber",
                location="Denver, CO",
                has_website=False,
            ),
        ]
        mock_gen_cls.return_value.generate.side_effect = [
            DemoGeneratorError("write failed"),
            Demo(
                slug="ok-biz",
                demo_path="templates/ok-biz/index.html",
                category="plumber",
            ),
        ]

        pipeline = self._make_pipeline(tmp_path)
        result = pipeline.run([SearchSpec(category="plumber", location="Denver, CO")])

        assert result.demos_generated == 1
        assert len(result.errors) == 1

    @patch("scraper.pipeline.BusinessDiscovery")
    def test_empty_specs(self, mock_disc_cls: MagicMock, tmp_path: Path) -> None:
        pipeline = self._make_pipeline(tmp_path)
        result = pipeline.run([])

        assert result.discovered == 0
        assert result.demos_generated == 0
        mock_disc_cls.return_value.search.assert_not_called()

    @patch("scraper.pipeline.BusinessDiscovery")
    def test_csv_persisted(self, mock_disc_cls: MagicMock, tmp_path: Path) -> None:
        mock_disc_cls.return_value.search.return_value = [
            Lead(
                name="Persist Biz",
                category="plumber",
                location="Denver, CO",
                has_website=False,
            ),
        ]

        csv_path = tmp_path / "leads.csv"
        config = PipelineConfig(
            rate_limit_delay_seconds=0.0,
            api=MagicMock(google_maps_api_key="fake-key"),
        )
        manager = LeadManager(csv_path=csv_path)
        pipeline = Pipeline(
            config=config, lead_manager=manager, templates_dir=tmp_path / "templates"
        )
        pipeline.run([SearchSpec(category="plumber", location="Denver, CO")])

        assert csv_path.exists()
        loaded = LeadManager.load_from_csv(csv_path)
        assert len(loaded) == 1
        assert loaded.leads[0].name == "Persist Biz"
