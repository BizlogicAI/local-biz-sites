"""Pipeline orchestration: discover, deduplicate, analyze, filter, generate."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path

from scraper.config import (
    LEAD_STATUS_ANALYZED,
    LEAD_STATUS_DEMO_GENERATED,
    QUALITY_THRESHOLD_DEMO,
    TEMPLATES_DIR,
    PipelineConfig,
)
from scraper.demo_generator import DemoGenerator, DemoGeneratorError
from scraper.discovery import BusinessDiscovery, DiscoveryError
from scraper.lead_manager import Lead, LeadManager
from scraper.web_analyzer import WebAnalyzer, WebAnalyzerError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SearchSpec:
    """Immutable search specification for business discovery."""

    category: str
    location: str


@dataclass(frozen=True)
class PipelineResult:
    """Immutable summary of a pipeline run."""

    discovered: int = 0
    duplicates_skipped: int = 0
    analyzed: int = 0
    demos_generated: int = 0
    errors: tuple[str, ...] = ()


def _normalize(value: str) -> str:
    """Normalize a string for comparison (lowercase, strip whitespace)."""
    return value.lower().strip()


def _deduplicate(
    new_leads: list[Lead], existing: tuple[Lead, ...]
) -> tuple[list[Lead], int]:
    """Remove leads that already exist (by name + location).

    Returns:
        Tuple of (unique_leads, count_skipped).
    """
    existing_keys = {
        (_normalize(lead.name), _normalize(lead.location)) for lead in existing
    }
    unique: list[Lead] = []
    skipped = 0
    for lead in new_leads:
        key = (_normalize(lead.name), _normalize(lead.location))
        if key in existing_keys:
            skipped += 1
        else:
            existing_keys.add(key)
            unique.append(lead)
    return unique, skipped


def _filter_for_demos(leads: tuple[Lead, ...]) -> tuple[Lead, ...]:
    """Filter leads that qualify for demo generation.

    Qualifies if: no website, OR analyzed with score below threshold.
    """
    return tuple(
        lead
        for lead in leads
        if not lead.has_website
        or (lead.quality_score != -1 and lead.quality_score < QUALITY_THRESHOLD_DEMO)
    )


class Pipeline:
    """Orchestrate the full lead-to-demo pipeline."""

    def __init__(
        self,
        config: PipelineConfig,
        lead_manager: LeadManager,
        templates_dir: Path = TEMPLATES_DIR,
    ) -> None:
        self._config = config
        self._manager = lead_manager
        self._templates_dir = templates_dir

    def run(self, search_specs: list[SearchSpec]) -> PipelineResult:
        """Execute the full pipeline.

        Args:
            search_specs: List of (category, location) pairs to search.

        Returns:
            PipelineResult with counts and any errors.
        """
        manager = self._manager

        # Stage 1: Discover
        new_leads, discover_errors = self._discover(search_specs)
        discovered_count = len(new_leads)

        # Stage 2: Deduplicate
        unique_leads, dupes_skipped = _deduplicate(new_leads, manager.leads)

        # Stage 3: Add leads to manager
        for lead in unique_leads:
            manager = manager.add_lead(lead)

        # Stage 4: Analyze websites
        analyzed_count, analyze_errors, manager = self._analyze(unique_leads, manager)

        # Stage 5: Filter for demos (use updated leads from manager)
        lead_map = {lead.id: lead for lead in manager.leads}
        updated_leads = tuple(
            lead_map[lead.id] for lead in unique_leads if lead.id in lead_map
        )
        demo_candidates = _filter_for_demos(updated_leads)

        # Stage 6: Generate demos
        demos_count, gen_errors, manager = self._generate(demo_candidates, manager)

        # Stage 7: Persist
        manager.save_to_csv()
        all_errors = (*discover_errors, *analyze_errors, *gen_errors)
        logger.info(
            "Pipeline complete: discovered=%d, dupes=%d, analyzed=%d, demos=%d",
            discovered_count,
            dupes_skipped,
            analyzed_count,
            demos_count,
        )

        return PipelineResult(
            discovered=discovered_count,
            duplicates_skipped=dupes_skipped,
            analyzed=analyzed_count,
            demos_generated=demos_count,
            errors=all_errors,
        )

    def _discover(
        self,
        search_specs: list[SearchSpec],
    ) -> tuple[list[Lead], tuple[str, ...]]:
        """Run discovery for all search specs.

        Returns:
            Tuple of (discovered_leads, error_messages).
        """
        discovery = BusinessDiscovery(self._config.api.google_maps_api_key)
        found: list[Lead] = []
        errors: list[str] = []
        for spec in search_specs:
            try:
                leads = discovery.search(
                    spec.category,
                    location=spec.location,
                    limit=self._config.batch_size,
                )
                found.extend(leads)
                logger.info(
                    "Discovered %d businesses for '%s' in '%s'",
                    len(leads),
                    spec.category,
                    spec.location,
                )
            except DiscoveryError as exc:
                errors.append(
                    f"Discovery failed for {spec.category}/{spec.location}: {exc}"
                )
                logger.warning(
                    "Discovery failed for %s/%s: %s", spec.category, spec.location, exc
                )
            if self._config.rate_limit_delay_seconds > 0:
                time.sleep(self._config.rate_limit_delay_seconds)
        return found, tuple(errors)

    def _analyze(
        self,
        leads: list[Lead],
        manager: LeadManager,
    ) -> tuple[int, tuple[str, ...], LeadManager]:
        """Analyze websites for leads that have one.

        Returns:
            Tuple of (analyzed_count, error_messages, updated_manager).
        """
        analyzer = WebAnalyzer(timeout_seconds=self._config.request_timeout_seconds)
        analyzed = 0
        errors: list[str] = []
        for lead in leads:
            if not lead.has_website or not lead.website:
                continue
            try:
                result = analyzer.analyze(lead.website)
                manager = manager.update_lead(
                    lead.id,
                    quality_score=result.quality_score,
                    recommendation=result.recommendation,
                    status=LEAD_STATUS_ANALYZED,
                )
                analyzed += 1
                logger.info("Analyzed %s: score=%d", lead.name, result.quality_score)
            except WebAnalyzerError as exc:
                errors.append(f"Analysis failed for {lead.name}: {exc}")
                logger.warning("Analysis failed for %s: %s", lead.name, exc)
            if self._config.rate_limit_delay_seconds > 0:
                time.sleep(self._config.rate_limit_delay_seconds)
        return analyzed, tuple(errors), manager

    def _generate(
        self,
        leads: tuple[Lead, ...],
        manager: LeadManager,
    ) -> tuple[int, tuple[str, ...], LeadManager]:
        """Generate demos for qualifying leads.

        Returns:
            Tuple of (generated_count, error_messages, updated_manager).
        """
        generator = DemoGenerator(templates_dir=self._templates_dir)
        generated = 0
        errors: list[str] = []
        for lead in leads:
            try:
                demo = generator.generate(lead)
                manager = manager.update_lead(
                    lead.id,
                    demo_path=demo.demo_path,
                    status=LEAD_STATUS_DEMO_GENERATED,
                )
                generated += 1
                logger.info("Generated demo for %s at %s", lead.name, demo.demo_path)
            except DemoGeneratorError as exc:
                errors.append(f"Demo generation failed for {lead.name}: {exc}")
                logger.warning("Demo generation failed for %s: %s", lead.name, exc)
        return generated, tuple(errors), manager
