"""Configuration and constants for the local-biz-sites scraper pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
LEADS_DIR = PROJECT_ROOT / "leads"
TEMPLATES_DIR = PROJECT_ROOT / "templates"
REFERENCES_DIR = PROJECT_ROOT / "references"

# Default CSV path
DEFAULT_LEADS_CSV = LEADS_DIR / "leads.csv"

# Lead statuses
LEAD_STATUS_DISCOVERED = "discovered"
LEAD_STATUS_ANALYZED = "analyzed"
LEAD_STATUS_DEMO_GENERATED = "demo_generated"
LEAD_STATUS_PITCHED = "pitched"
LEAD_STATUS_CONVERTED = "converted"
LEAD_STATUS_SKIPPED = "skipped"

VALID_LEAD_STATUSES = frozenset(
    {
        LEAD_STATUS_DISCOVERED,
        LEAD_STATUS_ANALYZED,
        LEAD_STATUS_DEMO_GENERATED,
        LEAD_STATUS_PITCHED,
        LEAD_STATUS_CONVERTED,
        LEAD_STATUS_SKIPPED,
    }
)

# Web analysis recommendations
RECOMMENDATION_GENERATE_DEMO = "generate_demo"
RECOMMENDATION_UPGRADE = "upgrade"
RECOMMENDATION_SKIP = "skip"

VALID_RECOMMENDATIONS = frozenset(
    {
        RECOMMENDATION_GENERATE_DEMO,
        RECOMMENDATION_UPGRADE,
        RECOMMENDATION_SKIP,
    }
)

# Quality score thresholds
QUALITY_THRESHOLD_DEMO = 30  # Score below this → generate_demo
QUALITY_THRESHOLD_UPGRADE = 60  # Score above this → upgrade suggestion


@dataclass(frozen=True)
class ApiConfig:
    """API configuration loaded from environment variables."""

    google_maps_api_key: str = ""
    yelp_api_key: str = ""

    @classmethod
    def from_env(cls) -> ApiConfig:
        """Load API keys from environment variables."""
        return cls(
            google_maps_api_key=os.environ.get("GOOGLE_MAPS_API_KEY", ""),
            yelp_api_key=os.environ.get("YELP_API_KEY", ""),
        )


@dataclass(frozen=True)
class PipelineConfig:
    """Pipeline execution configuration."""

    batch_size: int = 50
    max_retries: int = 3
    request_timeout_seconds: int = 30
    screenshot_timeout_seconds: int = 15
    rate_limit_delay_seconds: float = 1.0
    api: ApiConfig = field(default_factory=ApiConfig)

    @classmethod
    def default(cls) -> PipelineConfig:
        """Create default config with API keys from environment."""
        return cls(api=ApiConfig.from_env())


# Demo generation
DEMO_SLUG_MAX_LENGTH = 50
DEMO_DEFAULT_CATEGORY = "general"
