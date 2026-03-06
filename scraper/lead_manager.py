"""Lead data model and CSV-backed lead management."""

from __future__ import annotations

import csv
import uuid
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path

from scraper.config import (
    DEFAULT_LEADS_CSV,
    LEAD_STATUS_DISCOVERED,
    VALID_LEAD_STATUSES,
    VALID_RECOMMENDATIONS,
)


@dataclass(frozen=True)
class Lead:
    """Immutable representation of a business lead."""

    id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    name: str = ""
    phone: str = ""
    email: str = ""
    website: str = ""
    location: str = ""
    category: str = ""
    has_website: bool = False
    quality_score: int = -1  # -1 means not yet analyzed
    recommendation: str = ""
    demo_path: str = ""
    status: str = LEAD_STATUS_DISCOVERED
    created_date: str = field(
        default_factory=lambda: datetime.now(timezone.utc).strftime("%Y-%m-%d"),
    )
    notes: str = ""


class LeadValidationError(Exception):
    """Raised when lead data is invalid."""


def validate_lead(lead: Lead) -> None:
    """Validate lead data, raising LeadValidationError on issues."""
    if not lead.name.strip():
        raise LeadValidationError("Lead name cannot be empty")
    if lead.status not in VALID_LEAD_STATUSES:
        raise LeadValidationError(
            f"Invalid status '{lead.status}'. Must be one of: {VALID_LEAD_STATUSES}"
        )
    if lead.recommendation and lead.recommendation not in VALID_RECOMMENDATIONS:
        raise LeadValidationError(
            f"Invalid recommendation '{lead.recommendation}'. "
            f"Must be one of: {VALID_RECOMMENDATIONS}"
        )
    if lead.quality_score != -1 and not (0 <= lead.quality_score <= 100):
        raise LeadValidationError(
            f"Quality score must be -1 (unanalyzed) or 0-100, got {lead.quality_score}"
        )


CSV_FIELDS = [
    "id",
    "name",
    "phone",
    "email",
    "website",
    "location",
    "category",
    "has_website",
    "quality_score",
    "recommendation",
    "demo_path",
    "status",
    "created_date",
    "notes",
]


def _row_to_lead(row: dict[str, str]) -> Lead:
    """Convert a CSV row dict to a Lead, handling type coercion."""
    return Lead(
        id=row.get("id", uuid.uuid4().hex[:12]),
        name=row.get("name", ""),
        phone=row.get("phone", ""),
        email=row.get("email", ""),
        website=row.get("website", ""),
        location=row.get("location", ""),
        category=row.get("category", ""),
        has_website=row.get("has_website", "").lower() == "true",
        quality_score=int(row.get("quality_score", "-1")),
        recommendation=row.get("recommendation", ""),
        demo_path=row.get("demo_path", ""),
        status=row.get("status", LEAD_STATUS_DISCOVERED),
        created_date=row.get("created_date", ""),
        notes=row.get("notes", ""),
    )


def _lead_to_row(lead: Lead) -> dict[str, str]:
    """Convert a Lead to a CSV-safe dict."""
    data = asdict(lead)
    data["has_website"] = str(lead.has_website)
    data["quality_score"] = str(lead.quality_score)
    return data


class LeadManager:
    """CSV-backed lead storage with CRUD operations.

    All mutations return new LeadManager instances (immutable pattern).
    """

    def __init__(
        self, leads: tuple[Lead, ...] = (), csv_path: Path = DEFAULT_LEADS_CSV
    ) -> None:
        self._leads = leads
        self._csv_path = csv_path

    @property
    def leads(self) -> tuple[Lead, ...]:
        return self._leads

    @property
    def csv_path(self) -> Path:
        return self._csv_path

    @classmethod
    def load_from_csv(cls, csv_path: Path = DEFAULT_LEADS_CSV) -> LeadManager:
        """Load leads from a CSV file. Returns empty manager if file doesn't exist."""
        if not csv_path.exists():
            return cls(leads=(), csv_path=csv_path)

        with csv_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            leads = tuple(_row_to_lead(row) for row in reader)

        return cls(leads=leads, csv_path=csv_path)

    def save_to_csv(self, csv_path: Path | None = None) -> None:
        """Write all leads to CSV."""
        path = csv_path or self._csv_path
        path.parent.mkdir(parents=True, exist_ok=True)

        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()
            for lead in self._leads:
                writer.writerow(_lead_to_row(lead))

    def add_lead(self, lead: Lead) -> LeadManager:
        """Return a new LeadManager with the lead added. Validates before adding."""
        validate_lead(lead)
        if any(existing.id == lead.id for existing in self._leads):
            raise LeadValidationError(f"Lead with id '{lead.id}' already exists")
        return LeadManager(leads=(*self._leads, lead), csv_path=self._csv_path)

    def update_lead(self, lead_id: str, **updates: object) -> LeadManager:
        """Return a new LeadManager with the specified lead updated."""
        new_leads: list[Lead] = []
        found = False
        for lead in self._leads:
            if lead.id == lead_id:
                found = True
                updated = replace(lead, **updates)
                validate_lead(updated)
                new_leads.append(updated)
            else:
                new_leads.append(lead)

        if not found:
            raise LeadValidationError(f"Lead with id '{lead_id}' not found")

        return LeadManager(leads=tuple(new_leads), csv_path=self._csv_path)

    def get_lead_by_id(self, lead_id: str) -> Lead | None:
        """Find a lead by ID, or None if not found."""
        for lead in self._leads:
            if lead.id == lead_id:
                return lead
        return None

    def get_leads_by_status(self, status: str) -> tuple[Lead, ...]:
        """Filter leads by status."""
        return tuple(lead for lead in self._leads if lead.status == status)

    def get_leads_by_score(self, max_score: int) -> tuple[Lead, ...]:
        """Get leads with quality_score at or below max_score (analyzed only)."""
        return tuple(
            lead
            for lead in self._leads
            if lead.quality_score != -1 and lead.quality_score <= max_score
        )

    def __len__(self) -> int:
        return len(self._leads)
