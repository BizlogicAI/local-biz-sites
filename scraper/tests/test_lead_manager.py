"""Tests for Lead dataclass and LeadManager."""

from __future__ import annotations

import pytest
from pathlib import Path

from scraper.config import (
    LEAD_STATUS_ANALYZED,
    LEAD_STATUS_DISCOVERED,
    LEAD_STATUS_DEMO_GENERATED,
    RECOMMENDATION_GENERATE_DEMO,
)
from scraper.lead_manager import Lead, LeadManager, LeadValidationError, validate_lead


# --- Lead dataclass tests ---


class TestLead:
    def test_create_lead_with_defaults(self) -> None:
        lead = Lead(name="Joe's Plumbing")
        assert lead.name == "Joe's Plumbing"
        assert lead.status == LEAD_STATUS_DISCOVERED
        assert lead.quality_score == -1
        assert lead.has_website is False
        assert len(lead.id) == 12

    def test_lead_is_immutable(self) -> None:
        lead = Lead(name="Test Biz")
        with pytest.raises(AttributeError):
            lead.name = "Changed"  # type: ignore[misc]

    def test_create_lead_with_all_fields(self) -> None:
        lead = Lead(
            id="abc123",
            name="Best Salon",
            phone="555-1234",
            email="info@bestsalon.com",
            website="https://bestsalon.com",
            location="Denver, CO",
            category="Salon",
            has_website=True,
            quality_score=45,
            recommendation="upgrade",
            demo_path="templates/best-salon/index.html",
            status=LEAD_STATUS_ANALYZED,
            created_date="2026-03-05",
            notes="Owner seemed interested",
        )
        assert lead.id == "abc123"
        assert lead.quality_score == 45


# --- Validation tests ---


class TestValidation:
    def test_valid_lead_passes(self) -> None:
        lead = Lead(name="Valid Biz")
        validate_lead(lead)  # Should not raise

    def test_empty_name_fails(self) -> None:
        lead = Lead(name="")
        with pytest.raises(LeadValidationError, match="name cannot be empty"):
            validate_lead(lead)

    def test_whitespace_name_fails(self) -> None:
        lead = Lead(name="   ")
        with pytest.raises(LeadValidationError, match="name cannot be empty"):
            validate_lead(lead)

    def test_invalid_status_fails(self) -> None:
        lead = Lead(name="Biz", status="bogus")
        with pytest.raises(LeadValidationError, match="Invalid status"):
            validate_lead(lead)

    def test_invalid_recommendation_fails(self) -> None:
        lead = Lead(name="Biz", recommendation="bad_rec")
        with pytest.raises(LeadValidationError, match="Invalid recommendation"):
            validate_lead(lead)

    def test_quality_score_out_of_range_fails(self) -> None:
        lead = Lead(name="Biz", quality_score=150)
        with pytest.raises(LeadValidationError, match="Quality score"):
            validate_lead(lead)

    def test_quality_score_negative_not_minus_one_fails(self) -> None:
        lead = Lead(name="Biz", quality_score=-5)
        with pytest.raises(LeadValidationError, match="Quality score"):
            validate_lead(lead)

    def test_unanalyzed_score_minus_one_passes(self) -> None:
        lead = Lead(name="Biz", quality_score=-1)
        validate_lead(lead)  # Should not raise


# --- LeadManager CRUD tests ---


class TestLeadManagerCrud:
    def test_empty_manager(self) -> None:
        mgr = LeadManager()
        assert len(mgr) == 0
        assert mgr.leads == ()

    def test_add_lead(self) -> None:
        mgr = LeadManager()
        lead = Lead(name="New Biz")
        mgr2 = mgr.add_lead(lead)
        assert len(mgr2) == 1
        assert len(mgr) == 0  # Original unchanged (immutable)

    def test_add_duplicate_id_fails(self) -> None:
        lead = Lead(id="dup123", name="Biz A")
        mgr = LeadManager().add_lead(lead)
        lead2 = Lead(id="dup123", name="Biz B")
        with pytest.raises(LeadValidationError, match="already exists"):
            mgr.add_lead(lead2)

    def test_add_invalid_lead_fails(self) -> None:
        mgr = LeadManager()
        bad_lead = Lead(name="")
        with pytest.raises(LeadValidationError):
            mgr.add_lead(bad_lead)

    def test_update_lead(self) -> None:
        lead = Lead(id="upd1", name="Old Name")
        mgr = LeadManager().add_lead(lead)
        mgr2 = mgr.update_lead("upd1", name="New Name", status=LEAD_STATUS_ANALYZED)
        updated = mgr2.get_lead_by_id("upd1")
        assert updated is not None
        assert updated.name == "New Name"
        assert updated.status == LEAD_STATUS_ANALYZED
        # Original manager unchanged
        assert mgr.get_lead_by_id("upd1").name == "Old Name"  # type: ignore[union-attr]

    def test_update_nonexistent_lead_fails(self) -> None:
        mgr = LeadManager()
        with pytest.raises(LeadValidationError, match="not found"):
            mgr.update_lead("nope", name="X")

    def test_update_with_invalid_data_fails(self) -> None:
        lead = Lead(id="val1", name="Good Name")
        mgr = LeadManager().add_lead(lead)
        with pytest.raises(LeadValidationError, match="Invalid status"):
            mgr.update_lead("val1", status="bogus")

    def test_get_lead_by_id_found(self) -> None:
        lead = Lead(id="find1", name="Findable")
        mgr = LeadManager().add_lead(lead)
        assert mgr.get_lead_by_id("find1") == lead

    def test_get_lead_by_id_not_found(self) -> None:
        mgr = LeadManager()
        assert mgr.get_lead_by_id("nope") is None


# --- Filtering tests ---


class TestLeadManagerFiltering:
    @pytest.fixture()
    def populated_manager(self) -> LeadManager:
        leads = [
            Lead(id="a", name="Biz A", status=LEAD_STATUS_DISCOVERED, quality_score=-1),
            Lead(id="b", name="Biz B", status=LEAD_STATUS_ANALYZED, quality_score=20),
            Lead(id="c", name="Biz C", status=LEAD_STATUS_ANALYZED, quality_score=75),
            Lead(
                id="d",
                name="Biz D",
                status=LEAD_STATUS_DEMO_GENERATED,
                quality_score=15,
            ),
        ]
        mgr = LeadManager()
        for lead in leads:
            mgr = mgr.add_lead(lead)
        return mgr

    def test_filter_by_status(self, populated_manager: LeadManager) -> None:
        analyzed = populated_manager.get_leads_by_status(LEAD_STATUS_ANALYZED)
        assert len(analyzed) == 2
        assert all(lead.status == LEAD_STATUS_ANALYZED for lead in analyzed)

    def test_filter_by_status_empty(self, populated_manager: LeadManager) -> None:
        converted = populated_manager.get_leads_by_status("converted")
        assert len(converted) == 0

    def test_filter_by_score(self, populated_manager: LeadManager) -> None:
        low_quality = populated_manager.get_leads_by_score(30)
        assert len(low_quality) == 2  # b(20) and d(15), not a(-1 unanalyzed)
        assert all(lead.quality_score <= 30 for lead in low_quality)

    def test_filter_by_score_excludes_unanalyzed(
        self, populated_manager: LeadManager
    ) -> None:
        all_scored = populated_manager.get_leads_by_score(100)
        assert len(all_scored) == 3  # b, c, d — not a (unanalyzed)


# --- CSV persistence tests ---


class TestLeadManagerCsv:
    def test_save_and_load_roundtrip(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "test_leads.csv"
        lead = Lead(
            id="rt1",
            name="Roundtrip Biz",
            phone="555-0000",
            email="test@example.com",
            website="https://example.com",
            location="Denver, CO",
            category="Plumbing",
            has_website=True,
            quality_score=42,
            recommendation=RECOMMENDATION_GENERATE_DEMO,
            demo_path="templates/roundtrip/index.html",
            status=LEAD_STATUS_ANALYZED,
            created_date="2026-03-05",
            notes="Good prospect",
        )
        mgr = LeadManager(csv_path=csv_path).add_lead(lead)
        mgr.save_to_csv()

        loaded = LeadManager.load_from_csv(csv_path)
        assert len(loaded) == 1
        loaded_lead = loaded.get_lead_by_id("rt1")
        assert loaded_lead is not None
        assert loaded_lead.name == "Roundtrip Biz"
        assert loaded_lead.has_website is True
        assert loaded_lead.quality_score == 42
        assert loaded_lead.recommendation == RECOMMENDATION_GENERATE_DEMO

    def test_load_nonexistent_file_returns_empty(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "nope.csv"
        mgr = LeadManager.load_from_csv(csv_path)
        assert len(mgr) == 0

    def test_save_creates_parent_dirs(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "sub" / "dir" / "leads.csv"
        mgr = LeadManager(csv_path=csv_path).add_lead(Lead(name="Nested"))
        mgr.save_to_csv()
        assert csv_path.exists()

    def test_save_multiple_leads_preserves_order(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "order.csv"
        mgr = LeadManager(csv_path=csv_path)
        for i in range(5):
            mgr = mgr.add_lead(Lead(id=f"ord{i}", name=f"Biz {i}"))
        mgr.save_to_csv()

        loaded = LeadManager.load_from_csv(csv_path)
        assert len(loaded) == 5
        assert [lead.id for lead in loaded.leads] == [f"ord{i}" for i in range(5)]
