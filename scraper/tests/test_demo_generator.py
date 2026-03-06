"""Tests for demo_generator module."""

from __future__ import annotations

from pathlib import Path

import pytest

from scraper.demo_generator import (
    Demo,
    DemoGenerator,
    DemoGeneratorError,
    _generate_slug,
    _render_template,
    CATEGORY_TEMPLATES,
)
from scraper.lead_manager import Lead


# --- Slug generation ---


class TestGenerateSlug:
    def test_basic_name(self) -> None:
        assert _generate_slug("Joe's Plumbing") == "joes-plumbing"

    def test_strips_whitespace(self) -> None:
        assert _generate_slug("  My Business  ") == "my-business"

    def test_special_characters(self) -> None:
        assert _generate_slug("Bob & Sons #1") == "bob-sons-1"

    def test_collapses_multiple_hyphens(self) -> None:
        assert _generate_slug("A -- B --- C") == "a-b-c"

    def test_strips_leading_trailing_hyphens(self) -> None:
        assert _generate_slug("---hello---") == "hello"

    def test_max_length(self) -> None:
        long_name = "a" * 100
        slug = _generate_slug(long_name)
        assert len(slug) <= 50

    def test_empty_string_raises(self) -> None:
        with pytest.raises(DemoGeneratorError, match="cannot be empty"):
            _generate_slug("")

    def test_whitespace_only_raises(self) -> None:
        with pytest.raises(DemoGeneratorError, match="cannot be empty"):
            _generate_slug("   ")

    def test_idempotent(self) -> None:
        slug = _generate_slug("Joe's Plumbing")
        assert _generate_slug(slug) == slug


# --- Demo dataclass ---


class TestDemo:
    def test_frozen(self) -> None:
        demo = Demo(
            slug="test", demo_path="templates/test/index.html", category="general"
        )
        with pytest.raises(AttributeError):
            demo.slug = "other"  # type: ignore[misc]

    def test_fields(self) -> None:
        demo = Demo(
            slug="abc", demo_path="templates/abc/index.html", category="plumber"
        )
        assert demo.slug == "abc"
        assert demo.demo_path == "templates/abc/index.html"
        assert demo.category == "plumber"


# --- Template rendering ---


class TestRenderTemplate:
    @pytest.mark.parametrize("category", list(CATEGORY_TEMPLATES.keys()))
    def test_all_categories_render(self, category: str) -> None:
        html = _render_template(
            category=category,
            name="Test Biz",
            phone="555-1234",
            email="test@example.com",
            location="Denver, CO",
        )
        assert "<!DOCTYPE html>" in html

    def test_unknown_category_uses_general(self) -> None:
        html = _render_template(
            category="unknown_category_xyz",
            name="Test Biz",
            phone="555-1234",
            email="",
            location="Denver, CO",
        )
        assert "<!DOCTYPE html>" in html

    def test_business_info_in_output(self) -> None:
        html = _render_template(
            category="plumber",
            name="Joe Plumbing",
            phone="555-9999",
            email="joe@plumb.com",
            location="Boulder, CO",
        )
        assert "Joe Plumbing" in html
        assert "555-9999" in html
        assert "joe@plumb.com" in html
        assert "Boulder, CO" in html

    def test_tailwind_cdn_present(self) -> None:
        html = _render_template(
            category="general",
            name="Biz",
            phone="",
            email="",
            location="",
        )
        assert "tailwindcss" in html.lower() or "cdn.tailwindcss.com" in html

    def test_viewport_meta(self) -> None:
        html = _render_template(
            category="general",
            name="Biz",
            phone="",
            email="",
            location="",
        )
        assert "viewport" in html

    def test_missing_phone_handled(self) -> None:
        html = _render_template(
            category="general",
            name="Biz",
            phone="",
            email="",
            location="",
        )
        assert "<!DOCTYPE html>" in html

    def test_script_injection_escaped(self) -> None:
        html = _render_template(
            category="plumber",
            name="Test<script>alert('xss')</script>",
            phone="555-1234",
            email="",
            location="",
        )
        assert "<script>alert" not in html
        assert "&lt;script&gt;" in html

    def test_phone_injection_escaped(self) -> None:
        html = _render_template(
            category="general",
            name="Safe Biz",
            phone='555" onclick="alert(1)',
            email="",
            location="",
        )
        assert '555" onclick' not in html
        assert "&quot;" in html

    def test_location_injection_escaped(self) -> None:
        html = _render_template(
            category="general",
            name="Safe Biz",
            phone="",
            email="",
            location='<img src=x onerror="alert(1)">',
        )
        assert "<img src=x" not in html
        assert "&lt;img" in html


# --- DemoGenerator ---


class TestDemoGenerator:
    def test_generate_creates_file(self, tmp_path: Path) -> None:
        gen = DemoGenerator(templates_dir=tmp_path)
        lead = Lead(name="Joe's Plumbing", category="plumber", phone="555-1234")
        demo = gen.generate(lead)

        output_file = tmp_path / "joes-plumbing" / "index.html"
        assert output_file.exists()
        assert demo.slug == "joes-plumbing"
        assert demo.demo_path == "templates/joes-plumbing/index.html"

    def test_generate_html_content(self, tmp_path: Path) -> None:
        gen = DemoGenerator(templates_dir=tmp_path)
        lead = Lead(name="Bright Smiles Dental", category="dentist", phone="555-0000")
        gen.generate(lead)

        html = (tmp_path / "bright-smiles-dental" / "index.html").read_text(
            encoding="utf-8"
        )
        assert "Bright Smiles Dental" in html
        assert "555-0000" in html

    def test_generate_unknown_category(self, tmp_path: Path) -> None:
        gen = DemoGenerator(templates_dir=tmp_path)
        lead = Lead(name="Mystery Shop", category="mystery_biz")
        demo = gen.generate(lead)

        assert demo.category == "mystery_biz"
        assert (tmp_path / "mystery-shop" / "index.html").exists()

    def test_generate_overwrites_existing(self, tmp_path: Path) -> None:
        gen = DemoGenerator(templates_dir=tmp_path)
        lead = Lead(name="Repeat Biz", category="general")

        gen.generate(lead)
        demo2 = gen.generate(lead)

        assert (tmp_path / "repeat-biz" / "index.html").exists()
        assert demo2.slug == "repeat-biz"

    def test_generate_empty_name_raises(self, tmp_path: Path) -> None:
        gen = DemoGenerator(templates_dir=tmp_path)
        lead = Lead(name="", category="general")
        with pytest.raises(DemoGeneratorError, match="cannot be empty"):
            gen.generate(lead)

    def test_generate_returns_demo(self, tmp_path: Path) -> None:
        gen = DemoGenerator(templates_dir=tmp_path)
        lead = Lead(name="Test Co", category="electrician", location="LA, CA")
        demo = gen.generate(lead)

        assert isinstance(demo, Demo)
        assert demo.category == "electrician"
        assert "test-co" in demo.demo_path
