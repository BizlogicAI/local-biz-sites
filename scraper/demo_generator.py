"""Demo website generation for local business leads."""

from __future__ import annotations

import html
import re
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Callable

from scraper.config import DEMO_SLUG_MAX_LENGTH, TEMPLATES_DIR
from scraper.lead_manager import Lead


class DemoGeneratorError(Exception):
    """Raised when demo generation fails."""


@dataclass(frozen=True)
class Demo:
    """Immutable result of generating a demo website."""

    slug: str
    demo_path: str
    category: str


def _generate_slug(name: str) -> str:
    """Convert a business name to a filesystem-safe slug.

    Raises:
        DemoGeneratorError: If name is empty after normalization.
    """
    slug = name.lower().strip()
    slug = re.sub(r"['\"]", "", slug)
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    slug = slug.strip("-")

    if not slug:
        raise DemoGeneratorError("Business name cannot be empty")

    return slug[:DEMO_SLUG_MAX_LENGTH].rstrip("-")


def _phone_link(phone: str) -> str:
    """Build a safe tel: link, or empty string if no phone."""
    if not phone:
        return ""
    safe = html.escape(phone)
    return f'<a href="tel:{safe}" class="text-blue-600 hover:underline">{safe}</a>'


def _email_link(email: str) -> str:
    """Build a safe mailto: link, or empty string if no email."""
    if not email:
        return ""
    safe = html.escape(email)
    return f'<a href="mailto:{safe}" class="text-blue-600 hover:underline">{safe}</a>'


def _cta_href(phone: str) -> str:
    """Return tel: link for CTA button, or fallback to #contact."""
    if phone:
        return f"tel:{html.escape(phone)}"
    return "#contact"


def _base_html(name: str, phone: str, email: str, location: str, body: str) -> str:
    """Wrap body content in a complete HTML5 document with Tailwind CSS."""
    safe_name = html.escape(name)
    safe_location = html.escape(location)
    loc_html = f'<p class="text-gray-600">{safe_location}</p>' if location else ""
    ph_link = _phone_link(phone)
    em_link = _email_link(email)
    cta = _cta_href(phone)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{safe_name}</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-white text-gray-900">
    <header class="bg-blue-900 text-white">
        <nav class="max-w-6xl mx-auto px-4 py-4 flex justify-between items-center">
            <span class="text-2xl font-bold">{safe_name}</span>
            <div class="hidden md:flex gap-6">
                <a href="#services" class="hover:text-blue-200">Services</a>
                <a href="#about" class="hover:text-blue-200">About</a>
                <a href="#contact" class="hover:text-blue-200">Contact</a>
            </div>
        </nav>
    </header>

{body}

    <section id="contact" class="py-16 bg-gray-50">
        <div class="max-w-4xl mx-auto px-4 text-center">
            <h2 class="text-3xl font-bold mb-6">Contact Us</h2>
            {loc_html}
            <div class="mt-4 space-y-2">
                {f"<p>Phone: {ph_link}</p>" if phone else ""}
                {f"<p>Email: {em_link}</p>" if email else ""}
            </div>
            <a href="{cta}" class="mt-6 inline-block bg-blue-600 text-white
                px-8 py-3 rounded-lg font-semibold hover:bg-blue-700">
                Get in Touch</a>
        </div>
    </section>

    <footer class="bg-gray-900 text-gray-400 py-8">
        <div class="max-w-6xl mx-auto px-4 text-center">
            <p>&copy; {safe_name}. All rights reserved.</p>
        </div>
    </footer>
</body>
</html>"""


def _template_plumber(name: str, phone: str, email: str, location: str) -> str:
    safe_name = html.escape(name)
    safe_loc = html.escape(location) if location else "your area"
    cta = _cta_href(phone)
    body = f"""
    <section class="bg-blue-800 text-white py-20">
        <div class="max-w-4xl mx-auto px-4 text-center">
            <h1 class="text-5xl font-bold mb-4">Reliable Plumbing You Can Trust</h1>
            <p class="text-xl mb-8">Serving {safe_loc} with fast, professional plumbing services.</p>
            <a href="{cta}" class="bg-yellow-500 text-gray-900
                px-8 py-3 rounded-lg font-semibold hover:bg-yellow-400">Call Now</a>
        </div>
    </section>

    <section id="services" class="py-16">
        <div class="max-w-6xl mx-auto px-4">
            <h2 class="text-3xl font-bold text-center mb-12">Our Services</h2>
            <div class="grid md:grid-cols-3 gap-8">
                <article class="text-center p-6 rounded-lg shadow">
                    <h3 class="text-xl font-semibold mb-2">Emergency Repairs</h3>
                    <p class="text-gray-600">24/7 emergency plumbing when you need it most.</p>
                </article>
                <article class="text-center p-6 rounded-lg shadow">
                    <h3 class="text-xl font-semibold mb-2">Drain Cleaning</h3>
                    <p class="text-gray-600">Professional drain clearing and maintenance.</p>
                </article>
                <article class="text-center p-6 rounded-lg shadow">
                    <h3 class="text-xl font-semibold mb-2">Water Heaters</h3>
                    <p class="text-gray-600">Installation, repair, and replacement services.</p>
                </article>
            </div>
        </div>
    </section>

    <section id="about" class="py-16 bg-gray-50">
        <div class="max-w-4xl mx-auto px-4 text-center">
            <h2 class="text-3xl font-bold mb-6">Why Choose {safe_name}?</h2>
            <p class="text-lg text-gray-600">Licensed and insured professionals dedicated to quality workmanship and customer satisfaction.</p>
        </div>
    </section>"""
    return _base_html(name, phone, email, location, body)


def _template_dentist(name: str, phone: str, email: str, location: str) -> str:
    safe_name = html.escape(name)
    safe_loc = html.escape(location) if location else "your area"
    cta = _cta_href(phone)
    body = f"""
    <section class="bg-teal-700 text-white py-20">
        <div class="max-w-4xl mx-auto px-4 text-center">
            <h1 class="text-5xl font-bold mb-4">Your Smile, Our Priority</h1>
            <p class="text-xl mb-8">Gentle, modern dental care in {safe_loc}.</p>
            <a href="{cta}" class="bg-white text-teal-700
                px-8 py-3 rounded-lg font-semibold hover:bg-gray-100">Book Appointment</a>
        </div>
    </section>

    <section id="services" class="py-16">
        <div class="max-w-6xl mx-auto px-4">
            <h2 class="text-3xl font-bold text-center mb-12">Our Services</h2>
            <div class="grid md:grid-cols-3 gap-8">
                <article class="text-center p-6 rounded-lg shadow">
                    <h3 class="text-xl font-semibold mb-2">General Dentistry</h3>
                    <p class="text-gray-600">Cleanings, exams, fillings, and preventive care.</p>
                </article>
                <article class="text-center p-6 rounded-lg shadow">
                    <h3 class="text-xl font-semibold mb-2">Cosmetic Dentistry</h3>
                    <p class="text-gray-600">Whitening, veneers, and smile makeovers.</p>
                </article>
                <article class="text-center p-6 rounded-lg shadow">
                    <h3 class="text-xl font-semibold mb-2">Emergency Care</h3>
                    <p class="text-gray-600">Same-day appointments for dental emergencies.</p>
                </article>
            </div>
        </div>
    </section>

    <section id="about" class="py-16 bg-gray-50">
        <div class="max-w-4xl mx-auto px-4 text-center">
            <h2 class="text-3xl font-bold mb-6">About {safe_name}</h2>
            <p class="text-lg text-gray-600">A welcoming practice focused on comfortable, compassionate dental care for the whole family.</p>
        </div>
    </section>"""
    return _base_html(name, phone, email, location, body)


def _template_salon(name: str, phone: str, email: str, location: str) -> str:
    safe_name = html.escape(name)
    safe_loc = html.escape(location) if location else "your area"
    cta = _cta_href(phone)
    body = f"""
    <section class="bg-pink-800 text-white py-20">
        <div class="max-w-4xl mx-auto px-4 text-center">
            <h1 class="text-5xl font-bold mb-4">Look &amp; Feel Your Best</h1>
            <p class="text-xl mb-8">Premium salon services in {safe_loc}.</p>
            <a href="{cta}" class="bg-white text-pink-800
                px-8 py-3 rounded-lg font-semibold hover:bg-gray-100">Book Now</a>
        </div>
    </section>

    <section id="services" class="py-16">
        <div class="max-w-6xl mx-auto px-4">
            <h2 class="text-3xl font-bold text-center mb-12">Our Services</h2>
            <div class="grid md:grid-cols-3 gap-8">
                <article class="text-center p-6 rounded-lg shadow">
                    <h3 class="text-xl font-semibold mb-2">Haircuts &amp; Styling</h3>
                    <p class="text-gray-600">Precision cuts and styles for every occasion.</p>
                </article>
                <article class="text-center p-6 rounded-lg shadow">
                    <h3 class="text-xl font-semibold mb-2">Color &amp; Highlights</h3>
                    <p class="text-gray-600">Expert color services from balayage to full color.</p>
                </article>
                <article class="text-center p-6 rounded-lg shadow">
                    <h3 class="text-xl font-semibold mb-2">Treatments</h3>
                    <p class="text-gray-600">Deep conditioning, keratin, and scalp treatments.</p>
                </article>
            </div>
        </div>
    </section>

    <section id="about" class="py-16 bg-gray-50">
        <div class="max-w-4xl mx-auto px-4 text-center">
            <h2 class="text-3xl font-bold mb-6">About {safe_name}</h2>
            <p class="text-lg text-gray-600">A welcoming space where beauty meets artistry. Our talented team is passionate about helping you shine.</p>
        </div>
    </section>"""
    return _base_html(name, phone, email, location, body)


def _template_restaurant(name: str, phone: str, email: str, location: str) -> str:
    safe_name = html.escape(name)
    safe_loc = html.escape(location) if location else "your area"
    cta = _cta_href(phone)
    body = f"""
    <section class="bg-amber-800 text-white py-20">
        <div class="max-w-4xl mx-auto px-4 text-center">
            <h1 class="text-5xl font-bold mb-4">Great Food, Great Moments</h1>
            <p class="text-xl mb-8">Delicious dining in {safe_loc}.</p>
            <a href="{cta}" class="bg-yellow-500 text-gray-900
                px-8 py-3 rounded-lg font-semibold hover:bg-yellow-400">Reserve a Table</a>
        </div>
    </section>

    <section id="services" class="py-16">
        <div class="max-w-6xl mx-auto px-4">
            <h2 class="text-3xl font-bold text-center mb-12">What We Offer</h2>
            <div class="grid md:grid-cols-3 gap-8">
                <article class="text-center p-6 rounded-lg shadow">
                    <h3 class="text-xl font-semibold mb-2">Dine In</h3>
                    <p class="text-gray-600">Enjoy a warm atmosphere with friends and family.</p>
                </article>
                <article class="text-center p-6 rounded-lg shadow">
                    <h3 class="text-xl font-semibold mb-2">Takeout &amp; Delivery</h3>
                    <p class="text-gray-600">Your favorites, ready when you are.</p>
                </article>
                <article class="text-center p-6 rounded-lg shadow">
                    <h3 class="text-xl font-semibold mb-2">Catering</h3>
                    <p class="text-gray-600">Let us bring the feast to your next event.</p>
                </article>
            </div>
        </div>
    </section>

    <section id="about" class="py-16 bg-gray-50">
        <div class="max-w-4xl mx-auto px-4 text-center">
            <h2 class="text-3xl font-bold mb-6">About {safe_name}</h2>
            <p class="text-lg text-gray-600">Fresh ingredients, bold flavors, and a passion for hospitality that keeps our guests coming back.</p>
        </div>
    </section>"""
    return _base_html(name, phone, email, location, body)


def _template_electrician(name: str, phone: str, email: str, location: str) -> str:
    safe_name = html.escape(name)
    safe_loc = html.escape(location) if location else "your area"
    cta = _cta_href(phone)
    body = f"""
    <section class="bg-yellow-600 text-white py-20">
        <div class="max-w-4xl mx-auto px-4 text-center">
            <h1 class="text-5xl font-bold mb-4">Safe &amp; Reliable Electrical Service</h1>
            <p class="text-xl mb-8">Licensed electricians serving {safe_loc}.</p>
            <a href="{cta}" class="bg-gray-900 text-white
                px-8 py-3 rounded-lg font-semibold hover:bg-gray-800">Call for Estimate</a>
        </div>
    </section>

    <section id="services" class="py-16">
        <div class="max-w-6xl mx-auto px-4">
            <h2 class="text-3xl font-bold text-center mb-12">Our Services</h2>
            <div class="grid md:grid-cols-3 gap-8">
                <article class="text-center p-6 rounded-lg shadow">
                    <h3 class="text-xl font-semibold mb-2">Residential Wiring</h3>
                    <p class="text-gray-600">New installations, rewiring, and panel upgrades.</p>
                </article>
                <article class="text-center p-6 rounded-lg shadow">
                    <h3 class="text-xl font-semibold mb-2">Lighting</h3>
                    <p class="text-gray-600">Indoor, outdoor, and landscape lighting design.</p>
                </article>
                <article class="text-center p-6 rounded-lg shadow">
                    <h3 class="text-xl font-semibold mb-2">Inspections</h3>
                    <p class="text-gray-600">Safety inspections and code compliance checks.</p>
                </article>
            </div>
        </div>
    </section>

    <section id="about" class="py-16 bg-gray-50">
        <div class="max-w-4xl mx-auto px-4 text-center">
            <h2 class="text-3xl font-bold mb-6">About {safe_name}</h2>
            <p class="text-lg text-gray-600">Fully licensed and insured. We deliver safe, up-to-code electrical work with transparent pricing.</p>
        </div>
    </section>"""
    return _base_html(name, phone, email, location, body)


def _template_general(name: str, phone: str, email: str, location: str) -> str:
    safe_name = html.escape(name)
    safe_loc = html.escape(location) if location else "our community"
    cta = _cta_href(phone)
    body = f"""
    <section class="bg-blue-700 text-white py-20">
        <div class="max-w-4xl mx-auto px-4 text-center">
            <h1 class="text-5xl font-bold mb-4">Welcome to {safe_name}</h1>
            <p class="text-xl mb-8">Proudly serving {safe_loc}.</p>
            <a href="{cta}" class="bg-white text-blue-700
                px-8 py-3 rounded-lg font-semibold hover:bg-gray-100">Contact Us</a>
        </div>
    </section>

    <section id="services" class="py-16">
        <div class="max-w-6xl mx-auto px-4">
            <h2 class="text-3xl font-bold text-center mb-12">What We Do</h2>
            <div class="grid md:grid-cols-3 gap-8">
                <article class="text-center p-6 rounded-lg shadow">
                    <h3 class="text-xl font-semibold mb-2">Quality Service</h3>
                    <p class="text-gray-600">Professional results you can count on.</p>
                </article>
                <article class="text-center p-6 rounded-lg shadow">
                    <h3 class="text-xl font-semibold mb-2">Local Expertise</h3>
                    <p class="text-gray-600">Deep knowledge of our community&#x27;s needs.</p>
                </article>
                <article class="text-center p-6 rounded-lg shadow">
                    <h3 class="text-xl font-semibold mb-2">Customer First</h3>
                    <p class="text-gray-600">Your satisfaction is our top priority.</p>
                </article>
            </div>
        </div>
    </section>

    <section id="about" class="py-16 bg-gray-50">
        <div class="max-w-4xl mx-auto px-4 text-center">
            <h2 class="text-3xl font-bold mb-6">About {safe_name}</h2>
            <p class="text-lg text-gray-600">A trusted local business committed to excellence and community.</p>
        </div>
    </section>"""
    return _base_html(name, phone, email, location, body)


TemplateFunc = Callable[[str, str, str, str], str]

CATEGORY_TEMPLATES: dict[str, TemplateFunc] = {
    "plumber": _template_plumber,
    "plumbing": _template_plumber,
    "dentist": _template_dentist,
    "dental": _template_dentist,
    "salon": _template_salon,
    "hair_salon": _template_salon,
    "restaurant": _template_restaurant,
    "electrician": _template_electrician,
    "electrical": _template_electrician,
    "general": _template_general,
}


def _render_template(
    *,
    category: str,
    name: str,
    phone: str,
    email: str,
    location: str,
) -> str:
    """Render an HTML template for the given category and business info."""
    key = category.lower().strip()
    template_fn = CATEGORY_TEMPLATES.get(key, _template_general)
    return template_fn(name, phone, email, location)


class DemoGenerator:
    """Generate demo websites for business leads."""

    def __init__(self, templates_dir: Path = TEMPLATES_DIR) -> None:
        self._templates_dir = templates_dir

    def generate(self, lead: Lead) -> Demo:
        """Generate a demo website for a lead.

        Args:
            lead: The business lead to generate a demo for.

        Returns:
            Demo with slug, path, and category info.

        Raises:
            DemoGeneratorError: If generation fails.
        """
        slug = _generate_slug(lead.name)
        html = _render_template(
            category=lead.category,
            name=lead.name,
            phone=lead.phone,
            email=lead.email,
            location=lead.location,
        )

        try:
            output_dir = self._templates_dir / slug
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / "index.html"
            output_file.write_text(html, encoding="utf-8")
        except OSError as exc:
            raise DemoGeneratorError(
                f"Failed to write demo for '{lead.name}': {exc}"
            ) from exc

        demo_path = f"templates/{slug}/index.html"
        return Demo(slug=slug, demo_path=demo_path, category=lead.category)
