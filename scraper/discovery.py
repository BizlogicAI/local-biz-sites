"""Business discovery via Google Maps Places API (New)."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from scraper.config import LEAD_STATUS_DISCOVERED
from scraper.lead_manager import Lead

logger = logging.getLogger(__name__)

API_URL = "https://places.googleapis.com/v1/places:searchText"

FIELD_MASK = ",".join(
    [
        "places.displayName",
        "places.formattedAddress",
        "places.nationalPhoneNumber",
        "places.websiteUri",
        "places.types",
        "nextPageToken",
    ]
)

MAX_PAGE_SIZE = 20
MAX_PAGES = 3  # API returns max 60 results (3 pages of 20)


class DiscoveryError(Exception):
    """Raised when business discovery fails."""


def _parse_place(place: dict[str, Any], *, category: str) -> Lead:
    """Convert a Google Maps place result to a Lead."""
    display_name = place.get("displayName", {})
    name = display_name.get("text", "") if isinstance(display_name, dict) else ""
    website = place.get("websiteUri", "")

    return Lead(
        name=name,
        phone=place.get("nationalPhoneNumber", ""),
        location=place.get("formattedAddress", ""),
        website=website,
        has_website=bool(website),
        category=category,
        status=LEAD_STATUS_DISCOVERED,
    )


class BusinessDiscovery:
    """Discover local businesses using Google Maps Places API (New).

    Uses the Text Search endpoint to find businesses by category and location.
    """

    def __init__(self, api_key: str) -> None:
        if not api_key:
            raise DiscoveryError("API key is required for Google Maps Places API")
        self._api_key = api_key

    def search(
        self,
        category: str,
        *,
        location: str = "",
        limit: int = 20,
    ) -> list[Lead]:
        """Search for businesses by category and location.

        Args:
            category: Business type to search for (e.g., "Plumbing").
            location: Location to search in (e.g., "Denver, CO").
            limit: Maximum number of results to return (max 60).

        Returns:
            List of Lead objects for discovered businesses.

        Raises:
            DiscoveryError: If the API request fails.
        """
        query = f"{category} in {location}" if location else category
        page_size = min(limit, MAX_PAGE_SIZE)
        leads: list[Lead] = []
        page_token: str | None = None

        with httpx.Client(timeout=30) as client:
            for _ in range(MAX_PAGES):
                if len(leads) >= limit:
                    break

                body: dict[str, Any] = {
                    "textQuery": query,
                    "pageSize": page_size,
                }
                if page_token:
                    body["pageToken"] = page_token

                try:
                    response = client.post(
                        API_URL,
                        json=body,
                        headers={
                            "X-Goog-Api-Key": self._api_key,
                            "X-Goog-FieldMask": FIELD_MASK,
                        },
                    )
                    response.raise_for_status()
                except httpx.HTTPError as exc:
                    raise DiscoveryError(f"API request failed: {exc}") from exc

                data = response.json()
                places = data.get("places", [])

                for place in places:
                    if len(leads) >= limit:
                        break
                    leads.append(_parse_place(place, category=category))

                page_token = data.get("nextPageToken")
                if not page_token:
                    break

        logger.info(
            "Discovered %d businesses for '%s' in '%s'", len(leads), category, location
        )
        return leads
