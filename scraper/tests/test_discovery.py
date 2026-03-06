"""Tests for business discovery via Google Maps Places API."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from scraper.config import LEAD_STATUS_DISCOVERED
from scraper.discovery import (
    BusinessDiscovery,
    DiscoveryError,
    _parse_place,
)
from scraper.lead_manager import Lead


# --- Sample API responses ---

SAMPLE_PLACE = {
    "displayName": {"text": "Joe's Plumbing", "languageCode": "en"},
    "formattedAddress": "123 Main St, Denver, CO 80202",
    "nationalPhoneNumber": "(303) 555-1234",
    "websiteUri": "https://joesplumbing.com",
    "types": ["plumber", "point_of_interest", "establishment"],
}

SAMPLE_PLACE_NO_WEBSITE = {
    "displayName": {"text": "Bob's HVAC"},
    "formattedAddress": "456 Oak Ave, Denver, CO 80203",
    "nationalPhoneNumber": "(303) 555-5678",
    "types": ["hvac_contractor"],
}

SAMPLE_PLACE_MINIMAL = {
    "displayName": {"text": "Tiny Biz"},
}

SAMPLE_RESPONSE = {
    "places": [SAMPLE_PLACE, SAMPLE_PLACE_NO_WEBSITE],
}

SAMPLE_RESPONSE_WITH_PAGINATION = {
    "places": [SAMPLE_PLACE],
    "nextPageToken": "abc123token",
}

SAMPLE_RESPONSE_PAGE_2 = {
    "places": [SAMPLE_PLACE_NO_WEBSITE],
}

SAMPLE_RESPONSE_EMPTY = {
    "places": [],
}


# --- _parse_place tests ---


class TestParsePlace:
    def test_parse_full_place(self) -> None:
        lead = _parse_place(SAMPLE_PLACE, category="Plumbing")
        assert lead.name == "Joe's Plumbing"
        assert lead.location == "123 Main St, Denver, CO 80202"
        assert lead.phone == "(303) 555-1234"
        assert lead.website == "https://joesplumbing.com"
        assert lead.has_website is True
        assert lead.category == "Plumbing"
        assert lead.status == LEAD_STATUS_DISCOVERED

    def test_parse_place_no_website(self) -> None:
        lead = _parse_place(SAMPLE_PLACE_NO_WEBSITE, category="HVAC")
        assert lead.name == "Bob's HVAC"
        assert lead.website == ""
        assert lead.has_website is False

    def test_parse_minimal_place(self) -> None:
        lead = _parse_place(SAMPLE_PLACE_MINIMAL, category="General")
        assert lead.name == "Tiny Biz"
        assert lead.phone == ""
        assert lead.location == ""
        assert lead.website == ""
        assert lead.has_website is False

    def test_parse_place_generates_unique_ids(self) -> None:
        lead1 = _parse_place(SAMPLE_PLACE, category="A")
        lead2 = _parse_place(SAMPLE_PLACE, category="B")
        assert lead1.id != lead2.id


# --- BusinessDiscovery tests ---


class TestBusinessDiscovery:
    def test_init_without_api_key_raises(self) -> None:
        with pytest.raises(DiscoveryError, match="API key"):
            BusinessDiscovery(api_key="")

    def test_init_with_api_key(self) -> None:
        disco = BusinessDiscovery(api_key="test-key")
        assert disco._api_key == "test-key"

    @patch("scraper.discovery.httpx.Client")
    def test_search_returns_leads(self, mock_client_cls: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = SAMPLE_RESPONSE
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value = mock_client

        disco = BusinessDiscovery(api_key="test-key")
        leads = disco.search("Plumbing", location="Denver, CO")

        assert len(leads) == 2
        assert leads[0].name == "Joe's Plumbing"
        assert leads[1].name == "Bob's HVAC"

    @patch("scraper.discovery.httpx.Client")
    def test_search_sends_correct_request(self, mock_client_cls: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = SAMPLE_RESPONSE_EMPTY
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value = mock_client

        disco = BusinessDiscovery(api_key="my-key")
        disco.search("Salon", location="Boulder, CO", limit=10)

        call_args = mock_client.post.call_args
        assert "places:searchText" in call_args[0][0]
        body = call_args[1]["json"]
        assert "Salon" in body["textQuery"]
        assert "Boulder, CO" in body["textQuery"]
        assert body["pageSize"] == 10

        headers = call_args[1]["headers"]
        assert headers["X-Goog-Api-Key"] == "my-key"
        assert "places.displayName" in headers["X-Goog-FieldMask"]

    @patch("scraper.discovery.httpx.Client")
    def test_search_with_pagination(self, mock_client_cls: MagicMock) -> None:
        response1 = MagicMock()
        response1.status_code = 200
        response1.json.return_value = SAMPLE_RESPONSE_WITH_PAGINATION
        response1.raise_for_status = MagicMock()

        response2 = MagicMock()
        response2.status_code = 200
        response2.json.return_value = SAMPLE_RESPONSE_PAGE_2
        response2.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.side_effect = [response1, response2]
        mock_client_cls.return_value = mock_client

        disco = BusinessDiscovery(api_key="test-key")
        leads = disco.search("Plumbing", location="Denver, CO", limit=10)

        assert len(leads) == 2
        assert mock_client.post.call_count == 2

    @patch("scraper.discovery.httpx.Client")
    def test_search_respects_limit(self, mock_client_cls: MagicMock) -> None:
        many_places = {"places": [SAMPLE_PLACE] * 20, "nextPageToken": "more"}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = many_places
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value = mock_client

        disco = BusinessDiscovery(api_key="test-key")
        leads = disco.search("Plumbing", location="Denver", limit=5)

        assert len(leads) == 5

    @patch("scraper.discovery.httpx.Client")
    def test_search_empty_results(self, mock_client_cls: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = SAMPLE_RESPONSE_EMPTY
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value = mock_client

        disco = BusinessDiscovery(api_key="test-key")
        leads = disco.search("Nonexistent", location="Nowhere")

        assert len(leads) == 0

    @patch("scraper.discovery.httpx.Client")
    def test_search_api_error_raises(self, mock_client_cls: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.text = "Forbidden"
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "403 Forbidden", request=MagicMock(), response=mock_response
        )

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value = mock_client

        disco = BusinessDiscovery(api_key="bad-key")
        with pytest.raises(DiscoveryError, match="API request failed"):
            disco.search("Plumbing", location="Denver")

    @patch("scraper.discovery.httpx.Client")
    def test_search_default_limit(self, mock_client_cls: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = SAMPLE_RESPONSE
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value = mock_client

        disco = BusinessDiscovery(api_key="test-key")
        leads = disco.search("Plumbing", location="Denver")

        call_args = mock_client.post.call_args
        body = call_args[1]["json"]
        assert body["pageSize"] == 20
