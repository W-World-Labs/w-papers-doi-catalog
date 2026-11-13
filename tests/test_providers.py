import json
import unittest

import httpx

from app.adapters.http_providers import CrossrefProvider, OpenAlexProvider
from app.application.errors import ProviderUnavailable


def client(handler):
    return httpx.Client(
        transport=httpx.MockTransport(handler),
        headers={"User-Agent": "catalog-test; mailto:test@example.com"},
        follow_redirects=False,
        trust_env=False,
    )


class ProviderTests(unittest.TestCase):
    def common(self, http_client, maximum=10000):
        return {
            "user_agent": "catalog-test; mailto:test@example.com",
            "connect_timeout": 1,
            "read_timeout": 1,
            "max_response_bytes": maximum,
            "client": http_client,
        }

    def test_crossref_maps_legacy_contract_and_encodes_doi_as_path_segment(self):
        seen = []

        def handler(request):
            seen.append(request)
            return httpx.Response(
                200,
                json={"message": {
                    "DOI": "10.1234/EXAMPLE", "title": ["A title"],
                    "author": [{"given": "Ada", "family": "Lovelace"}],
                    "container-title": ["Journal"], "issued": {"date-parts": [[2024, 2, 3]]},
                    "type": "journal-article", "ISSN": ["1234-5678"],
                }},
            )

        provider = CrossrefProvider(
            "https://api.crossref.org/works", **self.common(client(handler))
        )
        work = provider.lookup("10.1234/example")
        self.assertEqual(work.authors, ("Ada Lovelace",))
        self.assertEqual(work.published_at, "2024-02-03")
        self.assertIn("10.1234%2Fexample", str(seen[0].url))

    def test_openalex_maps_record(self):
        payload = {
            "ids": {"doi": "https://doi.org/10.1234/example"},
            "display_name": "A title",
            "publication_date": "2024-01-01",
            "authorships": [{"author": {"display_name": "Grace Hopper"}}],
            "primary_location": {"source": {"display_name": "Journal"}},
            "biblio": {"first_page": "1", "last_page": "9"},
        }
        provider = OpenAlexProvider(
            "https://api.openalex.org/works",
            **self.common(client(lambda _: httpx.Response(200, json=payload))),
        )
        work = provider.lookup("10.1234/example")
        self.assertEqual(work.page, "1-9")
        self.assertEqual(work.authors, ("Grace Hopper",))

    def test_404_is_miss_redirect_is_failure_and_body_limit_is_hard(self):
        provider = CrossrefProvider(
            "https://api.crossref.org/works",
            **self.common(client(lambda _: httpx.Response(404))),
        )
        self.assertIsNone(provider.lookup("10.1234/example"))

        redirect = CrossrefProvider(
            "https://api.crossref.org/works",
            **self.common(client(lambda _: httpx.Response(302, headers={"location": "https://evil.invalid"}))),
        )
        with self.assertRaises(ProviderUnavailable):
            redirect.lookup("10.1234/example")

        oversized = CrossrefProvider(
            "https://api.crossref.org/works",
            **self.common(client(lambda _: httpx.Response(200, content=json.dumps({"message": {"x": "x" * 100}}))), maximum=32),
        )
        with self.assertRaises(ProviderUnavailable):
            oversized.lookup("10.1234/example")


if __name__ == "__main__":
    unittest.main()
