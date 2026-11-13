import unittest

from app.domain import DoiWork, normalize_doi


class DomainTests(unittest.TestCase):
    def test_normalizes_common_doi_forms(self):
        self.assertEqual(normalize_doi(" DOI:10.1234/ABC.DEF "), "10.1234/abc.def")
        self.assertEqual(
            normalize_doi("https://doi.org/10.1234%2FABC.DEF"), "10.1234/abc.def"
        )

    def test_rejects_arbitrary_urls_and_control_characters(self):
        self.assertIsNone(normalize_doi("https://evil.invalid/10.1234/example"))
        self.assertIsNone(normalize_doi("10.1234/example\nHost:evil.invalid"))
        self.assertIsNone(normalize_doi("not-a-doi"))

    def test_contract_keeps_legacy_fields(self):
        payload = DoiWork(doi="10.1234/example", source="crossref").to_contract()
        self.assertEqual(
            set(payload),
            {
                "doi", "title", "authors", "journal", "publisher",
                "published_at", "volume", "issue", "page", "article_type",
                "issn", "source", "from_local_catalog",
            },
        )
        self.assertTrue(payload["from_local_catalog"])


if __name__ == "__main__":
    unittest.main()
