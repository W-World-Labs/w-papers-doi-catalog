import unittest

from app.application.errors import DoiNotFound, ProviderUnavailable, UpstreamUnavailable
from app.application.ports import CacheEntry
from app.application.use_cases import DoiLookup
from tests.helpers import MemoryCache, Provider, work


class LookupTests(unittest.TestCase):
    def lookup(self, cache, providers, now=100.0):
        return DoiLookup(
            cache,
            tuple(providers),
            positive_ttl_seconds=50,
            negative_ttl_seconds=10,
            clock=lambda: now,
        )

    def test_fresh_hit_does_not_call_provider(self):
        cache = MemoryCache()
        cache.entries["10.1234/example"] = CacheEntry("found", work(), 101)
        provider = Provider(result=work())
        result = self.lookup(cache, [provider]).execute("10.1234/example")
        self.assertEqual(result.cache_status, "hit")
        self.assertEqual(provider.calls, [])

    def test_negative_hit_does_not_call_provider(self):
        cache = MemoryCache()
        cache.entries["10.1234/example"] = CacheEntry("missing", None, 101)
        provider = Provider(result=work())
        with self.assertRaises(DoiNotFound) as raised:
            self.lookup(cache, [provider]).execute("10.1234/example")
        self.assertEqual(raised.exception.cache_status, "negative")
        self.assertEqual(provider.calls, [])

    def test_provider_success_populates_positive_cache(self):
        cache = MemoryCache()
        result = self.lookup(cache, [Provider(result=work())]).execute("10.1234/example")
        self.assertEqual(result.cache_status, "miss")
        self.assertEqual(cache.entries["10.1234/example"].expires_at, 150)

    def test_all_provider_misses_populate_negative_cache(self):
        cache = MemoryCache()
        with self.assertRaises(DoiNotFound) as raised:
            self.lookup(cache, [Provider(name="a"), Provider(name="b")]).execute(
                "10.1234/example"
            )
        self.assertEqual(raised.exception.cache_status, "miss")
        self.assertEqual(cache.entries["10.1234/example"].expires_at, 110)

    def test_stale_positive_is_used_only_on_provider_failure(self):
        cache = MemoryCache()
        cache.entries["10.1234/example"] = CacheEntry("found", work(), 99)
        provider = Provider(error=ProviderUnavailable("down"))
        result = self.lookup(cache, [provider]).execute("10.1234/example")
        self.assertEqual(result.cache_status, "stale")

    def test_no_stale_record_returns_upstream_unavailable(self):
        provider = Provider(error=ProviderUnavailable("down"))
        with self.assertRaises(UpstreamUnavailable):
            self.lookup(MemoryCache(), [provider]).execute("10.1234/example")


if __name__ == "__main__":
    unittest.main()
