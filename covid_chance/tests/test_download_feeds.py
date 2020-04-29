from unittest import TestCase

from covid_chance.download_feeds import clean_url


class TestDownloadPages(TestCase):
    def test_clean_url(self):
        self.assertEqual(
            clean_url('https://example.com/foo?k=v&utm_source=spam&x=y#zzz'),
            'https://example.com/foo?k=v&x=y#zzz',
        )
