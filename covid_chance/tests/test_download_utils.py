from unittest import TestCase

from covid_chance.utils.download_utils import clean_url, simplify_url


class TestDownloadUtils(TestCase):
    def test_clean_url(self):
        self.assertEqual(
            clean_url('https://example.com/foo?k=v&utm_source=spam&x=y#zzz'),
            'https://example.com/foo?k=v&x=y#zzz',
        )

    def test_simplify_url(self):
        self.assertEqual(
            simplify_url('https://jakub:123@example.com/foo?k=v&x=y#zzz'),
            '//example.com/foo?k=v&x=y',
        )
