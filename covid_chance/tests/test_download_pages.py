from unittest import TestCase

from covid_chance.download_pages import clean_whitespace, simplify_url


class TestDownloadFeeds(TestCase):
    def test_clean_whitespace(self):
        self.assertEqual(
            clean_whitespace('\n  foo  bar \n\n \n baz\nspam  lorem\n\n'),
            'foo bar\nbaz\nspam lorem',
        )

    def test_simplify_url(self):
        self.assertEqual(
            simplify_url('https://jakub:123@example.com/foo?k=v&x=y#zzz'),
            '//example.com/foo?k=v&x=y',
        )
