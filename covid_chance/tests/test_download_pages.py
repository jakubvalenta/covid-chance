from unittest import TestCase

from covid_chance.download_pages import clean_whitespace


class TestDownloadPages(TestCase):
    def test_clean_whitespace(self):
        self.assertEqual(
            clean_whitespace('\n  foo  bar \n\n \n baz\nspam  lorem\n\n'),
            'foo bar\nbaz\nspam lorem',
        )
