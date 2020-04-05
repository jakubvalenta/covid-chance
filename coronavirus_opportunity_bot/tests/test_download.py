from unittest import TestCase

from coronavirus_opportunity_bot.download import clean_whitespace


class TestDownload(TestCase):
    def test_clean_whitespace(self):
        self.assertEqual(
            clean_whitespace('\n  foo  bar \n\n \n baz\nspam  lorem\n\n'),
            'foo bar\nbaz\nspam lorem',
        )
