import datetime
import json
import logging
import sys
from pathlib import Path
from string import Template
from typing import IO, Dict, Iterable, Iterator, List, Sequence, Tuple

import luigi
import regex

from coronavirus_opportunity_bot.download_feeds import (
    DownloadFeeds, simplify_url,
)
from coronavirus_opportunity_bot.file_utils import (
    read_csv_dict, read_first_line, safe_filename, write_csv_dict,
)


def filter_lines(f: IO, keywords: Sequence[str]) -> Iterator[str]:
    r = regex.compile(r'\L<keywords>', keywords=keywords)
    return (line.strip() for line in f if r.search(line))


def parse_lines(
    lines: Iterable[str], pattern: str
) -> Iterator[Tuple[str, str]]:
    r = regex.compile(pattern)
    for line in lines:
        m = r.search(line)
        if m:
            parsed = m.group('parsed')
        else:
            parsed = ''
        yield line, parsed


class CreatePageTweets(luigi.Task):
    data_path = luigi.Parameter()
    feed_name = luigi.Parameter()
    feed_twitter_handle = luigi.Parameter()
    page_url = luigi.Parameter()
    keywords = luigi.ListParameter()
    pattern = luigi.Parameter()
    template = luigi.Parameter()

    @staticmethod
    def get_output_path(data_path: str, feed_name: str, page_url: str) -> Path:
        return (
            Path(data_path)
            / feed_name
            / safe_filename(simplify_url(page_url))
            / 'page_tweets.csv'
        )

    @staticmethod
    def get_text_path(data_path: str, feed_name: str, page_url: str) -> Path:
        return (
            Path(data_path)
            / feed_name
            / safe_filename(simplify_url(page_url))
            / 'page_content.txt'
        )

    def output(self):
        return luigi.LocalTarget(
            self.get_output_path(self.data_path, self.feed_name, self.page_url)
        )

    def run(self):
        tweet_tmpl = Template(self.template)
        text_path = self.get_text_path(
            self.data_path, self.feed_name, self.page_url
        )
        with text_path.open('r') as f:
            lines = filter_lines(f, self.keywords)
            tweets = [
                {
                    'url': self.page_url,
                    'line': line,
                    'parsed': parsed,
                    'tweet': tweet_tmpl.substitute(
                        parsed=parsed,
                        url=self.page_url,
                        handle=self.feed_twitter_handle,
                    )
                    if parsed
                    else '',
                }
                for line, parsed in parse_lines(lines, self.pattern)
            ]
        with self.output().open('w') as f:
            write_csv_dict(tweets, f)


class CreateFeedTweets(luigi.Task):
    data_path = luigi.Parameter()
    feed_name = luigi.Parameter()
    feed_twitter_handle = luigi.Parameter()
    date_second = luigi.DateSecondParameter()
    keywords = luigi.ListParameter()
    pattern = luigi.Parameter()
    template = luigi.Parameter()

    @staticmethod
    def get_output_path(
        data_path: str, feed_name: str, date_second: datetime.date
    ) -> Path:
        return (
            Path(data_path)
            / feed_name
            / f'feed_tweets-{date_second.isoformat()}.csv'
        )

    @staticmethod
    def get_page_urls(data_path: str, feed_name: str) -> Iterator[str]:
        for page_url_path in (Path(data_path) / feed_name).glob(
            '*/page_url.txt'
        ):
            yield read_first_line(page_url_path)

    def output(self):
        return luigi.LocalTarget(
            self.get_output_path(
                self.data_path, self.feed_name, self.date_second
            )
        )

    def requires(self):
        page_urls = self.get_page_urls(self.data_path, self.feed_name)
        for page_url in page_urls:
            yield CreatePageTweets(
                data_path=self.data_path,
                feed_name=self.feed_name,
                feed_twitter_handle=self.feed_twitter_handle,
                page_url=page_url,
                keywords=self.keywords,
                pattern=self.pattern,
                template=self.template,
            )

    def run(self):
        feed_tweets: List[Dict[str, str]] = []
        for page_input in self.input():
            with page_input.open('r') as f:
                feed_tweets.extend(read_csv_dict(f))
        with self.output().open('w') as f:
            write_csv_dict(feed_tweets, f)


class CreateTweets(luigi.Task):
    data_path = luigi.Parameter(default='./data')
    date_second = luigi.DateSecondParameter(default=datetime.datetime.now())
    keywords = luigi.ListParameter()
    pattern = luigi.Parameter()
    template = luigi.Parameter()
    verbose = luigi.BoolParameter(default=False, significant=False)

    @staticmethod
    def get_output_path(data_path: str, date_second: datetime.date) -> Path:
        return Path(data_path) / f'all_tweets-{date_second.isoformat()}.csv'

    def output(self):
        return luigi.LocalTarget(
            self.get_output_path(self.data_path, self.date_second)
        )

    def requires(self):
        for feed_info_path in DownloadFeeds.get_feed_info_paths(
            self.data_path
        ):
            with feed_info_path.open('r') as f:
                feed_info = json.load(f)
            feed_name = feed_info_path.parent.name
            feed_twitter_handle = feed_info['twitter_handle']
            yield CreateFeedTweets(
                data_path=self.data_path,
                feed_name=feed_name,
                feed_twitter_handle=feed_twitter_handle,
                date_second=self.date_second,
                keywords=self.keywords,
                pattern=self.pattern,
                template=self.template,
            )

    def run(self):
        if self.verbose:
            logging.basicConfig(
                stream=sys.stderr, level=logging.INFO, format='%(message)s'
            )
        all_tweets: List[Dict[str, str]] = []
        for feed_input in self.input():
            with feed_input.open('r') as f:
                for page_tweet in read_csv_dict(f):
                    if page_tweet:
                        all_tweets.append(page_tweet)
        with self.output().open('w') as f:
            write_csv_dict(all_tweets, f)


class CleanTweets(luigi.Task):
    data_path = luigi.Parameter(default='./data')
    date_second = luigi.DateSecondParameter(default=datetime.datetime.now())
    verbose = luigi.BoolParameter(default=False, significant=False)

    def run(self):
        if self.verbose:
            logging.basicConfig(
                stream=sys.stderr, level=logging.INFO, format='%(message)s'
            )
        for feed_info_path in DownloadFeeds.get_feed_info_paths(
            self.data_path
        ):
            feed_name = feed_info_path.parent.name
            page_urls = CreateFeedTweets.get_page_urls(
                self.data_path, feed_name
            )
            for page_url in page_urls:
                CreatePageTweets.get_output_path(
                    self.data_path, feed_name, page_url
                ).unlink(missing_ok=True)

    def complete(self):
        return False
