import datetime
import json
import logging
import os
import sys
from pathlib import Path
from string import Template
from typing import Dict, Iterator, List, Set

import luigi

from coronavirus_opportunity_bot.analyze import filter_lines, parse_lines
from coronavirus_opportunity_bot.download import (
    download_feed, download_page, get_page_text, simplify_url,
)
from coronavirus_opportunity_bot.file_utils import (
    csv_cache, read_csv_dict, read_first_line, safe_filename, write_csv_dict,
    write_csv_dict_row,
)

logger = logging.getLogger(__name__)


class PostedTweets:
    path: Path
    urls: Set[str]
    texts: Set[str]

    def __init__(self, path: Path):
        self.path = path
        self.urls = set()
        self.texts = set()
        if self.path.is_file():
            with path.open('r') as f:
                for posted_tweet in read_csv_dict(f):
                    self.urls.add(posted_tweet['url'])
                    self.texts.add(posted_tweet['parsed'])

    def was_posted(self, tweet: Dict[str, str]) -> bool:
        return tweet['url'] in self.urls or tweet['parsed'] in self.texts

    def add(self, tweet: Dict[str, str], date_second: datetime.datetime):
        self.urls.add(tweet['url'])
        self.texts.add(tweet['parsed'])
        posted_tweet = {'posted': date_second.isoformat(), **tweet}
        if not self.path.is_file():
            with self.path.open('w') as f:
                write_csv_dict([posted_tweet], f)
        else:
            with self.path.open('a') as f:
                write_csv_dict_row(posted_tweet, f)


class SavePageURL(luigi.Task):
    data_path = luigi.Parameter()
    feed_name = luigi.Parameter()
    page_url = luigi.Parameter()

    @staticmethod
    def get_output_path(data_path, feed_name, page_url) -> Path:
        return (
            Path(data_path)
            / feed_name
            / safe_filename(simplify_url(page_url))
            / 'page_url.txt'
        )

    def output(self):
        return luigi.LocalTarget(
            self.get_output_path(self.data_path, self.feed_name, self.page_url)
        )

    def run(self):
        with self.output().open('w') as f:
            print(self.page_url, file=f)


class DownloadPageHTML(luigi.Task):
    data_path = luigi.Parameter()
    feed_name = luigi.Parameter()
    page_url = luigi.Parameter()

    @staticmethod
    def get_output_path(data_path, feed_name, page_url) -> Path:
        return (
            Path(data_path)
            / feed_name
            / safe_filename(simplify_url(page_url))
            / 'page_content.html'
        )

    def output(self):
        return luigi.LocalTarget(
            self.get_output_path(self.data_path, self.feed_name, self.page_url)
        )

    def requires(self):
        return SavePageURL(
            data_path=self.data_path,
            feed_name=self.feed_name,
            page_url=self.page_url,
        )

    def run(self):
        with self.output().open('w') as f:
            html = download_page(self.page_url)
            f.write(html)


class DownloadPage(luigi.Task):
    data_path = luigi.Parameter()
    feed_name = luigi.Parameter()
    page_url = luigi.Parameter()

    @staticmethod
    def get_output_path(data_path, feed_name, page_url) -> Path:
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

    def requires(self):
        return DownloadPageHTML(
            data_path=self.data_path,
            feed_name=self.feed_name,
            page_url=self.page_url,
        )

    def run(self):
        with self.input().open('r') as f:
            html = f.read()

        with self.output().open('w') as f:
            text = get_page_text(html)
            f.write(text)


class DownloadFeedPages(luigi.Task):
    data_path = luigi.Parameter()
    feed_name = luigi.Parameter()
    date_second = luigi.DateSecondParameter()

    @staticmethod
    def get_output_path(
        data_path: str, feed_name: str, date_second: datetime.date
    ) -> Path:
        return (
            Path(data_path)
            / feed_name
            / f'feed_downloaded-{date_second.isoformat()}.txt'
        )

    @staticmethod
    def get_feed_path(
        data_path: str, feed_name: str, date_second: datetime.date
    ) -> Path:
        return (
            Path(data_path)
            / feed_name
            / f'feed_pages-{date_second.isoformat()}.csv'
        )

    @staticmethod
    def get_feed_info_path(data_path: str, feed_name: str) -> Path:
        return Path(data_path) / feed_name / 'feed_info.json'

    def output(self):
        return luigi.LocalTarget(
            self.get_output_path(
                self.data_path, self.feed_name, self.date_second
            )
        )

    @classmethod
    def download_feed(
        cls, data_path: str, feed_name: str, date_second: str
    ) -> List[str]:
        @csv_cache(cls.get_feed_path(data_path, feed_name, date_second))
        def download_feed_with_cache():
            feed_info_path = cls.get_feed_info_path(data_path, feed_name)
            with feed_info_path.open('r') as f:
                feed_info = json.load(f)
            page_urls = download_feed(feed_info['url'])
            return [(page_url,) for page_url in page_urls]

        return [row[0] for row in download_feed_with_cache()]

    def requires(self):
        page_urls = self.download_feed(
            self.data_path, self.feed_name, self.date_second
        )
        for page_url in page_urls:
            yield DownloadPage(
                data_path=self.data_path,
                feed_name=self.feed_name,
                page_url=page_url,
            )

    def run(self):
        with self.output().open('w') as f:
            f.write('')


class Download(luigi.Task):
    data_path = luigi.Parameter(default='./data')
    date_second = luigi.DateSecondParameter(default=datetime.datetime.now())
    verbose = luigi.BoolParameter(default=False, significant=False)

    @staticmethod
    def get_output_path(data_path: str, date_second: datetime.date) -> Path:
        return (
            Path(data_path) / f'all_downloaded-{date_second.isoformat()}.txt'
        )

    @staticmethod
    def get_feed_info_paths(data_path: str) -> Iterator[Path]:
        return Path(data_path).glob('*/feed_info.json')

    def output(self):
        return luigi.LocalTarget(
            self.get_output_path(self.data_path, self.date_second)
        )

    def requires(self):
        for path in self.get_feed_info_paths(self.data_path):
            yield DownloadFeedPages(
                data_path=self.data_path,
                feed_name=path.parent.name,
                date_second=self.date_second,
            )

    def run(self):
        if self.verbose:
            logging.basicConfig(
                stream=sys.stderr, level=logging.INFO, format='%(message)s'
            )
        with self.output().open('w') as f:
            f.write('')


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
    def get_page_urls(data_path: str, feed_name: str) -> Path:
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
        for feed_info_path in Download.get_feed_info_paths(self.data_path):
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


class PostTweets(luigi.Task):
    data_path = luigi.Parameter(default='./data')
    date_second = luigi.DateSecondParameter(default=datetime.datetime.now())
    keywords = luigi.ListParameter()
    pattern = luigi.Parameter()
    template = luigi.Parameter()
    auth_token = luigi.Parameter(default='', significant=False)
    verbose = luigi.BoolParameter(default=False, significant=False)

    @staticmethod
    def get_output_path(data_path: str, date_second: datetime.date) -> Path:
        return Path(data_path) / f'posted_tweets-{date_second.isoformat()}.csv'

    @staticmethod
    def get_all_posted_tweets_path(
        data_path: str, date_second: datetime.date
    ) -> Path:
        return Path(data_path) / f'all_posted_tweets.csv'

    def output(self):
        return luigi.LocalTarget(
            self.get_output_path(self.data_path, self.date_second)
        )

    def requires(self):
        return CreateTweets(
            data_path=self.data_path,
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

        auth_token = self.auth_token or os.environ.get('AUTH_TOKEN')
        if not auth_token:
            ValueError('Auth token is not defined')

        with self.input().open('r') as f:
            all_tweets = list(read_csv_dict(f))
        all_posted_tweets = PostedTweets(
            self.get_all_posted_tweets_path(self.data_path, self.date_second)
        )
        now_posted_tweets = PostedTweets(Path(self.output().path))
        for tweet in all_tweets:
            if not tweet['tweet']:
                continue
            if all_posted_tweets.was_posted(tweet):
                logger.warn('SKIP %s', tweet['tweet'])
                continue
            logger.warn('POST %s', tweet['tweet'])
            all_posted_tweets.add(tweet, self.date_second)
            now_posted_tweets.add(tweet, self.date_second)
            # TODO: post

    def complete(self):
        return False


class CleanTweets(luigi.Task):
    data_path = luigi.Parameter(default='./data')
    date_second = luigi.DateSecondParameter(default=datetime.datetime.now())
    verbose = luigi.BoolParameter(default=False, significant=False)

    def run(self):
        if self.verbose:
            logging.basicConfig(
                stream=sys.stderr, level=logging.INFO, format='%(message)s'
            )
        for feed_info_path in Download.get_feed_info_paths(self.data_path):
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
