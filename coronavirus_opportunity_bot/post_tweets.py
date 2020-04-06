import datetime
import logging
import os
import sys
from pathlib import Path
from typing import Dict, Set

import luigi

from coronavirus_opportunity_bot.create_tweets import CreateTweets
from coronavirus_opportunity_bot.file_utils import (
    read_csv_dict, write_csv_dict, write_csv_dict_row,
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
