import argparse
import json
import logging
import sys
from string import Template
from typing import Dict, Iterator

import luigi
import luigi.contrib.postgres
import psycopg2

from covid_chance.hash_utils import hashobj


class CreateLineTweets(luigi.contrib.postgres.CopyToTable):
    page_url = luigi.Parameter()
    line = luigi.Parameter()
    parsed = luigi.Parameter()
    tweet_template = luigi.Parameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    columns = [
        ('url', 'TEXT'),
        ('line', 'TEXT'),
        ('parsed', 'TEXT'),
        ('tweet', 'TEXT'),
    ]

    @property
    def update_id(self):
        return hashobj(self.page_url, self.parsed, self.tweet_template)

    def rows(self):
        tweet = Template(self.tweet_template).substitute(
            parsed=self.parsed, url=self.page_url
        )
        yield (self.page_url, self.line, self.parsed, tweet)


class CreateTweets(luigi.WrapperTask):
    tweet_template = luigi.Parameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table_parsed = luigi.Parameter()
    table_tweets = luigi.Parameter()

    @classmethod
    def get_parsed(
        cls, database: str, user: str, password: str, table: str
    ) -> Iterator[tuple]:
        conn = psycopg2.connect(dbname=database, user=user, password=password)
        cur = conn.cursor()
        cur.execute(f'SELECT url, line, parsed FROM {table};')
        yield from cur
        cur.close()

    @classmethod
    def read_all_tweets(
        cls, database: str, user: str, password: str, table: str
    ) -> Iterator[Dict[str, str]]:
        conn = psycopg2.connect(dbname=database, user=user, password=password)
        cur = conn.cursor()
        cur.execute(f'SELECT url, line, parsed, tweet FROM {table};')
        for url, line, parsed, tweet in cur:
            yield {'url': url, 'line': line, 'parsed': parsed, 'tweet': tweet}
        cur.close()

    def requires(self):
        for page_url, line, parsed in self.get_parsed(
            self.database, self.user, self.password, self.table_parsed
        ):
            yield CreateLineTweets(
                page_url=page_url,
                line=line,
                parsed=parsed,
                tweet_template=self.tweet_template,
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                table=self.table_tweets,
            )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--data', help='Data path', default='./data')
    parser.add_argument(
        '-c', '--config', help='Configuration file path', required=True
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true', help='Enable debugging output'
    )
    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(
            stream=sys.stderr, level=logging.INFO, format='%(message)s'
        )
    with open(args.config, 'r') as f:
        config = json.load(f)
    luigi.build(
        [
            CreateTweets(
                tweet_template=config['tweet_template'],
                host=config['db']['host'],
                database=config['db']['database'],
                user=config['db']['user'],
                password=config['db']['password'],
                table_parsed=config['db']['table_parsed'],
                table_tweets=config['db']['table_tweets'],
            )
        ],
        workers=6,
        local_scheduler=True,
        parallel_scheduling=True,
        log_level='WARNING',
    )


if __name__ == '__main__':
    main()
