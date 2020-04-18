import argparse
import json
import logging
import sys
from string import Template
from typing import Dict, Iterable, Iterator, Sequence, Tuple

import luigi
import luigi.contrib.postgres
import psycopg2
import regex


def filter_lines(
    f: Iterable[str], match_line: Sequence[Sequence[str]]
) -> Iterator[str]:
    regexes = [
        regex.compile(r'\L<keywords>', keywords=keywords, flags=regex.I)
        for keywords in match_line
    ]
    return (line.strip() for line in f if all(r.search(line) for r in regexes))


def searchall(rx: regex.Regex, s: str) -> Iterator:
    m = rx.search(s)
    while m:
        yield m
        m = rx.search(s, pos=m.span()[1])


def parse_lines(
    lines: Iterable[str], pattern: str
) -> Iterator[Tuple[str, str]]:
    rx = regex.compile(pattern)
    for line in lines:
        matches = list(searchall(rx, line))
        if matches:
            for m in matches:
                yield (line, m.group('parsed'))
        else:
            yield line, ''


class CreatePageTweets(luigi.contrib.postgres.CopyToTable):
    page_url = luigi.Parameter()
    page_text = luigi.Parameter()
    match_line = luigi.ListParameter()
    parse_pattern = luigi.Parameter()
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
        return json.dumps(
            [
                self.page_url,
                self.match_line,
                self.parse_pattern,
                self.tweet_template,
            ]
        )

    def rows(self):
        tweet_template_obj = Template(self.tweet_template)
        lines = filter_lines(self.page_text.splitlines(), self.match_line)
        for line, parsed in parse_lines(lines, self.parse_pattern):
            tweet = (
                tweet_template_obj.substitute(parsed=parsed, url=self.page_url)
                if parsed
                else ''
            )
            yield (self.page_url, line, parsed, tweet)


class CreateTweets(luigi.WrapperTask):
    match_line = luigi.ListParameter()
    parse_pattern = luigi.Parameter()
    tweet_template = luigi.Parameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table_pages = luigi.Parameter()
    table_tweets = luigi.Parameter()

    @classmethod
    def get_pages(
        cls, database: str, user: str, password: str, table: str
    ) -> Iterator[tuple]:
        conn = psycopg2.connect(dbname=database, user=user, password=password)
        cur = conn.cursor()
        cur.execute(f'SELECT url, text FROM {table};')
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
        for page_url, page_text in self.get_pages(
            self.database, self.user, self.password, self.table_pages
        ):
            yield CreatePageTweets(
                page_url=page_url,
                page_text=page_text,
                match_line=self.match_line,
                parse_pattern=self.parse_pattern,
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
                match_line=config['match_line'],
                parse_pattern=config['parse_pattern'],
                tweet_template=config['tweet_template'],
                host=config['db']['host'],
                database=config['db']['database'],
                user=config['db']['user'],
                password=config['db']['password'],
                table_pages=config['db']['table_pages'],
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
