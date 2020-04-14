import argparse
import json
import logging
import sys
from pathlib import Path
from string import Template
from typing import IO, Dict, Iterable, Iterator, List, Sequence, Tuple

import luigi
import regex

from covid_chance.download_feeds import DownloadPageText, simplify_url
from covid_chance.file_utils import (
    read_csv_dict, read_first_line, safe_filename, write_csv_dict,
)


def filter_lines(f: IO, match_line: Sequence[Sequence[str]]) -> Iterator[str]:
    regexes = [
        regex.compile(r'\L<keywords>', keywords=keywords, flags=regex.I)
        for keywords in match_line
    ]
    return (line.strip() for line in f if all(r.search(line) for r in regexes))


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
    page_url = luigi.Parameter()
    match_line = luigi.ListParameter()
    parse_pattern = luigi.Parameter()
    tweet_template = luigi.Parameter()

    @staticmethod
    def get_output_path(data_path: str, feed_name: str, page_url: str) -> Path:
        return (
            Path(data_path)
            / safe_filename(feed_name)
            / safe_filename(simplify_url(page_url))
            / 'page_tweets.csv'
        )

    def output(self):
        return luigi.LocalTarget(
            self.get_output_path(self.data_path, self.feed_name, self.page_url)
        )

    def requires(self):
        return DownloadPageText(
            data_path=self.data_path,
            feed_name=self.feed_name,
            page_url=self.page_url,
        )

    def run(self):
        tweet_template_obj = Template(self.tweet_template)
        with self.input().open('r') as f:
            lines = filter_lines(f, self.match_line)
            tweets = [
                {
                    'url': self.page_url,
                    'line': line,
                    'parsed': parsed,
                    'tweet': tweet_template_obj.substitute(
                        parsed=parsed, url=self.page_url
                    )
                    if parsed
                    else '',
                }
                for line, parsed in parse_lines(lines, self.parse_pattern)
            ]
        with self.output().open('w') as f:
            write_csv_dict(tweets, f)


class CreateTweets(luigi.Task):
    data_path = luigi.Parameter()
    feeds = luigi.ListParameter()
    match_line = luigi.ListParameter()
    parse_pattern = luigi.Parameter()
    tweet_template = luigi.Parameter()

    @staticmethod
    def get_page_urls(data_path: str, feed_name: str) -> Iterator[str]:
        for page_url_path in (Path(data_path) / safe_filename(feed_name)).glob(
            '*/page_url.txt'
        ):
            yield read_first_line(page_url_path)

    @classmethod
    def read_all_tweets(
        cls, data_path: str, feeds: List[Dict[str, str]]
    ) -> Iterator[Dict[str, str]]:
        for feed in feeds:
            for page_url in cls.get_page_urls(data_path, feed['name']):
                page_tweets_path = CreatePageTweets.get_output_path(
                    data_path, feed['name'], page_url
                )
                if page_tweets_path.is_file():
                    with page_tweets_path.open('r') as f:
                        yield from read_csv_dict(f)

    def requires(self):
        for feed in self.feeds:
            for page_url in self.get_page_urls(self.data_path, feed['name']):
                yield CreatePageTweets(
                    data_path=self.data_path,
                    feed_name=feed['name'],
                    page_url=page_url,
                    match_line=self.match_line,
                    parse_pattern=self.parse_pattern,
                    tweet_template=self.tweet_template,
                )

    def run(self):
        pass

    def complete(self):
        return False


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
                data_path=args.data,
                feeds=config['feeds'],
                match_line=config['match_line'],
                parse_pattern=config['parse_pattern'],
                tweet_template=config['tweet_template'],
            )
        ],
        workers=6,
        local_scheduler=True,
        log_level='WARNING',
    )


if __name__ == '__main__':
    main()
