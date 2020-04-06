import argparse
import json
import logging
import sys
from pathlib import Path
from string import Template
from typing import IO, Iterable, Iterator, Sequence, Tuple

import luigi
import regex

from coronavirus_opportunity_bot.download_feeds import (
    DownloadFeeds, DownloadPageText, simplify_url,
)
from coronavirus_opportunity_bot.file_utils import (
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
    feed_twitter_handle = luigi.Parameter()
    page_url = luigi.Parameter()
    match_line = luigi.ListParameter()
    parse_pattern = luigi.Parameter()
    tweet_template = luigi.Parameter()

    @staticmethod
    def get_output_path(data_path: str, feed_name: str, page_url: str) -> Path:
        return (
            Path(data_path)
            / feed_name
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
                        parsed=parsed,
                        url=self.page_url,
                        handle=self.feed_twitter_handle,
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
    match_line = luigi.ListParameter()
    parse_pattern = luigi.Parameter()
    tweet_template = luigi.Parameter()

    @staticmethod
    def get_page_urls(data_path: str, feed_name: str) -> Iterator[str]:
        for page_url_path in (Path(data_path) / feed_name).glob(
            '*/page_url.txt'
        ):
            yield read_first_line(page_url_path)

    @classmethod
    def read_all_page_urls(cls, data_path: str) -> Iterator[Tuple[str, str]]:
        for feed_info_path in DownloadFeeds.get_feed_info_paths(data_path):
            feed_name = feed_info_path.parent.name
            page_urls = cls.get_page_urls(data_path, feed_name)
            for page_url in page_urls:
                yield feed_name, page_url

    @classmethod
    def read_all_tweets(cls, data_path: str):
        for feed_name, page_url in cls.read_all_page_urls(data_path):
            page_tweets_path = CreatePageTweets.get_output_path(
                data_path, feed_name, page_url
            )
            with page_tweets_path.open('r') as f:
                yield from read_csv_dict(f)

    def requires(self):
        for feed_info_path in DownloadFeeds.get_feed_info_paths(
            self.data_path
        ):
            with feed_info_path.open('r') as f:
                feed_info = json.load(f)
            feed_name = feed_info_path.parent.name
            feed_twitter_handle = feed_info['twitter_handle']
            page_urls = self.get_page_urls(self.data_path, feed_name)
            for page_url in page_urls:
                yield CreatePageTweets(
                    data_path=self.data_path,
                    feed_name=feed_name,
                    feed_twitter_handle=feed_twitter_handle,
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
    parser.add_argument(
        '-d', '--data-path', help='Data path', default='./data'
    )
    parser.add_argument(
        '-c', '--config-path', help='Configuration file path', required=True
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true', help='Enable debugging output'
    )
    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(
            stream=sys.stderr, level=logging.INFO, format='%(message)s'
        )
    with open(args.config_path, 'r') as f:
        config = json.load(f)
    luigi.build(
        [
            CreateTweets(
                data_path=args.data_path,
                match_line=config['match_line'],
                parse_pattern=config['parse_pattern'],
                tweet_template=config['tweet_template'],
            )
        ],
        workers=2,
        local_scheduler=True,
        log_level='INFO',
    )


if __name__ == '__main__':
    main()
