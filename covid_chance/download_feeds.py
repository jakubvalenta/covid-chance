import argparse
import csv
import datetime
import json
import logging
import sys
from pathlib import Path
from typing import List

import feedparser
import luigi
import requests

from covid_chance.utils.download_utils import clean_url
from covid_chance.utils.file_utils import safe_filename

logger = logging.getLogger(__name__)


def download_feed(url: str, timeout: int = 10) -> List[str]:
    logger.info('Downloading feed %s', url)
    # Fetch the feed content using requests, because feedparser seems to have
    # some trouble with the Basic Auth -- the feed object contains an error.
    r = requests.get(
        url,
        headers={
            'User-Agent': (
                'Mozilla/5.0 (X11; Linux x86_64; rv:75.0) '
                'Gecko/20100101 Firefox/75.0'
            )
        },
        timeout=timeout,
    )
    r.raise_for_status()
    feed = feedparser.parse(r.text)
    return [clean_url(entry.link) for entry in feed.entries]


class DownloadFeed(luigi.Task):
    data_path = luigi.Parameter()
    feed_name = luigi.Parameter()
    feed_url = luigi.Parameter()
    date_second = luigi.DateSecondParameter()

    timeout = luigi.NumericalParameter(
        var_type=int, min_value=0, max_value=99, significant=False
    )

    def output(self):
        return luigi.LocalTarget(
            Path(self.data_path)
            / safe_filename(self.feed_name)
            / f'feed_pages-{self.date_second.isoformat()}.csv'
        )

    def run(self):
        page_urls = download_feed(self.feed_url, timeout=self.timeout)
        with self.output().open('w') as f:
            writer = csv.writer(f, lineterminator='\n')
            writer.writerows((page_url,) for page_url in page_urls)


class DownloadFeeds(luigi.WrapperTask):
    data_path = luigi.Parameter()
    feeds = luigi.ListParameter()
    date_second = luigi.DateSecondParameter(default=datetime.datetime.now())

    timeout = luigi.NumericalParameter(
        var_type=int, min_value=0, max_value=99, significant=False
    )

    def requires(self):
        return (
            DownloadFeed(
                data_path=self.data_path,
                feed_name=feed['name'],
                feed_url=feed['url'],
                date_second=self.date_second,
                timeout=self.timeout,
            )
            for feed in self.feeds
            if feed.get('name') and feed.get('url')
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
            DownloadFeeds(
                data_path=args.data,
                feeds=config['feeds'],
                timeout=config['download_feed_timeout'],
            )
        ],
        workers=6,
        local_scheduler=True,
        parallel_scheduling=True,
        log_level='WARNING',
    )


if __name__ == '__main__':
    main()
