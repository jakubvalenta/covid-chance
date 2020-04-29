import argparse
import csv
import datetime
import json
import logging
import sys
import urllib.parse
from pathlib import Path
from typing import List, Sequence

import feedparser
import luigi
import requests

from covid_chance.utils.file_utils import safe_filename

logger = logging.getLogger(__name__)


def clean_url(
    url: str,
    remove_keys: Sequence[str] = (
        'fbclid',
        'ito',
        'ns_campaign',
        'ns_mchannel',
        'source',
        'utm_campaign',
        'utm_medium',
        'utm_source',
        'via',
    ),
) -> str:
    u = urllib.parse.urlsplit(url)
    qs = urllib.parse.parse_qs(u.query)
    for k in remove_keys:
        if k in qs:
            del qs[k]
    new_query = urllib.parse.urlencode(qs, doseq=True)
    return urllib.parse.urlunsplit(
        (u.scheme or 'https', u.netloc, u.path, new_query, u.fragment)
    )


def download_feed(url: str) -> List[str]:
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
    )
    r.raise_for_status()
    feed = feedparser.parse(r.text)
    return [clean_url(entry.link) for entry in feed.entries]


class DownloadFeed(luigi.Task):
    data_path = luigi.Parameter()
    feed_name = luigi.Parameter()
    feed_url = luigi.Parameter()
    date_second = luigi.DateSecondParameter()

    def output(self):
        return luigi.LocalTarget(
            Path(self.data_path)
            / safe_filename(self.feed_name)
            / f'feed_pages-{self.date_second.isoformat()}.csv'
        )

    def run(self):
        page_urls = download_feed(self.feed_url)
        with self.output().open('w') as f:
            writer = csv.writer(f, lineterminator='\n')
            writer.writerows((page_url,) for page_url in page_urls)


class DownloadFeeds(luigi.WrapperTask):
    data_path = luigi.Parameter()
    feeds = luigi.ListParameter()
    date_second = luigi.DateSecondParameter(default=datetime.datetime.now())

    def requires(self):
        return (
            DownloadFeed(
                data_path=self.data_path,
                feed_name=feed['name'],
                feed_url=feed['url'],
                date_second=self.date_second,
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
        [DownloadFeeds(data_path=args.data, feeds=config['feeds'])],
        workers=1,
        local_scheduler=True,
        parallel_scheduling=True,
        log_level='WARNING',
    )


if __name__ == '__main__':
    main()
