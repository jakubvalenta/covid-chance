import argparse
import datetime
import json
import logging
import sys
from pathlib import Path
from typing import Optional
from urllib.parse import urlsplit

import luigi
import requests

from covid_chance.file_utils import safe_filename

logger = logging.getLogger(__name__)


class ArchiveError(Exception):
    pass


def get_from_web_archive(url: str, *args, **kwargs) -> requests.Response:
    res = requests.get(
        url, *args, headers={'User-Agent': 'curl/7.69.1'}, **kwargs
    )
    res.raise_for_status()
    return res


def find_closest_snapshot_url(url: str, date: datetime.date) -> Optional[str]:
    u = urlsplit(url)
    if u.username:
        logger.warning('Skipping URL with Basic Auth %s', url)
        return None
    logger.info('Finding closest snaphot URL for %s %s', url, date)
    timestamp = date.strftime('%Y%m%d')
    api_url = f'https://web.archive.org/web/{timestamp}/{url}'
    res = get_from_web_archive(api_url, allow_redirects=False)
    snapshot_url = res.headers.get('location')
    if not snapshot_url:
        raise ArchiveError('Failed to find snapshot URL')
    if 'http://none' in snapshot_url:
        logger.warning('Empty snapshot URL returned %s', snapshot_url)
        return None
    logger.info(
        'Closest snapshot URL for %s %s is %s', url, date, snapshot_url
    )
    return snapshot_url


class DownloadFeedArchive(luigi.Task):
    data_path = luigi.Parameter()
    feed_name = luigi.ListParameter()
    feed_url = luigi.ListParameter()
    date = luigi.DateParameter()

    @classmethod
    def get_output_path(
        cls, data_path: str, feed_name: str, date: datetime.date,
    ) -> Path:
        return (
            Path(data_path)
            / safe_filename(feed_name)
            / f'feed_archived-{date.isoformat()}.json'
        )

    def output(self):
        return luigi.LocalTarget(
            self.get_output_path(self.data_path, self.feed_name, self.date)
        )

    def run(self):
        archived_feed_url = find_closest_snapshot_url(self.feed_url, self.date)
        with self.output().open('w') as f:
            data = {
                'timestamp': self.date.isoformat(),
                'url': archived_feed_url,
            }
            json.dump(data, f)


class DownloadFeedArchives(luigi.Task):
    data_path = luigi.Parameter()
    feeds = luigi.ListParameter()
    date = luigi.DateParameter()

    @staticmethod
    def get_output_path(data_path: str, date: datetime.date) -> Path:
        return (
            Path(data_path)
            / f'feed_archives_downloaded-{date.isoformat()}.txt'
        )

    def output(self):
        return luigi.LocalTarget(
            self.get_output_path(self.data_path, self.date)
        )

    def requires(self):
        for feed in self.feeds:
            if feed.get('name') and feed.get('url'):
                yield DownloadFeedArchive(
                    data_path=self.data_path,
                    feed_name=feed['name'],
                    feed_url=feed['url'],
                    date=self.date,
                )

    def run(self):
        with self.output().open('w') as f:
            f.write('')


class DownloadFeedsArchives(luigi.Task):
    data_path = luigi.Parameter()
    feeds = luigi.ListParameter()
    dates = luigi.ListParameter()
    date_second = luigi.DateSecondParameter(default=datetime.datetime.now())

    @staticmethod
    def get_output_path(data_path: str, date_second: datetime.date) -> Path:
        return (
            Path(data_path)
            / f'all_feeds_archives_downloaded-{date_second.isoformat()}.txt'
        )

    def output(self):
        return luigi.LocalTarget(
            self.get_output_path(self.data_path, self.date_second)
        )

    def requires(self):
        return (
            DownloadFeedArchives(
                data_path=self.data_path,
                feeds=self.feeds,
                date=datetime.date.fromisoformat(date_str),
            )
            for date_str in self.dates
        )

    def run(self):
        with self.output().open('w') as f:
            f.write('')


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
            DownloadFeedsArchives(
                data_path=args.data,
                feeds=config['feeds'],
                dates=config['archive_dates'],
            )
        ],
        workers=1,
        local_scheduler=True,
        log_level='INFO',
    )


if __name__ == '__main__':
    main()
