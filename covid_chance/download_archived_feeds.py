import argparse
import datetime
import json
import logging
import sys
from pathlib import Path
from typing import Iterator

import luigi

from covid_chance.download_feeds import DownloadFeedPages
from covid_chance.file_utils import safe_filename

logger = logging.getLogger(__name__)


class DownloadArchivedFeeds(luigi.Task):
    data_path = luigi.Parameter()
    feeds = luigi.ListParameter()
    date_second = luigi.DateSecondParameter(default=datetime.datetime.now())

    @staticmethod
    def get_output_path(data_path: str, date_second: datetime.date) -> Path:
        return (
            Path(data_path)
            / f'archived_feeds_downloaded-{date_second.isoformat()}.txt'
        )

    def output(self):
        return luigi.LocalTarget(
            self.get_output_path(self.data_path, self.date_second)
        )

    @staticmethod
    def get_archived_feeds(data_path: str, feed_name: str) -> Iterator[dict]:
        for p in (Path(data_path) / safe_filename(feed_name)).glob(
            'feed_archived*.json'
        ):
            with p.open('r') as f:
                data = json.load(f)
                archived_feed = {
                    'timestamp': datetime.datetime.fromisoformat(
                        data['timestamp']
                    ),
                    'url': data['url'],
                }
            yield archived_feed

    def requires(self):
        for feed in self.feeds:
            if not feed.get('name'):
                continue
            for archived_feed in self.get_archived_feeds(
                self.data_path, feed['name']
            ):
                logger.info(
                    'Found archive feed URL %s',
                    archived_feed['timestamp'],
                    archived_feed['url'],
                )
                yield DownloadFeedPages(
                    data_path=self.data_path,
                    feed_name=feed['name'],
                    feed_url=archived_feed['url'],
                    date_second=archived_feed['timestamp'],
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
        [DownloadArchivedFeeds(data_path=args.data, feeds=config['feeds'])],
        workers=2,
        local_scheduler=True,
        log_level='INFO',
    )


if __name__ == '__main__':
    main()
