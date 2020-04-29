import argparse
import datetime
import json
import logging
import sys
from pathlib import Path
from typing import Dict

import luigi

from covid_chance.download_feeds import DownloadFeed
from covid_chance.utils.file_utils import safe_filename

logger = logging.getLogger(__name__)


class DownloadArchivedFeeds(luigi.WrapperTask):
    data_path = luigi.Parameter()
    feeds = luigi.ListParameter()
    date_second = luigi.DateSecondParameter(default=datetime.datetime.now())

    def get_archived_feeds(
        self, feed_name: str
    ) -> Dict[str, datetime.datetime]:
        archived_feeds = {}
        for p in (Path(self.data_path) / safe_filename(feed_name)).glob(
            'feed_archived*.json'
        ):
            with p.open('r') as f:
                data = json.load(f)
                url = data['url']
                timestamp = data['timestamp']
                if (
                    url
                    and 'http://none' not in url
                    and url not in archived_feeds
                ):
                    logger.info('Found archive feed URL %s %s', timestamp, url)
                    archived_feeds[url] = datetime.datetime.fromisoformat(
                        timestamp
                    )
        return archived_feeds

    def requires(self):
        for feed in self.feeds:
            if not feed.get('name'):
                continue
            for feed_url, feed_timestamp in self.get_archived_feeds(
                feed['name']
            ).items():
                yield DownloadFeed(
                    data_path=self.data_path,
                    feed_name=feed['name'],
                    feed_url=feed_url,
                    date_second=feed_timestamp,
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
        [DownloadArchivedFeeds(data_path=args.data, feeds=config['feeds'])],
        workers=1,
        local_scheduler=True,
        parallel_scheduling=True,
        log_level='WARNING',
    )


if __name__ == '__main__':
    main()
