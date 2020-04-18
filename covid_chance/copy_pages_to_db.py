import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Iterator

import luigi

from covid_chance.download_feeds import SavePageText
from covid_chance.file_utils import read_first_line, safe_filename


class CopyPagesToDatabase(luigi.WrapperTask):
    data_path = luigi.Parameter()
    feeds = luigi.ListParameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    @staticmethod
    def get_page_urls(data_path: str, feed_name: str) -> Iterator[str]:
        for page_url_path in (Path(data_path) / safe_filename(feed_name)).glob(
            '*/page_url.txt'
        ):
            yield read_first_line(page_url_path)

    def requires(self):
        for feed in self.feeds:
            for page_url in self.get_page_urls(self.data_path, feed['name']):
                yield SavePageText(
                    data_path=self.data_path,
                    feed_name=feed['name'],
                    page_url=page_url,
                    host=self.host,
                    database=self.database,
                    user=self.user,
                    password=self.password,
                    table=self.table,
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
            CopyPagesToDatabase(
                data_path=args.data,
                feeds=config['feeds'],
                host=config['db']['host'],
                database=config['db']['database'],
                user=config['db']['user'],
                password=config['db']['password'],
                table=config['db']['table_pages'],
            )
        ],
        workers=6,
        local_scheduler=True,
        parallel_scheduling=True,
        log_level='INFO',
    )


if __name__ == '__main__':
    main()
