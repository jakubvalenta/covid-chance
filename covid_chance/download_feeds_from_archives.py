import argparse
import csv
import json
import logging
import sys
from pathlib import Path
from typing import Dict, Iterator, List

from covid_chance.download_feeds import (
    clean_url, create_table, download_feed, save_page_urls,
)
from covid_chance.utils.db_utils import db_connect
from covid_chance.utils.dict_utils import deep_get
from covid_chance.utils.file_utils import safe_filename

logger = logging.getLogger(__name__)


def get_archives(conn, table: str, feed_url: str) -> Iterator[tuple]:
    cur = conn.cursor()
    cur.execute(
        f'SELECT archived_url, date FROM {table} '
        'WHERE feed_url = %s AND archived_url IS NOT NULL;',
        (feed_url,),
    )
    yield from cur
    cur.close()


def download_archived_feeds(
    db: dict,
    table_archives: str,
    table_urls: str,
    cache_path: str,
    feeds: List[Dict[str, str]],
    timeout: int,
):
    conn = db_connect(
        host=db['host'],
        database=db['database'],
        user=db['user'],
        password=db['password'],
    )
    create_table(conn, table_urls)
    for feed in feeds:
        if not feed.get('name') or not feed.get('url'):
            continue
        for archived_url, date in get_archives(
            conn, table_archives, feed['url']
        ):
            logger.info('Found archived feed %s %s', date, archived_url)
            cache_file_path = (
                Path(cache_path)
                / safe_filename(archived_url)
                / 'feed_pages.csv'
            )
            if cache_file_path.is_file():
                logger.info('Reading from cache')
                with cache_file_path.open('r') as f:
                    page_urls = [
                        clean_url(page_url) for (page_url,) in csv.reader(f)
                    ]
            else:
                try:
                    page_urls = download_feed(archived_url, timeout=timeout)
                except Exception:
                    logger.error('Failed to download %s', archived_url)
                cache_file_path.parent.mkdir(parents=True, exist_ok=True)
                with cache_file_path.open('w') as f:
                    writer = csv.writer(f, lineterminator='\n')
                    writer.writerows((page_url,) for page_url in page_urls)
            save_page_urls(conn, table_urls, feed['name'], page_urls, date)
    conn.commit()
    conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--cache', help='Cache directory path', default='./cache'
    )
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
    download_archived_feeds(
        db=config['db'],
        table_archives=config['db']['table_archives'],
        table_urls=config['db']['table_urls'],
        cache_path=Path(args.cache) / 'feeds',
        feeds=config['feeds'],
        timeout=deep_get(
            config, ['download_feeds', 'timeout'], default=30, process=int
        ),
    )


if __name__ == '__main__':
    main()
