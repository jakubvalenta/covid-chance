import argparse
import csv
import datetime
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List

from covid_chance.download_feeds import (
    clean_url, create_table, download_feed, save_page_urls,
)
from covid_chance.utils.db_utils import db_connect
from covid_chance.utils.file_utils import safe_filename

logger = logging.getLogger(__name__)


def read_archived_feeds(
    data_path: str, feed_name: str
) -> Dict[str, datetime.datetime]:
    archived_feeds = {}
    for p in (Path(data_path) / safe_filename(feed_name)).glob(
        'feed_archived*.json'
    ):
        with p.open('r') as f:
            data = json.load(f)
            url = data['url']
            timestamp = data['timestamp']
            if url and 'http://none' not in url and url not in archived_feeds:
                archived_feeds[url] = datetime.datetime.fromisoformat(
                    timestamp
                )
    return archived_feeds


def download_feed_with_cache(
    data_path: str,
    feed_name: str,
    feed_url: str,
    date: datetime.date,
    timeout: int,
):
    cache_path = (
        Path(data_path)
        / safe_filename(feed_name)
        / f'feed_pages-{date.isoformat()}.csv'
    )
    if cache_path.is_file():
        logger.info('Reading from cache')
        with cache_path.open('r') as f:
            page_urls = [clean_url(page_url) for (page_url,) in csv.reader(f)]
        return page_urls
    page_urls = download_feed(feed_url, timeout=timeout)
    with cache_path.open('w') as f:
        writer = csv.writer(f, lineterminator='\n')
        writer.writerows((page_url,) for page_url in page_urls)
    return page_urls


def download_archived_feeds(
    db: dict,
    table: str,
    data_path: str,
    feeds: List[Dict[str, str]],
    timeout: int,
):
    conn = db_connect(
        host=db['host'],
        database=db['database'],
        user=db['user'],
        password=db['password'],
    )
    create_table(conn, table)
    for feed in feeds:
        if not feed.get('name'):
            continue
        for feed_url, date in read_archived_feeds(
            data_path, feed['name']
        ).items():
            logger.info('Found archive feed URL %s %s', date, feed_url)
            try:
                page_urls = download_feed_with_cache(
                    data_path,
                    feed_name=feed['name'],
                    feed_url=feed_url,
                    date=date,
                    timeout=timeout,
                )
                save_page_urls(conn, table, feed['name'], page_urls, date)
            except Exception:
                logger.error('Failed to download %s', feed_url)
    conn.commit()
    conn.close()


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
    download_archived_feeds(
        db=config['db'],
        table=config['db']['table_urls'],
        data_path=args.data,
        feeds=config['feeds'],
        timeout=config['download_feed_timeout'],
    )


if __name__ == '__main__':
    main()
