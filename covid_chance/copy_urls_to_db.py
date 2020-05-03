import argparse
import csv
import datetime
import json
import logging
import sys
from pathlib import Path
from typing import Dict, Iterable, Optional, cast

from covid_chance.download_feeds import create_table, save_page_urls
from covid_chance.utils.db_utils import db_connect
from covid_chance.utils.download_utils import clean_url
from covid_chance.utils.file_utils import safe_filename

logger = logging.getLogger(__name__)


def read_page_urls(
    data_path: str, feed_name: str
) -> Dict[str, datetime.datetime]:
    feed_dir = Path(data_path) / safe_filename(feed_name)
    page_urls = {}
    for p in sorted(feed_dir.glob('feed_pages*.csv')):
        mtime = datetime.datetime.fromtimestamp(p.stat().st_mtime)
        with p.open('r') as f:
            for (raw_page_url,) in csv.reader(f):
                page_url = clean_url(raw_page_url)
                if page_url not in page_urls:
                    page_urls[page_url] = mtime
    for p in sorted(feed_dir.glob('**/page_url.txt')):
        mtime = datetime.datetime.fromtimestamp(p.stat().st_mtime)
        raw_page_url = p.read_text().strip()
        if not raw_page_url:
            logger.error('%s contains invalid URL', str(p))
            continue
        page_url = clean_url(raw_page_url)
        if page_url not in page_urls:
            page_urls[page_url] = mtime
    return page_urls


def read_and_save_page_urls(conn, table: str, data_path: str, feed_name: str):
    page_urls = read_page_urls(data_path, feed_name)
    save_page_urls(conn, table, feed_name, page_urls)


def copy_urls_to_db(
    db: dict,
    table: str,
    data_path: str,
    feeds: Iterable[Dict[str, Optional[str]]],
):
    conn = db_connect(
        host=db['host'],
        database=db['database'],
        user=db['user'],
        password=db['password'],
    )
    create_table(conn, table)
    for feed in feeds:
        if feed.get('name'):
            read_and_save_page_urls(
                conn, table, data_path, cast(str, feed['name']),
            )
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
    copy_urls_to_db(
        db=config['db'],
        table=config['db']['table_urls'],
        data_path=args.data,
        feeds=config['feeds'],
    )


if __name__ == '__main__':
    main()
