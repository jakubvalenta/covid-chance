import argparse
import concurrent.futures
import datetime
import json
import logging
import sys
from typing import Dict, List

import feedparser
import psycopg2
import psycopg2.errorcodes
import requests

from covid_chance.utils.db_utils import db_connect, db_insert, db_select
from covid_chance.utils.dict_utils import deep_get
from covid_chance.utils.download_utils import clean_url

logger = logging.getLogger(__name__)


def create_table(conn, table: str):
    cur = conn.cursor()
    try:
        cur.execute(
            f'''
CREATE TABLE {table} (
  url text PRIMARY KEY,
  feed_name text,
  inserted timestamp DEFAULT NOW()
);
CREATE INDEX index_{table}_url ON {table} (url);
'''
        )
    except psycopg2.ProgrammingError as e:
        if e.pgcode == psycopg2.errorcodes.DUPLICATE_TABLE:
            pass
        else:
            raise
    conn.commit()
    cur.close()


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


def save_page_urls(
    conn,
    table: str,
    feed_name: str,
    page_urls: List[str],
    mtime: datetime.datetime,
):
    cur = conn.cursor()
    missing_page_urls = [
        page_url
        for page_url in set(page_urls)
        if not db_select(conn, table, cur=cur, url=page_url)
    ]
    for page_url in missing_page_urls:
        try:
            db_insert(
                conn,
                table,
                cur=cur,
                url=page_url,
                feed_name=feed_name,
                inserted=mtime,
            )
        except Exception as e:
            logger.error(
                'Error while inserting new URL in the db %s %s', page_url, e
            )
    conn.commit()
    cur.close()
    logger.info(
        'done %s %d urls inserted',
        feed_name.ljust(40),
        len(missing_page_urls),
    )


def download_and_save_feed(
    conn, table: str, feed_name: str, feed_url: str, timeout: int
):
    mtime = datetime.datetime.now()
    page_urls = download_feed(feed_url, timeout)
    save_page_urls(conn, table, feed_name, page_urls, mtime)


def download_feeds(
    db: dict, table: str, feeds: List[Dict[str, str]], timeout: int
):
    conn = db_connect(
        host=db['host'],
        database=db['database'],
        user=db['user'],
        password=db['password'],
    )
    create_table(conn, table)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                download_and_save_feed,
                conn,
                table=table,
                feed_name=feed['name'],
                feed_url=feed['url'],
                timeout=timeout,
            )
            for feed in feeds
            if feed.get('name') and feed.get('url')
        ]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error('Exception: %s', e)
    conn.commit()
    conn.close()


def main():
    parser = argparse.ArgumentParser()
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
    download_feeds(
        db=config['db'],
        table=config['db']['table_urls'],
        feeds=config['feeds'],
        timeout=deep_get(
            config, ['download_feeds', 'timeout'], default=30, process=int
        ),
    )


if __name__ == '__main__':
    main()
