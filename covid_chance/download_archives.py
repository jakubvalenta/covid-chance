import argparse
import datetime
import json
import logging
import sys
from typing import Dict, List, Optional
from urllib.parse import urlsplit

import psycopg2
import psycopg2.errorcodes
import requests

from covid_chance.utils.db_utils import db_connect, db_insert, db_select

logger = logging.getLogger(__name__)


class ArchiveError(Exception):
    pass


def create_table(conn, table: str):
    cur = conn.cursor()
    try:
        cur.execute(
            f'''
CREATE TABLE {table} (
  feed_url text,
  archived_url text,
  date timestamp DEFAULT NOW(),
  inserted timestamp DEFAULT NOW()
);
CREATE INDEX index_{table}_feed_url ON {table} (feed_url);
CREATE INDEX index_{table}_date ON {table} (date);
'''
        )
    except psycopg2.ProgrammingError as e:
        if e.pgcode == psycopg2.errorcodes.DUPLICATE_TABLE:
            pass
        else:
            raise
    conn.commit()
    cur.close()


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


def download_feed_archives(
    db: dict,
    table: str,
    feeds: List[Dict[str, str]],
    dates: List[datetime.datetime],
):
    conn = db_connect(
        host=db['host'],
        database=db['database'],
        user=db['user'],
        password=db['password'],
    )
    create_table(conn, table)
    for feed in feeds:
        if feed.get('name') and feed.get('url'):
            for date in dates:
                if not db_select(conn, table, feed_url=feed['url'], date=date):
                    archived_url = find_closest_snapshot_url(feed['url'], date)
                    db_insert(
                        conn,
                        table,
                        feed_url=feed['url'],
                        archived_url=archived_url,
                        date=date,
                    )
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
    download_feed_archives(
        db=config['db'],
        table=config['db']['table_archives'],
        feeds=config['feeds'],
        dates=[
            datetime.datetime.fromisoformat(d) for d in config['archive_dates']
        ],
    )


if __name__ == '__main__':
    main()
