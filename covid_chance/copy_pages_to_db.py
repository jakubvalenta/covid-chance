import argparse
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path
from typing import Dict, Iterable, Optional, cast

import psycopg2
import psycopg2.errorcodes

from covid_chance.utils.db_utils import db_connect, db_insert
from covid_chance.utils.download_utils import clean_url
from covid_chance.utils.file_utils import safe_filename

logger = logging.getLogger(__name__)


def create_table(conn, table: str):
    cur = conn.cursor()
    try:
        cur.execute(
            f'''
CREATE TABLE {table} (
  url text,
  text text,
  inserted timestamp DEFAULT NOW()
);
'''
        )
    except psycopg2.ProgrammingError as e:
        if e.pgcode == psycopg2.errorcodes.DUPLICATE_TABLE:
            pass
        else:
            raise
    conn.commit()
    cur.close()


def read_page_url(page_dir: Path) -> Optional[str]:
    path = page_dir / 'page_url.txt'
    if path.is_file():
        with path.open('r') as f:
            page_url = clean_url(f.readline().strip())
        return page_url
    return None


def read_page_content(page_dir: Path) -> Optional[str]:
    path = page_dir / 'page_content.txt'
    if path.is_file():
        return path.read_text()
    return None


def copy_feed_pages_to_db(conn, table: str, data_path: str, feed_name: str):
    cur = conn.cursor()
    feed_dir = Path(data_path) / safe_filename(feed_name)
    count = 0
    for page_dir in sorted(feed_dir.iterdir()):
        if not page_dir.is_dir():
            continue
        page_url = read_page_url(page_dir)
        if not page_url:
            continue
        page_content = read_page_content(page_dir)
        if not page_content:
            continue
        db_insert(conn, table, cur=cur, url=page_url, text=page_content)
        count += 1
    conn.commit()
    cur.close()
    logger.info('done %s %d pages', feed_name.ljust(40), count)


def copy_pages_to_db(
    conn, table: str, data_path: str, feeds: Iterable[Dict[str, Optional[str]]]
):
    create_table(conn, table)
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                copy_feed_pages_to_db,
                conn,
                table,
                data_path,
                cast(str, feed['name']),
            )
            for feed in feeds
            if feed.get('name')
        ]
        wait(futures)


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
    conn = db_connect(
        host=config['db']['host'],
        database=config['db']['database'],
        user=config['db']['user'],
        password=config['db']['password'],
    )
    copy_pages_to_db(
        conn,
        table=config['db']['table_pages'],
        data_path=args.data,
        feeds=config['feeds'],
    )
    conn.close()


if __name__ == '__main__':
    main()
