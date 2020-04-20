import argparse
import json
import logging
import sys
from typing import Iterator, Sequence

from covid_chance.db_utils import db_connect
from covid_chance.download_feeds import clean_url

logger = logging.getLogger(__name__)


def get_page_urls(conn, table: str) -> Iterator[str]:
    cur = conn.cursor()
    cur.execute(f'SELECT url FROM {table};')
    for row in cur:
        yield row[0]
    cur.close()


def clean_urls(
    conn,
    match_line: Sequence[Sequence[str]],
    table_lines: str,
    table_pages: str,
    table_parsed: str,
    table_tweets: str,
):
    cur = conn.cursor()
    for i, page_url in enumerate(get_page_urls(conn, table_pages)):
        clean_page_url = clean_url(page_url)
        if page_url == clean_page_url:
            logger.info('%d done %s', i, page_url)
            continue
        logger.warning('%d todo %s > %s', i, page_url, clean_page_url)
        for table in (table_lines, table_pages, table_parsed, table_tweets):
            cur.execute(
                f'UPDATE {table} SET url = %s WHERE url = %s;',
                (clean_page_url, page_url),
            )
        conn.commit()
    cur.close()


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
    clean_urls(
        conn,
        match_line=config['match_line'],
        table_lines=config['db']['table_lines'],
        table_pages=config['db']['table_pages'],
        table_parsed=config['db']['table_parsed'],
        table_tweets=config['db']['table_tweets'],
    )
    conn.close()


if __name__ == '__main__':
    main()
