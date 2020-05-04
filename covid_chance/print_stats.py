import argparse
import csv
import json
import logging
import sys
from typing import Dict, List

from covid_chance.review_tweets import REVIEW_STATUS_APPROVED
from covid_chance.utils.db_utils import db_connect

logger = logging.getLogger(__name__)


def calc_feed_stats(
    cur,
    feed_name: str,
    table_urls: str,
    table_pages: str,
    table_lines: str,
    table_parsed: str,
    table_reviewed: str,
) -> dict:
    cur.execute(
        f'SELECT url FROM {table_urls} WHERE feed_name = %s;', (feed_name,)
    )
    page_urls = tuple(
        page_url for (page_url,) in cur
    )  # Must be a tuple because of psycopg2
    n_pages = len(page_urls)
    if n_pages:
        cur.execute(
            f'SELECT COUNT(*) FROM {table_lines} '
            'WHERE line != %s AND url IN %s;',
            ('', page_urls,),
        )
        n_lines = int(cur.fetchone()[0])
    else:
        n_lines = 0
    if n_lines:
        cur.execute(
            f'SELECT COUNT(*) FROM {table_parsed} WHERE url IN %s;',
            (page_urls,),
        )
        n_parsed = int(cur.fetchone()[0])
    else:
        n_parsed = 0
    if n_parsed:
        cur.execute(
            f'SELECT COUNT(*) FROM {table_reviewed} '
            'WHERE status = %s AND url IN %s;',
            (REVIEW_STATUS_APPROVED, page_urls),
        )
        n_approved = int(cur.fetchone()[0])
    else:
        n_approved = 0
    return {
        'feed_name': feed_name,
        'n_pages': n_pages,
        'n_lines': n_lines,
        'n_parsed': n_parsed,
        'n_approved': n_approved,
    }


def print_stats(
    db: dict,
    table_urls: str,
    table_pages: str,
    table_lines: str,
    table_parsed: str,
    table_reviewed: str,
    feeds: List[Dict[str, str]],
):
    conn = db_connect(
        database=db['database'], user=db['user'], password=db['password'],
    )

    cur = conn.cursor()
    try:
        writer = csv.DictWriter(
            sys.stdout,
            fieldnames=(
                'feed_name',
                'n_pages',
                'n_lines',
                'n_parsed',
                'n_approved',
            ),
            quoting=csv.QUOTE_NONNUMERIC,
            lineterminator='\n',
        )
        writer.writeheader()
        for feed in feeds:
            if feed['name']:
                feed_stats = calc_feed_stats(
                    cur,
                    feed['name'],
                    table_urls,
                    table_pages,
                    table_lines,
                    table_parsed,
                    table_reviewed,
                )
                writer.writerow(feed_stats)
    finally:
        cur.close()
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
    print_stats(
        db=config['db'],
        table_urls=config['db']['table_urls'],
        table_pages=config['db']['table_pages'],
        table_lines=config['db']['table_lines'],
        table_parsed=config['db']['table_parsed'],
        table_reviewed=config['db']['table_reviewed'],
        feeds=config['feeds'],
    )


if __name__ == '__main__':
    main()
