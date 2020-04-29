import argparse
import csv
import json
import logging
import sys
from pathlib import Path
from typing import Iterator

from covid_chance.review_tweets import REVIEW_STATUS_APPROVED
from covid_chance.utils.db_utils import db_connect
from covid_chance.utils.file_utils import safe_filename

logger = logging.getLogger(__name__)


def get_feed_page_urls(data_path: str, feed_name: str) -> Iterator[str]:
    for page_url_path in (Path(data_path) / safe_filename(feed_name)).glob(
        '*/page_url.txt'
    ):
        with page_url_path.open('r') as f:
            page_url = f.readline().strip()
        yield page_url


def calc_feed_stats(
    cur,
    data_path: str,
    feed_name: str,
    table_pages: str,
    table_lines: str,
    table_parsed: str,
    table_reviewed: str,
) -> dict:
    page_urls = tuple(
        get_feed_page_urls(data_path, feed_name)
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
        database=config['db']['database'],
        user=config['db']['user'],
        password=config['db']['password'],
    )
    table_pages = config['db']['table_pages']
    table_lines = config['db']['table_lines']
    table_parsed = config['db']['table_parsed']
    table_reviewed = config['db']['table_reviewed']

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
        for feed in config['feeds']:
            if feed['name']:
                feed_stats = calc_feed_stats(
                    cur,
                    args.data,
                    feed['name'],
                    table_pages,
                    table_lines,
                    table_parsed,
                    table_reviewed,
                )
                writer.writerow(feed_stats)
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    main()
