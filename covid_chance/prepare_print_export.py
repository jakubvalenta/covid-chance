import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.errorcodes
from bs4 import BeautifulSoup

from covid_chance.download_pages import download_page
from covid_chance.post_tweet import read_approved_tweets
from covid_chance.utils.db_utils import db_connect, db_insert, db_select
from covid_chance.utils.download_utils import simplify_url
from covid_chance.utils.file_utils import safe_filename

logger = logging.getLogger(__name__)


def create_table(conn, table: str):
    cur = conn.cursor()
    try:
        cur.execute(
            f'''
CREATE TABLE {table} (
  tweet TEXT,
  image_url TEXT,
  inserted TIMESTAMP DEFAULT NOW()
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


def download_page_html(cache_path: str, page_url: str) -> Optional[str]:
    path = (
        Path(cache_path)
        / safe_filename(simplify_url(page_url))
        / 'page_content.html'
    )
    if path.is_file():
        logger.info('Reading from cache %s', page_url)
        return path.read_text()
    try:
        html = download_page(page_url)
    except Exception as e:
        logger.error('Failed to download %s: %s', page_url, e)
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html)
    return html


def get_meta_og_image_url(html: str) -> Optional[str]:
    if not html:
        return ''
    soup = BeautifulSoup(html, 'lxml')
    meta_og_image = soup.find('meta', property="og:image")
    if meta_og_image:
        return meta_og_image['content']
    return None


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
    cache_path = Path(args.cache) / 'pages'
    with open(args.config, 'r') as f:
        config = json.load(f)

    conn = db_connect(
        database=config['db']['database'],
        user=config['db']['user'],
        password=config['db']['password'],
    )
    table_reviewed = config['db']['table_reviewed']
    table_print = config['db']['table_print']

    create_table(conn, table_print)
    approved_tweets = list(read_approved_tweets(conn, table_reviewed))

    for i, tweet in enumerate(approved_tweets):
        if not db_select(conn, table_print, tweet=tweet.text):
            html = download_page_html(cache_path, tweet.page_url)
            image_url = get_meta_og_image_url(html)
            if image_url:
                logger.info(
                    'Saving tweet=%s, image_url=%s', tweet.text, image_url
                )
                db_insert(
                    conn, table_print, tweet=tweet.text, image_url=image_url
                )
            else:
                logger.warning('Not image found for %s', tweet.page_url)


if __name__ == '__main__':
    main()
