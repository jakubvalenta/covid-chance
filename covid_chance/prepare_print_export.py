import argparse
import json
import logging
import shutil
import sys
from pathlib import Path
from typing import Optional
from urllib.parse import urlsplit

import psycopg2
import psycopg2.errorcodes
import requests
from bs4 import BeautifulSoup
from PIL import Image

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
  image_path TEXT,
  approved TIMESTAMP,
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


def identify_image_ext(url: str) -> str:
    u = urlsplit(url)
    return Path(u.path).suffix


EXTENSIONS = {
    'JPEG': '.jpg',
    'GIF': '.gif',
    'PNG': '.png',
    'WEBP': '.webp',
}


def download_image(
    cache_path: Path,
    url: str,
    placeholder_str: str = 'placeholder',
    timeout: int = 10,
) -> Path:
    if url.startswith('//'):
        url = 'https:' + url
    path = Path(cache_path) / safe_filename(simplify_url(url)) / 'image'
    if path.is_file():
        logger.info('Image %s is already downloaded', url)
    else:
        logger.info('Downloading %s to %s', url, path)
        res = requests.get(
            url,
            headers={
                'User-Agent': (
                    'Mozilla/5.0 (X11; Linux x86_64; rv:75.0) '
                    'Gecko/20100101 Firefox/75.0'
                )
            },
            timeout=timeout,
        )
        res.raise_for_status()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(res.content)
    with Image.open(path) as im:
        ext = EXTENSIONS[im.format]
        path_with_ext = Path(str(path) + ext)
        if not path_with_ext.exists():
            logger.info('Copying %s to %s', path, path_with_ext)
            shutil.copy2(path, path_with_ext)
    return path_with_ext


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
    cache_path = Path(args.cache)
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
            html = download_page_html(cache_path / 'pages', tweet.page_url)
            image_url = get_meta_og_image_url(html)
            if image_url and image_url != 'null':
                try:
                    image_path = download_image(
                        cache_path / 'images', image_url
                    )
                except Exception as e:
                    logger.warning(
                        'Failed to download image %s: %s', image_url, e
                    )
                    continue
                logger.info(
                    'Saving tweet=%s, image_url=%s, image_path=%s',
                    tweet.text,
                    image_url,
                    image_path,
                )
                db_insert(
                    conn,
                    table_print,
                    tweet=tweet.text,
                    image_url=image_url,
                    image_path=str(image_path),
                    approved=tweet.inserted,
                )
            else:
                logger.warning('No image found for %s', tweet.page_url)


if __name__ == '__main__':
    main()
