import argparse
import csv
import datetime
import json
import logging
import random
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterator, Optional, Sequence, Set, Tuple, Type

import psycopg2
import psycopg2.errorcodes
import requests
from bs4 import (
    BeautifulSoup, CData, Comment, Declaration, Doctype, NavigableString,
    ProcessingInstruction,
)
from bs4.element import Script, Stylesheet, TemplateString

from covid_chance.utils.db_utils import db_connect, db_insert
from covid_chance.utils.download_utils import simplify_url
from covid_chance.utils.file_utils import safe_filename

logger = logging.getLogger(__name__)


def create_table(conn, table: str):
    cur = conn.cursor()
    try:
        cur.execute(
            f'''
CREATE TABLE {table} (
  url text UNIQUE,
  text text,
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


def download_page(
    url: str,
    wait_interval: Tuple[int, int] = (0, 0),
    timeout: int = 10,
    non_recoverable_error_codes: Sequence[int] = (
        requests.codes.not_found,
        requests.codes.gone,
        requests.codes.legal_reasons,
    ),
) -> str:
    if url.startswith('urn:'):
        logger.error('Unsupported scheme')
        return ''
    wait = random.randint(*wait_interval)
    if wait:
        logger.info('Waiting for %ds', wait)
        time.sleep(wait)
    try:
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
    except requests.TooManyRedirects:
        logger.error('Too many redirects')
        return ''
    if res.status_code in non_recoverable_error_codes:
        logger.error('Non-recoverable error encountered')
        return ''
    res.raise_for_status()
    return res.text


def get_element_text(
    soup: BeautifulSoup,
    ignore_tags: Sequence[str] = ('style', 'script'),
    ignore_classes: Sequence[Type] = (
        CData,
        Comment,
        Declaration,
        Doctype,
        ProcessingInstruction,
        Script,
        Stylesheet,
        TemplateString,
    ),
    block_elements: Sequence[str] = (
        'address',
        'article',
        'aside',
        'blockquote',
        'details',
        'dialog',
        'dd',
        'div',
        'dl',
        'dt',
        'fieldset',
        'figcaption',
        'figure',
        'footer',
        'form',
        'h1',
        'h2',
        'h3',
        'h4',
        'h5',
        'h6',
        'header',
        'hgroup',
        'hr',
        'li',
        'main',
        'nav',
        'ol',
        'p',
        'pre',
        'section',
        'table',
        'ul',
    ),
) -> Iterator[str]:
    if type(soup) not in ignore_classes and soup.name not in ignore_tags:
        if type(soup) == NavigableString:
            yield soup.string
        else:
            if soup.name in block_elements:
                yield '\n'
            for child in soup.children:
                yield from get_element_text(child, ignore_tags=ignore_tags)


def clean_whitespace(s: str) -> str:
    s = re.sub(r'[^\S\n\r]+', ' ', s)
    s = re.sub(r'\s*\n\s*', '\n', s)
    return s.strip()


def get_page_text(html: str) -> str:
    if not html:
        return ''
    soup = BeautifulSoup(html, 'lxml')
    text = ''.join(get_element_text(soup))
    return clean_whitespace(text)


def download_page_text(
    data_path: str,
    feed_name: str,
    page_url: str,
    wait_interval: Tuple[int, int],
    timeout: int,
) -> Optional[str]:
    path = (
        Path(data_path)
        / safe_filename(feed_name)
        / safe_filename(simplify_url(page_url))
        / 'page_content.txt'
    )
    if path.is_file():
        logger.info('Reading from cache %s', page_url)
        return path.read_text()
    try:
        html = download_page(
            page_url, wait_interval=wait_interval, timeout=timeout,
        )
    except Exception:
        logger.error('Failed to download %s', page_url)
        return None
    text = get_page_text(html)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return text


def select_page_urls_to_download(
    conn, table_urls: str, table_pages: str, since: datetime.datetime
) -> Dict[str, Set[str]]:
    cur = conn.cursor()
    cur.execute(
        'SELECT u.feed_name, u.url '
        f'FROM {table_urls} u LEFT JOIN {table_pages} p ON u.url = p.url '
        'WHERE p.url IS NULL AND u.inserted >= %s ORDER BY u.inserted;',
        (since,),
    )
    page_urls_by_feeds = defaultdict(set)
    for feed_name, page_url in cur:
        page_urls_by_feeds[feed_name].add(page_url)
    return page_urls_by_feeds


def print_stats(page_urls_by_feed: Dict[str, Set[str]]):
    writer = csv.DictWriter(
        sys.stdout,
        fieldnames=('feed_name', 'n_pages'),
        quoting=csv.QUOTE_NONNUMERIC,
        lineterminator='\n',
    )
    writer.writeheader()
    for feed_name, page_urls in page_urls_by_feed.items():
        if page_urls:
            writer.writerow(
                {'feed_name': feed_name, 'n_pages': len(page_urls)}
            )


def download_pages(
    db: dict,
    table_urls: str,
    table_pages: str,
    data_path: str,
    since: datetime.datetime,
    wait_interval: Tuple[int, int],
    timeout: int,
    dry_run: bool,
):
    conn = db_connect(
        host=db['host'],
        database=db['database'],
        user=db['user'],
        password=db['password'],
    )
    create_table(conn, table_pages)
    logger.info('Selecting pages to download since %s', since)
    page_urls_by_feeds = select_page_urls_to_download(
        conn, table_urls, table_pages, since
    )
    print_stats(page_urls_by_feeds)

    page_urls_with_feed_names = []
    for feed_name, page_urls in page_urls_by_feeds.items():
        for page_url in page_urls:
            page_urls_with_feed_names.append((page_url, feed_name))
    total = len(page_urls_with_feed_names)
    logger.info('Pages to download: %d', total)
    if dry_run:
        logger.warning('This is just a dry run, not downloading any pages')
        return

    random.shuffle(page_urls_with_feed_names)
    for i, (page_url, feed_name) in enumerate(page_urls_with_feed_names):
        logger.info('%d/%d Downloading %s', i + 1, total, page_url)
        text = download_page_text(
            data_path=data_path,
            feed_name=feed_name,
            page_url=page_url,
            wait_interval=wait_interval,
            timeout=timeout,
        )
        if text is not None:
            db_insert(conn, table_pages, url=page_url, text=text)
    conn.commit()
    conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--data', help='Data path', default='./data')
    parser.add_argument(
        '-c', '--config', help='Configuration file path', required=True
    )
    parser.add_argument('--dry-run', action='store_true', help='Dry run')
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
    download_pages(
        db=config['db'],
        table_urls=config['db']['table_urls'],
        table_pages=config['db']['table_pages'],
        data_path=args.data,
        since=datetime.datetime.fromisoformat(config['download_pages_since']),
        wait_interval=(
            int(config['download_page_wait_interval'][0]),
            int(config['download_page_wait_interval'][1]),
        ),
        timeout=int(config['download_page_timeout']),
        dry_run=args.dry_run,
    )


if __name__ == '__main__':
    main()
