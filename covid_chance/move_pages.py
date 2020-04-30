import argparse
import csv
import json
import logging
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, Optional, Set

from covid_chance.download_feeds import clean_url, safe_filename
from covid_chance.download_pages import simplify_url

logger = logging.getLogger(__name__)


def read_page_urls(feed_dir: Path) -> Dict[str, Set[str]]:
    page_urls = defaultdict(set)
    for p in feed_dir.glob('feed_pages*.csv'):
        with p.open('r') as f:
            for (page_url,) in csv.reader(f):
                clean_page_url = clean_url(page_url)
                page_urls[clean_page_url].add(page_url)
    return page_urls


def move_feed_page(feed_dir: Path, clean_page_url: str, page_urls: Set[str]):
    clean_page_dir = feed_dir / safe_filename(simplify_url(clean_page_url))
    if clean_page_dir.exists():
        return True
    for page_url in page_urls:
        page_dir = feed_dir / safe_filename(simplify_url(page_url))
        page_content = page_dir / 'page_content.html'
        if page_content.is_file():
            logger.info('Moving %s to %s', page_dir, clean_page_dir)
            os.rename(page_dir, clean_page_dir)
            return True
    return False


def move_feed_pages(data_path: str, feed_name: Optional[str]):
    if not feed_name:
        return
    feed_dir = Path(data_path) / safe_filename(feed_name)
    for clean_page_url, page_urls in read_page_urls(feed_dir).items():
        move_feed_page(feed_dir, clean_page_url, page_urls)


def move_pages(data_path: str, feeds: Iterable[Dict[str, Optional[str]]]):
    for feed in feeds:
        move_feed_pages(data_path, feed.get('name'))


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
    move_pages(data_path=args.data, feeds=config['feeds'])


if __name__ == '__main__':
    main()
