import argparse
import csv
import json
import logging
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional, Set, Tuple, cast

from covid_chance.utils.download_utils import clean_url, simplify_url
from covid_chance.utils.file_utils import safe_filename

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


def move_feed_pages(data_path: str, feed_name: str) -> int:
    feed_dir = Path(data_path) / safe_filename(feed_name)
    missing = 0
    for clean_page_url, page_urls in read_page_urls(feed_dir).items():
        if not move_feed_page(feed_dir, clean_page_url, page_urls):
            missing += 1
    return missing


def move_pages(
    data_path: str, feeds: Iterable[Dict[str, Optional[str]]]
) -> Iterator[Tuple[str, int]]:
    for feed in feeds:
        if feed.get('name'):
            feed_name = cast(str, feed['name'])
            missing = move_feed_pages(data_path, feed_name)
            yield feed_name, missing


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

    stats = move_pages(data_path=args.data, feeds=config['feeds'])

    writer = csv.DictWriter(
        sys.stdout,
        fieldnames=('feed_name', 'n_pages_to_download',),
        quoting=csv.QUOTE_NONNUMERIC,
        lineterminator='\n',
    )
    writer.writeheader()
    writer.writerows(
        {'feed_name': feed_name, 'n_pages_to_download': missing}
        for feed_name, missing in stats
        if missing
    )


if __name__ == '__main__':
    main()
