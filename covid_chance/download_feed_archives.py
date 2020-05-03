import argparse
import datetime
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlsplit

import requests

from covid_chance.utils.file_utils import safe_filename

logger = logging.getLogger(__name__)


class ArchiveError(Exception):
    pass


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


def download_feed_archive(
    data_path: str, feed_name: str, feed_url: str, date: datetime.date
):
    output_path = (
        Path(data_path)
        / safe_filename(feed_name)
        / f'feed_archived-{date.isoformat()}.json'
    )
    if output_path.exists():
        return
    archived_feed_url = find_closest_snapshot_url(feed_url, date)
    data = {
        'timestamp': date.isoformat(),
        'url': archived_feed_url,
    }
    with output_path.open('w') as f:
        json.dump(data, f)


def download_feed_archives(
    data_path: str, feeds: List[Dict[str, str]], dates: List[datetime.date]
):
    for feed in feeds:
        if feed.get('name') and feed.get('url'):
            for date in dates:
                download_feed_archive(
                    data_path=data_path,
                    feed_name=feed['name'],
                    feed_url=feed['url'],
                    date=date,
                )


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
    download_feed_archives(
        data_path=args.data,
        feeds=config['feeds'],
        dates=[
            datetime.date.fromisoformat(d) for d in config['archive_dates']
        ],
    )


if __name__ == '__main__':
    main()
