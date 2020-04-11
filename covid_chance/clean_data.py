import argparse
import json
import logging
import shutil
import sys
from pathlib import Path

from covid_chance.create_tweets import CreateTweets
from covid_chance.download_feeds import simplify_url
from covid_chance.file_utils import safe_filename

logger = logging.getLogger(__name__)


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
    for feed in config['feeds']:
        for page_url in CreateTweets.get_page_urls(args.data, feed['name']):
            old_page_dir = (
                Path(args.data)
                / safe_filename(feed['name'])
                / safe_filename(simplify_url(page_url), max_length=999)
            )
            new_page_dir = (
                Path(args.data)
                / safe_filename(feed['name'])
                / safe_filename(simplify_url(page_url))
            )
            if old_page_dir.exists():
                if new_page_dir.exists():
                    logger.info('rm %s', old_page_dir)
                    shutil.rmtree(old_page_dir)
                else:
                    logger.info('mv %s > %s', old_page_dir, new_page_dir)
                    old_page_dir.rename(new_page_dir)


if __name__ == '__main__':
    main()
