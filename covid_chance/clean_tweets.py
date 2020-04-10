import argparse
import json
import logging
import sys

from covid_chance.create_tweets import CreatePageTweets, CreateTweets

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
            page_tweets_path = CreatePageTweets.get_output_path(
                args.data, feed['name'], page_url
            )
            page_tweets_path.unlink(missing_ok=True)


if __name__ == '__main__':
    main()
