import argparse
import logging
import sys

from coronavirus_opportunity_bot.create_tweets import (
    CreatePageTweets, CreateTweets,
)

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-d', '--data-path', help='Data path', default='./data'
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true', help='Enable debugging output'
    )
    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(
            stream=sys.stderr, level=logging.INFO, format='%(message)s'
        )
    for feed_name, page_url in CreateTweets.read_all_page_urls(args.data_path):
        page_tweets_path = CreatePageTweets.get_output_path(
            args.data_path, feed_name, page_url
        )
        page_tweets_path.unlink(missing_ok=True)


if __name__ == '__main__':
    main()
