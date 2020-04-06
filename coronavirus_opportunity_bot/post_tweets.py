import argparse
import logging
import os
import sys
from pathlib import Path

from coronavirus_opportunity_bot.review_tweets import (
    REVIEW_STATUS_APPROVED, get_reviewed_tweets_path,
)
from coronavirus_opportunity_bot.tweet_list import TweetList

logger = logging.getLogger(__name__)


def get_posted_tweets_path(data_path: str) -> Path:
    return Path(data_path) / f'posted_tweets.csv'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-d', '--data-path', help='Data path', default='./data'
    )
    parser.add_argument(
        '-t',
        '--auth-token',
        help='Twitter authentication token',
        default=os.environ.get('AUTH_TOKEN'),
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true', help='Enable debugging output'
    )
    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(
            stream=sys.stderr, level=logging.INFO, format='%(message)s'
        )
    if not args.auth_token:
        ValueError('Auth token is not defined')
    reviewed_tweets = TweetList(get_reviewed_tweets_path(args.data_path))
    posted_tweets = TweetList(get_posted_tweets_path(args.data_path))
    for tweet in reviewed_tweets:
        if not tweet['status'] == REVIEW_STATUS_APPROVED:
            continue
        if tweet in posted_tweets:
            logger.warn('ALREADY POSTED %s', tweet['tweet'])
            continue
        logger.warn('POSTING NOW    %s', tweet['tweet'])
        posted_tweets.append(tweet)
        # TODO: post


if __name__ == '__main__':
    main()
