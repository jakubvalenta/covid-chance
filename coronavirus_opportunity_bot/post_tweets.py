import argparse
import logging
import os
import random
import sys
from pathlib import Path
from typing import Dict, List

from coronavirus_opportunity_bot.review_tweets import (
    REVIEW_STATUS_APPROVED, get_reviewed_tweets_path,
)
from coronavirus_opportunity_bot.tweet_list import TweetList

logger = logging.getLogger(__name__)


def get_posted_tweets_path(data_path: str) -> Path:
    return Path(data_path) / f'posted_tweets.csv'


def post_tweet(tweet: Dict[str, str]):
    logger.warning('POSTING NOW    %s', tweet['tweet'])


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
        '-s', '--single', help='Post a single tweet', action='store_true'
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
        raise ValueError('Auth token is not defined')
    reviewed_tweets = TweetList(get_reviewed_tweets_path(args.data_path))
    posted_tweets = TweetList(get_posted_tweets_path(args.data_path))
    pending_tweets: List[Dict[str, str]] = []
    for tweet in reviewed_tweets:
        if not tweet['status'] == REVIEW_STATUS_APPROVED:
            continue
        if tweet in posted_tweets:
            logger.warning('ALREADY POSTED %s', tweet['tweet'])
            continue
        pending_tweets.append(tweet)
    if not pending_tweets:
        logger.warning('Nothing to do, all tweets have already been posted')
        return
    if args.single:
        random.shuffle(pending_tweets)
        random_tweet = pending_tweets[0]
        post_tweet(random_tweet)
        posted_tweets.append(random_tweet)
        return
    for tweet in pending_tweets:
        post_tweet(tweet)
        posted_tweets.append(tweet)


if __name__ == '__main__':
    main()
