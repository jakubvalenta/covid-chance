import argparse
import json
import logging
import random
import sys
from pathlib import Path
from typing import Dict, List

import twitter

from covid_chance.review_tweets import (
    REVIEW_STATUS_APPROVED, get_reviewed_tweets_path,
)
from covid_chance.tweet_list import TweetList

logger = logging.getLogger(__name__)


def get_posted_tweets_path(data_path: str) -> Path:
    return Path(data_path) / f'posted_tweets.csv'


def post_tweet(
    tweet: Dict[str, str], secrets: Dict[str, str], dry_run: bool = True
):
    logger.warning('POSTING NOW    %s', tweet['tweet'])
    if dry_run:
        logger.warning('This is just a dry run, not calling Twitter API')
        return False
    api = twitter.Api(
        consumer_key=secrets['consumer_key'],
        consumer_secret=secrets['consumer_secret'],
        access_token_key=secrets['access_token'],
        access_token_secret=secrets['access_token_secret'],
    )
    status = api.PostUpdate(status=tweet['text'])
    logger.warning(
        'Posted tweet "%s" as user %s', status.test, status.user.name
    )
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-d', '--data-path', help='Data path', default='./data'
    )
    parser.add_argument(
        '-s', '--secrets', help='Secrets file path', required=True
    )
    parser.add_argument(
        '-o',
        '--one',
        help='Post a single randomly selected tweet',
        action='store_true',
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true', help='Enable debugging output'
    )
    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(
            stream=sys.stderr, level=logging.INFO, format='%(message)s'
        )
    with open(args.secrets, 'r') as f:
        secrets = json.load(f)
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
        post_tweet(random_tweet, secrets)
        posted_tweets.append(random_tweet)
        return
    for tweet in pending_tweets:
        if tweet in posted_tweets:
            logger.warning('JUST POSTED    %s', tweet['tweet'])
            continue
        post_tweet(tweet)
        posted_tweets.append(tweet)


if __name__ == '__main__':
    main()
