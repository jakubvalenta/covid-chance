import argparse
import json
import logging
import random
import sys
from pathlib import Path
from string import Template
from typing import Dict, List

import twitter

from covid_chance.review_tweets import (
    REVIEW_STATUS_APPROVED, get_reviewed_tweets_path,
)
from covid_chance.tweet_list import TweetList

logger = logging.getLogger(__name__)


def get_posted_tweets_path(data_path: str) -> Path:
    return Path(data_path) / f'posted_tweets.csv'


def format_tweet_text(parsed: str, page_url: str, template_str: str) -> str:
    return Template(template_str).substitute(parsed=parsed, url=page_url)


def post_tweet(tweet_text: str, secrets: Dict[str, str], dry_run: bool = True):
    logger.warning('POSTING NOW    %s', tweet_text)
    if dry_run:
        logger.warning('This is just a dry run, not calling Twitter API')
        return False
    api = twitter.Api(
        consumer_key=secrets['consumer_key'],
        consumer_secret=secrets['consumer_secret'],
        access_token_key=secrets['access_token_key'],
        access_token_secret=secrets['access_token_secret'],
    )
    status = api.PostUpdate(status=tweet_text)
    logger.warning(
        'Posted tweet "%s" as user %s', status.test, status.user.name
    )
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--data', help='Data path', default='./data')
    parser.add_argument(
        '-c', '--config', help='Configuration file path', required=True
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
        '--dry-run', action='store_true', default=True, help='Dry run'
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
    with open(args.secrets, 'r') as f:
        secrets = json.load(f)
    reviewed_tweets = TweetList(get_reviewed_tweets_path(args.data))
    posted_tweets = TweetList(get_posted_tweets_path(args.data))
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
        tweet_text = format_tweet_text(random_tweet, config['tweet_template'])
        post_tweet(tweet_text, secrets, args.dry_run)
        posted_tweets.append(random_tweet)
        return
    for tweet in pending_tweets:
        if tweet in posted_tweets:
            logger.warning('JUST POSTED    %s', tweet['tweet'])
            continue
        tweet_text = format_tweet_text(tweet, config['tweet_template'])
        post_tweet(tweet_text, secrets, args.dry_run)
        posted_tweets.append(tweet)


if __name__ == '__main__':
    main()
