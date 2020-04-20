import argparse
import json
import logging
import sys
from pathlib import Path

from covid_chance.db_utils import db_connect
from covid_chance.review_tweets import (
    Tweet, create_table, write_reviewed_tweet,
)
from covid_chance.tweet_list import TweetList

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

    conn = db_connect(
        database=config['db']['database'],
        user=config['db']['user'],
        password=config['db']['password'],
    )
    table_reviewed = config['db']['table_reviewed']

    create_table(conn, table_reviewed)
    reviewed_tweets_path = Path(args.data) / f'reviewed_tweets.csv'
    reviewed_tweets = TweetList(reviewed_tweets_path)
    for i, tweet_dict in enumerate(reviewed_tweets):
        logger.info('%s copy %s', i, tweet_dict['url'])
        tweet = Tweet(
            page_url=tweet_dict['url'],
            line=tweet_dict['line'],
            parsed=tweet_dict['parsed'],
            status=tweet_dict['status'],
            edited='',
            inserted=tweet_dict['added'],
        )
        write_reviewed_tweet(conn, table_reviewed, tweet)


if __name__ == '__main__':
    main()
