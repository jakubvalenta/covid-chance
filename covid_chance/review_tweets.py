import argparse
import json
import logging
import sys
from pathlib import Path
from textwrap import fill, indent
from typing import Dict, List

from covid_chance.create_tweets import CreateTweets
from covid_chance.tweet_list import TweetList

logger = logging.getLogger(__name__)

REVIEW_STATUS_APPROVED = 'approved'
REVIEW_STATUS_REJECTED = 'rejected'


def get_reviewed_tweets_path(data_path: str) -> Path:
    return Path(data_path) / f'reviewed_tweets.csv'


def print_tweet(
    tweet: Dict[str, str],
    status: str,
    status_width: int = 10,
    separator_width: int = 20,
    line_width: int = 80,
):
    print('-' * separator_width)
    print(status.upper().ljust(status_width) + tweet['tweet'])
    print()
    print(
        indent(
            fill(tweet['line'], line_width - status_width), ' ' * status_width
        )
    )
    print()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-d', '--data-path', help='Data path', default='./data'
    )
    parser.add_argument(
        '-c', '--config-path', help='Configuration file path', required=True
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true', help='Enable debugging output'
    )
    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(
            stream=sys.stderr, level=logging.INFO, format='%(message)s'
        )
    with open(args.config_path, 'r') as f:
        config = json.load(f)
    all_tweets = CreateTweets.read_all_tweets(args.data_path, config['feeds'])
    reviewed_tweets = TweetList(get_reviewed_tweets_path(args.data_path))
    pending_tweets: List[Dict[str, str]] = []
    for tweet in all_tweets:
        if not tweet['tweet']:
            continue
        reviewed_tweet = reviewed_tweets.find(tweet)
        if reviewed_tweet:
            print_tweet(reviewed_tweet, reviewed_tweet['status'])
            continue
        pending_tweets.append(tweet)
    if not pending_tweets:
        logger.warning('Nothing to do, all tweets have already been reviewed')
        return
    for tweet in pending_tweets:
        if tweet in reviewed_tweets:
            print_tweet(tweet, 'reviewed')
            continue
        print_tweet(tweet, 'review')
        inp = None
        while inp is None or (inp not in ('y', 'n', 'e', 'q', 's', '')):
            inp = input(
                'Do you like this tweet? '
                '"y" = yes, '
                '"n" = no, '
                '"e" = edit, '
                '"s" or nothing = skip (ask next time again), '
                '"q" = quit \n'
                '> '
            )
        if inp == 'q':
            break
        if inp in ('s', ''):
            continue
        if inp == 'y':
            status = REVIEW_STATUS_APPROVED
        elif inp == 'n':
            status = REVIEW_STATUS_REJECTED
        elif inp == 'e':
            inp_text = None
            while inp_text is not None:
                inp_text = input('Enter new text: ')  # TODO: Prefill
            tweet['text'] = inp_text
            status = REVIEW_STATUS_APPROVED
        else:
            raise NotImplementedError('Invalid input')
        tweet_with_status = {**tweet, 'status': status}
        reviewed_tweets.append(tweet_with_status)


if __name__ == '__main__':
    main()
