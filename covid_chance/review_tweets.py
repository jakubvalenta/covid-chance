import argparse
import json
import logging
import readline
import sys
from pathlib import Path
from textwrap import fill
from typing import Dict, Iterator, Optional

import colored

from covid_chance.db_utils import db_connect, db_count
from covid_chance.tweet_list import TweetList

logger = logging.getLogger(__name__)

REVIEW_STATUS_APPROVED = 'approved'
REVIEW_STATUS_REJECTED = 'rejected'


def rlinput(prompt, prefill: str = '') -> Optional[str]:
    """See https://stackoverflow.com/a/36607077"""
    readline.set_startup_hook(lambda: readline.insert_text(prefill))
    try:
        return input(prompt)
    finally:
        readline.set_startup_hook()


def highlight_substr(s: str, substr: str, fg_color: int = 2) -> str:
    return s.replace(substr, colored.stylize(substr, colored.fg(fg_color)))


def get_reviewed_tweets_path(data_path: str) -> Path:
    return Path(data_path) / f'reviewed_tweets.csv'


def read_all_tweets(conn, table: str) -> Iterator[Dict[str, str]]:
    cur = conn.cursor()
    cur.execute(f"SELECT url, line, parsed FROM {table} WHERE parsed != '';")
    for url, line, parsed in cur:
        yield {'url': url, 'line': line, 'parsed': parsed}
    cur.close()


def print_tweet(
    tweet: Dict[str, str],
    status: str,
    i: Optional[int] = None,
    total: Optional[int] = None,
    highlight: bool = False,
    counter_width: int = 10,
    line_width: int = 80,
):
    print('-' * line_width)
    print(
        '/'.join(str(num) for num in (i, total) if num is not None).ljust(
            counter_width
        ),
        end='',
    )
    print(status.upper())
    print()
    print(tweet['url'])
    print()
    if highlight:
        s = highlight_substr(tweet['line'], tweet['parsed'])
    else:
        s = tweet['line']
    print(fill(s, line_width))
    print()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--data', help='Data path', default='./data')
    parser.add_argument(
        '-c', '--config', help='Configuration file path', required=True
    )
    parser.add_argument(
        '-a',
        '--all',
        action='store_true',
        help='Review already reviewed tweets again',
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
    match_lines_count = db_count(conn, config['db']['table_lines'])
    tweets = list(read_all_tweets(conn, config['db']['table_parsed']))
    logger.info('Number of matching lines: %d', match_lines_count)
    logger.info('Number of all tweets: %d', len(tweets))
    reviewed_tweets = TweetList(get_reviewed_tweets_path(args.data))
    logger.info(
        'Number of approved tweets: %d',
        len(
            [
                tweet
                for tweet in reviewed_tweets
                if tweet['status'] == REVIEW_STATUS_APPROVED
            ]
        ),
    )
    logger.info(
        'Number of rejected tweets: %d',
        len(
            [
                tweet
                for tweet in reviewed_tweets
                if tweet['status'] == REVIEW_STATUS_REJECTED
            ]
        ),
    )
    if args.all:
        pending_tweets = tweets
    else:
        pending_tweets = [
            tweet for tweet in tweets if not reviewed_tweets.find(tweet)
        ]
    logger.info('Number of tweets to review: %d', len(pending_tweets))
    if not pending_tweets:
        return
    total_pending_tweets = len(pending_tweets)
    for i, tweet in enumerate(pending_tweets):
        if not args.all and tweet in reviewed_tweets:
            print_tweet(tweet, 'reviewed', i=i + 1, total=total_pending_tweets)
            continue
        print_tweet(
            tweet,
            'review',
            i=i + 1,
            total=total_pending_tweets,
            highlight=True,
        )
        inp = None
        while inp is None or (inp not in ('y', 'n', 'e', 'q', 's', '')):
            inp = rlinput(
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
            edited_text = None
            while not edited_text:
                edited_text = rlinput('Enter new text: \n> ', tweet['parsed'])
            tweet['edited'] = edited_text
            status = REVIEW_STATUS_APPROVED
        else:
            raise NotImplementedError('Invalid input')
        tweet_with_status = {**tweet, 'status': status}
        reviewed_tweets.append(tweet_with_status)


if __name__ == '__main__':
    main()
