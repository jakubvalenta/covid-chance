import argparse
import json
import logging
import sys
from pathlib import Path
from textwrap import fill, indent
from typing import Dict, Optional

import colored

from covid_chance.create_tweets import CreateTweets
from covid_chance.tweet_list import TweetList

logger = logging.getLogger(__name__)

REVIEW_STATUS_APPROVED = 'approved'
REVIEW_STATUS_REJECTED = 'rejected'


def highlight_substr(s: str, substr: str, fg_color: int = 2) -> str:
    fg = colored.fg(fg_color)
    reset = colored.attr(0)
    return s.replace(substr, rf'{fg}{substr}{reset}')


def get_reviewed_tweets_path(data_path: str) -> Path:
    return Path(data_path) / f'reviewed_tweets.csv'


def print_tweet(
    tweet: Dict[str, str],
    status: str,
    i: Optional[int] = None,
    total: Optional[int] = None,
    highlight: bool = False,
    counter_width: int = 7,
    status_width: int = 10,
    separator_width: int = 20,
    line_width: int = 80,
):
    print('-' * separator_width)
    counter = '/'.join(
        str(num) for num in (i, total) if num is not None
    ).ljust(counter_width)
    status = status.upper().ljust(status_width)
    text = tweet['tweet']
    print(''.join([counter, status, text]))
    print()
    if highlight:
        s = highlight_substr(tweet['line'], tweet['parsed'])
    else:
        s = tweet['line']
    print(
        indent(
            fill(s, line_width - status_width),
            ' ' * (counter_width + status_width),
        )
    )
    print()


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
    all_tweets = list(CreateTweets.read_all_tweets(args.data, config['feeds']))
    logger.info('Number of text lines that match pattern: %d', len(all_tweets))
    logger.info(
        'Number of all tweets:                    %d',
        len([tweet for tweet in all_tweets if tweet['tweet']]),
    )
    reviewed_tweets = TweetList(get_reviewed_tweets_path(args.data))
    logger.info(
        'Number of approved tweets:               %d',
        len(
            [
                tweet
                for tweet in reviewed_tweets
                if tweet['status'] == REVIEW_STATUS_APPROVED
            ]
        ),
    )
    logger.info(
        'Number of rejected tweets:               %d',
        len(
            [
                tweet
                for tweet in reviewed_tweets
                if tweet['status'] == REVIEW_STATUS_REJECTED
            ]
        ),
    )
    pending_tweets = [
        tweet
        for tweet in all_tweets
        if tweet['tweet'] and not reviewed_tweets.find(tweet)
    ]
    logger.info(
        'Number of tweets to review:              %d', len(pending_tweets)
    )
    if not pending_tweets:
        return
    total_pending_tweets = len(pending_tweets)
    for i, tweet in enumerate(pending_tweets):
        if tweet in reviewed_tweets:
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
