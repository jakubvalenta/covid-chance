import argparse
import logging
import sys
from pathlib import Path

from coronavirus_opportunity_bot.create_tweets import CreateTweets
from coronavirus_opportunity_bot.tweet_list import TweetList

logger = logging.getLogger(__name__)

REVIEW_STATUS_APPROVED = 'approved'
REVIEW_STATUS_REJECTED = 'rejected'


def get_reviewed_tweets_path(data_path: str) -> Path:
    return Path(data_path) / f'reviewed_tweets.csv'


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
    all_tweets = CreateTweets.read_all_tweets(args.data_path)
    reviewed_tweets = TweetList(get_reviewed_tweets_path(args.data_path))
    for tweet in all_tweets:
        if not tweet['tweet']:
            continue
        reviewed_tweet = reviewed_tweets.find(tweet)
        if reviewed_tweet:
            print('----------')
            print(reviewed_tweet['status'].upper(), tweet['tweet'])
            continue
        print('----------')
        print('REVIEW', tweet['tweet'])
        print('      ', tweet['line'])
        inp = None
        while inp is None or (inp not in ('y', 'n', 'e', 'q', '')):
            inp = input(
                'Do you like this tweet? '
                '"y" = yes, '
                '"n" or nothing = no, '
                '"e" = edit, '
                '"s" = skip (ask next time again), '
                '"q" = quit \n'
                '> '
            )
        if inp == 'q':
            break
        if inp == 's':
            continue
        if inp == 'y':
            status = REVIEW_STATUS_APPROVED
        elif inp in ('n', ''):
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
