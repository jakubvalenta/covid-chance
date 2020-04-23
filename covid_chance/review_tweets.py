import argparse
import json
import logging
import readline
import sys
from dataclasses import dataclass
from textwrap import fill
from typing import Iterator, Optional

import colored
import psycopg2
import psycopg2.errorcodes

from covid_chance.db_utils import db_connect, db_count, db_update_or_insert

logger = logging.getLogger(__name__)

REVIEW_STATUS_APPROVED = 'approved'
REVIEW_STATUS_REJECTED = 'rejected'


@dataclass
class Tweet:
    page_url: str
    line: str
    parsed: str
    status: str
    edited: str
    inserted: Optional[str]

    @property
    def text(self) -> str:
        return self.edited or self.parsed


def create_table(conn, table: str):
    cur = conn.cursor()
    try:
        cur.execute(
            f'''
CREATE TABLE {table} (
  url TEXT,
  line TEXT,
  parsed TEXT,
  status TEXT,
  edited TEXT,
  inserted TIMESTAMP DEFAULT NOW()
);
'''
        )
    except psycopg2.ProgrammingError as e:
        if e.pgcode == psycopg2.errorcodes.DUPLICATE_TABLE:
            pass
        else:
            raise
    conn.commit()
    cur.close()


def read_tweets(conn, table: str) -> Iterator[Tweet]:
    cur = conn.cursor()
    cur.execute(f"SELECT url, line, parsed FROM {table} WHERE parsed != '';")
    for url, line, parsed in cur:
        yield Tweet(
            page_url=url,
            line=line,
            parsed=parsed,
            status='',
            edited='',
            inserted=None,
        )
    cur.close()


def read_reviewed_tweets(conn, table: str) -> Iterator[Tweet]:
    cur = conn.cursor()
    cur.execute(
        f"SELECT url, line, parsed, status, edited, inserted FROM {table};"
    )
    for url, line, parsed, status, edited, inserted in cur:
        yield Tweet(
            page_url=url,
            line=line,
            parsed=parsed,
            status=status,
            edited=edited,
            inserted=inserted,
        )
    cur.close()


def write_reviewed_tweet(conn, table: str, tweet: Tweet):
    where = {
        'url': tweet.page_url,
        'line': tweet.line,
        'parsed': tweet.parsed,
    }
    values = {
        'status': tweet.status,
        'edited': tweet.edited,
    }
    if tweet.inserted:
        values['inserted'] = tweet.inserted
    db_update_or_insert(conn, table, where, **values)


def rlinput(prompt, prefill: str = '') -> Optional[str]:
    """See https://stackoverflow.com/a/36607077"""
    readline.set_startup_hook(lambda: readline.insert_text(prefill))
    try:
        return input(prompt)
    finally:
        readline.set_startup_hook()


def highlight_substr(s: str, substr: str, fg_color: int = 2) -> str:
    return s.replace(substr, colored.stylize(substr, colored.fg(fg_color)))


def print_tweet(
    tweet: Tweet,
    i: Optional[int] = None,
    total: Optional[int] = None,
    max_tweet_length: int = 280,
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
    print(tweet.status.upper(), end='')
    if len(tweet.text) > max_tweet_length:
        print(
            '  ',
            colored.stylize(
                f'too long ({len(tweet.text)}/{max_tweet_length})',
                colored.fg('red'),
            ),
            end='',
        )
    print()
    print()
    print(tweet.page_url)
    print()
    if highlight:
        s = highlight_substr(tweet.line, tweet.parsed)
    else:
        s = tweet.line
    print(fill(s, line_width))
    print()
    if tweet.edited:
        print(colored.stylize(tweet.edited, colored.fg('red')))
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
        help='Review all already reviewed tweets again',
    )
    parser.add_argument(
        '-p',
        '--approved',
        action='store_true',
        help='Review approved tweets again',
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
    table_lines = config['db']['table_lines']
    table_parsed = config['db']['table_parsed']
    table_reviewed = config['db']['table_reviewed']
    create_table(conn, table_reviewed)

    tweets = list(read_tweets(conn, table_parsed))
    reviewed_tweets = list(read_reviewed_tweets(conn, table_reviewed))
    approved_tweets = [
        t for t in reviewed_tweets if t.status == REVIEW_STATUS_APPROVED
    ]
    rejected_tweets = [
        t for t in reviewed_tweets if t.status == REVIEW_STATUS_REJECTED
    ]
    invalid_approved_tweets = [
        t for t in approved_tweets if len(t.text) > config['max_tweet_length']
    ]
    if args.all:
        pending_tweets = tweets
    else:
        reviewed_tweets_parsed = [t.parsed for t in reviewed_tweets]
        pending_tweets = [
            t for t in tweets if t.parsed not in reviewed_tweets_parsed
        ]
        if args.approved:
            pending_tweets += approved_tweets
        else:
            pending_tweets += invalid_approved_tweets
    total_pending_tweets = len(pending_tweets)

    logger.info('Number of matching lines:   %d', db_count(conn, table_lines))
    logger.info('Number of all tweets:       %d', len(tweets))
    logger.info('Number of approved tweets:  %d', len(approved_tweets))
    logger.info('Number of rejected tweets:  %d', len(rejected_tweets))
    logger.info('Number of tweets to review: %d', total_pending_tweets)

    for i, tweet in enumerate(pending_tweets):
        print_tweet(
            tweet,
            i=i + 1,
            total=total_pending_tweets,
            max_tweet_length=config['max_tweet_length'],
            highlight=True,
        )
        inp = None
        while inp is None or (inp not in ('y', 'n', 'e', 'q', 's', '')):
            inp = rlinput(
                'Do you like this tweet? '
                '"y" or Enter = yes, '
                '"n" = no, '
                '"e" = edit, '
                '"s" = skip (ask next time again), '
                '"q" = quit \n'
                '> '
            )
        if inp == 'q':
            break
        if inp == 's':
            continue
        if inp in ('y' or ''):
            tweet.status = REVIEW_STATUS_APPROVED
        elif inp == 'n':
            tweet.status = REVIEW_STATUS_REJECTED
        elif inp == 'e':
            edited_text = None
            while edited_text is None:
                edited_text = rlinput(
                    'Enter new text or delete it to reject the tweet.\n> ',
                    tweet.edited or tweet.parsed,
                )
            tweet.edited = edited_text
            if edited_text == '':
                tweet.status = REVIEW_STATUS_REJECTED
            else:
                tweet.status = REVIEW_STATUS_APPROVED
        else:
            raise NotImplementedError('Invalid input')
        write_reviewed_tweet(conn, table_reviewed, tweet)


if __name__ == '__main__':
    main()
