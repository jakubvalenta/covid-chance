import argparse
import json
import logging
import random
import sys
from string import Template
from typing import Dict, Iterator

import psycopg2
import psycopg2.errorcodes
import twitter

from covid_chance.review_tweets import REVIEW_STATUS_APPROVED, Tweet
from covid_chance.utils.db_utils import db_connect, db_insert

logger = logging.getLogger(__name__)


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
  tweet TEXT,
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


def read_approved_tweets(conn, table: str) -> Iterator[Tweet]:
    cur = conn.cursor()
    cur.execute(
        f"SELECT url, line, parsed, status, edited, inserted FROM {table} "
        "WHERE status = %s",
        (REVIEW_STATUS_APPROVED,),
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


def read_posted_tweets(conn, table: str) -> Iterator[Tweet]:
    cur = conn.cursor()
    cur.execute(
        f"SELECT url, line, parsed, status, edited, inserted FROM {table};",
        (REVIEW_STATUS_APPROVED,),
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


def update_profile(
    name: str, description: str, secrets: Dict[str, str], dry_run: bool = True
):
    if dry_run:
        logger.warning('This is just a dry run, not calling Twitter API')
        return False
    api = twitter.Api(
        consumer_key=secrets['consumer_key'],
        consumer_secret=secrets['consumer_secret'],
        access_token_key=secrets['access_token_key'],
        access_token_secret=secrets['access_token_secret'],
    )
    user = api.UpdateProfile(name=name, description=description)
    logger.warning('Updated profile of user %s', user.name)


def post_tweet(text: str, secrets: Dict[str, str], dry_run: bool = True):
    if dry_run:
        logger.warning('This is just a dry run, not calling Twitter API')
        return False
    api = twitter.Api(
        consumer_key=secrets['consumer_key'],
        consumer_secret=secrets['consumer_secret'],
        access_token_key=secrets['access_token_key'],
        access_token_secret=secrets['access_token_secret'],
    )
    status = api.PostUpdate(status=text)
    logger.warning(
        'Posted tweet "%s" as user %s', status.text, status.user.name
    )
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-c', '--config', help='Configuration file path', required=True
    )
    parser.add_argument(
        '-s', '--secrets', help='Secrets file path', required=True
    )
    parser.add_argument('--dry-run', action='store_true', help='Dry run')
    parser.add_argument(
        '-i',
        '--interactive',
        action='store_true',
        help='Ask before posting the tweet',
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

    conn = db_connect(
        database=config['db']['database'],
        user=config['db']['user'],
        password=config['db']['password'],
    )
    table_reviewed = config['db']['table_reviewed']
    table_posted = config['db']['table_posted']
    create_table(conn, table_posted)

    approved_tweets = list(read_approved_tweets(conn, table_reviewed))
    posted_tweets = list(read_posted_tweets(conn, table_posted))
    posted_tweets_parsed = [t.parsed for t in posted_tweets]
    pending_tweets = [
        t for t in approved_tweets if t.parsed not in posted_tweets_parsed
    ]
    total_approved_tweets = len(approved_tweets)
    total_posted_tweets = len(posted_tweets)
    total_pending_tweets = len(pending_tweets)

    logger.info('Number of approved tweets: %d', total_approved_tweets)
    logger.info('Number of posted tweets:   %d', total_posted_tweets)
    logger.info('Number of tweets to post:  %d', total_pending_tweets)

    if not total_pending_tweets:
        logger.warning('Nothing to do, all tweets have already been posted')
        return

    i = random.randint(0, total_pending_tweets - 1)
    tweet = pending_tweets[i]
    text = Template(config['tweet_template']).substitute(
        text=tweet.text, url=tweet.page_url
    )

    try:
        logger.warning(
            '%d/%d/%d posting tweet "%s"',
            i,
            total_pending_tweets,
            total_approved_tweets,
            text,
        )
        if args.interactive:
            inp = input('Are you sure you want to post this tweet? [y/N] ')
            if inp != 'y':
                print('Bailing out!')
                return
        post_tweet(text, secrets, args.dry_run)

        name = config['profile_name']
        description = Template(
            config['profile_description_template']
        ).substitute(
            n_posted=total_posted_tweets + 1, n_approved=total_approved_tweets
        )
        logger.warning(
            'Updating profile, name: "%s", description: "%s"',
            name,
            description,
        )
        update_profile(name, description, secrets, args.dry_run)
    finally:
        if not args.dry_run:
            db_insert(
                conn,
                table_posted,
                url=tweet.page_url,
                line=tweet.line,
                parsed=tweet.parsed,
                status=tweet.status,
                edited=tweet.edited,
                tweet=text,
            )


if __name__ == '__main__':
    main()
