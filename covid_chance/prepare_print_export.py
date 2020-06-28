import argparse
import json
import logging
import sys

import psycopg2
import psycopg2.errorcodes

from covid_chance.post_tweet import read_approved_tweets
from covid_chance.utils.db_utils import db_connect

logger = logging.getLogger(__name__)


def create_table(conn, table: str):
    cur = conn.cursor()
    try:
        cur.execute(
            f'''
CREATE TABLE {table} (
  tweet TEXT,
  image TEXT,
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


def main():
    parser = argparse.ArgumentParser()
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
    table_print = config['db']['table_print']

    create_table(conn, table_print)
    approved_tweets = list(read_approved_tweets(conn, table_reviewed))

    for i, tweet in enumerate(approved_tweets):
        logger.info('%d %s %s', i, tweet.inserted, tweet.text)


if __name__ == '__main__':
    main()
