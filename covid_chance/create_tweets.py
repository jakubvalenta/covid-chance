import argparse
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, wait
from string import Template
from typing import Dict, Iterator

import psycopg2
import psycopg2.errorcodes

from covid_chance.db_utils import db_connect, db_insert, db_select
from covid_chance.hash_utils import hashobj

logger = logging.getLogger(__name__)


def create_table(conn, table: str):
    cur = conn.cursor()
    try:
        cur.execute(
            f'''
CREATE TABLE {table} (
  update_id TEXT,
  url TEXT,
  line TEXT,
  parsed TEXT,
  tweet TEXT,
  inserted TIMESTAMP DEFAULT NOW()
);
CREATE INDEX index_{table}_update_id ON {table} (update_id);
'''
        )
    except psycopg2.ProgrammingError as e:
        if e.pgcode == psycopg2.errorcodes.DUPLICATE_TABLE:
            pass
        else:
            raise
    conn.commit()
    cur.close()


def get_parsed(conn, table: str) -> Iterator[tuple]:
    cur = conn.cursor()
    cur.execute(f"SELECT url, line, parsed FROM {table} WHERE parsed != '';")
    yield from cur
    cur.close()


def create_tweet(
    conn,
    table: str,
    i: int,
    page_url: str,
    line: str,
    parsed: str,
    tweet_template: str,
    tmpl: Template,
):
    update_id = hashobj(page_url, line, parsed, tweet_template)
    if db_select(conn, table, update_id=update_id):
        logger.info('%d done %s', i, page_url)
        return
    logger.warning('%d todo %s', i, page_url)
    tweet = tmpl.substitute(parsed=parsed, url=page_url)
    db_insert(
        conn,
        table,
        update_id=update_id,
        url=page_url,
        line=line,
        parsed=parsed,
        tweet=tweet,
    )


def create_tweets(
    conn, tweet_template: str, table_parsed: str, table_tweets: str
):
    tmpl = Template(tweet_template)
    create_table(conn, table_tweets)
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                create_tweet,
                conn,
                table_tweets,
                i,
                page_url,
                line,
                parsed,
                tweet_template,
                tmpl,
            )
            for i, (page_url, line, parsed) in enumerate(
                get_parsed(conn, table_parsed)
            )
        ]
        wait(futures)


def read_all_tweets(conn, table: str) -> Iterator[Dict[str, str]]:
    cur = conn.cursor()
    cur.execute(f'SELECT url, line, parsed, tweet FROM {table};')
    for url, line, parsed, tweet in cur:
        yield {'url': url, 'line': line, 'parsed': parsed, 'tweet': tweet}
    cur.close()


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
        host=config['db']['host'],
        database=config['db']['database'],
        user=config['db']['user'],
        password=config['db']['password'],
    )
    create_tweets(
        conn,
        tweet_template=config['tweet_template'],
        table_parsed=config['db']['table_parsed'],
        table_tweets=config['db']['table_tweets'],
    )
    conn.close()


if __name__ == '__main__':
    main()
