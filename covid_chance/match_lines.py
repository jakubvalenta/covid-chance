import argparse
import concurrent.futures
import json
import logging
import sys
from typing import Iterator, Sequence

import psycopg2
import psycopg2.errorcodes

from covid_chance.utils.db_utils import db_connect, db_insert, db_select
from covid_chance.utils.hash_utils import hashobj

logger = logging.getLogger(__name__)


def create_table(conn, table: str):
    cur = conn.cursor()
    try:
        cur.execute(
            f'''
CREATE TABLE {table} (
  url text,
  line text,
  param_hash text,
  inserted timestamp DEFAULT NOW()
);
CREATE INDEX index_{table}_url ON {table} (url);
CREATE INDEX index_{table}_param_hash ON {table} (param_hash);
'''
        )
    except psycopg2.ProgrammingError as e:
        if e.pgcode == psycopg2.errorcodes.DUPLICATE_TABLE:
            pass
        else:
            raise
    conn.commit()
    cur.close()


def get_pages(conn, table: str) -> Iterator[tuple]:
    cur = conn.cursor()
    cur.execute(f'SELECT url, text FROM {table};')
    yield from cur
    cur.close()


def contains_any_keyword(s: str, keyword_list: Sequence[str]) -> bool:
    s_lower = s.lower()
    for keyword in keyword_list:
        if keyword in s_lower:
            return True
    return False


def contains_keyword_from_each_list(
    s: str, keyword_lists: Sequence[Sequence[str]]
):
    for keyword_list in keyword_lists:
        if not contains_any_keyword(s, keyword_list):
            return False
    return True


def match_page_lines(
    conn,
    table: str,
    i: int,
    page_url: str,
    page_text: str,
    keyword_lists: Sequence[Sequence[str]],
    param_hash: str,
):
    if db_select(conn, table, url=page_url, param_hash=param_hash):
        return
    logger.info('%d Matched %s', i, page_url)
    cur = conn.cursor()
    inserted = False
    for line in page_text.splitlines():
        if contains_keyword_from_each_list(line, keyword_lists):
            db_insert(
                conn,
                table,
                cur=cur,
                url=page_url,
                line=line.strip(),
                param_hash=param_hash,
            )
            inserted = True
    if not inserted:
        db_insert(
            conn, table, cur=cur, url=page_url, line='', param_hash=param_hash,
        )
    conn.commit()
    cur.close()


def match_lines(
    db: dict,
    keyword_lists: Sequence[Sequence[str]],
    table_lines: str,
    table_pages: str,
):
    conn = db_connect(
        host=db['host'],
        database=db['database'],
        user=db['user'],
        password=db['password'],
    )
    param_hash = hashobj(keyword_lists)
    create_table(conn, table_lines)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                match_page_lines,
                conn,
                table_lines,
                i,
                page_url,
                page_text,
                keyword_lists,
                param_hash,
            )
            for i, (page_url, page_text) in enumerate(
                get_pages(conn, table_pages)
            )
        ]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error('Exception: %s', e)
    conn.close()


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
    match_lines(
        db=config['db'],
        keyword_lists=config['keyword_lists'],
        table_lines=config['db']['table_lines'],
        table_pages=config['db']['table_pages'],
    )


if __name__ == '__main__':
    main()
