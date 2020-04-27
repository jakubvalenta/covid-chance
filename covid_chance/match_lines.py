import argparse
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, wait
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
  url TEXT,
  line TEXT,
  match_line_hash TEXT,
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


def get_pages(conn, table: str) -> Iterator[tuple]:
    cur = conn.cursor()
    cur.execute(f'SELECT url, text FROM {table};')
    yield from cur
    cur.close()


def contains_any_substr(s: str, substrs: Sequence[str]) -> bool:
    for substr in substrs:
        if substr in s.lower():
            return True
    return False


def contains_all_substr_groups(s: str, groups: Sequence[Sequence[str]]):
    for substrs in groups:
        if not contains_any_substr(s, substrs):
            return False
    return True


def match_page_lines(
    conn,
    table: str,
    i: int,
    page_url: str,
    page_text: str,
    match_line: Sequence[Sequence[str]],
    match_line_hash: str,
):
    if db_select(conn, table, url=page_url, match_line_hash=match_line_hash):
        logger.info('%d done %s', i, page_url)
        return
    logger.warning('%d todo %s', i, page_url)
    cur = conn.cursor()
    inserted = False
    for line in page_text.splitlines():
        if contains_all_substr_groups(line, match_line):
            db_insert(
                conn,
                table,
                cur=cur,
                url=page_url,
                line=line.strip(),
                match_line_hash=match_line_hash,
            )
            inserted = True
    if not inserted:
        db_insert(
            conn,
            table,
            cur=cur,
            url=page_url,
            line='',
            match_line_hash=match_line_hash,
        )
    conn.commit()
    cur.close()


def match_lines(
    conn,
    match_line: Sequence[Sequence[str]],
    table_lines: str,
    table_pages: str,
):
    match_line_hash = hashobj(match_line)
    create_table(conn, table_lines)
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                match_page_lines,
                conn,
                table_lines,
                i,
                page_url,
                page_text,
                match_line,
                match_line_hash,
            )
            for i, (page_url, page_text) in enumerate(
                get_pages(conn, table_pages)
            )
        ]
        wait(futures)


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
    match_lines(
        conn,
        match_line=config['match_line'],
        table_lines=config['db']['table_lines'],
        table_pages=config['db']['table_pages'],
    )
    conn.close()


if __name__ == '__main__':
    main()
