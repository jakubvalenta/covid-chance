import argparse
import concurrent.futures
import json
import logging
import sys
from typing import Iterator, Tuple

import psycopg2
import psycopg2.errorcodes
import regex

from covid_chance.utils.db_utils import db_connect, db_insert, db_select
from covid_chance.utils.hash_utils import hashobj

logger = logging.getLogger(__name__)


def create_table(conn, table: str):
    with conn.cursor() as cur:
        try:
            cur.execute(
                f'''
CREATE TABLE {table} (
  url text,
  line text,
  parsed text,
  param_hash text,
  inserted timestamp DEFAULT NOW()
);
CREATE INDEX index_{table}_parsed ON {table} (parsed);
CREATE INDEX index_{table}_param_hash ON {table} (param_hash);
'''
            )
        except psycopg2.ProgrammingError as e:
            if e.pgcode == psycopg2.errorcodes.DUPLICATE_TABLE:
                pass
            else:
                raise
        conn.commit()


def searchall(rx: regex.Regex, s: str) -> Iterator:
    m = rx.search(s)
    while m:
        yield m
        m = rx.search(s, pos=m.span()[1])


def parse_line(rx, line: str) -> Iterator[Tuple[str, str]]:
    matches = list(searchall(rx, line))
    if matches:
        for m in matches:
            yield (line, m.group('parsed'))
    else:
        yield line, ''


def get_lines(conn, table: str) -> Iterator[tuple]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT url, line FROM {table} WHERE line != '';")
        yield from cur


def parse_lines_one(
    conn, table: str, i: int, page_url: str, line: str, pattern: str, rx
):
    param_hash = hashobj(pattern)
    if db_select(conn, table, line=line, param_hash=param_hash):
        return
    logger.info('%d Parsed %s', i, page_url)
    for line, parsed in parse_line(rx, line):
        db_insert(
            conn,
            table,
            url=page_url,
            line=line,
            parsed=parsed,
            param_hash=param_hash,
        )


def parse_lines(db: dict, pattern: str, table_lines: str, table_parsed: str):
    conn = db_connect(
        host=db['host'],
        database=db['database'],
        user=db['user'],
        password=db['password'],
    )
    with conn:
        rx = regex.compile(pattern)
        create_table(conn, table_parsed)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(
                    parse_lines_one,
                    conn,
                    table_parsed,
                    i,
                    page_url,
                    line,
                    pattern,
                    rx,
                )
                for i, (page_url, line) in enumerate(
                    get_lines(conn, table_lines)
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
    parse_lines(
        db=config['db'],
        pattern=config['parse_lines']['pattern'],
        table_lines=config['db']['table_lines'],
        table_parsed=config['db']['table_parsed'],
    )


if __name__ == '__main__':
    main()
