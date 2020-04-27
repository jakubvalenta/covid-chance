import argparse
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, wait
from typing import Iterator, Tuple

import psycopg2
import psycopg2.errorcodes
import regex

from covid_chance.utils.db_utils import db_connect, db_insert, db_select
from covid_chance.utils.hash_utils import hashobj

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
    cur = conn.cursor()
    cur.execute(f"SELECT url, line FROM {table} WHERE line != '';")
    yield from cur
    cur.close()


def parse_lines_one(
    conn, table: str, i: int, page_url: str, line: str, parse_pattern: str, rx
):
    update_id = hashobj(line, parse_pattern)
    if db_select(conn, table, update_id=update_id):
        logger.info('%d done %s', i, page_url)
        return
    logger.warning('%d todo %s', i, page_url)
    for line, parsed in parse_line(rx, line):
        db_insert(
            conn,
            table,
            update_id=update_id,
            url=page_url,
            line=line,
            parsed=parsed,
        )


def parse_lines(conn, parse_pattern: str, table_lines: str, table_parsed: str):
    rx = regex.compile(parse_pattern)
    create_table(conn, table_parsed)
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                parse_lines_one,
                conn,
                table_parsed,
                i,
                page_url,
                line,
                parse_pattern,
                rx,
            )
            for i, (page_url, line) in enumerate(get_lines(conn, table_lines))
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
    parse_lines(
        conn,
        parse_pattern=config['parse_pattern'],
        table_lines=config['db']['table_lines'],
        table_parsed=config['db']['table_parsed'],
    )
    conn.close()


if __name__ == '__main__':
    main()
