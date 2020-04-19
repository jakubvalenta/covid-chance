import argparse
import json
import logging
import sys
from typing import Iterable, Iterator, List, Sequence

import psycopg2
import regex

from covid_chance.hash_utils import hashobj

logger = logging.getLogger(__name__)


def filter_lines(
    f: Iterable[str], match_line: Sequence[Sequence[str]]
) -> Iterator[str]:
    regexes = [
        regex.compile(r'\L<keywords>', keywords=keywords, flags=regex.I)
        for keywords in match_line
    ]
    return (line.strip() for line in f if all(r.search(line) for r in regexes))


def db_connect(**kwargs):
    return psycopg2.connect(**kwargs)


def db_create_table(conn, table: str):
    cur = conn.cursor()
    try:
        cur.execute(
            f'''
CREATE TABLE {table} (
  update_id TEXT,
  url TEXT,
  line TEXT,
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
    cur.close()


def db_select(conn, table: str, **kwargs) -> tuple:
    cur = conn.cursor()
    params = ' AND '.join(f'{k} = %s' for k in kwargs.keys())
    values = tuple(kwargs.values())
    cur.execute(f'SELECT * FROM {table} WHERE {params} LIMIT 1;', values)
    row = cur.fetchone()
    cur.close()
    return row


def db_insert(conn, table: str, **kwargs):
    cur = conn.cursor()
    columns = ', '.join(kwargs.keys())
    placeholders = ', '.join('%s' for _ in kwargs.values())
    values = tuple(kwargs.values())
    cur.execute(
        f'INSERT INTO {table} ({columns}) VALUES ({placeholders});', values,
    )
    cur.close()


def get_pages(conn, table: str) -> Iterator[tuple]:
    cur = conn.cursor()
    cur.execute(f'SELECT url, text FROM {table};')
    yield from cur
    cur.close()


def count(conn, table: str) -> int:
    cur = conn.cursor()
    cur.execute(f'SELECT COUNT(*) FROM {table};')
    count = int(cur.fetchone()[0])
    cur.close()
    return count


def match_lines(
    conn, match_line: List[List[str]], table_lines: str, table_pages: str,
):
    db_create_table(conn, table_lines)
    for i, (page_url, page_text) in enumerate(get_pages(conn, table_pages)):
        update_id = hashobj(page_text, match_line)
        if db_select(conn, table_lines, update_id=update_id):
            logger.info('%d %s - already processed', i, page_url)
        else:
            logger.info('%d %s - processing', i, page_url)
            for line in filter_lines(page_text.splitlines(), match_line):
                db_insert(
                    conn,
                    table_lines,
                    update_id=update_id,
                    url=page_url,
                    line=line,
                )


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


if __name__ == '__main__':
    main()
