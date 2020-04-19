import argparse
import json
import logging
import sys
from typing import Iterator, Tuple

import luigi
import luigi.contrib.postgres
import psycopg2
import regex

from covid_chance.hash_utils import hashobj


def searchall(rx: regex.Regex, s: str) -> Iterator:
    m = rx.search(s)
    while m:
        yield m
        m = rx.search(s, pos=m.span()[1])


def parse_line(line: str, pattern: str) -> Iterator[Tuple[str, str]]:
    rx = regex.compile(pattern)
    matches = list(searchall(rx, line))
    if matches:
        for m in matches:
            yield (line, m.group('parsed'))
    else:
        yield line, ''


class ParseLine(luigi.contrib.postgres.CopyToTable):
    page_url = luigi.Parameter()
    line = luigi.Parameter()
    parse_pattern = luigi.Parameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    columns = [
        ('url', 'TEXT'),
        ('line', 'TEXT'),
        ('parsed', 'TEXT'),
    ]

    @property
    def update_id(self):
        return hashobj(self.page_url, self.line, self.parse_pattern)

    def rows(self):
        return (
            (self.page_url, line, parsed)
            for line, parsed in parse_line(self.line, self.parse_pattern)
            if parsed
        )


class ParseLines(luigi.WrapperTask):
    parse_pattern = luigi.Parameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table_lines = luigi.Parameter()
    table_parsed = luigi.Parameter()

    @classmethod
    def get_lines(
        cls, database: str, user: str, password: str, table: str
    ) -> Iterator[tuple]:
        conn = psycopg2.connect(dbname=database, user=user, password=password)
        cur = conn.cursor()
        cur.execute(f'SELECT url, line FROM {table};')
        yield from cur
        cur.close()

    def requires(self):
        for page_url, line in self.get_lines(
            self.database, self.user, self.password, self.table_lines
        ):
            yield ParseLine(
                page_url=page_url,
                line=line,
                parse_pattern=self.parse_pattern,
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                table=self.table_parsed,
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
    luigi.build(
        [
            ParseLines(
                parse_pattern=config['parse_pattern'],
                host=config['db']['host'],
                database=config['db']['database'],
                user=config['db']['user'],
                password=config['db']['password'],
                table_lines=config['db']['table_lines'],
                table_parsed=config['db']['table_parsed'],
            )
        ],
        workers=6,
        local_scheduler=True,
        parallel_scheduling=True,
        log_level='WARNING',
    )


if __name__ == '__main__':
    main()
