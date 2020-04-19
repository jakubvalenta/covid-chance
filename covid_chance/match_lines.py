import argparse
import json
import logging
import sys
from typing import Iterable, Iterator, Sequence

import luigi
import luigi.contrib.postgres
import psycopg2
import regex


def filter_lines(
    f: Iterable[str], match_line: Sequence[Sequence[str]]
) -> Iterator[str]:
    regexes = [
        regex.compile(r'\L<keywords>', keywords=keywords, flags=regex.I)
        for keywords in match_line
    ]
    return (line.strip() for line in f if all(r.search(line) for r in regexes))


class MatchPageLines(luigi.contrib.postgres.CopyToTable):
    page_url = luigi.Parameter()
    page_text = luigi.Parameter()
    match_line = luigi.ListParameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    columns = [
        ('url', 'TEXT'),
        ('line', 'TEXT'),
    ]

    @property
    def update_id(self):
        return json.dumps([self.page_url, self.match_line])

    def rows(self):
        return (
            (self.page_url, line)
            for line in filter_lines(
                self.page_text.splitlines(), self.match_line
            )
        )


class MatchLines(luigi.WrapperTask):
    match_line = luigi.ListParameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table_lines = luigi.Parameter()
    table_pages = luigi.Parameter()

    @classmethod
    def get_pages(
        cls, database: str, user: str, password: str, table: str
    ) -> Iterator[tuple]:
        conn = psycopg2.connect(dbname=database, user=user, password=password)
        cur = conn.cursor()
        cur.execute(f'SELECT url, text FROM {table};')
        yield from cur
        cur.close()

    def requires(self):
        for page_url, page_text in self.get_pages(
            self.database, self.user, self.password, self.table_pages
        ):
            yield MatchPageLines(
                page_url=page_url,
                page_text=page_text,
                match_line=self.match_line,
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                table=self.table_lines,
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
            MatchLines(
                match_line=config['match_line'],
                host=config['db']['host'],
                database=config['db']['database'],
                user=config['db']['user'],
                password=config['db']['password'],
                table_lines=config['db']['table_lines'],
                table_pages=config['db']['table_pages'],
            )
        ],
        workers=6,
        local_scheduler=True,
        parallel_scheduling=True,
        log_level='WARNING',
    )


if __name__ == '__main__':
    main()
