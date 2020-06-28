import argparse
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Iterator, Sequence

from jinja2 import Environment, PackageLoader

from covid_chance.utils.db_utils import db_connect

logger = logging.getLogger(__name__)


@dataclass
class TweetExport:
    text: str
    image_path: str


def read_print_export(conn, table: str) -> Iterator[TweetExport]:
    cur = conn.cursor()
    cur.execute(f"SELECT tweet, image_path FROM {table} ORDER BY inserted ASC")
    for tweet, image_path in cur:
        if Path(image_path).suffix in (
            '.jpg',
            '.png',
        ):  # TODO: Convert unsupported types to JPEG
            yield TweetExport(text=tweet, image_path=image_path)
    cur.close()


def render_template(package: Sequence[str], f: IO, **context):
    environment = Environment(loader=PackageLoader(*package[:-1]))
    template = environment.get_template(package[-1])
    stream = template.stream(**context)
    f.writelines(stream)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-c', '--config', help='Configuration file path', required=True
    )
    parser.add_argument(
        '-o', '--output', help='Output TeX file path', required=True
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true', help='Enable debugging output'
    )
    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(
            stream=sys.stderr, level=logging.INFO, format='%(message)s'
        )
    output_path = Path(args.output)
    with open(args.config, 'r') as f:
        config = json.load(f)

    conn = db_connect(
        database=config['db']['database'],
        user=config['db']['user'],
        password=config['db']['password'],
    )
    table_print = config['db']['table_print']

    tweets = list(read_print_export(conn, table_print))

    with output_path.open('w') as f:
        render_template(
            ['covid_chance', 'templates', 'print.tex'],
            f,
            tweets=tweets,
            title=config['print_export']['title'],
            author=config['print_export']['author'],
            year=config['print_export']['year'],
            url=config['print_export']['url'],
        )


if __name__ == '__main__':
    main()
