import argparse
import json
import logging
import sys
from pathlib import Path
from typing import IO, Sequence

from jinja2 import Environment, PackageLoader

from covid_chance.print_export import read_exported_tweets
from covid_chance.utils.db_utils import db_connect

logger = logging.getLogger(__name__)


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
    with open(args.config, 'r') as f:
        config = json.load(f)
    output_path = Path(args.output)

    conn = db_connect(
        database=config['db']['database'],
        user=config['db']['user'],
        password=config['db']['password'],
    )
    table_exported = config['db']['table_print_export']

    exported_tweets = list(read_exported_tweets(conn, table_exported))

    with output_path.open('w') as f:
        render_template(
            ['covid_chance', 'templates', 'print.html'],
            f,
            tweets=[x for x in exported_tweets if x],
            name=config['print_export']['name'],
            handle=config['print_export']['handle'],
            profile_picture=config['print_export']['profile_picture'],
        )


if __name__ == '__main__':
    main()
