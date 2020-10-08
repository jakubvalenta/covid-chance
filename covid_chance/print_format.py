import argparse
import json
import logging
import string
import sys
from pathlib import Path
from typing import IO, Optional, Sequence

import dateutil.tz
from jinja2 import BaseLoader, Environment, FileSystemLoader, PackageLoader

from covid_chance.print_export import read_exported_tweets
from covid_chance.utils.db_utils import db_connect

logger = logging.getLogger(__name__)


def render_template(
    f: IO,
    tweets: list,
    context: dict,
    package: Optional[Sequence[str]] = None,
    path: Optional[str] = None,
):
    if package:
        loader: BaseLoader = PackageLoader(*package[:-1])
        template_name = package[-1]
    elif path:
        p = Path(path)
        loader = FileSystemLoader(p.parent)
        template_name = p.name
    else:
        raise Exception('Invalid arguments')
    environment = Environment(loader=loader)
    template = environment.get_template(template_name)
    stream = template.stream(tweets=tweets, **context)
    f.writelines(stream)


def main(config: dict, output_dir: Path):
    conn = db_connect(
        database=config['db']['database'],
        user=config['db']['user'],
        password=config['db']['password'],
    )
    table_exported = config['db']['table_print_export']

    default_tz = dateutil.tz.gettz(config['print_export']['default_tz'])
    if not default_tz:
        raise Exception('Invalid time zone')
    exported_tweets = list(
        read_exported_tweets(conn, table_exported, default_tz)
    )
    tweets_per_page = config['print_export']['tweets_per_page']

    output_filename_template = string.Template(
        config['print_export']['output_filename_template']
    )
    for i in range(len(exported_tweets) // tweets_per_page + 1):
        start = i * tweets_per_page
        end = start + tweets_per_page
        tweets = exported_tweets[start:end]
        if not tweets:
            return
        output_filename = output_filename_template.substitute(
            i=f'{i:03}', **tweets[0].to_dict()
        )
        output_path = output_dir / output_filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open('w') as f:
            render_template(
                f,
                tweets=tweets,
                context=config['print_export']['context'],
                package=config['print_export'].get('template_package'),
                path=config['print_export'].get('template_path'),
            )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-c', '--config', help='Configuration file path', required=True
    )
    parser.add_argument(
        '-o', '--output', help='Output directory', required=True
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
    output_dir = Path(args.output)
    main(config, output_dir)
