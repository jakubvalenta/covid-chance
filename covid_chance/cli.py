import argparse
import json
import logging
import sys
from pathlib import Path

from covid_chance import __title__

logger = logging.getLogger(__name__)


def print_export(config, args):
    from covid_chance.print_export import main

    cache_path = Path(args.cache)
    main(config, cache_path, approved=args.approved)


def print_format(config, args):
    from covid_chance.print_format import main

    output_dir = Path(args.output)
    main(config, output_dir)


def main():
    parser = argparse.ArgumentParser(prog=__title__)
    parser.add_argument(
        '-c', '--config', help='Configuration file path', required=True
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true', help='Enable debugging output'
    )
    subparsers = parser.add_subparsers()
    print_export_parser = subparsers.add_parser(
        'print-export', help='Print export'
    )
    print_export_parser.set_defaults(func=print_export)
    print_export_parser.add_argument(
        '-c',
        '--cache',
        help='Cache directory path',
        required=True,
    )
    print_export_parser.add_argument(
        '-a',
        '--approved',
        action='store_true',
        help='Export all approved tweets (instead of only the posted ones)',
    )
    print_format_parser = subparsers.add_parser(
        'print-format', help='Print format'
    )
    print_format_parser.set_defaults(func=print_format)
    print_format_parser.add_argument(
        '-o',
        '--output',
        help='Output directory',
        required=True,
    )
    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(
            stream=sys.stderr, level=logging.INFO, format='%(message)s'
        )
    with open(args.config, 'r') as f:
        config = json.load(f)
    args.func(config, args)


if __name__ == '__main__':
    main()
