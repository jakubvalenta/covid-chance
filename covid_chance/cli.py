import argparse
import json
import logging
import sys
from pathlib import Path

from covid_chance import __title__

logger = logging.getLogger(__name__)


def download_archives(config, args):
    from covid_chance.download_archives import main

    main(config)


def download_feeds(config, args):
    from covid_chance.download_feeds import main

    main(config)


def download_feeds_from_archives(config, args):
    from covid_chance.download_feeds_from_archives import main

    cache_path = Path(args.cache)
    main(config, cache_path)


def download_pages(config, args):
    from covid_chance.download_pages import main

    cache_path = Path(args.cache)
    main(config, cache_path, dry_run=args.dry_run)


def match_lines(config, args):
    from covid_chance.match_lines import main

    main(config)


def parse_lines(config, args):
    from covid_chance.parse_lines import main

    main(config)


def review_tweets(config, args):
    from covid_chance.review_tweets import main

    main(config, review_all=args.all, incl_approved=args.approved)


def post_tweet(config, args):
    from covid_chance.post_tweet import main

    with open(args.secrets, 'r') as f:
        secrets = json.load(f)
    main(
        config,
        secrets,
        interactive=args.interactive,
        dry_run=args.dry_run,
    )


def print_export(config, args):
    from covid_chance.print_export import main

    cache_path = Path(args.cache)
    main(config, cache_path, approved=args.approved)


def print_format(config, args):
    from covid_chance.print_format import main

    output_dir = Path(args.output)
    main(config, output_dir)


def show_stats(config, args):
    from covid_chance.show_stats import main

    main(config)


def migrate(config, args):
    from covid_chance.migrate import main

    main(config)


def main():
    parser = argparse.ArgumentParser(prog=__title__)
    parser.add_argument(
        '-c', '--config', help='Configuration file path', required=True
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true', help='Enable debugging output'
    )
    subparsers = parser.add_subparsers()

    download_archives_parser = subparsers.add_parser(
        'download-archives', help='Download archives'
    )
    download_archives_parser.set_defaults(func=download_archives)

    download_feeds_parser = subparsers.add_parser(
        'download-feeds', help='Download feeds'
    )
    download_feeds_parser.set_defaults(func=download_feeds)

    download_feeds_from_archives_parser = subparsers.add_parser(
        'download-feeds-from-archives', help='Download feeds from archives'
    )
    download_feeds_from_archives_parser.set_defaults(
        func=download_feeds_from_archives
    )
    download_feeds_from_archives_parser.add_argument(
        '-c',
        '--cache',
        help='Cache directory path',
        required=True,
    )

    download_pages_parser = subparsers.add_parser(
        'download-pages', help='Download pages'
    )
    download_pages_parser.set_defaults(func=download_pages)
    download_pages_parser.add_argument(
        '-c',
        '--cache',
        help='Cache directory path',
        required=True,
    )
    download_pages_parser.add_argument(
        '--dry-run', action='store_true', help='Dry run'
    )

    match_lines_parser = subparsers.add_parser(
        'match-lines', help='Match lines'
    )
    match_lines_parser.set_defaults(func=match_lines)

    parse_lines_parser = subparsers.add_parser(
        'parse-lines', help='Parse lines'
    )
    parse_lines_parser.set_defaults(func=parse_lines)

    review_tweets_parser = subparsers.add_parser(
        'review-tweets', help='Review tweets'
    )
    review_tweets_parser.set_defaults(func=review_tweets)
    review_tweets_parser.add_argument(
        '-a',
        '--all',
        action='store_true',
        help='Review all already reviewed tweets again',
    )
    review_tweets_parser.add_argument(
        '-p',
        '--approved',
        action='store_true',
        help='Review approved tweets again',
    )

    post_tweet_parser = subparsers.add_parser('post-tweet', help='Post tweet')
    post_tweet_parser.set_defaults(func=post_tweet)
    post_tweet_parser.add_argument(
        '-s', '--secrets', help='Secrets file path', required=True
    )
    post_tweet_parser.add_argument(
        '-i',
        '--interactive',
        action='store_true',
        help='Ask before posting the tweet',
    )
    post_tweet_parser.add_argument(
        '--dry-run', action='store_true', help='Dry run'
    )

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

    show_stats_parser = subparsers.add_parser(
        'show-stats', help='Show statistics'
    )
    show_stats_parser.set_defaults(func=show_stats)

    migrate_parser = subparsers.add_parser('migrate', help='Migrate database')
    migrate_parser.set_defaults(func=migrate)

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
