import argparse
import json
import logging
import sys
from pprint import pprint as pp

import twitter

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-s', '--secrets', help='Secrets file path', required=True
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true', help='Enable debugging output'
    )
    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(
            stream=sys.stderr, level=logging.INFO, format='%(message)s'
        )
    with open(args.secrets, 'r') as f:
        secrets = json.load(f)
    api = twitter.Api(
        consumer_key=secrets['consumer_key'],
        consumer_secret=secrets['consumer_secret'],
        access_token_key=secrets['access_token_key'],
        access_token_secret=secrets['access_token_secret'],
    )
    logger.info('Posting a status update...')
    status = api.PostUpdates('Test')
    pp(status)
    logger.info('Done')


if __name__ == '__main__':
    main()
