import html
import logging
from typing import Optional

import regex as re
import twitter
from sqlalchemy.exc import MultipleResultsFound

from covid_chance.model import PostedTweet, count, create_session

logger = logging.getLogger(__name__)


def check_posted_tweets(
    session, api, screen_name: str, max_id: Optional[int] = None
) -> Optional[int]:
    logger.info(f'Fetching user timeline, {max_id=}')
    statuses = api.GetUserTimeline(
        screen_name='covid_chance', count=100, max_id=max_id
    )
    last_id = None
    for status in statuses:
        last_id = status.id
        logger.info('Checking %d "%s"', status.id, status.full_text)
        if count(
            session.query(PostedTweet).filter(
                PostedTweet.status_id == status.id
            )
        ):
            continue
        m = re.match(r'(?P<raw_text>.+) https://t\.co/\w+$', status.full_text)
        raw_text = html.unescape(m.group('raw_text'))
        try:
            posted_tweet = (
                session.query(PostedTweet)
                .filter(PostedTweet.text.like(f'{raw_text}%'))
                .one_or_none()
            )
        except MultipleResultsFound:
            logger.error(
                'Multiple tweets with the same text found: %d "%s"',
                status.id,
                raw_text,
            )
            continue
        if posted_tweet:
            if not posted_tweet.status_id:
                logger.warning(
                    'Updating status id: %d "%s"', status.id, raw_text
                )
                posted_tweet.status_id = status.id
        else:
            logger.warning('Adding: %d "%s"', status.id, raw_text)
            new_posted_tweet = PostedTweet.from_status(status)
            session.add(new_posted_tweet)
    session.commit()
    return last_id


def main(config: dict, secrets: dict, dry_run: bool):
    session = create_session(config['db']['url'])
    api = twitter.Api(
        consumer_key=secrets['consumer_key'],
        consumer_secret=secrets['consumer_secret'],
        access_token_key=secrets['access_token_key'],
        access_token_secret=secrets['access_token_secret'],
        tweet_mode='extended',
    )
    screen_name = 'covid_chance'
    max_pages = 100
    max_id = None
    for _ in range(max_pages):
        last_id = check_posted_tweets(session, api, screen_name, max_id)
        if not last_id:
            break
        max_id = last_id - 1
    session.close()
