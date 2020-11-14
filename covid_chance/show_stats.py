import csv
import logging
import sys

from sqlalchemy.orm.session import Session

from covid_chance.model import (
    PageLine, PageURL, ParsedPageLine, Tweet, TweetReviewStatus, count,
    create_session,
)

logger = logging.getLogger(__name__)


def calc_feed_stats(session: Session, feed_name: str) -> dict:
    page_urls = [
        page_url.url
        for page_url in session.query(PageURL).filter(
            PageURL.feed_name == feed_name
        )
    ]
    n_pages = len(page_urls)
    if n_pages:
        n_lines = count(
            session.query(PageLine).filter(
                PageLine.line != '', PageLine.url.in_(page_urls)
            )
        )
    else:
        n_lines = 0
    if n_lines:
        n_parsed = count(
            session.query(ParsedPageLine).filter(
                ParsedPageLine.url.in_(page_urls)
            )
        )
    else:
        n_parsed = 0
    if n_parsed:
        n_approved = count(
            session.query(Tweet).filter(
                Tweet.status == TweetReviewStatus.approved,
                Tweet.url.in_(page_urls),
            )
        )
    else:
        n_approved = 0
    return {
        'feed_name': feed_name,
        'n_pages': n_pages,
        'n_lines': n_lines,
        'n_parsed': n_parsed,
        'n_approved': n_approved,
    }


def main(config: dict):
    session = create_session(config['db']['url'])
    feeds = config['feeds']
    writer = csv.DictWriter(
        sys.stdout,
        fieldnames=(
            'feed_name',
            'n_pages',
            'n_lines',
            'n_parsed',
            'n_approved',
        ),
        quoting=csv.QUOTE_NONNUMERIC,
        lineterminator='\n',
    )
    writer.writeheader()
    for feed in feeds:
        if feed['name']:
            feed_stats = calc_feed_stats(session, feed['name'])
            writer.writerow(feed_stats)
    session.close()
