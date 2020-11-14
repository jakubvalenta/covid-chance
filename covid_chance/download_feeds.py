import concurrent.futures
import datetime
import logging
from typing import List

import feedparser
import requests
from sqlalchemy.orm.session import Session

from covid_chance.model import PageURL, count, create_session
from covid_chance.utils.dict_utils import deep_get
from covid_chance.utils.download_utils import clean_url

logger = logging.getLogger(__name__)


def download_feed(url: str, timeout: int = 10) -> List[str]:
    logger.info('Downloading feed %s', url)
    # Fetch the feed content using requests, because feedparser seems to have
    # some trouble with the Basic Auth -- the feed object contains an error.
    r = requests.get(
        url,
        headers={
            'User-Agent': (
                'Mozilla/5.0 (X11; Linux x86_64; rv:75.0) '
                'Gecko/20100101 Firefox/75.0'
            )
        },
        timeout=timeout,
    )
    r.raise_for_status()
    feed = feedparser.parse(r.text)
    return [clean_url(entry.link) for entry in feed.entries]


def save_page_urls(
    session: Session,
    feed_name: str,
    page_urls: List[str],
    mtime: datetime.datetime,
):
    counter = 0
    for page_url in set(page_urls):
        if count(session.query(PageURL).filter(PageURL.url == page_url)):
            continue
        try:
            page_url = PageURL(
                url=page_url, feed_name=feed_name, inserted=mtime
            )
            session.add(page_url)
            counter += 1
        except Exception as e:
            logger.error(
                'Error while inserting new URL in the db %s %s',
                page_url,
                e,
            )
    session.commit()
    logger.info(
        'done %s %d urls inserted',
        feed_name.ljust(40),
        counter,
    )


def download_and_save_feed(
    session: Session, feed_name: str, feed_url: str, timeout: int
):
    mtime = datetime.datetime.now()
    page_urls = download_feed(feed_url, timeout)
    save_page_urls(session, feed_name, page_urls, mtime)


def main(config: dict):
    feeds = config['feeds']
    timeout = deep_get(
        config, ['download_feeds', 'timeout'], default=30, process=int
    )
    session = create_session(config['db']['url'])
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                download_and_save_feed,
                session,
                feed_name=feed['name'],
                feed_url=feed['url'],
                timeout=timeout,
            )
            for feed in feeds
            if feed.get('name') and feed.get('url')
        ]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error('Exception: %s', e)
    session.close()
