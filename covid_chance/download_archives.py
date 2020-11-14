import datetime
import logging
from typing import Optional
from urllib.parse import urlsplit

import requests

from covid_chance.model import ArchivedPageURL, count, create_session

logger = logging.getLogger(__name__)


class ArchiveError(Exception):
    pass


def get_from_web_archive(url: str, *args, **kwargs) -> requests.Response:
    res = requests.get(
        url, *args, headers={'User-Agent': 'curl/7.69.1'}, **kwargs
    )
    res.raise_for_status()
    return res


def find_closest_snapshot_url(url: str, date: datetime.date) -> Optional[str]:
    u = urlsplit(url)
    if u.username:
        logger.warning('Skipping URL with Basic Auth %s', url)
        return None
    logger.info('Finding closest snaphot URL for %s %s', url, date)
    timestamp = date.strftime('%Y%m%d')
    api_url = f'https://web.archive.org/web/{timestamp}/{url}'
    res = get_from_web_archive(api_url, allow_redirects=False)
    snapshot_url = res.headers.get('location')
    if not snapshot_url:
        raise ArchiveError('Failed to find snapshot URL')
    if 'http://none' in snapshot_url:
        logger.warning('Empty snapshot URL returned %s', snapshot_url)
        return None
    logger.info(
        'Closest snapshot URL for %s %s is %s', url, date, snapshot_url
    )
    return snapshot_url


def main(config: dict):
    feeds = config['feeds']
    dates = [
        datetime.datetime.fromisoformat(d) for d in config['archive_dates']
    ]
    session = create_session(config['db']['url'])
    for feed in feeds:
        if not feed.get('name') or not feed.get('url'):
            continue
        for date in dates:
            if count(
                session.query(ArchivedPageURL).filter(
                    ArchivedPageURL.feed_url == feed['url'],
                    ArchivedPageURL.date == date,
                )
            ):
                continue
            archived_url = find_closest_snapshot_url(feed['url'], date)
            archived_page_url = ArchivedPageURL(
                feed_url=feed['url'],
                archived_url=archived_url,
                date=date,
            )
            session.add(archived_page_url)
            session.commit()
    session.close()
