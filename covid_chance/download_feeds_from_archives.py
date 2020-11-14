import csv
import logging
from pathlib import Path

from covid_chance.download_feeds import (
    clean_url, download_feed, save_page_urls,
)
from covid_chance.model import ArchivedPageURL, create_session
from covid_chance.utils.dict_utils import deep_get
from covid_chance.utils.file_utils import safe_filename

logger = logging.getLogger(__name__)


def main(config: dict, cache_path: str):
    feeds = config['feeds']
    timeout = deep_get(
        config, ['download_feeds', 'timeout'], default=30, process=int
    )
    session = create_session(config['db']['url'])
    for feed in feeds:
        if not feed.get('name') or not feed.get('url'):
            continue
        for archived_page_url in session.query(ArchivedPageURL).filter(
            ArchivedPageURL.feed_url == feed['url'],
            ArchivedPageURL.archived_url.isnot(None),
        ):
            logger.info(
                'Found archived feed %s %s',
                archived_page_url.date,
                archived_page_url.archived_url,
            )
            cache_file_path = (
                Path(cache_path)
                / 'feeds'
                / safe_filename(archived_page_url.archived_url)
                / 'feed_pages.csv'
            )
            if cache_file_path.is_file():
                logger.info('Reading from cache')
                with cache_file_path.open('r') as f:
                    page_urls = [
                        clean_url(page_url) for (page_url,) in csv.reader(f)
                    ]
            else:
                try:
                    page_urls = download_feed(
                        archived_page_url.archived_url, timeout=timeout
                    )
                except Exception:
                    logger.error(
                        'Failed to download %s', archived_page_url.archived_url
                    )
                cache_file_path.parent.mkdir(parents=True, exist_ok=True)
                with cache_file_path.open('w') as f:
                    writer = csv.writer(f, lineterminator='\n')
                    writer.writerows((page_url,) for page_url in page_urls)
            save_page_urls(
                session, feed['name'], page_urls, archived_page_url.date
            )
    session.close()
