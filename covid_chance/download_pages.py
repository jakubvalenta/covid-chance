import csv
import datetime
import logging
import random
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterator, Optional, Sequence, Set, Tuple, Type

import requests
from bs4 import (
    BeautifulSoup, CData, Comment, Declaration, Doctype, NavigableString,
    ProcessingInstruction,
)
from bs4.element import Script, Stylesheet, TemplateString
from sqlalchemy.orm.session import Session

from covid_chance.model import Page, PageURL, create_session
from covid_chance.utils.dict_utils import deep_get
from covid_chance.utils.download_utils import simplify_url
from covid_chance.utils.file_utils import safe_filename

logger = logging.getLogger(__name__)


def download_page(
    url: str,
    wait_interval: Tuple[int, int] = (0, 0),
    timeout: int = 10,
    non_recoverable_error_codes: Sequence[int] = (
        requests.codes.not_found,
        requests.codes.gone,
        requests.codes.legal_reasons,
    ),
) -> str:
    if url.startswith('urn:'):
        logger.error('Unsupported scheme')
        return ''
    wait = random.randint(*wait_interval)
    if wait:
        logger.info('Waiting for %ds', wait)
        time.sleep(wait)
    try:
        res = requests.get(
            url,
            headers={
                'User-Agent': (
                    'Mozilla/5.0 (X11; Linux x86_64; rv:75.0) '
                    'Gecko/20100101 Firefox/75.0'
                )
            },
            timeout=timeout,
        )
    except requests.TooManyRedirects:
        logger.error('Too many redirects')
        return ''
    if res.status_code in non_recoverable_error_codes:
        logger.error('Non-recoverable error encountered')
        return ''
    res.raise_for_status()
    return res.text


def get_element_text(
    soup: BeautifulSoup,
    ignore_tags: Sequence[str] = ('head', 'meta', 'script', 'style', 'title'),
    ignore_classes: Sequence[Type] = (
        CData,
        Comment,
        Declaration,
        Doctype,
        ProcessingInstruction,
        Script,
        Stylesheet,
        TemplateString,
    ),
    block_elements: Sequence[str] = (
        'address',
        'article',
        'aside',
        'blockquote',
        'details',
        'dialog',
        'dd',
        'div',
        'dl',
        'dt',
        'fieldset',
        'figcaption',
        'figure',
        'footer',
        'form',
        'h1',
        'h2',
        'h3',
        'h4',
        'h5',
        'h6',
        'header',
        'hgroup',
        'hr',
        'li',
        'main',
        'nav',
        'ol',
        'p',
        'pre',
        'section',
        'table',
        'ul',
    ),
) -> Iterator[str]:
    if type(soup) not in ignore_classes and soup.name not in ignore_tags:
        if type(soup) == NavigableString:
            yield soup.string
        else:
            if soup.name in block_elements:
                yield '\n'
            for child in soup.children:
                yield from get_element_text(
                    child,
                    ignore_tags=ignore_tags,
                    ignore_classes=ignore_classes,
                    block_elements=block_elements,
                )


def clean_whitespace(s: str) -> str:
    s = re.sub(r'[^\S\n\r]+', ' ', s)
    s = re.sub(r'\s*\n\s*', '\n', s)
    return s.strip()


def get_page_text(html: str) -> str:
    if not html:
        return ''
    soup = BeautifulSoup(html, 'lxml')
    text = ''.join(get_element_text(soup))
    return clean_whitespace(text)


def download_page_text(
    cache_path: str,
    feed_name: str,
    page_url: str,
    wait_interval: Tuple[int, int],
    timeout: int,
) -> Optional[str]:
    path = (
        Path(cache_path)
        / 'pages'
        / safe_filename(simplify_url(page_url))
        / 'page_content.txt'
    )
    if path.is_file():
        logger.info('Reading from cache %s', page_url)
        return path.read_text()
    try:
        html = download_page(
            page_url,
            wait_interval=wait_interval,
            timeout=timeout,
        )
    except Exception as e:
        logger.error('Failed to download %s: %s', page_url, e)
        return None
    try:
        text = get_page_text(html)
    except RecursionError:
        logger.error(
            'Recursion error while trying to parse page text.'
            'It\'s probably not HTML.'
        )
        text = ''
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return text


def select_page_urls_to_download(
    session: Session, since: Optional[datetime.datetime]
) -> Dict[str, Set[str]]:
    query = (
        session.query(PageURL)
        .outerjoin(Page, PageURL.url == Page.url)
        .filter(Page.url.is_(None))
    )
    if since:
        query = query.filter(PageURL.inserted >= since)
    query = query.order_by(PageURL.inserted)
    page_urls_by_feeds = defaultdict(set)
    for page_url in query:
        page_urls_by_feeds[page_url.feed_name].add(page_url.url)
    return page_urls_by_feeds


def print_stats(page_urls_by_feed: Dict[str, Set[str]]):
    writer = csv.DictWriter(
        sys.stdout,
        fieldnames=('feed_name', 'n_pages'),
        quoting=csv.QUOTE_NONNUMERIC,
        lineterminator='\n',
    )
    writer.writeheader()
    for feed_name, page_urls in page_urls_by_feed.items():
        if page_urls:
            writer.writerow(
                {'feed_name': feed_name, 'n_pages': len(page_urls)}
            )


def main(
    config: dict,
    cache_path: str,
    dry_run: bool,
):
    since = deep_get(
        config,
        ['download_pages', 'since'],
        default=None,
        process=datetime.datetime.fromisoformat,
    )
    wait_interval = (
        deep_get(
            config, ['download_pages', 'wait_min'], default=0, process=int
        ),
        deep_get(
            config, ['download_pages', 'wait_max'], default=0, process=int
        ),
    )
    timeout = deep_get(
        config, ['download_pages', 'timeout'], default=10, process=int
    )
    session = create_session(config['db']['url'])
    logger.info(
        'Selecting pages to download since %s',
        since or 'the beginning of time',
    )
    page_urls_by_feeds = select_page_urls_to_download(session, since)
    print_stats(page_urls_by_feeds)
    page_urls_with_feed_names = {}
    for feed_name, page_urls in page_urls_by_feeds.items():
        for page_url in page_urls:
            if page_url not in page_urls_with_feed_names:
                page_urls_with_feed_names[page_url] = feed_name
    total = len(page_urls_with_feed_names)
    logger.info('Pages to download: %d', total)
    if dry_run:
        logger.warning('This is just a dry run, not downloading any pages')
        return
    page_urls_with_feed_names_list = list(page_urls_with_feed_names.items())
    random.shuffle(page_urls_with_feed_names_list)
    for i, (page_url, feed_name) in enumerate(page_urls_with_feed_names_list):
        logger.info('%d/%d Downloading %s', i + 1, total, page_url)
        text = download_page_text(
            cache_path=cache_path,
            feed_name=feed_name,
            page_url=page_url,
            wait_interval=wait_interval,
            timeout=timeout,
        )
        if text is not None:
            page = Page(url=page_url, text=text)
            session.add(page)
    session.commit()
    session.close()
