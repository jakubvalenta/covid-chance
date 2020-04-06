import logging
import re
from typing import Iterator, List, Sequence, Type
from urllib.parse import urlsplit, urlunsplit

import feedparser
import requests
from bs4 import (
    BeautifulSoup, CData, Comment, Declaration, Doctype, NavigableString,
    ProcessingInstruction,
)

logger = logging.getLogger(__name__)


def simplify_url(url: str) -> str:
    u = urlsplit(url)
    return urlunsplit(('', u.netloc, u.path, u.query, ''))


def download_page(url: str) -> str:
    logger.info('Downloading page %s', url)
    res = requests.get(url)
    res.raise_for_status()
    return res.text


def download_feed(url: str) -> List[str]:
    logger.info('Downloading feed %s', url)
    feed = feedparser.parse(url)
    return [entry.link for entry in feed.entries]


def get_element_text(
    soup: BeautifulSoup,
    ignore_tags: Sequence[str] = ('style', 'script'),
    ignore_classes: Sequence[Type] = (
        CData,
        Comment,
        Declaration,
        Doctype,
        ProcessingInstruction,
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
                yield from get_element_text(child, ignore_tags=ignore_tags)


def clean_whitespace(s: str) -> str:
    s = re.sub(r'[^\S\n\r]+', ' ', s)
    s = re.sub(r'\s*\n\s*', '\n', s)
    return s.strip()


def get_page_text(html: str,) -> str:
    soup = BeautifulSoup(html, 'lxml')
    text = ''.join(get_element_text(soup))
    return clean_whitespace(text)
