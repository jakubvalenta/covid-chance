import datetime
import json
import logging
import re
import sys
from pathlib import Path
from typing import Iterator, List, Sequence, Type
from urllib.parse import urlsplit, urlunsplit

import feedparser
import luigi
import requests
from bs4 import (
    BeautifulSoup, CData, Comment, Declaration, Doctype, NavigableString,
    ProcessingInstruction,
)

from coronavirus_opportunity_bot.file_utils import csv_cache, safe_filename

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


class SavePageURL(luigi.Task):
    data_path = luigi.Parameter()
    feed_name = luigi.Parameter()
    page_url = luigi.Parameter()

    @staticmethod
    def get_output_path(data_path, feed_name, page_url) -> Path:
        return (
            Path(data_path)
            / feed_name
            / safe_filename(simplify_url(page_url))
            / 'page_url.txt'
        )

    def output(self):
        return luigi.LocalTarget(
            self.get_output_path(self.data_path, self.feed_name, self.page_url)
        )

    def run(self):
        with self.output().open('w') as f:
            print(self.page_url, file=f)


class DownloadPageHTML(luigi.Task):
    data_path = luigi.Parameter()
    feed_name = luigi.Parameter()
    page_url = luigi.Parameter()

    @staticmethod
    def get_output_path(data_path, feed_name, page_url) -> Path:
        return (
            Path(data_path)
            / feed_name
            / safe_filename(simplify_url(page_url))
            / 'page_content.html'
        )

    def output(self):
        return luigi.LocalTarget(
            self.get_output_path(self.data_path, self.feed_name, self.page_url)
        )

    def requires(self):
        return SavePageURL(
            data_path=self.data_path,
            feed_name=self.feed_name,
            page_url=self.page_url,
        )

    def run(self):
        with self.output().open('w') as f:
            html = download_page(self.page_url)
            f.write(html)


class DownloadPage(luigi.Task):
    data_path = luigi.Parameter()
    feed_name = luigi.Parameter()
    page_url = luigi.Parameter()

    @staticmethod
    def get_output_path(data_path, feed_name, page_url) -> Path:
        return (
            Path(data_path)
            / feed_name
            / safe_filename(simplify_url(page_url))
            / 'page_content.txt'
        )

    def output(self):
        return luigi.LocalTarget(
            self.get_output_path(self.data_path, self.feed_name, self.page_url)
        )

    def requires(self):
        return DownloadPageHTML(
            data_path=self.data_path,
            feed_name=self.feed_name,
            page_url=self.page_url,
        )

    def run(self):
        with self.input().open('r') as f:
            html = f.read()

        with self.output().open('w') as f:
            text = get_page_text(html)
            f.write(text)


class DownloadFeedPages(luigi.Task):
    data_path = luigi.Parameter()
    feed_name = luigi.Parameter()
    date_second = luigi.DateSecondParameter()

    @staticmethod
    def get_output_path(
        data_path: str, feed_name: str, date_second: datetime.date
    ) -> Path:
        return (
            Path(data_path)
            / feed_name
            / f'feed_downloaded-{date_second.isoformat()}.txt'
        )

    @staticmethod
    def get_feed_path(
        data_path: str, feed_name: str, date_second: datetime.date
    ) -> Path:
        return (
            Path(data_path)
            / feed_name
            / f'feed_pages-{date_second.isoformat()}.csv'
        )

    @staticmethod
    def get_feed_info_path(data_path: str, feed_name: str) -> Path:
        return Path(data_path) / feed_name / 'feed_info.json'

    def output(self):
        return luigi.LocalTarget(
            self.get_output_path(
                self.data_path, self.feed_name, self.date_second
            )
        )

    @classmethod
    def download_feed(
        cls, data_path: str, feed_name: str, date_second: datetime.datetime
    ) -> List[str]:
        @csv_cache(cls.get_feed_path(data_path, feed_name, date_second))
        def download_feed_with_cache():
            feed_info_path = cls.get_feed_info_path(data_path, feed_name)
            with feed_info_path.open('r') as f:
                feed_info = json.load(f)
            page_urls = download_feed(feed_info['url'])
            return [(page_url,) for page_url in page_urls]

        return [row[0] for row in download_feed_with_cache()]

    def requires(self):
        page_urls = self.download_feed(
            self.data_path, self.feed_name, self.date_second
        )
        for page_url in page_urls:
            yield DownloadPage(
                data_path=self.data_path,
                feed_name=self.feed_name,
                page_url=page_url,
            )

    def run(self):
        with self.output().open('w') as f:
            f.write('')


class DownloadFeeds(luigi.Task):
    data_path = luigi.Parameter(default='./data')
    date_second = luigi.DateSecondParameter(default=datetime.datetime.now())
    verbose = luigi.BoolParameter(default=False, significant=False)

    @staticmethod
    def get_output_path(data_path: str, date_second: datetime.date) -> Path:
        return (
            Path(data_path) / f'all_downloaded-{date_second.isoformat()}.txt'
        )

    @staticmethod
    def get_feed_info_paths(data_path: str) -> Iterator[Path]:
        return Path(data_path).glob('*/feed_info.json')

    def output(self):
        return luigi.LocalTarget(
            self.get_output_path(self.data_path, self.date_second)
        )

    def requires(self):
        for path in self.get_feed_info_paths(self.data_path):
            yield DownloadFeedPages(
                data_path=self.data_path,
                feed_name=path.parent.name,
                date_second=self.date_second,
            )

    def run(self):
        if self.verbose:
            logging.basicConfig(
                stream=sys.stderr, level=logging.INFO, format='%(message)s'
            )
        with self.output().open('w') as f:
            f.write('')
