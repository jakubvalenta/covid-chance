import argparse
import datetime
import json
import logging
import re
import sys
import urllib.parse
from pathlib import Path
from typing import Iterator, List, Sequence, Type

import feedparser
import luigi
import requests
from bs4 import (
    BeautifulSoup, CData, Comment, Declaration, Doctype, NavigableString,
    ProcessingInstruction,
)

from covid_chance.file_utils import csv_cache, safe_filename

logger = logging.getLogger(__name__)


def clean_url(
    url: str,
    remove_keys: Sequence[str] = ('utm_source', 'utm_medium', 'utm_campaign'),
) -> str:
    u = urllib.parse.urlsplit(url)
    qs = urllib.parse.parse_qs(u.query)
    for k in remove_keys:
        if k in qs:
            del qs[k]
    new_query = urllib.parse.urlencode(qs, doseq=True)
    return urllib.parse.urlunsplit(
        (u.scheme, u.netloc, u.path, new_query, u.fragment)
    )


def simplify_url(url: str) -> str:
    u = urllib.parse.urlsplit(url)
    netloc = re.sub('^.+@', '', u.netloc)
    return urllib.parse.urlunsplit(('', netloc, u.path, u.query, ''))


def download_page(url: str) -> str:
    logger.info('Downloading page %s', url)
    res = requests.get(url)
    res.raise_for_status()
    return res.text


def download_feed(url: str) -> List[str]:
    logger.info('Downloading feed %s', url)
    feed = feedparser.parse(url)
    return [clean_url(entry.link) for entry in feed.entries]


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
            / safe_filename(feed_name)
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
            / safe_filename(feed_name)
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


class DownloadPageText(luigi.Task):
    data_path = luigi.Parameter()
    feed_name = luigi.Parameter()
    page_url = luigi.Parameter()

    @staticmethod
    def get_output_path(data_path, feed_name, page_url) -> Path:
        return (
            Path(data_path)
            / safe_filename(feed_name)
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
    feed_url = luigi.Parameter()
    date_second = luigi.DateSecondParameter()

    @staticmethod
    def get_output_path(
        data_path: str, feed_name: str, date_second: datetime.date
    ) -> Path:
        return (
            Path(data_path)
            / safe_filename(feed_name)
            / f'feed_downloaded-{date_second.isoformat()}.txt'
        )

    @staticmethod
    def get_feed_path(
        data_path: str, feed_name: str, date_second: datetime.date
    ) -> Path:
        return (
            Path(data_path)
            / safe_filename(feed_name)
            / f'feed_pages-{date_second.isoformat()}.csv'
        )

    def output(self):
        return luigi.LocalTarget(
            self.get_output_path(
                self.data_path, self.feed_name, self.date_second
            )
        )

    @classmethod
    def download_feed(
        cls,
        data_path: str,
        feed_name: str,
        feed_url: str,
        date_second: datetime.datetime,
    ) -> List[str]:
        @csv_cache(cls.get_feed_path(data_path, feed_name, date_second))
        def download_feed_with_cache():
            if not feed_url:
                return []
            page_urls = download_feed(feed_url)
            return [(page_url,) for page_url in page_urls]

        return [row[0] for row in download_feed_with_cache()]

    def requires(self):
        page_urls = self.download_feed(
            self.data_path, self.feed_name, self.feed_url, self.date_second
        )
        for page_url in page_urls:
            yield DownloadPageText(
                data_path=self.data_path,
                feed_name=self.feed_name,
                page_url=page_url,
            )

    def run(self):
        with self.output().open('w') as f:
            f.write('')


class DownloadFeeds(luigi.Task):
    data_path = luigi.Parameter()
    feeds = luigi.ListParameter()
    date_second = luigi.DateSecondParameter(default=datetime.datetime.now())

    @staticmethod
    def get_output_path(data_path: str, date_second: datetime.date) -> Path:
        return (
            Path(data_path) / f'all_downloaded-{date_second.isoformat()}.txt'
        )

    def output(self):
        return luigi.LocalTarget(
            self.get_output_path(self.data_path, self.date_second)
        )

    def requires(self):
        return (
            DownloadFeedPages(
                data_path=self.data_path,
                feed_name=feed['name'],
                feed_url=feed['url'],
                date_second=self.date_second,
            )
            for feed in self.feeds
            if feed.get('name')
        )

    def run(self):
        with self.output().open('w') as f:
            f.write('')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--data', help='Data path', default='./data')
    parser.add_argument(
        '-c', '--config', help='Configuration file path', required=True
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true', help='Enable debugging output'
    )
    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(
            stream=sys.stderr, level=logging.INFO, format='%(message)s'
        )
    with open(args.config, 'r') as f:
        config = json.load(f)
    luigi.build(
        [DownloadFeeds(data_path=args.data, feeds=config['feeds'])],
        workers=2,
        local_scheduler=True,
        log_level='INFO',
    )


if __name__ == '__main__':
    main()
