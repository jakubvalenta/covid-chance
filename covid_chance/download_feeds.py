import argparse
import csv
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
import luigi.contrib.postgres
import requests
from bs4 import (
    BeautifulSoup, CData, Comment, Declaration, Doctype, NavigableString,
    ProcessingInstruction,
)
from bs4.element import Script, Stylesheet, TemplateString

from covid_chance.utils.file_utils import safe_filename

logger = logging.getLogger(__name__)


def clean_url(
    url: str,
    remove_keys: Sequence[str] = (
        'fbclid',
        'ito',
        'ns_campaign',
        'ns_mchannel',
        'source',
        'utm_campaign',
        'utm_medium',
        'utm_source',
        'via',
    ),
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
    res = requests.get(
        url,
        headers={
            'User-Agent': (
                'Mozilla/5.0 (X11; Linux x86_64; rv:75.0) '
                'Gecko/20100101 Firefox/75.0'
            )
        },
    )
    if res.status_code == requests.codes.not_found:
        return ''
    res.raise_for_status()
    return res.text


def download_feed(url: str) -> List[str]:
    logger.info('Downloading feed %s', url)
    # Fetch the feed content using requests, because feedparser seems to have
    # some trouble with the Basic Auth -- the feed object contains an error.
    r = requests.get(url)
    feed = feedparser.parse(r.text)
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

    def output(self):
        return luigi.LocalTarget(
            Path(self.data_path)
            / safe_filename(self.feed_name)
            / safe_filename(simplify_url(self.page_url))
            / 'page_url.txt'
        )

    def run(self):
        with self.output().open('w') as f:
            print(self.page_url, file=f)


class DownloadPageHTML(luigi.Task):
    data_path = luigi.Parameter()
    feed_name = luigi.Parameter()
    page_url = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
            Path(self.data_path)
            / safe_filename(self.feed_name)
            / safe_filename(simplify_url(self.page_url))
            / 'page_content.html'
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

    def output(self):
        return luigi.LocalTarget(
            Path(self.data_path)
            / safe_filename(self.feed_name)
            / safe_filename(simplify_url(self.page_url))
            / 'page_content.txt'
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


class SavePageText(luigi.contrib.postgres.CopyToTable):
    data_path = luigi.Parameter()
    feed_name = luigi.Parameter()
    page_url = luigi.Parameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    columns = [('url', 'TEXT'), ('text', 'TEXT')]

    @property
    def update_id(self):
        return self.page_url

    def requires(self):
        return DownloadPageText(
            data_path=self.data_path,
            feed_name=self.feed_name,
            page_url=self.page_url,
        )

    def rows(self):
        with self.input().open('r') as f:
            text = f.read()
        yield (self.page_url, text)


class DownloadFeedPages(luigi.WrapperTask):
    data_path = luigi.Parameter()
    feed_name = luigi.Parameter()
    feed_url = luigi.Parameter()
    date_second = luigi.DateSecondParameter()

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    def read_manual_page_urls(self) -> List[str]:
        feed_manual_path = (
            Path(self.data_path)
            / safe_filename(self.feed_name)
            / 'feed_pages_manual.csv'
        )
        if not feed_manual_path.is_file():
            return []
        with feed_manual_path.open('r') as f:
            page_urls = [row[0] for row in csv.reader(f)]
        return page_urls

    def download_feed_page_urls(self) -> List[str]:
        feed_path = (
            Path(self.data_path)
            / safe_filename(self.feed_name)
            / f'feed_pages-{self.date_second.isoformat()}.csv'
        )
        if feed_path.is_file():
            with feed_path.open('r') as f:
                page_urls = [row[0] for row in csv.reader(f)]
        elif self.feed_url:
            page_urls = download_feed(self.feed_url)
            feed_path.parent.mkdir(parents=True, exist_ok=True)
            with feed_path.open('w') as f:
                writer = csv.writer(f, lineterminator='\n')
                writer.writerows(page_urls)
        return page_urls

    def requires(self):
        return (
            SavePageText(
                data_path=self.data_path,
                feed_name=self.feed_name,
                page_url=page_url,
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                table=self.table,
            )
            for page_url in (
                self.read_manual_page_urls() + self.download_feed_page_urls()
            )
        )


class DownloadFeeds(luigi.WrapperTask):
    data_path = luigi.Parameter()
    feeds = luigi.ListParameter()
    date_second = luigi.DateSecondParameter(default=datetime.datetime.now())

    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    def requires(self):
        return (
            DownloadFeedPages(
                data_path=self.data_path,
                feed_name=feed['name'],
                feed_url=feed['url'],
                date_second=self.date_second,
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                table=self.table,
            )
            for feed in self.feeds
            if feed.get('name')
        )


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
        [
            DownloadFeeds(
                data_path=args.data,
                feeds=config['feeds'],
                host=config['db']['host'],
                database=config['db']['database'],
                user=config['db']['user'],
                password=config['db']['password'],
                table=config['db']['table_pages'],
            )
        ],
        workers=1,
        local_scheduler=True,
        parallel_scheduling=True,
        log_level='INFO',
    )


if __name__ == '__main__':
    main()
