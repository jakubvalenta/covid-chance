import argparse
import csv
import datetime
import json
import logging
import random
import re
import sys
import time
from pathlib import Path
from typing import Iterator, Sequence, Set, Tuple, Type

import luigi
import requests
from bs4 import (
    BeautifulSoup, CData, Comment, Declaration, Doctype, NavigableString,
    ProcessingInstruction,
)
from bs4.element import Script, Stylesheet, TemplateString

from covid_chance.utils.download_utils import clean_url, simplify_url
from covid_chance.utils.file_utils import safe_filename

logger = logging.getLogger(__name__)


def download_page(
    url: str,
    wait_interval: Tuple[int, int] = (0, 0),
    timeout: int = 10,
    non_recoverable_error_codes: Sequence[int] = (
        requests.codes.not_found,
        requests.codes.legal_reasons,
    ),
) -> str:
    wait = random.randint(*wait_interval)
    logger.info('Downloading page in %ss %s', wait, url)
    if wait:
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
        return ''
    if res.status_code in non_recoverable_error_codes:
        return ''
    res.raise_for_status()
    return res.text


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

    wait_lower = luigi.NumericalParameter(
        var_type=int, min_value=0, max_value=99, significant=False
    )
    wait_upper = luigi.NumericalParameter(
        var_type=int, min_value=5, max_value=99, significant=False
    )
    timeout = luigi.NumericalParameter(
        var_type=int, min_value=0, max_value=99, significant=False
    )

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
            html = download_page(
                self.page_url,
                wait_interval=(self.wait_lower, self.wait_upper),
                timeout=self.timeout,
            )
            f.write(html)


class DownloadPageText(luigi.Task):
    data_path = luigi.Parameter()
    feed_name = luigi.Parameter()
    page_url = luigi.Parameter()

    wait_lower = luigi.NumericalParameter(
        var_type=int, min_value=0, max_value=99, significant=False
    )
    wait_upper = luigi.NumericalParameter(
        var_type=int, min_value=5, max_value=99, significant=False
    )
    timeout = luigi.NumericalParameter(
        var_type=int, min_value=0, max_value=99, significant=False
    )

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
            wait_lower=self.wait_lower,
            wait_upper=self.wait_upper,
            timeout=self.timeout,
        )

    def run(self):
        with self.input().open('r') as f:
            html = f.read()
        with self.output().open('w') as f:
            text = get_page_text(html)
            f.write(text)


class DownloadFeedPages(luigi.WrapperTask):
    data_path = luigi.Parameter()
    feed_name = luigi.Parameter()
    feed_url = luigi.Parameter()
    date_second = luigi.DateSecondParameter()

    limit = luigi.NumericalParameter(var_type=int, min_value=0, max_value=999)
    wait_lower = luigi.NumericalParameter(
        var_type=int, min_value=0, max_value=99, significant=False
    )
    wait_upper = luigi.NumericalParameter(
        var_type=int, min_value=5, max_value=99, significant=False
    )
    timeout = luigi.NumericalParameter(
        var_type=int, min_value=0, max_value=99, significant=False
    )

    def read_page_urls(self) -> Set[str]:
        feed_dir = Path(self.data_path) / safe_filename(self.feed_name)
        page_urls = set()
        paths = sorted(feed_dir.glob('feed_pages*.csv'))
        if self.limit:
            paths = paths[-self.limit :]
        for p in paths:
            with p.open('r') as f:
                for (page_url,) in csv.reader(f):
                    page_urls.add(clean_url(page_url))
        return page_urls

    def print_stats(self, page_urls: Set[str]):
        missing = 0
        for page_url in page_urls:
            page_content_path = (
                Path(self.data_path)
                / safe_filename(self.feed_name)
                / safe_filename(simplify_url(page_url))
                / 'page_content.html'
            )
            if not page_content_path.exists():
                missing += 1
        if missing:
            logger.info(
                '"%s",%d / %d', self.feed_name, missing, len(page_urls)
            )
        return page_urls

    def requires(self):
        page_urls = self.read_page_urls()
        self.print_stats(page_urls)
        for page_url in page_urls:
            yield DownloadPageText(
                data_path=self.data_path,
                feed_name=self.feed_name,
                page_url=page_url,
                wait_lower=self.wait_lower,
                wait_upper=self.wait_upper,
                timeout=self.timeout,
            )


class DownloadPages(luigi.WrapperTask):
    data_path = luigi.Parameter()
    feeds = luigi.ListParameter()
    date_second = luigi.DateSecondParameter(default=datetime.datetime.now())

    limit = luigi.NumericalParameter(var_type=int, min_value=0, max_value=999)
    wait_lower = luigi.NumericalParameter(
        var_type=int, min_value=0, max_value=99, significant=False
    )
    wait_upper = luigi.NumericalParameter(
        var_type=int, min_value=5, max_value=99, significant=False
    )
    timeout = luigi.NumericalParameter(
        var_type=int, min_value=0, max_value=99, significant=False
    )

    def requires(self):
        return (
            DownloadFeedPages(
                data_path=self.data_path,
                feed_name=feed['name'],
                feed_url=feed['url'],
                date_second=self.date_second,
                limit=self.limit,
                wait_lower=self.wait_lower,
                wait_upper=self.wait_upper,
                timeout=self.timeout,
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
            DownloadPages(
                data_path=args.data,
                feeds=config['feeds'],
                limit=config['download_num_latest_feed_pages_csvs'],
                wait_lower=config['download_wait_lower'],
                wait_upper=config['download_wait_upper'],
                timeout=config['download_page_timeout'],
            )
        ],
        workers=1,
        local_scheduler=True,
        parallel_scheduling=True,
        log_level='WARNING',
    )


if __name__ == '__main__':
    main()
