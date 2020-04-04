import csv
import logging
import os
import re
import sys
from pathlib import Path

import luigi

from coronavirus_opportunity_bot.download import download_feed, download_page


def url_to_dirname(self) -> str:
    return re.sub(r'[^A-Za-z0-9_\-\.]', '_', self.url)


class DownloadPage(luigi.Task):
    data_path = luigi.Parameter()
    name = luigi.Parameter()
    page_url = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
            Path(self.data_path)
            / self.name
            / url_to_dirname(self.page_url)
            / 'page.html'
        )

    def run(self):
        download_page(self.url, self.output().path)


class AnalyzePage(luigi.Task):
    data_path = luigi.Parameter()
    name = luigi.Parameter()
    page_url = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
            Path(self.data_path)
            / self.name
            / url_to_dirname(self.page_url)
            / 'analysis.csv'
        )

    def requires(self):
        return DownloadPage(
            data_path=Path(self.data_path),
            name=self.name,
            page_url=self.page_url,
        )


class AnalyzeFeed(luigi.Task):
    data_path = luigi.Parameter()
    name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
            Path(self.data_path) / self.name / 'analysis.csv'
        )

    def requires(self):
        feed_path = Path(self.data_path) / self.name / 'feed.csv'
        if feed_path.exists():
            with feed_path.open('r') as f:
                reader = csv.reader(f)
                page_urls = list(reader)
        else:
            feed_url_path = Path(self.data_path) / self.name / 'url.txt'
            with feed_url_path.open('r') as f:
                feed_url = f.readline()
            page_urls = download_feed(feed_url)
            with feed_path.open('w') as f:
                writer = csv.writer(f)
                writer.writerows(page_urls)
        for page_url in page_urls:
            yield AnalyzePage(
                data_path=self.data_path, name=self.name, page_url=page_url
            )

    def run(self):
        with self.output().open('w') as f:
            print('', file=f)


class CreateFeedTweets(luigi.Task):
    data_path = luigi.Parameter()
    name = luigi.Parameter()
    auth_token = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
            Path(self.data_path) / self.name / 'tweets.csv'
        )

    def requires(self):
        return AnalyzeFeed(data_path=self.data_path, name=self.name)

    def run(self):
        auth_token = self.auth_token or os.environ.get('AUTH_TOKEN')
        if not auth_token:
            pass
            # raise ValueError('Auth token is not defined')
        with self.output().open('w') as f:
            print('', file=f)


class CreateTweets(luigi.Task):
    data_path = luigi.Parameter(default='./data')
    auth_token = luigi.Parameter(default='')
    verbose = luigi.BoolParameter(default=False)

    def requires(self):
        for path in Path(self.data_path).glob('*/url.txt'):
            yield CreateFeedTweets(
                data_path=self.data_path,
                name=path.parent.name,
                auth_token=self.auth_token,
            )

    def run(self):
        if self.verbose:
            logging.basicConfig(
                stream=sys.stderr, level=logging.INFO, format='%(message)s'
            )


class CleanAnalysis(luigi.Task):
    data_path = luigi.Parameter(default='./data')

    def run(self):
        pass
