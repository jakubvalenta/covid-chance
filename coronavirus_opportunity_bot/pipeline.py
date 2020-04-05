import logging
import os
import sys
from pathlib import Path

import luigi

from coronavirus_opportunity_bot.download import download_feed, download_page
from coronavirus_opportunity_bot.file_utils import (
    csv_cache, read_first_line, safe_filename,
)


class DownloadPage(luigi.Task):
    data_path = luigi.Parameter()
    name = luigi.Parameter()
    page_url = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
            Path(self.data_path)
            / self.name
            / safe_filename(self.page_url)
            / 'page.html'
        )

    def run(self):
        with self.output().open('w') as f:
            html = download_page(self.page_url)
            f.write(html)


class AnalyzePage(luigi.Task):
    data_path = luigi.Parameter()
    name = luigi.Parameter()
    page_url = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
            Path(self.data_path)
            / self.name
            / safe_filename(self.page_url)
            / 'analysis.csv'
        )

    def requires(self):
        return DownloadPage(
            data_path=Path(self.data_path),
            name=self.name,
            page_url=self.page_url,
        )

    def run(self):
        with self.output().open('w') as f:
            f.write('')


class AnalyzeFeed(luigi.Task):
    data_path = luigi.Parameter()
    name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
            Path(self.data_path) / self.name / 'analysis.csv'
        )

    @staticmethod
    def download_feed(data_path: str, name: str):
        @csv_cache(Path(data_path) / name / 'feed.csv')
        def download_feed_with_cache():
            feed_url = read_first_line(Path(data_path) / name / 'url.txt')
            return [(page_url,) for page_url in download_feed(feed_url)]

        return [row[0] for row in download_feed_with_cache()]

    def requires(self):
        page_urls = self.download_feed(self.data_path, self.name)
        for page_url in page_urls:
            yield AnalyzePage(
                data_path=self.data_path, name=self.name, page_url=page_url
            )

    def run(self):
        with self.output().open('w') as f:
            f.write('')


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
            f.write('')


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
        for url_path in Path(self.data_path).glob('*/url.txt'):
            name = url_path.parent.name
            page_urls = AnalyzeFeed.download_feed(self.data_path, name)
            for page_url in page_urls:
                pass
            (Path(self.data_path) / name / 'analysis.csv').unlink(
                missing_ok=True
            )
            (Path(self.data_path) / name / 'tweets.csv').unlink(
                missing_ok=True
            )
