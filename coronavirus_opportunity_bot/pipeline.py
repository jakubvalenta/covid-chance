import logging
import os
import sys
from pathlib import Path

import luigi

from coronavirus_opportunity_bot.download import (
    download_feed, download_page, get_page_text,
)
from coronavirus_opportunity_bot.file_utils import (
    csv_cache, read_first_line, safe_filename,
)


class DownloadPage(luigi.Task):
    data_path = luigi.Parameter()
    feed_name = luigi.Parameter()
    page_url = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
            Path(self.data_path)
            / self.feed_name
            / safe_filename(self.page_url)
            / 'page.html'
        )

    def run(self):
        with self.output().open('w') as f:
            html = download_page(self.page_url)
            f.write(html)


class GetPageText(luigi.Task):
    data_path = luigi.Parameter()
    feed_name = luigi.Parameter()
    page_url = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
            Path(self.data_path)
            / self.feed_name
            / safe_filename(self.page_url)
            / 'page.txt'
        )

    def requires(self):
        return DownloadPage(
            data_path=Path(self.data_path),
            feed_name=self.feed_name,
            page_url=self.page_url,
        )

    def run(self):
        with self.input().open('r') as f:
            html = f.read()

        with self.output().open('w') as f:
            text = get_page_text(html)
            f.write(text)


class AnalyzePage(luigi.Task):
    data_path = luigi.Parameter()
    feed_name = luigi.Parameter()
    page_url = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
            Path(self.data_path)
            / self.feed_name
            / safe_filename(self.page_url)
            / 'analysis.csv'
        )

    def requires(self):
        return GetPageText(
            data_path=Path(self.data_path),
            feed_name=self.feed_name,
            page_url=self.page_url,
        )

    def run(self):
        pass


class AnalyzeFeed(luigi.Task):
    data_path = luigi.Parameter()
    feed_name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
            Path(self.data_path) / self.feed_name / 'analysis.csv'
        )

    @staticmethod
    def download_feed(data_path: str, feed_name: str):
        @csv_cache(Path(data_path) / feed_name / 'feed.csv')
        def download_feed_with_cache():
            feed_url = read_first_line(Path(data_path) / feed_name / 'url.txt')
            return [(page_url,) for page_url in download_feed(feed_url)]

        return [row[0] for row in download_feed_with_cache()]

    def requires(self):
        page_urls = self.download_feed(self.data_path, self.feed_name)
        for page_url in page_urls:
            yield AnalyzePage(
                data_path=self.data_path,
                feed_name=self.feed_name,
                page_url=page_url,
            )

    def run(self):
        pass


class CreateFeedTweets(luigi.Task):
    data_path = luigi.Parameter()
    feed_name = luigi.Parameter()
    auth_token = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
            Path(self.data_path) / self.feed_name / 'tweets.csv'
        )

    def requires(self):
        return AnalyzeFeed(data_path=self.data_path, feed_name=self.feed_name)

    def run(self):
        auth_token = self.auth_token or os.environ.get('AUTH_TOKEN')
        if not auth_token:
            pass
            # raise ValueError('Auth token is not defined')


class CreateTweets(luigi.Task):
    data_path = luigi.Parameter(default='./data')
    auth_token = luigi.Parameter(default='')
    verbose = luigi.BoolParameter(default=False)

    def requires(self):
        for path in Path(self.data_path).glob('*/url.txt'):
            yield CreateFeedTweets(
                data_path=self.data_path,
                feed_name=path.parent.name,
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
            feed_name = url_path.parent.name
            feed_dir = Path(self.data_path) / feed_name
            (feed_dir / 'analysis.csv').unlink(missing_ok=True)
            (feed_dir / 'tweets.csv').unlink(missing_ok=True)
            page_urls = AnalyzeFeed.download_feed(self.data_path, feed_name)
            for page_url in page_urls:
                page_dir = feed_dir / safe_filename(page_url)
                (page_dir / 'page.txt').unlink(missing_ok=True)
                (page_dir / 'analysis.csv').unlink(missing_ok=True)
