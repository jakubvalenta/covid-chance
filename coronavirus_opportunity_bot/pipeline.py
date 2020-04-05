import logging
import os
import sys
from pathlib import Path
from string import Template

import luigi

from coronavirus_opportunity_bot.analyze import filter_lines, parse_lines
from coronavirus_opportunity_bot.download import (
    download_feed, download_page, get_page_text,
)
from coronavirus_opportunity_bot.file_utils import (
    csv_cache, read_csv_dict, read_first_line, safe_filename, write_csv_dict,
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


class AnalyzePage(luigi.Task):
    data_path = luigi.Parameter()
    feed_name = luigi.Parameter()
    page_url = luigi.Parameter()
    keywords = luigi.ListParameter()
    pattern = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(
            Path(self.data_path)
            / self.feed_name
            / safe_filename(self.page_url)
            / 'analysis.csv'
        )

    def requires(self):
        return GetPageText(
            data_path=self.data_path,
            feed_name=self.feed_name,
            page_url=self.page_url,
        )

    def run(self):
        with self.input().open('r') as f:
            lines = filter_lines(f, self.keywords)
            analysis = [
                {'line': line, 'parsed': parsed}
                for line, parsed in parse_lines(lines, self.pattern)
            ]
        with self.output().open('w') as f:
            write_csv_dict(analysis, f, ['line', 'parsed'])


class AnalyzeFeed(luigi.Task):
    data_path = luigi.Parameter()
    feed_name = luigi.Parameter()
    keywords = luigi.ListParameter()
    pattern = luigi.Parameter()

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

    def run(self):
        page_urls = self.download_feed(self.data_path, self.feed_name)
        joined_analysis = []
        for page_url in page_urls:
            page_input = yield AnalyzePage(
                data_path=self.data_path,
                feed_name=self.feed_name,
                page_url=page_url,
                keywords=self.keywords,
                pattern=self.pattern,
            )
            with page_input.open('r') as f:
                for page_analysis in read_csv_dict(f):
                    joined_analysis.append({'url': page_url, **page_analysis})
        with self.output().open('w') as f:
            write_csv_dict(joined_analysis, f, ['url', 'line', 'parsed'])


class CreateTweets(luigi.Task):
    keywords = luigi.ListParameter()
    pattern = luigi.Parameter()
    template = luigi.Parameter()
    data_path = luigi.Parameter(default='./data')
    auth_token = luigi.Parameter(default='')
    verbose = luigi.BoolParameter(default=False)

    def requires(self):
        for path in Path(self.data_path).glob('*/url.txt'):
            yield AnalyzeFeed(
                data_path=self.data_path,
                feed_name=path.parent.name,
                keywords=self.keywords,
                pattern=self.pattern,
            )

    def run(self):
        if self.verbose:
            logging.basicConfig(
                stream=sys.stderr, level=logging.INFO, format='%(message)s'
            )
        auth_token = self.auth_token or os.environ.get('AUTH_TOKEN')
        if not auth_token:
            pass
            # raise ValueError('Auth token is not defined')
        tweets = {}
        tweet_template = Template(self.template)
        for feed_input in self.input():
            with feed_input.open('r') as f:
                feed_analysis = list(read_csv_dict(f))
            for page_analysis in feed_analysis:
                if (
                    page_analysis['parsed']
                    and page_analysis['url'] not in tweets
                ):
                    tweets[page_analysis['url']] = tweet_template.substitute(
                        parsed=page_analysis['parsed'],
                        url=page_analysis['url'],
                    )
        for tweet_text in tweets.values():
            print(tweet_text)

    def complete(self):
        return False


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
                # (page_dir / 'page.txt').unlink(missing_ok=True)
                (page_dir / 'analysis.csv').unlink(missing_ok=True)
