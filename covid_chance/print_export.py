import argparse
import datetime
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Sequence, Tuple
from urllib.parse import urlsplit

import psycopg2
import psycopg2.errorcodes
import regex
import requests
from bs4 import BeautifulSoup
from PIL import Image

from covid_chance.download_pages import download_page
from covid_chance.post_tweet import (
    Tweet, read_approved_tweets, read_posted_tweets,
)
from covid_chance.utils.db_utils import db_connect, db_insert, db_select
from covid_chance.utils.download_utils import simplify_url
from covid_chance.utils.file_utils import safe_filename
from covid_chance.utils.hash_utils import md5str

logger = logging.getLogger(__name__)


@dataclass
class PageMeta:
    title: str
    description: str
    image_url: str

    def is_empty(self) -> bool:
        return not (self.title and self.description and self.image_url)


@dataclass
class ExportedTweet:
    page_url: str
    text: str
    title: str
    description: str
    image_path: str
    domain: str
    timestamp: Optional[datetime.datetime]

    @property
    def image_path_rel(self) -> str:
        p = Path(self.image_path)
        return str(p.relative_to(p.parents[1]))

    @property
    def timestamp_safe(self) -> str:
        return (
            regex.sub(
                '[^A-Za-z0-9_-]',
                '-',
                self.timestamp.replace(tzinfo=None).isoformat(),
            )
            if self.timestamp
            else ''
        )

    @property
    def text_hash(self) -> str:
        return md5str(self.text)[:7]

    def to_dict(self) -> Dict[str, Any]:
        return {
            'text': self.text,
            'text_hash': self.text_hash,
            'title': self.title,
            'description': self.description,
            'timestamp': self.timestamp,
            'timestamp_safe': self.timestamp_safe,
            'image_path': self.image_path,
            'domain': self.domain,
        }


def create_table(conn, table: str):
    with conn.cursor() as cur:
        try:
            cur.execute(
                f'''
CREATE TABLE {table} (
  url text,
  text text,
  title text,
  description text,
  image_path text,
  domain text,
  timestamp timestamp,
  inserted timestamp DEFAULT NOW()
);
'''
            )
        except psycopg2.ProgrammingError as e:
            if e.pgcode == psycopg2.errorcodes.DUPLICATE_TABLE:
                pass
            else:
                raise
        conn.commit()


def read_exported_tweets(
    conn,
    table: str,
    default_tz: datetime.tzinfo,
) -> Iterator[ExportedTweet]:
    with conn.cursor() as cur:
        cur.execute(
            'SELECT '
            'url, text, title, description, image_path, domain, timestamp '
            f"FROM {table};",
        )
        for (
            url,
            text,
            title,
            description,
            image_path,
            domain,
            timestamp,
        ) in cur:
            yield ExportedTweet(
                page_url=url,
                text=text,
                title=title,
                description=description,
                image_path=image_path,
                domain=domain,
                timestamp=timestamp.replace(tzinfo=default_tz),
            )


def download_page_html(cache_path: str, page_url: str) -> Optional[str]:
    path = (
        Path(cache_path)
        / safe_filename(simplify_url(page_url))
        / 'page_content.html'
    )
    if path.is_file():
        return path.read_text()
    try:
        logger.info('Downloading %s', page_url)
        html = download_page(page_url)
    except Exception as e:
        logger.error('Failed to download %s: %s', page_url, e)
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html)
    return html


def get_page_meta(
    html: str, invalid_content: Sequence = ('null')
) -> Optional[PageMeta]:
    if not html:
        return None
    soup = BeautifulSoup(html, 'lxml')
    el_title = soup.find('title')
    title = el_title.string
    el_description = (
        soup.find('meta', attrs={'name': 'description'})
        or soup.find('meta', attrs={'name': 'Description'})
        or soup.find('meta', property='og:description')
    )
    if el_description:
        description = el_description['content']
    else:
        description = ''
    el_og_image = soup.find('meta', property='og:image')
    if el_og_image and el_og_image['content'] not in invalid_content:
        image_url = el_og_image['content']
    else:
        image_url = ''
    return PageMeta(title=title, description=description, image_url=image_url)


def identify_image_ext(url: str) -> str:
    u = urlsplit(url)
    return Path(u.path).suffix


def download_image(
    cache_path: Path,
    url: str,
    placeholder_str: str = 'placeholder',
    timeout: int = 10,
) -> Path:
    if url.startswith('//'):
        url = 'https:' + url
    path = Path(cache_path) / safe_filename(simplify_url(url)) / 'image'
    if not path.is_file():
        logger.info('Downloading image %s', url)
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
        res.raise_for_status()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(res.content)
    return path


def convert_image(path: Path, max_size: Tuple[int, int] = (1200, 630)) -> Path:
    path_out = Path(str(path) + '.jpg')
    if not path_out.exists():
        logger.info('Converting %s to JPEG', path)
        with Image.open(path) as im:
            im = im.convert('RGB')
            im.thumbnail(max_size)
            im.save(path_out, 'JPEG')
    return path_out


def print_export_tweet(
    cache_path: Path, tweet: Tweet
) -> Optional[ExportedTweet]:
    html = download_page_html(str(cache_path / 'pages'), tweet.page_url)
    if not html:
        return None
    page_meta = get_page_meta(html)
    if not page_meta or page_meta.is_empty():
        logger.warning('No image found for %s', tweet.page_url)
        return None
    try:
        orig_image_path = download_image(
            cache_path / 'images', page_meta.image_url
        )
        image_path = convert_image(orig_image_path)
    except Exception as e:
        logger.warning(
            'Failed to download image %s: %s', page_meta.image_url, e
        )
        return None
    domain = urlsplit(tweet.page_url).netloc
    domain = regex.sub(r'^www\.', '', domain)
    if tweet.inserted:
        timestamp: Optional[datetime.datetime] = tweet.inserted
    else:
        timestamp = None
    return ExportedTweet(
        page_url=tweet.page_url,
        text=tweet.text,
        title=page_meta.title,
        description=page_meta.description,
        image_path=str(image_path),
        domain=domain,
        timestamp=timestamp,
    )


def main(config: dict, cache_path: Path, approved: bool = False):
    conn = db_connect(
        database=config['db']['database'],
        user=config['db']['user'],
        password=config['db']['password'],
    )
    with conn:
        table_posted = config['db']['table_posted']
        table_reviewed = config['db']['table_reviewed']
        table_exported = config['db']['table_print_export']
        create_table(conn, table_exported)

        if approved:
            tweets = list(read_approved_tweets(conn, table_reviewed))
        else:
            tweets = list(read_posted_tweets(conn, table_posted))

        for i, tweet in enumerate(tweets):
            exported_tweet = print_export_tweet(cache_path, tweet)
            if not exported_tweet:
                continue
            if db_select(conn, table_exported, text=exported_tweet.text):
                continue
            db_insert(
                conn,
                table_exported,
                url=exported_tweet.page_url,
                text=exported_tweet.text,
                title=exported_tweet.title,
                description=exported_tweet.description,
                image_path=exported_tweet.image_path,
                domain=exported_tweet.domain,
                timestamp=exported_tweet.timestamp,
            )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--cache', help='Cache directory path', default='./cache'
    )
    parser.add_argument(
        '-c', '--config', help='Configuration file path', required=True
    )
    parser.add_argument(
        '-a',
        '--approved',
        action='store_true',
        help='Export all approved tweets (instead of only the posted ones)',
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
    cache_path = Path(args.cache)
    main(config, cache_path, approved=args.approved)
