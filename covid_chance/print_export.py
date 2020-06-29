import argparse
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Optional, Sequence, Tuple
from urllib.parse import urlsplit

import requests
from bs4 import BeautifulSoup
from jinja2 import Environment, PackageLoader
from PIL import Image

from covid_chance.download_pages import download_page
from covid_chance.post_tweet import Tweet, read_approved_tweets
from covid_chance.utils.db_utils import db_connect
from covid_chance.utils.download_utils import simplify_url
from covid_chance.utils.file_utils import safe_filename

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
    text: str
    title: str
    description: str
    image_path: str
    approved: Optional[str]


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
    return ExportedTweet(
        text=tweet.text,
        title=page_meta.title,
        description=page_meta.description,
        image_path=str(image_path),
        approved=tweet.inserted,
    )


def render_template(package: Sequence[str], f: IO, **context):
    environment = Environment(loader=PackageLoader(*package[:-1]))
    template = environment.get_template(package[-1])
    stream = template.stream(**context)
    f.writelines(stream)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--cache', help='Cache directory path', default='./cache'
    )
    parser.add_argument(
        '-c', '--config', help='Configuration file path', required=True
    )
    parser.add_argument(
        '-o', '--output', help='Output TeX file path', required=True
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true', help='Enable debugging output'
    )
    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(
            stream=sys.stderr, level=logging.INFO, format='%(message)s'
        )
    cache_path = Path(args.cache)
    with open(args.config, 'r') as f:
        config = json.load(f)
    output_path = Path(args.output)

    conn = db_connect(
        database=config['db']['database'],
        user=config['db']['user'],
        password=config['db']['password'],
    )
    table_reviewed = config['db']['table_reviewed']

    approved_tweets = list(read_approved_tweets(conn, table_reviewed))

    exported_tweets = [
        print_export_tweet(cache_path, tweet)
        for i, tweet in enumerate(approved_tweets)
    ]

    with output_path.open('w') as f:
        render_template(
            ['covid_chance', 'templates', 'print.tex'],
            f,
            tweets=[x for x in exported_tweets if x],
            title=config['print_export']['title'],
            author=config['print_export']['author'],
            year=config['print_export']['year'],
            url=config['print_export']['url'],
        )


if __name__ == '__main__':
    main()
