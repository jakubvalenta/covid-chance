import datetime
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence, Tuple
from urllib.parse import urlsplit

import regex
import requests
from bs4 import BeautifulSoup
from PIL import Image

from covid_chance.download_pages import download_page
from covid_chance.model import (
    ExportedTweet, PostedTweet, Tweet, TweetReviewStatus, count,
    create_session,
)
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
    html = download_page_html(str(cache_path / 'pages'), tweet.url)
    if not html:
        return None
    page_meta = get_page_meta(html)
    if not page_meta or page_meta.is_empty():
        logger.warning('No image found for %s', tweet.url)
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
    domain = urlsplit(tweet.url).netloc
    domain = regex.sub(r'^www\.', '', domain)
    if tweet.inserted:
        timestamp: Optional[datetime.datetime] = tweet.inserted
    else:
        timestamp = None
    return ExportedTweet(
        url=tweet.url,
        # Use edited or parsed, becase tweet.text the final tweet including
        # hashtags etc
        text=tweet.edited or tweet.parsed,
        title=page_meta.title,
        description=page_meta.description,
        image_path=str(image_path),
        domain=domain,
        timestamp=timestamp,
    )


def main(config: dict, cache_path: Path, approved: bool = False):
    session = create_session(config['db']['url'])
    if approved:
        tweets = session.query(Tweet).filter(
            Tweet.status == TweetReviewStatus.approved
        )
    else:
        tweets = session.query(PostedTweet).all()
    for tweet in tweets:
        exported_tweet = print_export_tweet(cache_path, tweet)
        if exported_tweet and not count(
            session.query(ExportedTweet).filter(
                ExportedTweet.text == exported_tweet.text
            )
        ):
            session.add(exported_tweet)
            session.flush()
    session.commit()
    session.close()
