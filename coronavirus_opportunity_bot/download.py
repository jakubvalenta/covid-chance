import logging
from typing import List

import requests

logger = logging.getLogger(__name__)


def download_page(url: str) -> str:
    logger.info('Downloading page %s', url)
    res = requests.get(url)
    res.raise_for_status()
    return res.text


def download_feed(url: str) -> List[str]:
    logger.info('Downloading feed %s', url)
    return []
