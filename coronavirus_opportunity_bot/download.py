import logging
from pathlib import Path
from typing import List

import requests

logger = logging.getLogger(__name__)


def download_page(url: str, path: str):
    logger.info('Downloading page %s', url)
    res = requests.get(url)
    res.raise_for_status()
    Path(path).write_text(res.text)


def download_feed(url: str) -> List[str]:
    logger.info('Downloading feed %s', url)
    return []
