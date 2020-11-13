import concurrent.futures
import logging
from typing import Sequence

from sqlalchemy.orm.session import Session

from covid_chance.model import Page, PageLine, create_session
from covid_chance.utils.hash_utils import hashobj

logger = logging.getLogger(__name__)


def contains_any_keyword(s: str, keyword_list: Sequence[str]) -> bool:
    s_lower = s.lower()
    for keyword in keyword_list:
        if keyword in s_lower:
            return True
    return False


def contains_keyword_from_each_list(
    s: str, keyword_lists: Sequence[Sequence[str]]
):
    for keyword_list in keyword_lists:
        if not contains_any_keyword(s, keyword_list):
            return False
    return True


def match_page_lines(
    session: Session,
    i: int,
    page: Page,
    keyword_lists: Sequence[Sequence[str]],
    param_hash: str,
):
    if (
        session.query(PageLine)
        .filter(url=page.url, param_hash=param_hash)
        .exists()
    ):
        return
    logger.info('%d Matched %s', i, page.url)
    inserted = False
    for line in page.text.splitlines():
        if contains_keyword_from_each_list(line, keyword_lists):
            page_line = PageLine(
                url=page.url, line=line.strip(), param_hash=param_hash
            )
            session.add(page_line)
            inserted = True
    if not inserted:
        page_line = PageLine(url=page.url, param_hash=param_hash)
        session.add(page_line)
    session.commit()


def log_future_exception(future: concurrent.futures.Future):
    try:
        future.result()
    except Exception as e:
        logger.error('Exception: %s', e)


def main(config: dict):
    keyword_lists = config['match_lines']['keyword_lists']
    session = create_session(config['db']['url'])
    param_hash = hashobj(keyword_lists)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for i, page in enumerate(session.query(Page).all()):
            future = executor.submit(
                match_page_lines,
                session,
                i,
                page,
                keyword_lists,
                param_hash,
            )
            future.add_done_callback(log_future_exception)
