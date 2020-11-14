import concurrent.futures
import logging
from typing import List, Sequence, Tuple

from sqlalchemy import select
from sqlalchemy.orm import scoped_session

from covid_chance.model import Page, PageLine, count, create_session_factory
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
    session_factory,
    partition: List[Tuple[Page]],
    keyword_lists: Sequence[Sequence[str]],
    param_hash: str,
):
    session = scoped_session(session_factory)
    for (page,) in partition:
        if count(
            session.query(PageLine).filter(
                PageLine.url == page.url, PageLine.param_hash == param_hash
            )
        ):
            continue
        logger.info('Matched %s', page.url)
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
    session.close()


def log_future_exception(future: concurrent.futures.Future):
    try:
        future.result()
    except Exception as e:
        logger.error('Exception: %s', e)


def main(config: dict):
    keyword_lists = config['match_lines']['keyword_lists']
    param_hash = hashobj(keyword_lists)
    session_factory = create_session_factory(config['db']['url'])
    session = scoped_session(session_factory)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        stmt = select(Page).execution_options(yield_per=1000)
        for i, partition in enumerate(session.execute(stmt).partitions(1000)):
            logger.info('Submitting partition %d', i + 1)
            future = executor.submit(
                match_page_lines,
                session_factory,
                partition,
                keyword_lists,
                param_hash,
            )
            future.add_done_callback(log_future_exception)
    session.close()
