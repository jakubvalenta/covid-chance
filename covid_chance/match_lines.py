import logging
from typing import List, Sequence, Tuple

from sqlalchemy import select
from sqlalchemy.orm import scoped_session

from covid_chance.model import Page, PageLine, count, create_session_factory
from covid_chance.utils.concurrent_utils import process_iterable_in_thread_pool
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


def match_pages(
    pages: List[Tuple[Page]],
    session_factory,
    keyword_lists: Sequence[Sequence[str]],
    param_hash: str,
):
    session = scoped_session(session_factory)
    for (page,) in pages:
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


def main(config: dict):
    keyword_lists = config['match_lines']['keyword_lists']
    param_hash = hashobj(keyword_lists)

    session_factory = create_session_factory(config['db']['url'])
    session = scoped_session(session_factory)

    partition_size = config['db'].get('partition_size', 1000)
    stmt = select(Page).execution_options(yield_per=partition_size)
    pages = session.execute(stmt).partitions(partition_size)

    process_iterable_in_thread_pool(
        pages,
        match_pages,
        session_factory=session_factory,
        keyword_lists=keyword_lists,
        param_hash=param_hash,
    )
    session.close()
