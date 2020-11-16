import logging
from typing import Iterator, List, Tuple

import regex
from sqlalchemy import select
from sqlalchemy.orm import scoped_session

from covid_chance.model import (
    PageLine, ParsedPageLine, count, create_session_factory,
)
from covid_chance.utils.concurrent_utils import process_iterable_in_thread_pool
from covid_chance.utils.hash_utils import hashobj

logger = logging.getLogger(__name__)


def searchall(rx: regex.Regex, s: str) -> Iterator:
    m = rx.search(s)
    while m:
        yield m
        m = rx.search(s, pos=m.span()[1])


def parse_line(rx: regex.Regex, line: str) -> Iterator[str]:
    matches = list(searchall(rx, line))
    if matches:
        for m in matches:
            yield m.group('parsed')
    else:
        yield ''


def parse_page_lines(
    page_lines: List[Tuple[PageLine]],
    session_factory,
    pattern: str,
    param_hash: str,
):
    rx = regex.compile(pattern)
    session = scoped_session(session_factory)
    for (page_line,) in page_lines:
        if count(
            session.query(ParsedPageLine).filter(
                ParsedPageLine.line == page_line.line,
                ParsedPageLine.param_hash == param_hash,
            )
        ):
            continue
        logger.info('Parsed %s', page_line.url)
        for parsed in parse_line(rx, page_line.line):
            parsed_page_line = ParsedPageLine(
                url=page_line.url,
                line=page_line.line,
                parsed=parsed,
                param_hash=param_hash,
            )
            session.add(parsed_page_line)
    session.commit()
    session.close()


def main(config: dict):
    pattern = config['parse_lines']['pattern']
    param_hash = hashobj(pattern)

    session_factory = create_session_factory(config['db']['url'])
    session = scoped_session(session_factory)

    partition_size = config['db'].get('partition_size', 1000)
    stmt = (
        select(PageLine)
        .filter(PageLine.line != '')
        .execution_options(yield_per=1000)
    )
    page_lines = session.execute(stmt).partitions(partition_size)

    process_iterable_in_thread_pool(
        page_lines,
        parse_page_lines,
        session_factory=session_factory,
        pattern=pattern,
        param_hash=param_hash,
    )
    session.close()
