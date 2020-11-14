import concurrent.futures
import logging
from typing import Iterator, List, Tuple

import regex
from sqlalchemy import select
from sqlalchemy.orm import scoped_session

from covid_chance.model import (
    PageLine, ParsedPageLine, count, create_session_factory,
)
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


def parse_page_line(
    session_factory,
    partition: List[Tuple[PageLine]],
    pattern: str,
    rx: regex.Regex,
    param_hash: str,
):
    session = scoped_session(session_factory)
    for (page_line,) in partition:
        if count(
            session.query(ParsedPageLine).filter(
                ParsedPageLine.line == page_line.line,
                ParsedPageLine.param_hash == param_hash,
            )
        ):
            continue
        logger.info('Parsed %s', page_line.url)
        for parsed in parse_line(rx, page_line.line):
            parsed_page_line = ParsedPageLine.from_page_line(page_line, parsed)
            session.add(parsed_page_line)
    session.commit()
    session.close()


def log_future_exception(future: concurrent.futures.Future):
    try:
        future.result()
    except Exception as e:
        logger.error('Exception: %s', e)


def main(config: dict):
    pattern = config['parse_lines']['pattern']
    rx = regex.compile(pattern)
    param_hash = hashobj(pattern)
    session_factory = create_session_factory(config['db']['url'])
    session = scoped_session(session_factory)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        stmt = (
            select(PageLine)
            .filter(PageLine.line != '')
            .execution_options(yield_per=1000)
        )
        for i, partition in enumerate(session.execute(stmt).partitions(1000)):
            logger.info('Submitting partition %d', i + 1)
            future = executor.submit(
                parse_page_line,
                session_factory,
                partition,
                pattern,
                rx,
                param_hash,
            )
            future.add_done_callback(log_future_exception)
    session.close()
