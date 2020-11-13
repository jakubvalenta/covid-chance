import concurrent.futures
import logging
from typing import Iterator

import regex
from sqlalchemy.orm.session import Session

from covid_chance.model import PageLine, ParsedPageLine, create_session
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
    session: Session,
    i: int,
    page_line: PageLine,
    pattern: str,
    rx: regex.Regex,
):
    param_hash = hashobj(pattern)
    if (
        session.query(ParsedPageLine)
        .filter(
            ParsedPageLine.line == page_line.line,
            ParsedPageLine.param_hash == param_hash,
        )
        .exists()
    ):
        return
    logger.info('%d Parsed %s', i, page_line.url)
    for parsed in parse_line(rx, page_line.line):
        parsed_page_line = ParsedPageLine.from_page_line(page_line, parsed)
        session.add(parsed_page_line)
    session.commit()


def main(config: dict):
    pattern = config['parse_lines']['pattern']
    rx = regex.compile(pattern)
    session = create_session(config['db']['url'])
    page_lines = session.query(PageLine).filter(PageLine.line != '')
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                parse_page_line,
                session,
                i,
                page_line,
                pattern,
                rx,
            )
            for i, page_line in enumerate(page_lines)
        ]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error('Exception: %s', e)
    session.commit()
