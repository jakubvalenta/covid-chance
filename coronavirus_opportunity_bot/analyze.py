from typing import IO, Iterable, Iterator, Sequence, Tuple

import regex


def filter_lines(f: IO, keywords: Sequence[str]) -> Iterator[str]:
    r = regex.compile(r'\L<keywords>', keywords=keywords)
    return (line.strip() for line in f if r.search(line))


def parse_lines(
    lines: Iterable[str], pattern: str
) -> Iterator[Tuple[str, str]]:
    r = regex.compile(pattern)
    for line in lines:
        m = r.search(line)
        if m:
            parsed = m.group('parsed')
        else:
            parsed = ''
        yield line, parsed
