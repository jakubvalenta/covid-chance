import re
from hashlib import sha256

import regex


def safe_filename(s: str, max_length: int = 64) -> str:
    short_hash = sha256(s.encode()).hexdigest()[:7]
    safe_str = re.sub(r'[^A-Za-z0-9_\-\.]', '_', s).strip('_')[:max_length]
    return f'{safe_str}--{short_hash}'


def safe_timestamp(timestamp) -> str:
    return (
        regex.sub(
            '[^A-Za-z0-9_-]',
            '-',
            timestamp.replace(tzinfo=None).isoformat(),
        )
        if timestamp
        else ''
    )
