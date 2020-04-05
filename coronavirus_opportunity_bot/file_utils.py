import csv
import re
from functools import wraps
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable


def safe_filename(s: str) -> str:
    short_hash = sha256(s.encode()).hexdigest()[:7]
    safe_str = re.sub(r'[^A-Za-z0-9_\-\.]', '_', s)
    return f'{safe_str}--{short_hash}'


def csv_cache(path: Path) -> Callable:
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            if path.is_file():
                with path.open('r') as f:
                    out = list(csv.reader(f))
                return out
            func(*args, **kwargs)
            out = func(*args, **kwargs)
            with path.open('w') as f:
                writer = csv.writer(f)
                writer.writerows(out)
            return out

        return wrapper

    return decorator


def read_first_line(path: Path) -> str:
    with path.open('r') as f:
        res = f.readline()
    return res
