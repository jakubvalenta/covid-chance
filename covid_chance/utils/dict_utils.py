from typing import Any, Callable, Optional, Sequence


def deep_get(
    dictionary: Any,
    path: Sequence[str],
    default: Any = None,
    process: Optional[Callable[[Any], Any]] = None,
) -> Any:
    acc = dictionary
    for key in path:
        try:
            acc = acc[key]
        except (IndexError, KeyError, TypeError):
            return default
    if process is not None:
        return process(acc)
    return acc
