import concurrent.futures
import logging
import os
from typing import Callable, Iterable

logger = logging.getLogger(__name__)


def log_future_exception(future: concurrent.futures.Future):
    try:
        future.result()
    except Exception as e:
        logger.error('Exception: %s', e)


def process_iterable_in_thread_pool(
    iterable: Iterable,
    func: Callable,
    max_futures=((os.cpu_count() or 1) + 4) * 10,
    *args,
    **kwargs,
):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for i, item in enumerate(iterable):
            logger.info('Submitting future %d', i + 1)
            future = executor.submit(func, item, *args, **kwargs)
            future.add_done_callback(log_future_exception)
            futures.append(future)
            if (i + 1) % max_futures == 0:
                logger.info('Waiting for %d futures', max_futures)
                concurrent.futures.wait(futures)
