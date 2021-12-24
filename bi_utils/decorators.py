import time
import logging
from typing import Callable, Sequence, Union


logger = logging.getLogger(__name__)


def retry(
    times: int,
    exceptions: Union[Exception, Sequence[Exception]] = Exception,
    sleep_secs: int = 0,
) -> Callable:
    """
    Retry the wrapped function `times` times if the exceptions listed
    in `exceptions` are thrown. Sleep for `sleep_secs` seconds every time
    """
    def decorator(func):
        def newfn(*args, **kwargs):
            attempt = 1
            while attempt <= times:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    logger.info(
                        f'{e.__class__.__name__} thrown when trying to run {func.__name__}. '
                        f'{attempt}/{times} attempt'
                    )
                    attempt += 1
                    if sleep_secs:
                        time.sleep(sleep_secs)
            return func(*args, **kwargs)
        return newfn
    return decorator
