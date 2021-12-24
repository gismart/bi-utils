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
    Retry Decorator
    Retries the wrapped function/method `times` times if the exceptions listed
    in ``exceptions`` are thrown
    :param times: The number of times to repeat the wrapped function/method
    :type times: Int
    :param Exceptions: Lists of exceptions that trigger a retry attempt
    :type Exceptions: Tuple of Exceptions
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