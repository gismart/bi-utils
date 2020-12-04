import os
import sys
import locopy
import threading


class ProgressPercentage:
    '''
    Modified locopy progressbar

    Reduced update frequency, added line break when completed
    '''
    def __init__(self, filename: str) -> None:
        self._filename = filename
        self._size = os.path.getsize(filename)
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self._update_freq = 10
        self._updated_count = 0

    def __call__(self, bytes_amount: int) -> None:
        with self._lock:
            self._seen_so_far += bytes_amount
            self._updated_count += 1
            percentage = self._seen_so_far / self._size
            if self._updated_count % self._update_freq == 0 or percentage == 1.0:
                sys.stdout.write(
                    '\rTransfering [{}] {:.0%}'.format('#' * int(percentage * 10), percentage)
                )
                if percentage == 1.0:
                    sys.stdout.write('\n')
                sys.stdout.flush()


locopy.utility.ProgressPercentage.__init__ = ProgressPercentage.__init__
locopy.utility.ProgressPercentage.__call__ = ProgressPercentage.__call__
