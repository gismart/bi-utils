import pytest

from bi_utils import decorators


def test_retry():
    @decorators.retry(1, exceptions=KeyError)
    def unstable_func():
        try:
            return dictionary["key"]
        finally:
            dictionary["key"] = "value"

    dictionary = {}
    result = unstable_func()
    assert result == "value"


def test_retry_different_exception():
    @decorators.retry(1, exceptions=ValueError)
    def unstable_func():
        try:
            return dictionary["key"]
        finally:
            dictionary["key"] = "value"

    dictionary = {}
    with pytest.raises(KeyError):
        unstable_func()
