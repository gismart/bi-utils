import pytest

from bi_utils.aws import locopy


def test_progress_percentage(data_path):
    try:
        percentage = locopy.ProgressPercentage(data_path)
        percentage(0)
        percentage(percentage._size)
    except Exception as e:
        pytest.fail(str(e))
