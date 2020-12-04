import pytest

from bi_utils import system


def test_fill_message():
    message = 'hello world'
    filled_message = system.fill_message('hello world', '*')
    assert f' {message} ' in filled_message


@pytest.mark.parametrize(
    'size_bytes, expected_verbose_size',
    [
        (1, '1B'),
        (73856, '72.12KB'),
        (374563485, '357.21MB'),
        (678426357923, '631.83GB'),
        (8293576827356345, '7.37PB'),
    ],
)
def test_verbose_size(size_bytes, expected_verbose_size):
    actual_verbose_size = system.verbose_size(size_bytes)
    assert actual_verbose_size == expected_verbose_size


def test_ram_usage():
    usage = system.ram_usage()
    assert 'B' in usage
