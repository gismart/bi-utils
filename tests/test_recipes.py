import pytest

from bi_utils import recipes


@pytest.mark.parametrize(
    "reciever, updater, expected_dict",
    [
        (
            {"a": {"one": 1, "two": [2, 2]}, "b": 3},
            {"a": {"one": 11}},
            {"a": {"one": 11, "two": [2, 2]}, "b": 3},
        ),
        (
            {"a": {"one": 1, "two": [2, 2]}, "b": 3},
            {"a": {"two": [2, 2, 2]}},
            {"a": {"one": 1, "two": [2, 2, 2]}, "b": 3},
        ),
    ],
)
def test_dict_merge(reciever, updater, expected_dict):
    result = recipes.dict_merge(reciever, updater)
    assert result == expected_dict
