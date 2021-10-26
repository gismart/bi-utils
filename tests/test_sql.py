import pytest
import datetime as dt

from bi_utils import sql
from . import utils


@pytest.mark.parametrize(
    "args, kwargs, expected_query",
    [
        (
            ("*",),
            {"table": "sub_ltv", "predict_dt": dt.date(2020, 10, 1)},
            "SELECT *\nFROM sub_ltv\nWHERE predict_dt = 2020-10-01",
        ),
        (
            ("ltv",),
            {"table": "source.ads_ltv", "predict_dt": "2020-10-01", "version": 2},
            "SELECT ltv\nFROM source.ads_ltv\nWHERE predict_dt = 2020-10-01",
        ),
    ],
)
def test_get_query(args, kwargs, expected_query):
    query = sql.get_query(utils.data_path("query.sql"), *args, **kwargs)
    assert query == expected_query


@pytest.mark.parametrize(
    "params, expected_set",
    [
        ({"predict_dt": "2020-10-01"}, "SET predict_dt = '2020-10-01'"),
        ({"predict_dt": dt.date(2020, 10, 1)}, "SET predict_dt = '2020-10-01'"),
        ({"version": 4, "predict_dt": None}, "SET version = 4, predict_dt = NULL"),
    ],
)
def test_build_set(params, expected_set):
    set_str = sql.build_set(**params)
    assert set_str == expected_set


def test_build_set_wo_conditions():
    with pytest.raises(ValueError, match=".* at least 1 equal condition .*"):
        sql.build_set()


@pytest.mark.parametrize(
    "params, expected_where",
    [
        ({"predict_dt": "2020-10-01"}, "WHERE predict_dt = '2020-10-01'"),
        ({"predict_dt": dt.date(2020, 10, 1)}, "WHERE predict_dt = '2020-10-01'"),
        (
            {"version": 4, "predict_dt": None},
            "WHERE version = 4 AND predict_dt IS NULL",
        ),
        ({}, "WHERE 1 = 1"),
        ({"os_name": ["ios", "web"]}, "WHERE os_name IN ('ios', 'web')"),
    ],
)
def test_build_where(params, expected_where):
    where_str = sql.build_where(**params)
    assert where_str == expected_where
