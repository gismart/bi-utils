import pytest
import datetime as dt

from bi_utils import sql
from . import utils


@pytest.mark.parametrize(
    'args, kwargs, expected_query',
    [
        (
            ('*',),
            {'table': 'sub_ltv', 'predict_dt': dt.date(2020, 10, 1)},
            'SELECT *\nFROM sub_ltv\nWHERE predict_dt = 2020-10-01',
        ),
        (
            ('ltv',),
            {'table': 'source.ads_ltv', 'predict_dt': '2020-10-01', 'version': 2},
            'SELECT ltv\nFROM source.ads_ltv\nWHERE predict_dt = 2020-10-01',
        ),
    ],
)
def test_get_query(args, kwargs, expected_query):
    query = sql.get_query(utils.data_path('query.sql'), *args, **kwargs)
    assert query == expected_query


@pytest.mark.parametrize(
    'params, expected_where',
    [
        ({'predict_dt': '2020-10-01'}, "WHERE predict_dt = '2020-10-01'"),
        ({'predict_dt': dt.date(2020, 10, 1)}, "WHERE predict_dt = '2020-10-01'"),
        ({'version': 4, 'predict_dt': None}, 'WHERE version = 4 AND predict_dt IS NULL'),
        ({}, 'WHERE 1 = 1'),
    ],
)
def test_build_where(params, expected_where):
    where = sql.build_where(**params)
    assert where == expected_where
