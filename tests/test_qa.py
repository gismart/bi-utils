import pytest
import numpy as np
import pandas as pd

from bi_utils import qa


def test_passert_negative():
    assert qa._passert(False, 'failed') == 1


def test_passert_positive():
    assert qa._passert(True, 'passed') == 0


@pytest.mark.parametrize('df', [pd.DataFrame(), pd.DataFrame({1: [], 2: []})])
def test_empty_df_positive(df):
    with pytest.raises(ValueError):
        qa.df_test(df)


def test_empty_df_negative(sample_data):
    assert qa.df_test(sample_data, nullable_cols=[1]) == 0


@pytest.mark.parametrize(
    'df, unique_index, expected_status',
    [
        (
            pd.DataFrame({
                'country': ['US', 'US', 'CN', 'CN'],
                'payment_number': [1, 2, 3, 1],
            }),
            ['country', 'payment_number'],
            0,
        ),
        (
            pd.DataFrame({
                'country': ['US', 'US', 'CN', 'CN'],
                'payment_number': [1, 2, 3, 3],
            }),
            ['country', 'payment_number'],
            1,
        ),
        (
            pd.DataFrame({
                'country': ['US', 'US', 'CN', 'CN'],
                'payment_number': [1, 2, 3, 1],
            }),
            ['payment_number'],
            1,
        ),
        (
            pd.DataFrame({
                'country': ['US', 'US', 'CN', 'CN'],
                'payment_number': [1, 2, 3, np.nan],
            }),
            ['payment_number'],
            0,
        ),
    ]
)
def test_unique(df, unique_index, expected_status):
    assert qa.unique_index_test(df, unique_index) == expected_status


@pytest.mark.parametrize(
    'df, thresholds, expected_status',
    [
        (
            pd.DataFrame({
                'conv_1': [0.5, 1.01],
                'conv_2': [0.001, 0.999],
            }),
            {'conv_1': [0, 1]},
            1,
        ),
        (
            pd.DataFrame({
                'conv_1': [0.5, 2],
                'conv_2': [0.001, 0.999],
            }),
            {'conv_2': [0, 1]},
            0,
        ),
    ]
)
def test_thresholds(df, thresholds, expected_status):
    assert qa.thresholds_test(df, thresholds) == expected_status


@pytest.mark.parametrize(
    'df, q, expected_status',
    [
        (
            pd.DataFrame({
                'ltv_180': [0.5, 1.01, 0.75, 8.99, 70],
                'ltv_365': [0.5, 1.01, 1.3, 12.3, 70],
            }),
            {'ltv_180': (0.75, 2)},
            1,
        ),
        (
            pd.DataFrame({
                'ltv_180': [0.5, 1.01, 0.75, 0.76, 1.3],
                'ltv_365': [0.5, 1.01, 1.3, 12.3, 70],
            }),
            {'ltv_180': (0.75, 2)},
            0,
        ),
        (
            pd.DataFrame({
                'ltv_180': [-0.5, 1.01, 0.75, 0.76, 1.3],
                'ltv_365': [0.5, 1.01, 1.3, 12.3, 70],
            }),
            {'ltv_180': (0.75, 2)},
            0,
        ),
    ]
)
def test_quantiles(df, q, expected_status):
    assert qa.quantile_test(df, q) == expected_status


@pytest.mark.parametrize(
    'df, q, expected_status',
    [
        (
            pd.DataFrame({
                'ltv_180': [0.5, 1.01, 0.75, 0.76, 1.3],
                'ltv_365': [0.5, 1, 1.3, 12.3, 70],
            }),
            ['ltv_365 < ltv_180'],
            1,
        ),
        (
            pd.DataFrame({
                'ltv_180': [0.5, 1.01, 0.75, 0.76, 1.3],
                'ltv_365': [0.5, 1.01, 1.3, 12.3, 70],
            }),
            ['ltv_365 <= ltv_180'],
            1,
        ),
        (
            pd.DataFrame({
                'ltv_180': [0.5, 1.01, 0.75, 0.76, 1.3],
                'ltv_365': [0.5, 1.01, 1.3, 12.3, 70],
            }),
            ['ltv_365 < ltv_180'],
            0,
        ),
        (
            pd.DataFrame({
                'ltv_180': [0.5, 1.01, 0.75, 0.76, 1.3],
                'ltv_365': [0.5, 1.01, 1.3, 12.3, np.nan],
            }),
            ['ltv_365 < ltv_180'],
            0,
        )
    ]
)
def test_queries(df, q, expected_status):
    assert qa.query_test(df, q) == expected_status


def test_df_test_nullable(sample_data):
    assert qa.df_test(sample_data, nullable_cols=[1], strict=False) == 0


def test_df_test_strict(sample_data):
    with pytest.raises(ValueError):
        qa.df_test(sample_data, strict=True)
    assert qa.df_test(sample_data, strict=False) == 1
