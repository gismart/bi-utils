import pandas as pd
from typing import Any, Dict, Hashable, Optional, Sequence, Tuple, Union

from .logger import get_logger


logger = get_logger(__name__)


def df_test(
    df: pd.DataFrame,
    strict: bool = False,
    nullable_cols: Optional[Sequence[Hashable]] = None,
    unique_index: Optional[Sequence[Hashable]] = None,
    thresholds: Optional[Dict[Any, Tuple[Union[int, float], Union[int, float]]]] = None,
    max_quantiles: Optional[Dict[Any, Tuple[float, int]]] = None,
    verify_queries: Optional[Sequence[str]] = None,
) -> int:
    '''
    Check dataframe for emptiness, check non-nullable_cols columns for NAs, run quantile and
    threshold tests for numeric data, verify that all rows comply with custom requirements (queries)

    Keyword arguments:
    nullable_cols -- columns allowed to contain NAs
    unique_index -- list of columns implied to be a unique row identifier
    thresholds -- columns with numeric values that should be within the specified [min, max] range
    max_quantiles -- dict of positive numeric columns {column_name: [Q, K]} that should not
                         contain values that are K+ times greater then quantile Q (float)
    verify_queries -- list of positive requirements (queries) all rows should comply with
    '''

    failcount = 0
    if df.empty:
        raise ValueError('The dataframe is empty')
    cols = df.columns

    if nullable_cols is None:
        nullable_cols = []
    not_nullable_cols = [c for c in cols if c not in nullable_cols]
    nans = df[not_nullable_cols].isna().sum()
    failcount += _passert(
        nans.sum() == 0,
        f'Found NA in columns: {nans[nans > 0]}',
    )

    failcount += thresholds_test(df, thresholds)
    failcount += quantile_test(df, max_quantiles)
    failcount += query_test(df, verify_queries)
    failcount += unique_index_test(df, unique_index)
    if strict and failcount > 0:
        raise ValueError(f'Data qa failcount: {failcount}, force exit since in strict mode')
    return failcount


def thresholds_test(
    df: pd.DataFrame,
    thresholds: Optional[Dict[Hashable, Tuple[Union[int, float], Union[int, float]]]],
) -> int:
    failcount = 0
    if thresholds is None:
        thresholds = {}
    for col, (min_threshold, max_threshold) in thresholds.items():
        failcount += _passert(
            df[col].min() >= min_threshold,
            f'Found {col} value below {min_threshold}',
        )
        failcount += _passert(
            df[col].max() <= max_threshold,
            f'Found {col} value above {max_threshold}',
        )
    return failcount


def quantile_test(
    df: pd.DataFrame,
    max_quantiles: Optional[Dict[Hashable, Tuple[float, int]]],
) -> int:
    failcount = 0
    if max_quantiles is None:
        max_quantiles = {}
    for col, (quantile, max_multiplier) in max_quantiles.items():
        q_threshold = df[col].quantile(quantile)
        if q_threshold <= 0:
            logger.warning(f'Skipping {col} max_quantiles because threshold <= 0')
            continue
        failcount += _passert(
            (df[col].max() / q_threshold) <= max_multiplier,
            f'Found {col} value above {q_threshold}',
        )
    return failcount


def query_test(df: pd.DataFrame, verify_queries: Optional[Sequence[str]]) -> int:
    failcount = 0
    if verify_queries is None:
        verify_queries = []
    for q in verify_queries:
        n_compliant_rows = df.query(q).shape[0]
        df_len = df.shape[0]
        failcount += _passert(
            n_compliant_rows == df_len,
            f'Found {df_len - n_compliant_rows} rows incompliant with {q}',
        )
    return failcount


def unique_index_test(df: pd.DataFrame, unique_index: Optional[Sequence[Hashable]]) -> int:
    failcount = 0
    if unique_index is not None:
        failcount += _passert(
            ~df.duplicated(subset=unique_index, keep='first').any(),
            'Found duplicates',
        )
    return failcount


def _passert(passed: bool, message: str) -> int:
    if passed:
        return 0
    logger.warning(message)
    return 1
