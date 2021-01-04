import pandas as pd
from typing import List, Set, Dict, Tuple, Optional, Union, Iterator
from .logger import get_logger

logger = get_logger(__name__)


def passert(passed: bool, message: str) -> int:
    if not passed:
        logger.warning(msg=message)
        return 1
    else:
        return 0


def thresholds_test(df, thresholds):
    failcount = 0
    if thresholds is not None:
        for c_tuple in thresholds:
            for c in c_tuple:
                failcount += passert(
                    df[c].min() >= thresholds[c_tuple][0],
                    f'Found {c} value below {thresholds[c_tuple][0]}')
                failcount += passert(
                    df[c].max() <= thresholds[c_tuple][1],
                    f'Found {c} value above {thresholds[c_tuple][1]}')
    return failcount


def quantile_test(df, max_quantile):
    failcount = 0
    if max_quantile is not None:
        for c in max_quantile:
            q_threshold = df[c].quantile(max_quantile[c][0])
            if q_threshold <= 0:
                logger.warning(
                    f'Skipping {c} max_quantile because threshold <= 0')
                continue
            failcount += passert(
                (df[c].max() / q_threshold) <= max_quantile[c][1],
                f'Found {c} value above {q_threshold}')
    return failcount


def query_test(df, verify_query):
    failcount = 0
    if verify_query is not None:
        for q in verify_query:
            n_compliant_rows = df.query(q).shape[0]
            failcount += passert(
                n_compliant_rows == df.shape[0],
                f'Found {n_compliant_rows} rows incompliant with {q}')
    return failcount


def unique_index_test(df, unique_index):
    failcount = 0
    if unique_index is not None:
        failcount += passert(
            ~df.duplicated(subset=unique_index, keep='first').any(),
            'Found dups')
    return failcount


def df_test(
    df: pd.DataFrame(),
    strict: bool = False,
    nullable_cols: List = [],
    unique_index: List or None = None,
    thresholds: Dict[tuple, list] or None = None,
    max_quantile: Dict or None = None,
    verify_query: List[str] or None = None,
) -> int:
    '''Check dataframe for emptiness, check non-nullable_cols columns for NAs, run quantile and
    threshold tests for numeric data, verify that all rows comply with custom requirements (queries)

    Keyword arguments:
    nullable_cols -- columns allowed to contain NAs
    unique_index -- list of columns implied to be a unique row identifier
    thresholds -- columns with numeric values that should be within the specified [min, max] range
    max_quantile -- dict of positive numeric columns {column_name: [Q, K]} that should not
                         contain values that are K+ times greater then quantile Q (float)
    verify_query -- list of positive requirements (queries) all rows should comply with
    '''

    failcount = 0
    if df.empty:
        raise ValueError('The dataframe is empty.')
    cols = df.columns

    not_nullable_cols = [c for c in cols if c not in nullable_cols]
    nans = df[not_nullable_cols].isna().sum()
    failcount += passert(nans.sum() == 0,
                         f'Found NA in columns: {nans[nans>0]}')

    failcount += thresholds_test(df, thresholds)
    failcount += quantile_test(df, max_quantile)
    failcount += query_test(df, verify_query)
    failcount += unique_index_test(df, unique_index)
    if strict and failcount > 0:
        raise ValueError(
            f'Data qa failcount: {failcount}, force exit since in strict mode')
    return failcount
