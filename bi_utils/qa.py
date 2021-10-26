import pandas as pd
from typing import Dict, Hashable, Optional, Sequence, Tuple, Union

from .logger import get_logger


logger = get_logger(__name__)


def df_test(
    df: pd.DataFrame,
    strict: bool = False,
    ai: bool = True,
    nullable_cols: Optional[Sequence[Hashable]] = None,
    unique_index: Optional[Sequence[Hashable]] = None,
    thresholds: Optional[
        Dict[Hashable, Tuple[Union[int, float], Union[int, float]]]
    ] = None,
    max_quantiles: Optional[Dict[Hashable, Tuple[float, int]]] = None,
    verify_queries: Optional[Sequence[str]] = None,
) -> int:
    """
    Check dataframe for emptiness, check non-nullable_cols columns for NAs, run quantile and
    thresholds tests for numeric data, verify that all rows comply with custom requirements
    (queries)

    Keyword arguments:
        nullable_cols: columns allowed to contain NAs
        unique_index: list of columns implied to be a unique row identifier
        thresholds: dict of columns with numeric values that should be within the specified
                    [min, max] range
        max_quantiles: dict of positive numeric columns {column_name: [Q, K]} that should not
                       contain values that are K+ times greater then quantile Q (float)
        verify_queries: sequence of negative requirements (queries) all rows should comply with.
                        F.e. "current_date < birth_date" will trigger an alert
                        (warning or ValueError) if any rows where current_date < birth_date
                        are found in the dataset
    """

    failcount = 0
    if df.empty:
        raise ValueError("The dataframe is empty")
    cols = df.columns

    if nullable_cols is None:
        nullable_cols = []
    not_nullable_cols = [c for c in cols if c not in nullable_cols]
    nans = df[not_nullable_cols].isna().sum()
    message = f"Found NA in columns:\n{nans[nans > 0].to_string()}"
    failcount += _passert(
        nans.sum() == 0,
        message,
    )

    failcount += thresholds_test(df, thresholds, ai=ai)
    failcount += quantile_test(df, max_quantiles, ai=ai)
    failcount += query_test(df, verify_queries, ai=ai)
    failcount += unique_index_test(df, unique_index)
    if strict and failcount > 0:
        raise ValueError(
            f"Data qa failcount: {failcount}, force exit since in strict mode"
        )
    return failcount


def thresholds_test(
    df: pd.DataFrame,
    thresholds: Optional[Dict[Hashable, Tuple[Union[int, float], Union[int, float]]]],
    ai: bool = True,
) -> int:
    failcount = 0
    if thresholds is None:
        thresholds = {}
    for col, (min_threshold, max_threshold) in thresholds.items():
        failcount += _check(
            violations=df[df[col] < min_threshold],
            condition_message=f"{col} is below {min_threshold}",
            ai=ai,
        )
        failcount += _check(
            violations=df[df[col] > max_threshold],
            condition_message=f"{col} is above {max_threshold}",
            ai=ai,
        )
    return failcount


def quantile_test(
    df: pd.DataFrame,
    max_quantiles: Optional[Dict[Hashable, Tuple[float, int]]],
    ai: bool = True,
) -> int:
    failcount = 0
    if max_quantiles is None:
        max_quantiles = {}
    for col, (quantile, max_multiplier) in max_quantiles.items():
        q_threshold = df[col].quantile(quantile) * max_multiplier
        if q_threshold <= 0:
            logger.warning(f"Skipping {col} max_quantiles because threshold <= 0")
            continue
        failcount += _check(
            violations=df[df[col] > q_threshold],
            condition_message=f"{col} is above {q_threshold}",
            ai=ai,
        )
    return failcount


def query_test(
    df: pd.DataFrame,
    verify_queries: Optional[Sequence[str]],
    ai: bool = True,
) -> int:
    failcount = 0
    if verify_queries is None:
        verify_queries = []
    for q in verify_queries:
        failcount += _check(
            violations=df.query(q),
            condition_message=q,
            ai=ai,
        )
    return failcount


def unique_index_test(
    df: pd.DataFrame,
    unique_index: Optional[Sequence[Hashable]],
) -> int:
    failcount = 0
    if unique_index is not None:
        n_dups = df.duplicated(subset=unique_index, keep="first").sum()
        failcount += _passert(
            n_dups == 0,
            f"Found {n_dups} duplicates for index {unique_index}",
        )
    return failcount


def find_common_features(
    incompliant_rows: pd.DataFrame,
    cols: Optional[Sequence[Hashable]] = None,
    min_rows: int = 30,
) -> None:
    if incompliant_rows.shape[0] > min_rows:
        cols = cols or incompliant_rows.columns
        incompliant_rows = incompliant_rows[cols]
        nunique = incompliant_rows.nunique()
        common_cols = list(nunique[nunique == 1].index)
        if len(common_cols) > 0:
            common_cols_values = incompliant_rows.head(1)[common_cols]
            logger.warning(
                f"Analyzer: the incompliant rows have common features:\n"
                f"{common_cols_values.reset_index(drop=True).T[0].to_string()}"
            )
        else:
            logger.warning("Analyzer: no pattern detected")


def _check(
    violations: pd.DataFrame,
    condition_message: str,
    cols: Optional[list] = None,
    ai: bool = True,
) -> int:
    n_violations = violations.shape[0]
    failcount = _passert(
        n_violations == 0,
        f"Found {n_violations} rows where {condition_message}",
    )
    if failcount > 0 and ai:
        find_common_features(incompliant_rows=violations, cols=cols)
    return failcount


def _passert(passed: bool, message: str) -> int:
    if passed:
        return 0
    logger.warning(message)
    return 1
