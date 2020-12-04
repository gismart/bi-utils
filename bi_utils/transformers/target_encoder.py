from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.utils.validation import check_is_fitted
from typing import Optional, Sequence, Union

from ..logger import get_logger


logger = get_logger(__name__)


class TargetEncoder(BaseEstimator, TransformerMixin):
    '''Target encoding'''
    def __init__(
        self,
        cols: Optional[Sequence[str]] = None,
        sample_weight_col: Optional[str] = None,
        C: int = 30,
        verbose: bool = False,
    ) -> None:
        self.C = C
        self.cols = cols
        self.sample_weight_col = sample_weight_col
        self.verbose = verbose

    def _check_params(self, X: pd.DataFrame) -> None:
        if self.C <= 1:
            raise ValueError(f'C={self.C} must be > 1')

    def fit(self, X: pd.DataFrame, y: Union[pd.Series, np.ndarray]) -> TargetEncoder:
        if self.cols is None:
            self.cols = [c for c in list(X.columns) if not c == self.sample_weight_col]
        if self.sample_weight_col is None:
            sample_weight = pd.Series(1, index=X.index)
        else:
            sample_weight = X[self.sample_weight_col]
        X = X[self.cols].copy()
        y = y.copy()
        if isinstance(y, pd.Series):
            y = y.values
        self._validate_data(X, y, dtype=None, y_numeric=True)
        self._check_params(X)

        if self.verbose:
            logger.info('Fitting...')

        X['target_denominator'] = sample_weight
        X['target_numerator'] = y * sample_weight

        self.total_ratio_ = np.average(y, weights=sample_weight)

        self.groups_ = {}
        for col in self.cols:
            self.groups_[col] = X.groupby(col)[['target_numerator', 'target_denominator']].sum()
            self.groups_[col]['ratio'] = (
                self.groups_[col]['target_numerator']
                + self.total_ratio_ * self.C
            ) / (self.groups_[col]['target_denominator'] + self.C)
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        check_is_fitted(self)
        X = X[self.cols].copy()
        self._validate_data(X, dtype=None)

        if self.verbose:
            logger.info('Transforming...')
        has_na = False
        self.result = pd.DataFrame(index=X.index, dtype=np.float)
        for col in self.cols:
            if self.verbose:
                logger.info(f'Mapping {col}...')
            mean_target = self.groups_[col]['ratio']
            self.result[col] = X[col].map(mean_target)
            has_na = self.result.isna().any().any()
        if has_na:
            self.result = self.result.fillna(self.total_ratio_)
        if self.verbose:
            logger.info('Completed.')
        return self.result
