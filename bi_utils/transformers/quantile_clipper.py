from __future__ import annotations

import logging
import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.utils.validation import check_is_fitted
from typing import Optional, Sequence, Union


logger = logging.getLogger(__name__)


class QuantileClipper(BaseEstimator, TransformerMixin):
    """Clip grouped features by certain quantile"""

    def __init__(
            self,
            *,
            cols: Optional[Sequence[str]] = None,
            q: float = 0.001,
            interpolation: str = "linear",
            ) -> None:
        self.uq = 1 - q
        self.lq = q
        self.cols = cols
        self.q = q
        self.quantile_params = {
            "interpolation": interpolation,
            "numeric_only": True,
        }

    def _check_params(self, X: pd.DataFrame) -> None:
        if not 0 < self.lq <= 0.5:
            raise ValueError(f"q={self.lq} must be in (0, 0.5] interval")
        if len(X) < 1 / self.lq:
            logger.warning(
                f"q={self.lq} is too small for given data. Quatiles are equivalent to min/max"
            )

    def fit(self, X: pd.DataFrame, y: Union[pd.Series, np.ndarray]) -> QuantileClipper:
        if self.cols is None:
            self.cols = list(X.columns)
        X = X[self.cols].copy()
        y = y.copy()
        if isinstance(y, pd.Series):
            y = y.values
        self._validate_data(X, y, dtype=None, y_numeric=True)
        self._check_params(X)

        X["target"] = y
        groups = X.groupby(self.cols, observed=True)
        self.groups_u_ = groups["target"].quantile(self.uq, **self.quantile_params)
        self.groups_u_ = self.groups_u_.fillna(y.max()).rename("target_u")
        self.groups_l_ = groups["target"].quantile(self.lq, **self.quantile_params)
        self.groups_l_ = self.groups_l_.fillna(y.min()).rename("target_l")
        self.n_groups_ = len(groups)
        return self

    def transform(self, X: pd.DataFrame, y: Union[pd.Series, np.ndarray]) -> np.ndarray:
        check_is_fitted(self)
        X = X[self.cols].copy()
        y = y.copy()
        if isinstance(y, pd.Series):
            y = y.values
        self._validate_data(X, y, dtype=None, y_numeric=True)

        X["target"] = y
        X = X.set_index(self.cols)
        X = X.join(self.groups_l_, how="left")
        X = X.join(self.groups_u_, how="left")
        X["target_l"] = X["target_l"].fillna(y.min())
        X["target_u"] = X["target_u"].fillna(y.max())
        X["modified_target"] = X["target"].clip(X["target_l"], X["target_u"])

        assert ~(X["target_l"] > X["target_u"]).any()
        return X["modified_target"].values

    def fit_transform(self, X: pd.DataFrame, y: Union[pd.Series, np.ndarray]) -> np.ndarray:
        return self.fit(X, y).transform(X, y)
