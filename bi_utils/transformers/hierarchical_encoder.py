from __future__ import annotations

import logging
import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.utils.validation import check_is_fitted
from typing import Optional, Sequence, Union


logger = logging.getLogger(__name__)


class HierarchicalEncoder(BaseEstimator, TransformerMixin):
    """Hierarchical target encoding"""

    def __init__(
        self,
        cols: Optional[Sequence[str]] = None,
        sample_weight_col: Optional[str] = None,
        C: int = 30,
        disambiguate: bool = True,
        verbose: bool = False,
    ) -> None:
        self.C = C
        self.cols = cols
        self.sample_weight_col = sample_weight_col
        self.disambiguate = disambiguate
        self.verbose = verbose

    def _check_params(self, X: pd.DataFrame) -> None:
        if self.C <= 0:
            raise ValueError(f"C={self.C} must be > 0")

    def _disambiguate(self, X: pd.DataFrame, sep: str = "__") -> pd.DataFrame:
        """
        Disambiguate hierarchical categorical columns,
        f.e. distinguish Paris, US from Paris, France
        by concatenating the parent categories values with child categories values.

        Order of cols matters:
        the feature at the beginning of the cols list is considered to be the parent feature.
        F.e.: [country, city, street].

        `sep` is used as a value separator in the concatenated values.
        """
        for i, col in enumerate(self.cols):
            if i > 0:
                X[col] = X[col].astype("str") + sep + X[self.cols[i - 1]].astype("str")
        return X

    def _reset_dead_level(self, lvl: int) -> None:
        """
        If the level contains the same observations as the parent level,
        reset the level's ratio to the parent's one
        (to avoid chain-effect overfitting in one-of-a-kind case)
        """
        dead_level_mask = (
            self.lvl_groups_[lvl]["target_denominator"]
            == self.lvl_groups_[lvl]["parent_denominator"]
        )
        self.lvl_groups_[lvl].loc[dead_level_mask, "ratio"] = self.lvl_groups_[lvl].loc[
            dead_level_mask, "parent_ratio"
        ]

    def fit(self, X: pd.DataFrame, y: Union[pd.Series, np.ndarray]) -> HierarchicalEncoder:
        if self.cols is None:
            self.cols = [c for c in X.columns if not c == self.sample_weight_col]
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
            logger.info("Fitting...")
        if self.disambiguate:
            X = self._disambiguate(X)

        X["target_denominator"] = sample_weight
        X["target_numerator"] = y * sample_weight

        self.total_ratio_ = np.average(y, weights=sample_weight)
        min_sample_std = np.sqrt(self.total_ratio_ * (1 - self.total_ratio_) / self.C)
        self.std_mean_ratio_ = round(min_sample_std / self.total_ratio_, 3)
        if self.verbose:
            logger.info(f"STD/AVG for min sample: {self.std_mean_ratio_}")

        self.lvl_groups_ = {}
        for lvl, _ in enumerate(self.cols):
            self.lvl_groups_[lvl] = (
                X.groupby(self.cols[: lvl + 1])[["target_numerator", "target_denominator"]]
                .sum()
                .reset_index()
            )
            if lvl == 0:
                self.lvl_groups_[lvl]["parent_ratio"] = self.total_ratio_
                self.lvl_groups_[lvl]["parent_denominator"] = -1
            else:
                parent_col = self.cols[lvl - 1]
                parent_groups = self.lvl_groups_[lvl - 1]
                parent_ratio = parent_groups.set_index(parent_col)["ratio"]
                self.lvl_groups_[lvl]["parent_ratio"] = self.lvl_groups_[lvl][parent_col].map(
                    parent_ratio
                )
                parent_denominator = parent_groups.set_index(parent_col)["target_denominator"]
                self.lvl_groups_[lvl]["parent_denominator"] = self.lvl_groups_[lvl][parent_col].map(
                    parent_denominator
                )
            self.lvl_groups_[lvl]["ratio"] = (
                self.lvl_groups_[lvl]["target_numerator"]
                + self.lvl_groups_[lvl]["parent_ratio"] * self.C
            ) / (self.lvl_groups_[lvl]["target_denominator"] + self.C)
            self._reset_dead_level(lvl=lvl)
        return self

    def transform(self, X: pd.DataFrame) -> np.ndarray:
        check_is_fitted(self)
        X = X[self.cols].copy()
        self._validate_data(X, dtype=None)

        if self.verbose:
            logger.info("Transforming...")
        if self.disambiguate:
            X = self._disambiguate(X)
        expected_len = X.shape[0]
        self.result_ = pd.Series(index=X.index, dtype=float)
        for lvl in reversed(range(len(self.cols))):
            if self.verbose:
                logger.info(f"Mapping {self.cols[lvl]}...")
            bavg = (
                self.lvl_groups_[lvl]
                .reset_index()[[self.cols[lvl], "ratio"]]
                .set_index(self.cols[lvl])["ratio"]
            )
            mapping = X[self.cols[lvl]].map(bavg)
            self.result_ = self.result_.fillna(mapping)
            n_na = self.result_.isna().sum()
            if self.verbose:
                logger.info(f"Mapping completed, missing values to fill: {n_na}/{expected_len}")
            if n_na == 0:
                break
        n_na = self.result_.isna().sum()
        if n_na > 0 and self.verbose:
            logger.info(f"Imputing {n_na} unknown values with global average...")
        self.result_ = self.result_.fillna(self.total_ratio_)
        if self.verbose:
            logger.info("Completed.")
        return self.result_.values
