import sys
import numpy as np
from sklearn.utils import validation
from typing import Optional


def mean_absolute_percentage_error(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    *,
    sample_weight: Optional[np.ndarray] = None,
    epsilon: float = sys.float_info.epsilon,
) -> float:
    """Mean absolute percentage error regression loss"""
    _check_arrays(y_true, y_pred, sample_weight)
    errors = np.abs(y_pred - y_true) / np.maximum(np.abs(y_true), epsilon)
    mape = np.average(errors, weights=sample_weight, axis=0)
    return mape


def mean_percentage_bias(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    *,
    sample_weight: Optional[np.ndarray] = None,
) -> float:
    """Mean percentage bias"""
    _check_arrays(y_true, y_pred, sample_weight)
    delta = np.average(y_pred - y_true, weights=sample_weight, axis=0)
    avg = np.average(y_true, weights=sample_weight, axis=0)
    bias = delta / avg
    return bias


def _check_arrays(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    sample_weight: Optional[np.ndarray] = None,
) -> None:
    validation.check_consistent_length(y_true, y_pred, sample_weight)
    y_true = validation.check_array(y_true, ensure_2d=False)
    y_pred = validation.check_array(y_pred, ensure_2d=False)
