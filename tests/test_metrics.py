import pytest
import numpy as np

from bi_utils import metrics


@pytest.mark.parametrize(
    'epsilon,expected_mape',
    [
        (1, 0.23582490978914566),
        (1e-3, 37.387087575859525),
        (1e-9, 35722329.182639904),
    ],
)
def test_mean_absolute_percentage_error(data, epsilon, expected_mape):
    data = data.dropna()
    mape = metrics.mean_absolute_percentage_error(
        data['conversion'],
        data['conversion_predict'],
        epsilon=epsilon,
    )
    assert np.isclose(mape, expected_mape)


def test_mean_percentage_bias(data):
    data = data.dropna()
    bias = metrics.mean_percentage_bias(data['conversion'], data['conversion_predict'])
    assert np.isclose(bias, -0.08733334958801997)
