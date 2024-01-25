import pytest
import numpy as np
import pandas as pd

from bi_utils import transformers
from .. import utils


@pytest.mark.parametrize("cols", [["media_source", "campaign_id"], ["media_source"], None])
@pytest.mark.parametrize("q", [0.01, 0.2, 0.5])
def test_quantile_clipper(cols, q, data):
    data = data.dropna()
    target_data = pd.read_csv(utils.data_path("quantile_clipper.csv"))
    target_data = target_data[(target_data.cols.fillna("None") == str(cols)) & (target_data.q == q)]
    clipper = transformers.QuantileClipper(cols=cols, q=q)
    X = data.drop(["conversion", "conversion_predict"], axis=1)
    y = data["conversion"]
    clipper.fit(X, y)
    result = clipper.transform(X, y)
    expected_result = target_data["conversion"].values
    assert np.allclose(result, expected_result)
