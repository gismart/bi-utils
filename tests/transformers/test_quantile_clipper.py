import pytest
import numpy as np
import pandas as pd

from bi_utils import transformers
from .. import utils

df = pd.DataFrame({
    "conversion": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.6, 0.6, 0.6, 0.6],
    "media_source": ["google", "google", "google", "google", "google", "fb", "fb", "fb", "fb", "fb"],
    "campaign_id": ["1", "1", "1", "1", "1", "1", "1", "1", "1", "1"],
})

@pytest.mark.parametrize(
        "cols, q, expected", 
        [
            (["media_source"], 0.5, [0.3] * 5 + [0.6] * 5,), # 0.3 is the 0.5 quantile of google, 0.6 is the 0.5 quantile of fb
            (["media_source", "campaign_id"], 0.5, [0.3] * 5 + [0.6] * 5,), # Same as above, because campaign_id is uniform
            (None, 0.5, [0.3] * 5 + [0.6] * 5,), # Same as above, because both columns are used if cols is None
            (["campaign_id"], 0.5, [0.55] * 10,), # 0.55 is the 0.5 quantile of the whole dataset
            # In an array with n=10 values, the position of the 0.2 quantile is calculated as 
            # (n - 1) * 0.2 = 1.8
            # Since the quantile falls between two values (0.2 and 0.3 in this case), 
            # numpy linearly interpolates between these values:
            # 0.2 + 0.8 * (0.3 - 0.2) = 0.28
            # The upper quantile is 0.6 since the upper part of the array is filled with 0.6
            # Values between 0.28 and 0.6 remain unchanged, so:
            (["campaign_id"], 0.2, [0.28, 0.28, 0.3, 0.4, 0.5] + [0.6] * 5,),
        ],
)
def test_quantile_clipper(cols, q, expected, *kwargs):
    clipper = transformers.QuantileClipper(cols=cols, q=q)
    X = df.drop(["conversion"], axis=1)
    y = df["conversion"]
    clipper.fit(X, y)
    result = clipper.transform(X, y)
    expected_result = np.array(expected)
    assert np.allclose(result, expected_result, atol=1e-8, rtol=1e-5)
