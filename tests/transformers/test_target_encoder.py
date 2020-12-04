import pytest
import numpy as np
import pandas as pd

from bi_utils import transformers
from .. import utils


@pytest.mark.parametrize('cols', [['media_source', 'campaign_id'], ['media_source'], None])
@pytest.mark.parametrize('C', [2, 10, 100])
def test_target_encoder(cols, C, data):
    data = data.dropna()
    target_data = pd.read_csv(utils.data_path('target_encoder.csv'))
    target_data = target_data[(target_data.cols == str(cols)) & (target_data.C == C)]
    clipper = transformers.TargetEncoder(cols=cols, C=C)
    X = data.drop(['conversion', 'conversion_predict'], axis=1)
    y = data['conversion']
    clipper.fit(X, y)
    result = clipper.transform(X)
    expected_result = target_data[result.columns]
    assert np.allclose(result, expected_result)
