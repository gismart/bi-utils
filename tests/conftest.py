import pytest
import numpy as np
import pandas as pd

from . import utils


@pytest.fixture
def data_path():
    return utils.data_path('cohorts.csv')


@pytest.fixture
def data():
    return pd.read_csv(utils.data_path('cohorts.csv'))


@pytest.fixture
def sample_data():
    return pd.DataFrame({1: [np.nan, 4], 2: [1, 2]})
