import pytest
import pandas as pd

from . import utils


@pytest.fixture
def data_path():
    return utils.data_path('cohorts.csv')


@pytest.fixture
def data():
    return pd.read_csv(utils.data_path('cohorts.csv'))
