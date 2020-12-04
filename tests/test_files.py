import pytest
import datetime as dt

from bi_utils import files
from . import utils


@pytest.mark.parametrize(
    'data_name, date, ext, expected_filename',
    [
        ('data', '2020-10-01', 'csv', '2020-10-01_data.csv'),
        ('cohorts', dt.date(2020, 10, 1), 'pkl', '2020-10-01_cohorts.pkl'),
        ('model', '2077-10-01', None, '2077-10-01_model'),
    ],
)
def test_data_filename(data_name, date, ext, expected_filename):
    filename = files.data_filename(data_name, date, ext=ext)
    assert filename == expected_filename


@pytest.mark.parametrize(
    'csv_filename, separator, expected_columns',
    [
        ('hierarchical_encoder.csv', ',', ['conversion', 'C', 'cols']),
        ('quantile_clipper.csv', ',', ['conversion', 'cols', 'q']),
        ('target_encoder.csv', ';', ['media_source,campaign_id,C,cols,application_dm_id']),
    ],
)
def test_csv_columns(csv_filename, separator, expected_columns):
    csv_path = utils.data_path(csv_filename)
    columns = files.csv_columns(csv_path, separator=separator)
    assert columns == expected_columns
