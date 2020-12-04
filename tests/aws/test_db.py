import pytest
import pandas as pd
import datetime as dt

from bi_utils.aws import db


def test_delete_wo_conditions():
    with pytest.raises(ValueError, match='.* at least 1 equal condition .*'):
        db.delete('dqc_bi_utils_tests', schema='data_quality_monitoring')


def test_upload_download_delete():
    table = 'dqc_bi_utils_tests'
    schema = 'data_quality_monitoring'
    version = 1
    db.delete(table, schema=schema, version=1)
    data = pd.DataFrame({
        'text': ['hello', 'bye'],
        'predict_dt': ['2020-01-01', dt.date.today()],
        'version': [version, version],
    })
    data.predict_dt = pd.to_datetime(data.predict_dt)
    db.upload_data(data, '/tmp/data.csv', schema=schema, table=table)
    query = f'SELECT * FROM {schema}.{table} WHERE version = {version}'
    downloaded_data = db.download_data(query, parse_dates=['predict_dt'], dtype={'version': 'int'})
    downloaded_data.pop('load_dttm')
    assert downloaded_data.equals(data)
    db.delete(table, schema=schema, version=version)
    downloaded_data = db.download_data(query, parse_dates=['predict_dt'])
    assert downloaded_data.empty
