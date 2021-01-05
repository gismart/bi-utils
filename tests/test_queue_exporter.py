import os
import pytest
import pandas as pd

from bi_utils import queue_exporter


def test_queue_exporter_alive():
    with queue_exporter.QueueExporter() as exporter:
        assert exporter.alive
    assert not exporter.alive


def test_queue_exporter_close(data):
    with pytest.raises(ValueError, match='.*closed.*'):
        exporter = queue_exporter.QueueExporter()
        exporter.close()
        exporter.export_df(data, 'data.csv')


def test_queue_exporter_export_df(data):
    temp_data_path = '/tmp/data.pkl'
    if os.path.exists(temp_data_path):
        os.remove(temp_data_path)
    with queue_exporter.QueueExporter() as exporter:
        exporter.export_df(data, temp_data_path)
    exporter.join()
    exported_data = pd.read_pickle(temp_data_path)
    assert exported_data.equals(data)


@pytest.mark.parametrize(
    'table, schema, s3_bucket, s3_bucket_dir',
    [
        ('ltv_subscription_dd', None, 'gismart-analytics', None),
        (None, 'data_analytics_sandbox', 'gismart-analytics', 'dwh/temp'),
        ('ltv_subscription_dd', 'data_analytics_sandbox', None, 'dwh/temp'),
        ('ltv_subscription_dd', None, None, None),
        (None, None, None, 'dwh/temp'),
    ],
)
def test_queue_exporter_bad_args(data, table, schema, s3_bucket, s3_bucket_dir):
    with pytest.raises(ValueError, match='.*Pass both.*'):
        with queue_exporter.QueueExporter() as exporter:
            exporter.export_df(
                data,
                'data.csv',
                table=table,
                schema=schema,
                s3_bucket=s3_bucket,
                s3_bucket_dir=s3_bucket_dir,
            )
