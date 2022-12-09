import pytest
import pandas as pd
import datetime as dt

from bi_utils.aws import db


table = "dqc_bi_utils_tests"
schema = "data_quality_monitoring"


def test_delete_wo_conditions():
    with pytest.raises(ValueError, match=".* at least 1 equal condition .*"):
        db.delete(table, schema=schema)


def test_upload_download_delete():
    version = 1
    db.delete(table, schema=schema, version=version)
    data = pd.DataFrame(
        {
            "text": ["hello", "bye"],
            "predict_dt": ["2020-01-01", dt.date.today()],
            "version": [version, version],
        }
    )
    data.predict_dt = pd.to_datetime(data.predict_dt)
    db.upload_data(data, "/tmp/data.csv", schema=schema, table=table)
    query = f"SELECT * FROM {schema}.{table} WHERE version = {version}"
    downloaded_data = db.download_data(query, parse_dates=["predict_dt"], dtype={"version": "int"})
    downloaded_data.pop("load_dttm")
    assert downloaded_data.equals(data)
    db.delete(table, schema=schema, version=version)
    downloaded_data = db.download_data(query, parse_dates=["predict_dt"])
    assert downloaded_data.empty


def test_upload_update_download():
    version = 1
    new_version = 2
    db.delete(table, schema=schema, version=version)
    db.delete(table, schema=schema, version=new_version)
    data = pd.DataFrame(
        {
            "text": ["hello", "bye"],
            "predict_dt": ["2020-01-01", dt.date.today()],
            "version": [version, version],
        }
    )
    data.predict_dt = pd.to_datetime(data.predict_dt)
    db.upload_data(data, "/tmp/data.csv", schema=schema, table=table)
    params_set = {"version": new_version}
    params_where = {"text": "bye"}
    db.update(table, schema=schema, params_set=params_set, params_where=params_where)
    query = f"SELECT * FROM {schema}.{table} WHERE version IN ({version}, {new_version})"
    downloaded_data = db.download_data(query, parse_dates=["predict_dt"], dtype={"version": "int"})
    downloaded_data.pop("load_dttm")
    assert not downloaded_data.equals(data)
    version2_rows = (downloaded_data["version"] == new_version).sum()
    assert version2_rows == 1
    db.delete(table, schema=schema, version=version)


def test_get_columns():
    db_columns = db.get_columns(table=table, schema=schema)
    assert db_columns == ["text", "predict_dt", "version", "load_dttm"]
