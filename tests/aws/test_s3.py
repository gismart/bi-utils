import os
import pandas as pd
import datetime as dt

from bi_utils.aws import s3, db


def test_upload_download_file(data_path):
    filename = os.path.basename(data_path)
    bucket_dir = "dwh/temp"
    bucket_path = f"{bucket_dir}/{filename}"
    download_path = f"/tmp/{filename}"
    s3.upload_file(data_path, bucket_dir)
    s3.download_file(bucket_path, download_path)
    local_data = pd.read_csv(data_path)
    downloaded_data = pd.read_csv(download_path)
    assert local_data.equals(downloaded_data)


def test_upload_file_download_dir_read_files(data_path):
    filename = os.path.basename(data_path)
    timestamp_dir_name = dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S_%f")
    bucket_dir = "dwh/temp/" + timestamp_dir_name
    bucket_path = f"{bucket_dir}/{filename}"
    download_dir = f"/tmp/{timestamp_dir_name}/{filename}/"
    s3.upload_file(data_path, bucket_path)
    s3.download_dir(bucket_dir, download_dir)
    downloaded_data = db.read_files(download_dir)
    local_data = pd.read_csv(data_path)
    assert downloaded_data.equals(local_data)
