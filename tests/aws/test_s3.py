import os
import pandas as pd

from bi_utils.aws import s3


def test_upload_download_file(data_path):
    filename = os.path.basename(data_path)
    bucket_dir = "dwh/temp"
    bucket_path = f"{bucket_dir}/{filename}"
    download_path = f"/tmp/{filename}"
    s3.upload_file(data_path)
    s3.download_file(bucket_path, download_path)
    local_data = pd.read_csv(data_path)
    downloaded_data = pd.read_csv(download_path)
    assert local_data.equals(downloaded_data)
