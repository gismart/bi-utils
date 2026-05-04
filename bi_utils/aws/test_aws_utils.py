import pytest
from bi_utils.aws import db
import pandas as pd 

def test_download_data():
    query = '''
    SELECT * from dm_main.dim_applications
    '''
    file_format = "csv"

    downloaded_data  = db.download_data(
    query=query,
    file_format=file_format
    )

    assert isinstance(downloaded_data, pd.DataFrame) and len(downloaded_data)!=0