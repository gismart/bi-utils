import os
import boto3
from botocore.exceptions import ClientError

from ..logger import get_logger


logger = get_logger(__name__)


def upload_file(
    file_path: str,
    bucket_dir: str = 'dwh/temp',
    *,
    bucket: str = 'gismart-analytics',
) -> bool:
    '''Upload file to S3'''
    client = boto3.client('s3')
    filename = os.path.basename(file_path)
    try:
        client.upload_file(file_path, bucket, f'{bucket_dir}/{filename}')
        logger.info(f'{filename} is exported to S3')
        return True
    except ClientError as e:
        logger.error(f'Failed to upload {filename}: {e}')
        return False


def download_file(
    bucket_path: str,
    file_path: str,
    *,
    bucket: str = 'gismart-analytics',
) -> bool:
    '''Download file from S3'''
    filename = os.path.basename(bucket_path)
    if os.path.isdir(file_path):
        file_path = os.path.join(file_path, filename)
    client = boto3.client('s3')
    try:
        client.download_file(bucket, bucket_path, file_path)
        logger.info(f'{filename} downloaded from S3')
        return True
    except ClientError as e:
        logger.error(f'Failed to download {filename}: {e}')
        return False
