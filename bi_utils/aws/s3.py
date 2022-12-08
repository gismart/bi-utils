import os
import boto3
import logging
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)


def upload_file(
    file_path: str,
    bucket_dir: str = "dwh/temp",
    *,
    bucket: str = "gismart-analytics",
) -> bool:
    """Upload file to S3"""
    client = boto3.client("s3")
    filename = os.path.basename(file_path)
    try:
        client.upload_file(file_path, bucket, f"{bucket_dir}/{filename}")
        logger.info(f"{filename} is exported to S3")
        return True
    except ClientError as e:
        logger.error(f"Failed to upload {filename}: {e}")
        return False


def download_file(
    bucket_path: str,
    file_path: str,
    *,
    bucket: str = "gismart-analytics",
) -> bool:
    """Download file from S3"""
    filename = os.path.basename(bucket_path)
    if os.path.isdir(file_path):
        file_path = os.path.join(file_path, filename)
    client = boto3.client("s3")
    try:
        client.download_file(bucket, bucket_path, file_path)
        logger.info(f"{filename} downloaded from S3")
        return True
    except ClientError as e:
        logger.error(f"Failed to download {filename}: {e}")
        return False


def download_dir(
    bucket_dir_path: str,
    dir_path: str,
    *,
    bucket: str = "gismart-analytics",
) -> bool:
    """Download directory contents from S3 without subfolders"""
    resource = boto3.resource("s3")
    bucket_resource = resource.Bucket(bucket)
    try:
        for obj in bucket_resource.objects.filter(Prefix=bucket_dir_path):
            filename = os.path.basename(obj.key)
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
            file_path = os.path.join(dir_path, filename)
            bucket_resource.download_file(obj.key, file_path)
            logger.debug(f"{filename} downloaded from S3")
        logger.info(f"{bucket_dir_path} downloaded from S3")
        return True
    except ClientError as e:
        logger.error(f"Failed to download {bucket_dir_path}: {e}")
        return False
