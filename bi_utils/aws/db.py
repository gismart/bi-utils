import os
import glob
import shutil
import locopy
import posixpath
import pandas as pd
import datetime as dt
from typing import Any, Iterable, Iterator, Sequence, Optional, Union

from ..logger import get_logger
from .. import files, sql
from . import connection


logger = get_logger(__name__)


def upload_csv(
    csv_path: str,
    schema: str,
    table: str,
    *,
    separator: str = ',',
    bucket: str = 'gismart-analytics',
    bucket_dir: str = 'dwh/temp',
    columns: Optional[Sequence] = None,
    delete_s3_after: bool = True,
    secret_id: str = 'prod/redshift/analytics',
    database: Optional[str] = None,
    retries: int = 0,
) -> None:
    '''Upload csv file to S3 and copy to Redshift'''

    def upload(redshift: locopy.Redshift) -> None:
        redshift.load_and_copy(
            local_file=csv_path,
            s3_bucket=bucket,
            s3_folder=bucket_dir,
            table_name=table_columns,
            delim=separator,
            copy_options=['IGNOREHEADER AS 1', 'REMOVEQUOTES'],
            delete_s3_after=delete_s3_after,
            compress=False,
        )

    if not columns:
        columns = files.csv_columns(csv_path, separator=separator)
    table_columns = f'{schema}.{table} ({",".join(columns)})'
    bucket_dir = _add_timestamp_dir(bucket_dir, postfix='_', posix=True)
    with connection.get_redshift(secret_id, database=database) as redshift_locopy:
        for attempt_number in range(retries + 1):
            try:
                upload(redshift_locopy)
            except locopy.errors.S3UploadError as error:
                if attempt_number == retries:
                    raise error
                else:
                    logger.warning(f'Failed upload attempt #{attempt_number + 1}')
            else:
                filename = os.path.basename(csv_path)
                logger.info(f'{filename} is uploaded to db')
                return


def download_csv(
    query: str,
    data_dir: Optional[str] = None,
    *,
    separator: str = ',',
    bucket: str = 'gismart-analytics',
    bucket_dir: str = 'dwh/temp',
    delete_s3_after: bool = True,
    secret_id: str = 'prod/redshift/analytics',
    database: Optional[str] = None,
    retries: int = 0,
) -> Sequence[str]:
    '''Copy data from RedShift to S3 and download csv files up to 6.2 GB'''

    def download(redshift: locopy.Redshift) -> None:
        redshift.unload_and_copy(
            query=query,
            s3_bucket=bucket,
            s3_folder=bucket_dir,
            export_path=False,
            raw_unload_path=data_dir,
            delimiter=separator,
            delete_s3_after=delete_s3_after,
            parallel_off=False,
            unload_options=['CSV', 'HEADER', 'GZIP', 'PARALLEL ON', 'ALLOWOVERWRITE'],
        )

    if data_dir and not os.path.exists(data_dir):
        os.makedirs(data_dir)
    bucket_dir = _add_timestamp_dir(bucket_dir, postfix='_', posix=True)
    with connection.get_redshift(secret_id, database=database) as redshift_locopy:
        for attempt_number in range(retries + 1):
            try:
                download(redshift_locopy)
            except locopy.errors.S3DownloadError as error:
                if attempt_number == retries:
                    raise error
                else:
                    logger.warning(f'Failed download attempt #{attempt_number + 1}')
            else:
                logger.info('Data is downloaded to csv files')
                filenames = glob.glob(os.path.join(data_dir or os.getcwd(), '*part_00.gz'))
                return filenames


def upload_data(
    data: pd.DataFrame,
    csv_path: str,
    schema: str,
    table: str,
    *,
    separator: str = ',',
    bucket: str = 'gismart-analytics',
    bucket_dir: str = 'dwh/temp',
    columns: Optional[Sequence] = None,
    remove_csv: bool = False,
    secret_id: str = 'prod/redshift/analytics',
    database: Optional[str] = None,
    retries: int = 0,
) -> None:
    '''Save data to csv and upload it to RedShift via S3'''
    filename = os.path.basename(csv_path)
    filedir = os.path.dirname(csv_path)
    if not os.path.exists(filedir):
        os.mkdir(filedir)
    data.to_csv(csv_path, index=False, columns=columns)
    logger.info(f'Data is saved to {filename}')
    upload_csv(
        csv_path=csv_path,
        schema=schema,
        table=table,
        separator=separator,
        bucket=bucket,
        bucket_dir=bucket_dir,
        columns=columns,
        secret_id=secret_id,
        database=database,
        retries=retries,
    )
    if remove_csv:
        os.remove(csv_path)
        logger.info(f'{filename} is removed')


def download_data(
    query: str,
    *,
    temp_dir: str = '/tmp',
    separator: str = ',',
    bucket: str = 'gismart-analytics',
    bucket_dir: str = 'dwh/temp',
    parse_dates: Optional[Sequence[str]] = None,
    parse_bools: Optional[Sequence[str]] = None,
    dtype: Optional[dict] = None,
    chunking: bool = False,
    secret_id: str = 'prod/redshift/analytics',
    database: Optional[str] = None,
    retries: int = 0,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    '''Download data from Redshift via S3'''
    dtype = dtype or {}
    parse_bools = parse_bools or []
    temp_path = _add_timestamp_dir(temp_dir)
    filenames = download_csv(
        query=query,
        data_dir=temp_path,
        separator=separator,
        bucket=bucket,
        bucket_dir=bucket_dir,
        secret_id=secret_id,
        database=database,
        retries=retries,
    )
    chunks = _read_chunks(
        filenames,
        parse_bools=parse_bools,
        separator=separator,
        parse_dates=parse_dates,
        dtype=dtype,
        temp_dir=temp_path,
    )
    if chunking:
        return chunks
    data = pd.concat(chunks, ignore_index=True)
    for bool_col in parse_bools:
        dtype[bool_col] = 'boolean'
    data = data.astype(dtype)
    logger.info('Data is loaded from csv files')
    return data


def delete(
    table: str,
    schema: str,
    secret_id: str = 'prod/redshift/analytics',
    database: Optional[str] = None,
    **equal_conditions: Any,
) -> None:
    '''Delete data from `table` in `schema` with keyword arguments `equal_conditions`'''
    if not equal_conditions:
        raise ValueError('Pass at least 1 equal condition as keyword argument')
    with connection.connect(schema, secret_id=secret_id, database=database) as conn:
        with conn.cursor() as cursor:
            where = sql.build_where(**equal_conditions)
            query = f'DELETE FROM {schema}.{table} {where}'
            cursor.execute(query)
    logger.info(f'Deleted data from {table}')


def _add_timestamp_dir(dir_path: str, postfix: str = '', posix: bool = False) -> str:
    timestamp = f'{dt.datetime.now()}{postfix}'
    if posix:
        dir_path = posixpath.join(dir_path, timestamp)
    else:
        dir_path = os.path.join(dir_path, timestamp)
    return dir_path


def _read_chunks(
    filenames: Sequence[str],
    parse_bools: Iterable[str],
    separator: str = ',',
    parse_dates: Optional[Sequence[str]] = None,
    dtype: Optional[dict] = None,
    temp_dir: Optional[str] = None,
) -> Iterator[pd.DataFrame]:
    boolean_values = {'t': True, 'f': False}
    converters = {
        col: lambda value: boolean_values.get(value, pd.NA) for col in parse_bools
    }
    try:
        for i, filename in enumerate(filenames):
            chunk = pd.read_csv(
                filename,
                na_values=[''],
                converters=converters,
                keep_default_na=False,
                parse_dates=parse_dates,
                sep=separator,
                dtype=dtype,
                low_memory=False,
            )
            yield chunk
            logger.debug(f'Loaded chunk #{i + 1}')
            os.remove(filename)
    finally:
        if temp_dir:
            shutil.rmtree(temp_dir, ignore_errors=True)
