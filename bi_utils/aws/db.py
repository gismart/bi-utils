import os
import glob
import shutil
import locopy
import logging
import posixpath
import pandas as pd
import datetime as dt
from typing import Any, Iterable, Iterator, Sequence, Optional, Union
import pyarrow.parquet as pp

from .. import files, sql
from . import connection


logger = logging.getLogger(__name__)


def upload_file(
    file_path: str,
    schema: str,
    table: str,
    *,
    separator: str = ",",
    bucket: str = "gismart-analytics",
    bucket_dir: str = "dwh/temp",
    columns: Optional[Sequence] = None,
    delete_s3_after: bool = True,
    secret_id: str = connection.DEFAULT_SECRET_ID,
    database: Optional[str] = None,
    host: Optional[str] = None,
    retries: int = 0,
    add_s3_timestamp_dir: bool = True,
) -> None:
    """Upload csv or parquet file to S3 and copy to Redshift"""

    copy_options = []
    if file_path.lower().endswith(".csv"):
        copy_options.append("CSV")
        copy_options.append("IGNOREHEADER 1")
        if not columns:
            columns = files.csv_columns(file_path, separator=separator)
    elif file_path.lower().endswith(".parquet"):
        copy_options.append("PARQUET")
        separator = None
        if not columns:
            columns = pp.ParquetFile(file_path).schema.names
    else:
        raise ValueError(f"{os.path.basename(file_path)} file extension is not supported")
    table_name = f"{schema}.{table}"
    if columns:
        table_name += f" ({','.join(columns)})"
    if add_s3_timestamp_dir:
        bucket_dir = _add_timestamp_dir(bucket_dir, posix=True)
    elif not bucket_dir.endswith("/"):
        bucket_dir += "/"
    with connection.get_redshift(secret_id, database=database, host=host) as redshift_locopy:
        for attempt_number in range(retries + 1):
            try:
                redshift_locopy.load_and_copy(
                    local_file=file_path,
                    s3_bucket=bucket,
                    s3_folder=bucket_dir,
                    table_name=table_name,
                    delim=separator,
                    copy_options=copy_options,
                    delete_s3_after=delete_s3_after,
                    compress=False,
                )
            except locopy.errors.S3UploadError as error:
                if attempt_number == retries:
                    raise error
                else:
                    logger.warning(f"Failed upload attempt #{attempt_number + 1}")
            else:
                logger.info(f"{os.path.basename(file_path)} is uploaded to {schema}.{table}")
                return


def download_files(
    query: str,
    data_dir: Optional[str] = None,
    file_format: str = "csv",
    *,
    separator: str = ",",
    bucket: str = "gismart-analytics",
    bucket_dir: str = "dwh/temp",
    delete_s3_before: bool = False,
    delete_s3_after: bool = True,
    secret_id: str = connection.DEFAULT_SECRET_ID,
    database: Optional[str] = None,
    host: Optional[str] = None,
    retries: int = 0,
    max_chunk_size_mb: int = 6000,
    add_timestamp_dir: bool = True,
    add_s3_timestamp_dir: bool = True,
) -> Sequence[str]:
    """Copy data from RedShift to S3 and download csv or parquet files up to `max_chunk_size_mb`"""
    unload_options = _get_unload_options(file_format, delete_s3_before, max_chunk_size_mb)
    if file_format.lower() == "parquet":
        separator = None
    if add_s3_timestamp_dir:
        bucket_dir = _add_timestamp_dir(bucket_dir, postfix="/", posix=True)
    elif not bucket_dir.endswith("/"):
        bucket_dir += "/"
    if add_timestamp_dir:
        data_dir = _add_timestamp_dir(data_dir or os.getcwd())
    if data_dir and not os.path.exists(data_dir):
        os.makedirs(data_dir)
    with connection.get_redshift(secret_id, database=database, host=host) as redshift_locopy:
        for attempt_number in range(retries + 1):
            try:
                redshift_locopy.unload_and_copy(
                    query=query,
                    s3_bucket=bucket,
                    s3_folder=bucket_dir,
                    export_path=False,
                    raw_unload_path=data_dir,
                    delim=separator,
                    delete_s3_after=delete_s3_after,
                    parallel_off=False,
                    unload_options=unload_options,
                )
            except locopy.errors.S3DownloadError as error:
                if attempt_number == retries:
                    raise error
                else:
                    logger.warning(f"Failed download attempt #{attempt_number + 1}")
            except Exception as error:
                if "no files" in str(error).lower():
                    return []
                raise error
            else:
                logger.info(f"Data is downloaded to {file_format} files")
                filenames = glob.glob(os.path.join(data_dir, "*_part_*"))
                return filenames


def upload_data(
    data: pd.DataFrame,
    file_path: str,
    schema: str,
    table: str,
    *,
    separator: str = ",",
    bucket: str = "gismart-analytics",
    bucket_dir: str = "dwh/temp",
    columns: Optional[Sequence] = None,
    remove_file: bool = False,
    secret_id: str = connection.DEFAULT_SECRET_ID,
    database: Optional[str] = None,
    host: Optional[str] = None,
    retries: int = 0,
    delete_s3_after: bool = True,
    add_s3_timestamp_dir: bool = True,
    partition_cols: Optional[Sequence] = None,
) -> None:
    """Save data to csv or parquet and upload it to RedShift via S3"""
    filename = os.path.basename(file_path)
    filedir = os.path.dirname(file_path)
    if filedir and not os.path.exists(filedir):
        os.mkdir(filedir)
    if columns:
        data = data[columns]
    if file_path.lower().endswith(".csv"):
        if partition_cols:
            logger.warning(f"Partitions are not supported for csv files: {filename}")
        data.to_csv(file_path, index=False, sep=separator)
    elif file_path.lower().endswith(".parquet"):
        data.to_parquet(
            file_path,
            partition_cols=partition_cols,
            coerce_timestamps="us",
            allow_truncated_timestamps=True,
            index=False,
        )
    else:
        raise ValueError(f"{filename} file extension is not supported")
    logger.info(f"Data is saved to {filename} ({len(data)} rows)")
    upload_file(
        file_path=file_path,
        schema=schema,
        table=table,
        separator=separator,
        bucket=bucket,
        bucket_dir=bucket_dir,
        columns=list(data.columns),
        secret_id=secret_id,
        database=database,
        host=host,
        retries=retries,
        delete_s3_after=delete_s3_after,
        add_s3_timestamp_dir=add_s3_timestamp_dir,
    )
    if remove_file:
        os.remove(file_path)
        logger.info(f"{filename} is removed")


def download_data(
    query: str,
    file_format: str = "csv",
    *,
    temp_dir: str = "/tmp",
    separator: str = ",",
    bucket: str = "gismart-analytics",
    bucket_dir: str = "dwh/temp",
    parse_dates: Optional[Sequence[str]] = None,
    parse_bools: Optional[Sequence[str]] = None,
    dtype: Optional[dict] = None,
    chunking: bool = False,
    secret_id: str = connection.DEFAULT_SECRET_ID,
    database: Optional[str] = None,
    host: Optional[str] = None,
    retries: int = 0,
    max_chunk_size_mb: int = 6000,
    remove_files: bool = True,
    delete_s3_before: bool = False,
    delete_s3_after: bool = True,
    add_timestamp_dir: bool = True,
    add_s3_timestamp_dir: bool = True,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Download data from Redshift via S3"""
    filenames = download_files(
        query=query,
        data_dir=temp_dir,
        file_format=file_format,
        separator=separator,
        bucket=bucket,
        bucket_dir=bucket_dir,
        secret_id=secret_id,
        database=database,
        host=host,
        retries=retries,
        max_chunk_size_mb=max_chunk_size_mb,
        delete_s3_before=delete_s3_before,
        delete_s3_after=delete_s3_after,
        add_timestamp_dir=add_timestamp_dir,
        add_s3_timestamp_dir=add_s3_timestamp_dir,
    )
    if filenames:
        data = read_files(
            filenames,
            separator=separator,
            parse_dates=parse_dates,
            parse_bools=parse_bools,
            dtype=dtype,
            chunking=chunking,
            remove_dir=remove_files,
        )
    else:
        data = (df for df in [pd.DataFrame()]) if chunking else pd.DataFrame()
    return data


def unload_data(
    query: str,
    file_format: str = "csv",
    *,
    bucket: str = "gismart-analytics",
    bucket_dir: str = "dwh/temp",
    delete_s3_before: bool = False,
    secret_id: str = connection.DEFAULT_SECRET_ID,
    database: Optional[str] = None,
    host: Optional[str] = None,
    max_chunk_size_mb: int = 6000,
    partition_by: Optional[list[str]] = None,
) -> Sequence[str]:
    """Unload data from RedShift to S3 into csv or parquet files up to `max_chunk_size_mb`"""
    unload_options = _get_unload_options(
        file_format, delete_s3_before, max_chunk_size_mb, partition_by
    )
    if not bucket_dir.endswith("/"):
        bucket_dir += "/"
    with connection.get_redshift(secret_id, database=database, host=host) as redshift_locopy:
        s3_path = redshift_locopy._generate_unload_path(bucket, bucket_dir)
        redshift_locopy.unload(
            query=query,
            s3path=s3_path,
            unload_options=unload_options,
        )


def read_files(
    file_path: Union[str, Sequence[str]],
    *,
    separator: str = ",",
    parse_dates: Optional[Sequence[str]] = None,
    parse_bools: Optional[Sequence[str]] = None,
    dtype: Optional[dict] = None,
    chunking: bool = False,
    remove_dir: bool = False,
) -> pd.DataFrame:
    """Read data from csv or parquet chunks"""
    dtype = dtype or {}
    parse_bools = parse_bools or []
    if isinstance(file_path, str):
        if os.path.isfile(file_path):
            filenames = [file_path]
            file_dir = os.path.dirname(file_path)
        else:
            filenames = [
                os.path.join(file_path, filename)
                for filename in os.listdir(file_path)
                if os.path.isfile(os.path.join(file_path, filename))
            ]
            file_dir = file_path
    else:
        filenames = file_path
        file_dir = os.path.dirname(file_path[0])
    chunks = _read_chunks(
        filenames,
        parse_bools=parse_bools,
        separator=separator,
        parse_dates=parse_dates,
        dtype=dtype,
        temp_dir=file_dir if remove_dir else None,
    )
    if chunking:
        return chunks
    chunks = list(chunks)
    if not chunks:
        return pd.DataFrame()
    data = pd.concat(chunks, ignore_index=True)
    dtype = {
        **dtype,
        **{bool_col: "boolean" for bool_col in parse_bools}
    }
    data = data.astype(dtype)
    logger.info(f"Data is loaded from files ({len(data)} rows)")
    return data


def update(
    table: str,
    schema: str,
    params_set: dict,
    params_where: Optional[dict],
    secret_id: str = connection.DEFAULT_SECRET_ID,
    database: Optional[str] = None,
    host: Optional[str] = None,
) -> None:
    """
    Update data in `table` in `schema`.
    Set `params_set` where `params_where`
    """
    if params_where is None:
        params_where = {}
    with connection.connect(schema, secret_id=secret_id, database=database, host=host) as conn:
        with conn.cursor() as cursor:
            set_str = sql.build_set(**params_set)
            where_str = sql.build_where(**params_where)
            query = f"UPDATE {schema}.{table} {set_str} {where_str}"
            cursor.execute(query)
    logger.info(f"Updated data in {schema}.{table}")


def delete(
    table: str,
    schema: str,
    secret_id: str = connection.DEFAULT_SECRET_ID,
    database: Optional[str] = None,
    host: Optional[str] = None,
    **conditions: Any,
) -> None:
    """
    Delete data from `table` in `schema` with keyword arguments `conditions`.
    It will be equal condition in case if condition value has primitive type or
    include condition in case if it is an iterable
    """
    if not conditions:
        raise ValueError("Pass at least 1 equal condition as keyword argument")
    with connection.connect(schema, secret_id=secret_id, database=database, host=host) as conn:
        with conn.cursor() as cursor:
            where_str = sql.build_where(**conditions)
            query = f"DELETE FROM {schema}.{table} {where_str}"
            cursor.execute(query)
    logger.info(f"Deleted data from {schema}.{table}")


def get_columns(
    table: str,
    schema: str,
    secret_id: str = connection.DEFAULT_SECRET_ID,
    database: Optional[str] = None,
    host: Optional[str] = None,
) -> Sequence[str]:
    with connection.connect(schema, secret_id=secret_id, database=database, host=host) as conn:
        with conn.cursor() as cursor:
            query = f"SELECT * FROM {schema}.{table} LIMIT 0"
            cursor.execute(query)
            return [desc[0] for desc in cursor.description]


def _add_timestamp_dir(dir_path: str, postfix: str = "", posix: bool = False) -> str:
    timestamp = dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S_%f") + postfix
    if posix:
        dir_path = posixpath.join(dir_path, timestamp)
    else:
        dir_path = os.path.join(dir_path, timestamp)
    return dir_path


def _get_unload_options(
    file_format: str = "csv",
    delete_s3_before: bool = False,
    max_chunk_size_mb: int = 6000,
    partition_by: Optional[list[str]] = None,
) -> list[str]:
    max_chunk_size_opt = f"MAXFILESIZE {max_chunk_size_mb} MB"
    if file_format.lower() == "csv":
        unload_options = ["CSV", "HEADER", "GZIP", "PARALLEL ON", max_chunk_size_opt]
    elif file_format.lower() == "parquet":
        unload_options = ["PARQUET", "PARALLEL ON", max_chunk_size_opt]
    else:
        raise ValueError(f"{file_format} file format is not supported")
    if delete_s3_before:
        unload_options.append("CLEANPATH")
    else:
        unload_options.append("ALLOWOVERWRITE")
    if partition_by:
        unload_options.append(f"PARTITION BY ({', '.join(partition_by)}) INCLUDE")
    return unload_options


def _read_chunks(
    filenames: Sequence[str],
    parse_bools: Iterable[str],
    separator: str = ",",
    parse_dates: Optional[Sequence[str]] = None,
    dtype: Optional[dict] = None,
    temp_dir: Optional[str] = None,
) -> Iterator[pd.DataFrame]:
    boolean_values = {"t": True, "f": False}
    converters = {col: lambda value: boolean_values.get(value, pd.NA) for col in parse_bools}
    full_dtype = {
        **dtype,
        **{bool_col: "boolean" for bool_col in parse_bools}
    }
    try:
        for i, filename in enumerate(filenames):
            if filename.lower().endswith((".csv", ".gz")):
                chunk = pd.read_csv(
                    filename,
                    na_values=[""],
                    converters=converters,
                    keep_default_na=False,
                    parse_dates=parse_dates,
                    sep=separator,
                    dtype=dtype,
                    low_memory=False,
                )
            elif filename.lower().endswith(".parquet"):
                chunk = pd.read_parquet(filename)
                if not chunk.empty:
                    chunk = chunk.astype(full_dtype)
            else:
                raise ValueError(f"{os.path.basename(filename)} file extension is not supported")
            if chunk.empty:
                logger.debug(f"Chunk #{i + 1} is empty")
            else:
                logger.debug(f"Loaded chunk #{i + 1}")
                yield chunk
            if temp_dir:
                os.remove(filename)
    finally:
        if temp_dir:
            shutil.rmtree(temp_dir, ignore_errors=True)
