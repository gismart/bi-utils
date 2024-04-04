from __future__ import annotations

import os
import logging
import pandas as pd
import dataclasses as dc
import multiprocessing as mp
from typing import Any, Dict, Optional, Sequence, Type
from types import TracebackType

from . import aws


logger = logging.getLogger(__name__)


@dc.dataclass
class QueueItem:
    """Item of Queue to be exported"""

    item_type: str
    kwargs: Dict[str, Any]


class QueueExporter:
    """Export queue in its own process"""

    def __init__(self, *, process_name: str = "exporter") -> None:
        self._queue = mp.Queue()
        self._item_type_funcs = {
            "df": self._export_df,
            "file": self._export_file,
        }
        self._close_signal = "close"
        self._process_name = process_name
        self._alive = True
        self._process = mp.Process(target=self._worker, name=process_name)
        self._process.start()

    def __enter__(self) -> QueueExporter:
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        if self._alive and self._process.is_alive():
            self.close()

    @property
    def alive(self) -> bool:
        return self._alive

    def close(self) -> None:
        item = QueueItem(item_type=self._close_signal, kwargs={})
        self._queue.put(item)
        self._queue.close()
        self._alive = False

    def join(self) -> None:
        self._process.join()

    def export_df(
        self,
        df: pd.DataFrame,
        /,
        file_path: str,
        *,
        separator: str = ",",
        columns: Optional[Sequence] = None,
        s3_bucket: Optional[str] = None,
        s3_bucket_dir: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
        delete_file_after: bool = False,
        delete_s3_after: bool = False,
        partition_cols: Optional[Sequence] = None,
        secret_id: str = aws.connection.DEFAULT_SECRET_ID,
    ) -> None:
        """
        Save dataframe to `filepath` if `s3_bucket`, `s3_bucket_dir`, `schema`, `table` not passed

        Export dataframe to S3 if `s3_bucket` and `s3_bucket_dir` passed

        Export dataframe to DB if `schema` and `table` passed

        Export dataframe to DB via S3 if `s3_bucket`, `s3_bucket_dir`, `schema`, `table` passed
        """
        self._check_args(
            file_path, s3_bucket, s3_bucket_dir, schema, table, delete_s3_after, delete_file_after
        )
        self._check_process()
        kwargs = {
            "df": df,
            "file_path": file_path,
            "separator": separator,
            "columns": columns,
            "s3_bucket": s3_bucket,
            "s3_bucket_dir": s3_bucket_dir,
            "schema": schema,
            "table": table,
            "delete_file_after": delete_file_after,
            "delete_s3_after": delete_s3_after,
            "partition_cols": partition_cols,
            "secret_id": secret_id,
        }
        item = QueueItem(item_type="df", kwargs=kwargs)
        self._queue.put(item)

    def export_file(
        self,
        file_path: str,
        /,
        s3_bucket: str,
        s3_bucket_dir: str,
        *,
        separator: str = ",",
        schema: Optional[str] = None,
        table: Optional[str] = None,
        delete_file_after: bool = False,
        delete_s3_after: bool = False,
        secret_id: str = aws.connection.DEFAULT_SECRET_ID,
    ) -> None:
        """
        Export file to S3 if `s3_bucket` and `s3_bucket_dir` passed

        Export csv or parquet file to DB via S3
        if `s3_bucket`, `s3_bucket_dir`, `schema`, `table` passed
        """
        self._check_args(
            file_path, s3_bucket, s3_bucket_dir, schema, table, delete_s3_after, delete_file_after
        )
        self._check_process()
        kwargs = {
            "file_path": file_path,
            "separator": separator,
            "s3_bucket": s3_bucket,
            "s3_bucket_dir": s3_bucket_dir,
            "schema": schema,
            "table": table,
            "delete_file_after": delete_file_after,
            "delete_s3_after": delete_s3_after,
            "secret_id": secret_id,
        }
        item = QueueItem(item_type="file", kwargs=kwargs)
        self._queue.put(item)

    def _export_df(
        self,
        df: pd.DataFrame,
        file_path: str,
        *,
        separator: str = ",",
        columns: Optional[Sequence] = None,
        s3_bucket: Optional[str] = None,
        s3_bucket_dir: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
        delete_file_after: bool = False,
        delete_s3_after: bool = False,
        partition_cols: Optional[Sequence] = None,
        secret_id: str = aws.connection.DEFAULT_SECRET_ID,
    ) -> None:
        filename = os.path.basename(file_path)
        if columns:
            df = df[columns]
        if s3_bucket or s3_bucket_dir or not delete_file_after:
            if ".csv" in file_path.lower():
                df.to_csv(file_path, index=False)
            elif ".parquet" in file_path.lower():
                if partition_cols:
                    logger.warning(f"Partitions are not supported for csv files: {filename}")
                df.to_parquet(
                    file_path,
                    partition_cols=partition_cols,
                    coerce_timestamps="us",
                    allow_truncated_timestamps=True,
                    index=False,
                )
            else:
                df.to_pickle(file_path)
            logger.info(f"Saved df to {filename} ({len(df)} rows)")
        if s3_bucket and s3_bucket_dir:
            self._export_file(
                file_path,
                separator=separator,
                s3_bucket=s3_bucket,
                s3_bucket_dir=s3_bucket_dir,
                schema=schema,
                table=table,
                delete_file_after=delete_file_after,
                delete_s3_after=delete_s3_after,
                secret_id=secret_id,
            )
        elif schema and table:
            engine = aws.connection.create_engine(secret_id=secret_id)
            with engine.connect() as connection, connection.begin():
                df.to_sql(
                    table,
                    con=connection,
                    schema=schema,
                    index=False,
                    if_exists="append",
                    method="multi",
                )

    def _export_file(
        self,
        file_path: str,
        s3_bucket: str,
        s3_bucket_dir: str,
        *,
        separator: str = ",",
        schema: Optional[str] = None,
        table: Optional[str] = None,
        delete_file_after: bool = False,
        delete_s3_after: bool = False,
        secret_id: str = aws.connection.DEFAULT_SECRET_ID,
    ) -> None:
        if schema and table and (".csv" in file_path.lower() or ".parquet" in file_path.lower()):
            aws.db.upload_file(
                file_path,
                schema=schema,
                table=table,
                separator=separator,
                bucket=s3_bucket,
                bucket_dir=s3_bucket_dir,
                delete_s3_after=delete_s3_after,
                secret_id=secret_id,
            )
        else:
            aws.s3.upload_file(file_path, bucket=s3_bucket, bucket_dir=s3_bucket_dir)
        if delete_file_after:
            os.remove(file_path)
            logger.info(f"Removed {os.path.basename(file_path)}")

    def _worker(self) -> None:
        while True:
            item = self._queue.get()
            if item.item_type == self._close_signal:
                logger.info(f"Closing {self._process_name}")
                break
            try:
                filename = os.path.basename(item.kwargs.get("file_path", "unknown"))
                logger.info(f"Started {filename} {item.item_type} export")
                func = self._item_type_funcs[item.item_type]
                func(**item.kwargs)
                logger.info(f"Finished {filename} {item.item_type} export")
            except Exception as e:
                logger.error(f"Exception occured: {e}")

    def _check_args(
        self,
        file_path: str,
        s3_bucket: Optional[str],
        s3_bucket_dir: Optional[str],
        schema: Optional[str],
        table: Optional[str],
        delete_s3_after: Optional[bool],
        delete_file_after: Optional[bool],
    ) -> None:
        if sum(1 for arg in [s3_bucket, s3_bucket_dir] if arg is None) == 1:
            raise ValueError("Pass both s3_bucket and s3_bucket_dir arguments for S3 export")
        if sum(1 for arg in [schema, table] if arg is None) == 1:
            raise ValueError("Pass both schema and table arguments for DB export")
        if not schema and not table and delete_s3_after:
            raise ValueError("Only files exported to DB via S3 can be deleted from S3")
        if not schema and not table and not s3_bucket and not s3_bucket_dir and delete_file_after:
            raise ValueError("Only files exported to DB or S3 can be deleted")
        if schema and table and s3_bucket and s3_bucket_dir and (
            ".csv" not in file_path.lower() and ".parquet" not in file_path.lower()
        ):
            raise ValueError("Only csv or parquet files can be exported to DB via S3")

    def _check_process(self) -> None:
        if not self._process.is_alive():
            raise ValueError(f"Process {self._process_name} is closed")
        if not self._alive:
            raise ValueError("Queue is closed")
