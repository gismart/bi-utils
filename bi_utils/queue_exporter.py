from __future__ import annotations

import os
import pandas as pd
import dataclasses as dc
import multiprocessing as mp
from typing import Any, Dict, Optional, Sequence, Type
from types import TracebackType

from .logger import get_logger
from . import aws


logger = get_logger(__name__)


@dc.dataclass
class QueueItem:
    item_type: str
    kwargs: Dict[str, Any]


class QueueExporter:
    '''Export queue in its own process'''
    def __init__(
        self,
        *,
        process_name: str = 'exporter',
        temp_s3_bucket: str = 'gismart-analytics',
        temp_s3_bucket_dir: str = 'dwh/temp',
    ) -> None:
        self._queue = mp.Queue()
        self._item_type_funcs = {
            'df': self._export_df,
            'file': self._export_file,
        }
        self._close_signal = 'close'
        self._process_name = process_name
        self._temp_s3_bucket = temp_s3_bucket
        self._temp_s3_bucket_dir = temp_s3_bucket_dir
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
        separator: str = ',',
        columns: Optional[Sequence] = None,
        s3_bucket: Optional[str] = None,
        s3_bucket_dir: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
    ) -> None:
        self._check_args(file_path, s3_bucket, s3_bucket_dir, schema, table)
        self._check_process()
        kwargs = {
            'df': df,
            'file_path': file_path,
            'separator': separator,
            'columns': columns,
            's3_bucket': s3_bucket,
            's3_bucket_dir': s3_bucket_dir,
            'schema': schema,
            'table': table,
        }
        item = QueueItem(item_type='df', kwargs=kwargs)
        self._queue.put(item)

    def export_file(
        self,
        file_path: str,
        /,
        *,
        separator: str = ',',
        s3_bucket: Optional[str] = None,
        s3_bucket_dir: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
    ) -> None:
        self._check_args(file_path, s3_bucket, s3_bucket_dir, schema, table)
        self._check_process()
        kwargs = {
            'file_path': file_path,
            'separator': separator,
            's3_bucket': s3_bucket,
            's3_bucket_dir': s3_bucket_dir,
            'schema': schema,
            'table': table,
        }
        item = QueueItem(item_type='file', kwargs=kwargs)
        self._queue.put(item)

    def _export_df(
        self,
        df: pd.DataFrame,
        file_path: str,
        *,
        separator: str = ',',
        columns: Optional[Sequence] = None,
        s3_bucket: Optional[str] = None,
        s3_bucket_dir: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
    ) -> None:
        filename = os.path.basename(file_path)
        if '.csv' in file_path.lower():
            df.to_csv(file_path, index=False, columns=columns)
        else:
            df.to_pickle(file_path)
        logger.info(f'Saved df to {filename}')
        self._export_file(
            file_path,
            separator=separator,
            s3_bucket=s3_bucket,
            s3_bucket_dir=s3_bucket_dir,
            schema=schema,
            table=table,
        )

    def _export_file(
        self,
        file_path: str,
        *,
        separator: str = ',',
        s3_bucket: Optional[str] = None,
        s3_bucket_dir: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
    ) -> None:
        if schema and table and '.csv' in file_path.lower():
            aws.db.upload_csv(
                file_path,
                schema=schema,
                table=table,
                separator=separator,
                bucket=s3_bucket or self._temp_s3_bucket,
                bucket_dir=s3_bucket_dir or self._temp_s3_bucket_dir,
                delete_s3_after=(s3_bucket is None or s3_bucket_dir is None),
            )
        elif s3_bucket and s3_bucket_dir:
            aws.s3.upload_file(file_path, bucket=s3_bucket, bucket_dir=s3_bucket_dir)

    def _worker(self) -> None:
        while True:
            item = self._queue.get()
            if item.item_type == self._close_signal:
                logger.info(f'Closing {self._process_name}')
                break
            try:
                filename = os.path.basename(item.kwargs.get('file_path', 'unknown'))
                logger.info(f'Started {filename} {item.item_type} export')
                func = self._item_type_funcs[item.item_type]
                func(**item.kwargs)
                logger.info(f'Finished {filename} {item.item_type} export')
            except Exception as e:
                logger.error(f'Exception occured: {e}')

    def _check_args(
        self,
        file_path: str,
        s3_bucket: Optional[str],
        s3_bucket_dir: Optional[str],
        schema: Optional[str],
        table: Optional[str],
    ) -> None:
        if sum(1 for arg in [s3_bucket, s3_bucket_dir] if arg is None) == 1:
            raise ValueError('Pass both s3_bucket and s3_bucket_dir arguments for S3 export')
        if sum(1 for arg in [schema, table] if arg is None) == 1:
            raise ValueError('Pass both schema and table arguments for DB export')
        if schema and table and '.csv' not in file_path.lower():
            raise ValueError('Only csv files can be exported to DB')

    def _check_process(self) -> None:
        if not self._process.is_alive():
            raise ValueError(f'Process {self._process_name} is closed')
        if not self._alive:
            raise ValueError('Queue is closed')
