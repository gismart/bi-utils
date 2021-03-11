import csv
import datetime as dt
from typing import Sequence, Optional


def data_filename(
    data_name: str,
    date: dt.date = dt.date.today(),
    *,
    ext: Optional[str] = "pkl",
) -> str:
    '''Build filename for data at specific date'''
    if ext:
        return f"{date}_{data_name}.{ext}"
    return f"{date}_{data_name}"


def csv_columns(csv_path: str, *, separator: str = ",") -> Sequence[str]:
    '''Get column list from csv file'''
    with open(csv_path) as file:
        reader = csv.DictReader(file, delimiter=separator)
        columns = reader.fieldnames or []
    return columns
