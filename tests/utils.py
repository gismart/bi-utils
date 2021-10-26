import os
import pandas as pd
from sklearn.model_selection import ParameterGrid
from typing import Any, Dict


def data_path(filename: str) -> str:
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    data_path = os.path.join(data_dir, filename)
    return data_path


def make_target_data(
    filename: str,
    estimator_type: Any,
    param_grid: Dict[str, list],
    transform_y: bool = False,
) -> None:
    data = pd.read_csv(data_path("cohorts.csv"))
    data = data.drop("conversion_predict", axis=1).dropna()
    grid = ParameterGrid(param_grid)
    target_data = pd.DataFrame()
    for params in grid:
        estimator = estimator_type(**params)
        X = data.drop("conversion", axis=1)
        y = data["conversion"]
        estimator.fit(X, y)
        estimator_data = estimator.transform(X, y) if transform_y else estimator.transform(X)
        if not isinstance(estimator_data, pd.DataFrame):
            estimator_data = pd.DataFrame({"conversion": estimator_data})
        for param, value in params.items():
            estimator_data[param] = str(value)
        target_data = target_data.append(estimator_data, ignore_index=True)
    target_data.to_csv(data_path(filename), index=False)
