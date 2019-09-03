
import pytest
from primrose.configuration.configuration import Configuration
from primrose.dag_runner import DagRunner
import os
import math

def test_run():
    config = {
        "implementation_config": {
            "reader_config": {
                "read_data": {
                    "class": "SklearnDatasetReader",
                    "dataset": "iris",
                    "destinations": [
                        "train_test_split"
                    ]
                }
            },
            "pipeline_config": {
                "train_test_split": {
                    "class": "TrainTestSplit",
                    "features": ["sepal length (cm)", "petal length (cm)", "petal width (cm)"],
                    "target_variable": "sepal width (cm)",
                    "training_fraction": 0.65,
                    "is_training": True,
                    "seed": 42,
                    "destinations": [
                        "regression_model"
                    ]
                }
            },
            "model_config": {
                "regression_model":{
                    "class": "SklearnRegressionModel",
                    "mode": "train",
                    "model": {"class": "linear_model.LinearRegression"},
                    "destinations": []
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)
    data_object = DagRunner(configuration).run()
    scores = data_object.data_dict['regression_model']['scores']
    print(scores)

    assert math.isclose(scores['Explained variance'], 0.531103247696713, abs_tol = 0.00001)
#{'Explained variance': 0.531103247696713, 'Max error': 0.8037485197869163, 'Mean absolute error': 0.2050877131438389, 'MSE': 0.09214453355442812, 'Mean squared log error': 0.005413269099842543, 'R2': 0.5252494593646577, 'eval_time': datetime.datetime(2019, 7, 26, 13, 12, 13, 185343)}
