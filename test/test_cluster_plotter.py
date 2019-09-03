
import pytest
from primrose.configuration.configuration import Configuration
from primrose.dag_runner import DagRunner
import os

def test_run():
    config = {
        "metadata": {
            "section_registry": [
                "reader_config",
                "pipeline_config",
                "model_config",
                "dataviz_config"
            ]
        },
        "implementation_config": {
            "reader_config": {
                "read_data": {
                    "class": "CsvReader",
                    "filename": "test/unclustered.csv",
                    "destinations": [
                        "normalize_data"
                    ]
                }
            },
            "pipeline_config": {
                "normalize_data": {
                    "class": "SklearnPreprocessingPipeline",
                    "operations": [
                        {"class":"preprocessing.StandardScaler", "columns": ["x", "y"], "args": {"with_mean": True, "with_std": True}},
                    ],
                    "is_training": True,
                    "training_fraction": 0.65,
                    "seed": 42,
                    "destinations": [
                        "cluster_model"
                    ]
                }
            },
            "model_config": {
                "cluster_model":{
                    "class": "SklearnClusterModel",
                    "mode": "train",
                    "features": ["x","y"],
                    "model": {"class": "cluster.KMeans", "args": {"n_clusters": 6, "random_state": 42}},
                    "destinations": [
                        "cluster_plotter"
                    ]
                }
            },
            "dataviz_config": {
                "cluster_plotter": {
                    "class": "ClusterPlotter",
                    "id_col": "predictions",
                    "filename": "clusters.png",
                    "title": "Results of KMeans(k=6)",
                    "destinations": []
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)

    fname = "clusters.png"

    if os.path.exists(fname):
        os.remove(fname)

    DagRunner(configuration).run()

    assert os.path.exists(fname)

    if os.path.exists(fname):
        os.remove(fname)
