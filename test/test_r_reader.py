
import pytest
import sys
import os
import pandas as pd
from primrose.configuration.configuration import Configuration
from primrose.readers.r_reader import RReader
from primrose.node_factory import NodeFactory
from primrose.data_object import DataObject
from primrose.base.node import AbstractNode
from primrose.data_object import DataObject, DataObjectResponseType


def check_rpy2():
    try:
        import rpy2
        return False
    except ImportError as e:
        return True

@pytest.mark.skipif(check_rpy2(), reason="primrose[R] is optional")
def test_run():
    config = {
        "implementation_config": {
            "reader_config": {
                "read_data": {
                    "class": "RReader",
                    "dataset": "iris",
                    "destinations": []
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)
    data_object = DataObject(configuration)

    reader = RReader(configuration, "read_data")
    data_object, terminate = reader.run(data_object)
    assert not terminate
    df = data_object.get('read_data', rtype=DataObjectResponseType.VALUE.value)
    assert df is not None
    assert df.shape == (150, 6)
    assert list(df.columns) == ["Sepal.Length", "Sepal.Width", "Petal.Length", "Petal.Width", "Species", "row_names"]

@pytest.mark.skipif(check_rpy2(), reason="primrose[R] is optional")
def test_run2():
    config = {
        "implementation_config": {
            "reader_config": {
                "read_data": {
                    "class": "RReader",
                    "dataset": "euro",
                    "destinations": []
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)
    data_object = DataObject(configuration)

    reader = RReader(configuration, "read_data")
    data_object, terminate = reader.run(data_object)
    assert not terminate
    df = data_object.get('read_data', rtype=DataObjectResponseType.VALUE.value)
    assert df is not None
    assert df.shape == (11, 2)
    assert list(df.columns) == ["euro", "row_names"]
