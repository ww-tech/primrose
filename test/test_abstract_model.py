import pytest
import logging
from primrose.node_factory import NodeFactory
from primrose.base.model import AbstractModel
from primrose.data_object import DataObject
from testfixtures import LogCapture
from primrose.configuration.configuration import Configuration
from primrose.readers.csv_reader import CsvReader
import pandas as pd

def test_run():
    class TestModel(AbstractModel):

        @staticmethod 
        def necessary_config(node_config): return set(['mode']) 

        def train_model(self, data_object):
            logging.info("TRAIN called")
            return data_object

        def eval_model(self, data_object):
            logging.info("EVAL called")
            return data_object

        def predict(self, data_object):
            logging.info("PREDICT called")
            return data_object

    NodeFactory().register("TestModel", TestModel)

    config = {
        "implementation_config": {
            "reader_config": {
                "myreader": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ["mymodel"]
                }
            },
            "model_config": {
                "mymodel": {
                    "class": "TestModel",
                    "mode": "train"
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)

    data_object = DataObject(configuration)

    reader = CsvReader(configuration, "myreader")
    df = pd.read_csv("test/minimal.csv")

    data_object.add(reader, df)

    model = TestModel(configuration, "mymodel")


    with LogCapture() as l:
        model.run(data_object)
    l.check(
        ('root', 'INFO', 'TRAIN called'),
        ('root', 'INFO', 'EVAL called'),
        ('root', 'INFO', 'PREDICT called')
    )


    config = {
        "implementation_config": {
            "reader_config": {
                "myreader": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ["mymodel"]
                }
            },
            "model_config": {
                "mymodel": {
                    "class": "TestModel",
                    "mode": "eval"
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)

    data_object = DataObject(configuration)

    reader = CsvReader(configuration, "myreader")

    data_object.add(reader, df)

    model = TestModel(configuration, "mymodel")

    with LogCapture() as l:
        model.run(data_object)
    l.check(
        ('root', 'INFO', 'EVAL called'),
        ('root', 'INFO', 'PREDICT called')
    )
