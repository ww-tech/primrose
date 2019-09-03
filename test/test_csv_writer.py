
import pytest
import sys
import os
import pandas as pd
from primrose.configuration.configuration import Configuration
from primrose.writers.csv_writer import CsvWriter
from primrose.readers.csv_reader import CsvReader
from primrose.data_object import DataObject

@pytest.fixture()
def config():
    config = {
        "implementation_config": {
            "reader_config": {
                "csv_reader": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ["recipe_csv_writer"]
                }
            },
            "writer_config": {
                "recipe_csv_writer": {
                    "class": "CsvWriter",
                    "key":"test_data",
                    "dir": "cache",
                    "filename": "unittest_similar_recipes.csv"
                }
            }
        }
    }
    return config

def test_necessary_config(config):
    configuration = Configuration(None, is_dict_config=True, dict_config=config)
    writer = CsvWriter(configuration, 'recipe_csv_writer')
    node_config = {
                    "class": "CsvWriter",
                    "key":"test_data",
                    "dir": "cache",
                    "filename": "unittest_similar_recipes.csv"
                }
    assert isinstance(writer.necessary_config(node_config), set)
    assert len(writer.necessary_config(node_config)) > 0

def test_init_ok(config):
    corpus = pd.read_csv("test/minimal.csv")

    configuration = Configuration(None, is_dict_config=True, dict_config=config)

    writer = CsvWriter(configuration, 'recipe_csv_writer')

    data_object = DataObject(configuration)
    requestor = CsvReader(configuration, 'csv_reader')
    data_object.add(requestor, key='test_data', data=corpus)


    c = configuration.config_for_instance('recipe_csv_writer') #configuration.sec .writer_config['recipe_csv_writer']
    filename = c['dir'] + os.path.sep + c['filename']

    #clean out test file location 
    if os.path.exists(filename):
        os.remove(filename)

    writer.run(data_object)

    assert os.path.exists(filename)

    df = pd.read_csv(filename)

    assert corpus.equals(df)

@pytest.fixture()
def kwargs_config():
    config = {
        "implementation_config": {
            "reader_config": {
                "csv_reader": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ["recipe_csv_writer"]
                }
            },
            "writer_config": {
                "recipe_csv_writer": {
                    "class": "CsvWriter",
                    "key":"test_data",
                    "dir": "cache",
                    "filename": "unittest_similar_recipes.csv",
                    "kwargs": {
                        "header": None,
                        "sep": ":"
                    }
                }
            }
        }
    }
    return config

def test_kwargs(kwargs_config):
    corpus = pd.read_csv("test/minimal.csv")

    configuration = Configuration(None, is_dict_config=True, dict_config=kwargs_config)
    writer = CsvWriter(configuration, 'recipe_csv_writer')

    data_object = DataObject(configuration)
    requestor = CsvReader(configuration, 'csv_reader')
    data_object.add(requestor, key='test_data', data=corpus)


    c = configuration.config_for_instance('recipe_csv_writer') #configuration.sec .writer_config['recipe_csv_writer']
    filename = c['dir'] + os.path.sep + c['filename']

    #clean out test file location
    if os.path.exists(filename):
        os.remove(filename)

    writer.run(data_object)

    assert os.path.exists(filename)

    df = pd.read_csv(filename,header=None, sep=':')

    assert df.shape == (2,2)