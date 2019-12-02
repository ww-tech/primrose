import dill
import os
import pickle
import pytest
from primrose.configuration.configuration import Configuration
from primrose.data_object import DataObject
from primrose.readers.csv_reader import CsvReader
from primrose.writers.serializer import Serializer

@pytest.fixture()
def config():
    config = {
        "implementation_config": {
            "reader_config": {
                "csv_reader": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ['recipe_file_writer']
                }
            },
            "writer_config": {
                "recipe_file_writer": {
                    "class": "Serializer",
                    "key": "test_data",
                    "dir": "cache",
                    "filename": "unittest_file_writer.dill",
                    "serializer": "dill"
                }
            }
        }
    }
    return config


def test_necessary_config(config):
    configuration = Configuration(None, is_dict_config=True, dict_config=config)
    writer = Serializer(configuration, 'recipe_file_writer')
    node_config = {
                    "class": "Serializer",
                    "key": "test_data",
                    "dir": "cache",
                    "filename": "unittest_file_writer.dill",
                    "serializer": "dill"
                }
    assert isinstance(writer.necessary_config(node_config), set)
    assert len(writer.necessary_config(node_config)) > 0


def test_init_dill_ok(config):

    test_data_string = "some test data"

    configuration = Configuration(None, is_dict_config=True, dict_config=config)

    data_object = DataObject(configuration)

    requestor = CsvReader(configuration, 'csv_reader')

    data_object.add(requestor, test_data_string, 'test_data')

    writer = Serializer(configuration, 'recipe_file_writer')

    c = configuration.config_for_instance('recipe_file_writer')
    filename = c['dir'] + os.path.sep + c['filename']

    #clean out test file location
    if os.path.exists(filename):
        os.remove(filename)

    data_object, terminate = writer.run(data_object)

    assert not terminate

    assert os.path.exists(filename)

    read_data = dill.load(open(filename, 'rb'))

    assert test_data_string == read_data

def test_init_pickle_ok(config):
    config["implementation_config"]["writer_config"]["recipe_file_writer"]["filename"] = "unittest_file_writer.pickle"
    config["implementation_config"]["writer_config"]["recipe_file_writer"]["serializer"] = "pickle"

    test_data_string = "some test data"

    configuration = Configuration(None, is_dict_config=True, dict_config=config)

    data_object = DataObject(configuration)

    requestor = CsvReader(configuration, 'csv_reader')

    data_object.add(requestor, test_data_string, 'test_data')

    writer = Serializer(configuration, 'recipe_file_writer')

    c = configuration.config_for_instance('recipe_file_writer')
    filename = c['dir'] + os.path.sep + c['filename']

    #clean out test file location
    if os.path.exists(filename):
        os.remove(filename)

    data_object, terminate = writer.run(data_object)

    assert not terminate

    assert os.path.exists(filename)

    read_data = pickle.load(open(filename, 'rb'))

    assert test_data_string == read_data

def test_init_other_ok(config):
    config["implementation_config"]["writer_config"]["recipe_file_writer"]["filename"] = "unittest_file_writer.other"
    config["implementation_config"]["writer_config"]["recipe_file_writer"]["serializer"] = "other"

    test_data_string = "some test data"

    configuration = Configuration(None, is_dict_config=True, dict_config=config)

    data_object = DataObject(configuration)

    requestor = CsvReader(configuration, 'csv_reader')

    data_object.add(requestor, test_data_string, 'test_data')

    writer = Serializer(configuration, 'recipe_file_writer')

    c = configuration.config_for_instance('recipe_file_writer')
    filename = c['dir'] + os.path.sep + c['filename']

    #clean out test file location
    if os.path.exists(filename):
        os.remove(filename)

    with pytest.raises(Exception, match=r"Unsupported"):
        writer.run(data_object)
