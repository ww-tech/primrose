
import pytest
import os
import dill
from primrose.configuration.configuration import Configuration
from primrose.writers.dill_writer import DillWriter
from primrose.data_object import DataObject
from primrose.readers.csv_reader import CsvReader

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
                    "class": "FileWriter",
                    "key": "test_data",
                    "dir": "cache",
                    "filename": "unittest_file_writer.dill"
                }
            }
        }
    }
    return config


def test_necessary_config(config):
    configuration = Configuration(None, is_dict_config=True, dict_config=config)
    writer = DillWriter(configuration, 'recipe_file_writer')
    node_config = {
                    "class": "FileWriter",
                    "key": "test_data",
                    "dir": "cache",
                    "filename": "unittest_file_writer.dill"
                }
    assert isinstance(writer.necessary_config(node_config), set)
    assert len(writer.necessary_config(node_config)) > 0


def test_init_ok(config):

    test_data_string = "some test data"

    configuration = Configuration(None, is_dict_config=True, dict_config=config)

    data_object = DataObject(configuration)

    requestor = CsvReader(configuration, 'csv_reader')

    data_object.add(requestor, test_data_string, 'test_data')

    writer = DillWriter(configuration, 'recipe_file_writer')

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