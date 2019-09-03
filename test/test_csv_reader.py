import pytest
from primrose.configuration.configuration import Configuration
from primrose.readers.csv_reader import CsvReader
from primrose.data_object import DataObject, DataObjectResponseType

def test_init_ok():
    config = {
        "implementation_config":{
            "reader_config": {
            "csv_reader": {
                "class": "CsvReader",
                "filename": "test/minimal.csv",
                "destinations": []
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)
    data_object = DataObject(configuration)

    reader = CsvReader(configuration, "csv_reader")
    data_object, terminate = reader.run(data_object)
    assert not terminate
    df = data_object.get('csv_reader', rtype=DataObjectResponseType.VALUE.value)
    assert df is not None
    assert df.shape == (2, 2)

    node_config = {
                "class": "CsvReader",
                "filename": "test/minimal.csv",
                "destinations": []
                }

    assert isinstance(CsvReader.necessary_config(node_config), set)
    assert len(CsvReader.necessary_config(node_config)) > 0


def test_kwargs():
    config = {
        "implementation_config":{
            "reader_config": {
            "csv_reader": {
                "class": "CsvReader",
                "filename": "test/minimal.csv",
                "kwargs": {
                    "header": None,
                    "sep": ":"
                },
                "destinations": []
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)
    data_object = DataObject(configuration)

    reader = CsvReader(configuration, "csv_reader")
    data_object, terminate = reader.run(data_object)
    assert not terminate
    df = data_object.get('csv_reader', rtype=DataObjectResponseType.VALUE.value)
    assert df is not None
    assert df.shape == (3, 1)
