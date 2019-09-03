import pytest
from sklearn.tree import DecisionTreeClassifier
from primrose.configuration.configuration import Configuration
from primrose.readers.dill_reader import DillReader
from primrose.data_object import DataObject, DataObjectResponseType


def test_init_ok():
    config = {
        "implementation_config": {
            "reader_config": {
                "dill_reader": {
                    "class": "DillReader",
                    "filename": "test/tinymodel.dill",
                    "destinations": []
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)
    data_object = DataObject(configuration)

    reader = DillReader(configuration, "dill_reader")
    data_object, terminate = reader.run(data_object)
    assert not terminate
    data = data_object.get('dill_reader', rtype=DataObjectResponseType.VALUE.value)

    assert data is not None
    assert set(data.keys()) == {'test', 'model'}

    node_config = {
                    "class": "DillReader",
                    "filename": "test/tinymodel.dill",
                    "destinations": []
                }

    assert isinstance(DillReader.necessary_config(node_config), set)
    assert len(DillReader.necessary_config(node_config)) > 0

    assert data['test'] == [1, 2, 3]
    assert isinstance(data['model'], DecisionTreeClassifier)
