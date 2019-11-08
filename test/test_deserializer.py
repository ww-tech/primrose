import pytest
from sklearn.tree import DecisionTreeClassifier
from primrose.configuration.configuration import Configuration
from primrose.readers.deserializer import Deserializer
from primrose.data_object import DataObject, DataObjectResponseType


def test_init_ok_dill():
    config = {
        "implementation_config": {
            "reader_config": {
                "dill_reader": {
                    "class": "Deserializer",
                    "filename": "test/tinymodel.dill",
                    "deserializer": 'dill',
                    "destinations": []
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)
    data_object = DataObject(configuration)

    reader = Deserializer(configuration, "dill_reader")
    data_object, terminate = reader.run(data_object)
    assert not terminate
    data = data_object.get('dill_reader', rtype=DataObjectResponseType.VALUE.value)

    assert data is not None
    assert set(data.keys()) == {'test', 'model'}

    node_config = {
                    "class": "Deserializer",
                    "filename": "test/tinymodel.dill",
                    "deserializer": 'dill',
                    "destinations": []
                }

    assert isinstance(Deserializer.necessary_config(node_config), set)
    assert len(Deserializer.necessary_config(node_config)) > 0

    assert data['test'] == [1, 2, 3]
    assert isinstance(data['model'], DecisionTreeClassifier)


def test_init_ok_pickle():
    config = {
        "implementation_config": {
            "reader_config": {
                "pickle_reader": {
                    "class": "Deserializer",
                    "filename": "test/tinymodel.pickle",
                    "deserializer": 'pickle',
                    "destinations": []
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)
    data_object = DataObject(configuration)

    reader = Deserializer(configuration, "pickle_reader")
    data_object, terminate = reader.run(data_object)
    assert not terminate
    data = data_object.get('pickle_reader', rtype=DataObjectResponseType.VALUE.value)

    assert data is not None
    assert set(data.keys()) == {'test', 'model'}

    assert data['test'] == [1, 2, 3]
    assert isinstance(data['model'], DecisionTreeClassifier)

def test_init_ok_unsupported():
    config = {
        "implementation_config": {
            "reader_config": {
                "other_reader": {
                    "class": "Deserializer",
                    "filename": "test/tinymodel.pickle",
                    "deserializer": 'other',
                    "destinations": []
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)
    data_object = DataObject(configuration)

    reader = Deserializer(configuration, "other_reader")
    with pytest.raises(Exception, match=r"Unsupported"):
        reader.run(data_object)




