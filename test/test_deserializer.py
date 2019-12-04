import dill
import pickle
import pytest
import os
from sklearn.tree import DecisionTreeClassifier

from primrose.configuration.configuration import Configuration
from primrose.data_object import DataObject, DataObjectResponseType
from primrose.readers.csv_reader import CsvReader
from primrose.readers.deserializer import Deserializer, GcsDeserializer


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

def test_gcsdeserializer_necessary_config():
    assert len(GcsDeserializer.necessary_config({})) == 3

def test_run_dill(monkeypatch):
    # returns 2 objects from dill reader
    config = {
        "implementation_config": {
            "reader_config": {
                "myreader": {
                    "class": "GcsDeserializer",
                    "bucket_name": "test1",
                    "blob_name": "test2",
                    "deserializer": "dill",
                    "destinations": []
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)

    reader = GcsDeserializer(configuration, "myreader")

    data_object = DataObject(configuration)

    def fake_blobs():
        with open("test/dill_reader_blob1.pkl", "wb") as dill_file:
            dill.dump("some_data", dill_file)
        with open("test/dill_reader_blob2.pkl", "wb") as dill_file:
            dill.dump("some_other_data", dill_file)
        dat1 = open("test/dill_reader_blob1.pkl", "rb").read()
        dat2 = open("test/dill_reader_blob2.pkl", "rb").read()
        return [dat1, dat2]

    monkeypatch.setattr(reader,'download_blobs_as_strings',fake_blobs)

    reader_object, terminate = reader.run(data_object)

    assert not terminate

    assert "myreader" in reader_object.data_dict

    dat = reader_object.data_dict['myreader']

    assert "reader_data" in dat
    datlist = dat['reader_data']

    assert len(datlist) == 2

    assert "some_data" in datlist
    assert "some_other_data" in datlist

    files = ["test/dill_reader_blob1.pkl", "test/dill_reader_blob2.pkl"]
    for f in files:
        if os.path.exists(f):
            os.remove(f)

def test_run_dill_2(monkeypatch):
    # returns 1 objects from dill reader
    config = {
        "implementation_config": {
            "reader_config": {
                "myreader": {
                    "class": "GcsDillReader",
                    "bucket_name": "test1",
                    "blob_name": "test2",
                    "deserializer": "dill",
                    "destinations": []
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)

    reader = GcsDeserializer(configuration, "myreader")

    data_object = DataObject(configuration)

    def fake_blobs():
        with open("test/dill_reader_blob1.pkl", "wb") as dill_file:
            dill.dump("some_data", dill_file)
        dat1 = open("test/dill_reader_blob1.pkl", "rb").read()
        return [dat1]

    monkeypatch.setattr(reader,'download_blobs_as_strings',fake_blobs)

    reader_object, terminate = reader.run(data_object)

    assert not terminate

    assert "myreader" in reader_object.data_dict

    dat = reader_object.data_dict['myreader']

    assert "reader_data" in dat
    data = dat['reader_data']

    assert "some_data" == data

    files = ["test/dill_reader_blob1.pkl"]
    for f in files:
        if os.path.exists(f):
            os.remove(f)

def test_run_pickle(monkeypatch):
    # returns 2 objects from pickle reader
    config = {
        "implementation_config": {
            "reader_config": {
                "myreader": {
                    "class": "GcsDeserializer",
                    "bucket_name": "test1",
                    "blob_name": "test2",
                    "deserializer": "pickle",
                    "destinations": []
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)

    reader = GcsDeserializer(configuration, "myreader")

    data_object = DataObject(configuration)

    def fake_blobs():
        with open("test/pickle_reader_blob1.pkl", "wb") as pickle_file:
            pickle.dump("some_data", pickle_file)
        with open("test/pickle_reader_blob2.pkl", "wb") as pickle_file:
            pickle.dump("some_other_data", pickle_file)
        dat1 = open("test/pickle_reader_blob1.pkl", "rb").read()
        dat2 = open("test/pickle_reader_blob2.pkl", "rb").read()
        return [dat1, dat2]

    monkeypatch.setattr(reader,'download_blobs_as_strings',fake_blobs)

    reader_object, terminate = reader.run(data_object)

    assert not terminate

    assert "myreader" in reader_object.data_dict

    dat = reader_object.data_dict['myreader']

    assert "reader_data" in dat
    datlist = dat['reader_data']

    assert len(datlist) == 2

    assert "some_data" in datlist
    assert "some_other_data" in datlist

    files = ["test/pickle_reader_blob1.pkl", "test/pickle_reader_blob2.pkl"]
    for f in files:
        if os.path.exists(f):
            os.remove(f)

def test_run_pickle_2(monkeypatch):
    # returns 1 objects from pickle reader
    config = {
        "implementation_config": {
            "reader_config": {
                "myreader": {
                    "class": "GcsDillReader",
                    "bucket_name": "test1",
                    "blob_name": "test2",
                    "deserializer": "pickle",
                    "destinations": []
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)

    reader = GcsDeserializer(configuration, "myreader")

    data_object = DataObject(configuration)

    def fake_blobs():
        with open("test/pickle_reader_blob1.pkl", "wb") as pickle_file:
            pickle.dump("some_data", pickle_file)
        dat1 = open("test/pickle_reader_blob1.pkl", "rb").read()
        return [dat1]

    monkeypatch.setattr(reader,'download_blobs_as_strings',fake_blobs)

    reader_object, terminate = reader.run(data_object)

    assert not terminate

    assert "myreader" in reader_object.data_dict

    dat = reader_object.data_dict['myreader']

    assert "reader_data" in dat
    data = dat['reader_data']

    assert "some_data" == data

    files = ["test/pickle_reader_blob1.pkl"]
    for f in files:
        if os.path.exists(f):
            os.remove(f)

def test_run_other(monkeypatch):
    config = {
        "implementation_config": {
            "reader_config": {
                "myreader": {
                    "class": "GcsDillReader",
                    "bucket_name": "test1",
                    "blob_name": "test2",
                    "deserializer": "other",
                    "destinations": []
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)

    reader = GcsDeserializer(configuration, "myreader")

    data_object = DataObject(configuration)

    with pytest.raises(Exception, match=r"Unsupported"):
        reader.run(data_object)




