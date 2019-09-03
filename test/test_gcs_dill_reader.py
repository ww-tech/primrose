
import pytest
from primrose.readers.gcs_dill_reader import GcsDillReader
from primrose.data_object import DataObject
from primrose.readers.csv_reader import CsvReader
from primrose.configuration.configuration import Configuration
import dill
import os

def test_necessary_config():
    assert len(GcsDillReader.necessary_config({})) == 2

def test_run(monkeypatch):
    # returns 2 objects from dill reader
    config = {
        "implementation_config": {
            "reader_config": {
                "myreader": {
                    "class": "GcsDillReader",
                    "bucket_name": "test1",
                    "blob_name": "test2",
                    "destinations": []
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)

    reader = GcsDillReader(configuration, "myreader")

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

def test_run2(monkeypatch):
    # returns 1 objects from dill reader
    config = {
        "implementation_config": {
            "reader_config": {
                "myreader": {
                    "class": "GcsDillReader",
                    "bucket_name": "test1",
                    "blob_name": "test2",
                    "destinations": []
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)

    reader = GcsDillReader(configuration, "myreader")

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

