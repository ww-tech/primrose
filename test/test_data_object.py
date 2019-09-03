
import pytest
import sys
import os
from primrose.data_object import DataObject
from primrose.data_object import DataObjectResponseType
from primrose.configuration.configuration import Configuration
from primrose.configuration.util import OperationType
from primrose.readers.csv_reader import CsvReader

@pytest.fixture()
def setup_vars():
    config = {
        "implementation_config": {
            "reader_config": {
                "csv_reader": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ['recipe_s3_writer']
                }
            },
            "writer_config": {
                "recipe_s3_writer": {
                    "class": "S3Writer",
                    "dir": "cache",
                    "key": "data",
                    "bucket_name": "does_not_exist_bucket_name",
                    "bucket_filename": "does_not_exist.csv"
                }
            }
        }
    }
    configuration = Configuration(None, is_dict_config=True, dict_config=config)
    data_object = DataObject(configuration)
    return configuration, data_object

def test_repr(setup_vars):
    configuration, data_object = setup_vars
    reader = CsvReader(configuration, 'csv_reader')
    data_to_save = "TESTING"
    data_object.add(reader, data_to_save)

    assert str(data_object) == "DataObject:defaultdict(<class 'dict'>, {'csv_reader': {'data': 'TESTING'}})"

def test_add(setup_vars):
    '''test value'''
    configuration, data_object = setup_vars
    reader = CsvReader(configuration, 'csv_reader')
    data_to_save = "TESTING"
    data_object.add(reader, data_to_save)

    response = data_object.get('csv_reader',rtype=DataObjectResponseType.VALUE.value)
    assert response == data_to_save

def test_add2(setup_vars):
    '''test key value'''
    configuration, data_object = setup_vars
    reader = CsvReader(configuration, 'csv_reader')
    data_to_save = "TESTING"
    data_object.add(reader, data_to_save)

    response = data_object.get('csv_reader',rtype=DataObjectResponseType.KEY_VALUE.value)
    assert isinstance(response, dict)
    assert DataObject.DATA_KEY in response
    assert response[DataObject.DATA_KEY] == data_to_save

def test_add3(setup_vars):
    '''test instance key value'''
    configuration, data_object = setup_vars
    reader = CsvReader(configuration, 'csv_reader')
    data_to_save = "TESTING"
    data_object.add(reader, data_to_save)

    response = data_object.get('csv_reader',rtype=DataObjectResponseType.INSTANCE_KEY_VALUE.value)
    assert isinstance(response, dict)
    assert 'csv_reader' in response
    assert 1 == len(list(response.keys()))
    assert response['csv_reader'][DataObject.DATA_KEY] == data_to_save

def test_add4(setup_vars):
    '''test add 2 items'''
    configuration, data_object = setup_vars
    reader = CsvReader(configuration, 'csv_reader')
    data_to_save = "TESTING"
    data_object.add(reader, data_to_save)

    data_object.add(reader, data_to_save, overwrite=True)

    with pytest.raises(Exception) as e:
        data_object.add(reader, data_to_save, overwrite=False)
    assert "Key already exists for csv_reader" in str(e)

def test_get(setup_vars):
    configuration, data_object = setup_vars
    reader = CsvReader(configuration, 'csv_reader')
    data_to_save = "TESTING"
    data_object.add(reader, data_to_save)

    with pytest.raises(Exception) as e:
        data_object.get('reader', rtype='junk')
    assert "Unrecognized rtype: junk" in str(e)

def test_get2(setup_vars):
    configuration, data_object = setup_vars
    with pytest.raises(Exception) as e:
        data_object.get('junk')
    assert "Key not found: junk" in str(e)

def test_get3(setup_vars):
    configuration, data_object = setup_vars
    assert len(data_object.data_dict.keys()) == 0

    reader = CsvReader(configuration, 'csv_reader')
    data_to_save = "TESTING"
    data_object.add(reader, data_to_save)
    assert len(data_object.data_dict.keys()) == 1

    data_object.get('csv_reader',pop_data=False)
    assert len(data_object.data_dict.keys()) == 1

    data_object.get('csv_reader',pop_data=True)
    assert len(data_object.data_dict.keys()) == 0

def test_get4(setup_vars):
    configuration, data_object = setup_vars
    reader = CsvReader(configuration, 'csv_reader')
    data_object.add(reader, key='k1', data='v1')
    data_object.add(reader, key='k2', data='v2')
    response = data_object.get(reader.instance_name, rtype=DataObjectResponseType.VALUE.value)
    assert isinstance(response, dict)
    assert len(response.keys()) == 2

def test_upstream_keys(setup_vars):
    configuration, data_object = setup_vars
    reader = CsvReader(configuration, 'csv_reader')
    data_to_save = "TESTING"
    data_object.add(reader, data_to_save)

    keys = data_object.upstream_keys('recipe_s3_writer')
    assert keys == ['csv_reader']

    keys = data_object.upstream_keys('recipe_s3_writer', operation_type_filter=OperationType.reader)
    assert keys == ['csv_reader']

    keys = data_object.upstream_keys('recipe_s3_writer', operation_type_filter=OperationType.writer)
    assert keys == []

def test_get_upstream_data(setup_vars):
    configuration, data_object = setup_vars
    with pytest.raises(Exception) as e:
        data_object.get_upstream_data('recipe_s3_writer')
    assert "No upstream keys with data found for recipe_s3_writer" in str(e)

def test_get_upstream_data2(setup_vars):
    configuration, data_object = setup_vars
    reader = CsvReader(configuration, 'csv_reader')
    data_to_save = "TESTING"
    data_object.add(reader, data_to_save)
    response = data_object.get_upstream_data('recipe_s3_writer')
    assert isinstance(response, dict)
    assert DataObject.DATA_KEY in response
    assert response[DataObject.DATA_KEY] == data_to_save

def test_get_upstream_data3(setup_vars):
    configuration, data_object = setup_vars
    reader = CsvReader(configuration, 'csv_reader')
    data_to_save = "TESTING"
    data_object.add(reader, data_to_save)
    response = data_object.get_upstream_data('recipe_s3_writer')
    assert isinstance(response, dict)
    assert DataObject.DATA_KEY in response
    assert response[DataObject.DATA_KEY] == data_to_save

def test_get_upstream_data4():
    config = {
        "implementation_config": {
            "reader_config": {
                "csv_reader1": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ['recipe_s3_writer']
                },

                "csv_reader2": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ['recipe_s3_writer']
                }
            },
            "writer_config": {
                "recipe_s3_writer": {
                    "class": "S3Writer",
                    "dir": "cache",
                    "key": "data",
                    "bucket_name": "does_not_exist_bucket_name",
                    "bucket_filename": "does_not_exist.csv"
                }
            }
        }
    }

    configuration = Configuration(None, is_dict_config=True, dict_config=config)

    data_object = DataObject(configuration)

    reader1 = CsvReader(configuration, 'csv_reader1')
    reader2 = CsvReader(configuration, 'csv_reader2')

    data_object.add(reader1, "data1")
    data_object.add(reader2, "data2")

    response = data_object.get_upstream_data('recipe_s3_writer')
    assert isinstance(response, dict)
    assert 'csv_reader1' in response
    assert 'csv_reader2' in response
    assert response['csv_reader1'][DataObject.DATA_KEY] == "data1"
    assert response['csv_reader2'][DataObject.DATA_KEY] == "data2"

    response = data_object.get_upstream_data('recipe_s3_writer')
    assert isinstance(response, dict)
    assert response['csv_reader1'][DataObject.DATA_KEY] == "data1"
    assert response['csv_reader2'][DataObject.DATA_KEY] == "data2"

def test_caching():
    config = {
        "implementation_config": {
            "reader_config": {
                "csv_reader": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": []
                }
            }
        }
    }
    configuration = Configuration(None, is_dict_config=True, dict_config=config)
    data_object = DataObject(configuration)

    writer = CsvReader(configuration, "csv_reader")

    data_object.add(writer, "some_data")

    filename = "test_data_object_cache.pkl"
    if os.path.exists(filename):
        os.remove(filename)

    data_object.write_to_cache(filename)

    assert os.path.exists(filename)

    restored_data_object = DataObject.read_from_cache(filename)

    assert isinstance(restored_data_object, DataObject)

    assert restored_data_object.get("csv_reader", rtype=DataObjectResponseType.VALUE.value) == "some_data"

    if os.path.exists(filename):
        os.remove(filename)

def test_get_filtered_upstream_data():
    config = {
        "implementation_config": {
            "reader_config": {
                "csv_reader": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ['recipe_s3_writer']
                }
            },
            "writer_config": {
                "recipe_s3_writer": {
                    "class": "S3Writer",
                    "dir": "cache",
                    "key": "data",
                    "bucket_name": "does_not_exist_bucket_name",
                    "bucket_filename": "does_not_exist.csv"
                }
            }
        }
    }

    configuration = Configuration(None, is_dict_config=True, dict_config=config)

    data_object = DataObject(configuration)

    reader = CsvReader(configuration, 'csv_reader')

    data_object.add(reader, "some_data_to_save")

    data = data_object.get_filtered_upstream_data("recipe_s3_writer", "data")
    assert data == {'data': 'some_data_to_save'}
    assert not isinstance(data, list)

    data = data_object.get_filtered_upstream_data("recipe_s3_writer", "JUNK")
    assert not data


def test_get_filtered_multiple_upstream_data():
    config = {
        "implementation_config": {
            "reader_config": {
                "csv_reader1": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ['recipe_s3_writer']
                },
                "csv_reader2": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ['recipe_s3_writer']
                }
            },
            "writer_config": {
                "recipe_s3_writer": {
                    "class": "S3Writer",
                    "dir": "cache",
                    "key": "data",
                    "bucket_name": "does_not_exist_bucket_name",
                    "bucket_filename": "does_not_exist.csv"
                }
            }
        }
    }

    configuration = Configuration(None, is_dict_config=True, dict_config=config)

    data_object = DataObject(configuration)

    reader1 = CsvReader(configuration, 'csv_reader1')
    reader2 = CsvReader(configuration, 'csv_reader2')

    data_object.add(reader1, "some_data_to_save")
    data_object.add(reader2, "some_data_to_save")

    data = data_object.get_filtered_upstream_data("recipe_s3_writer", "data")
    assert data == [{'data': 'some_data_to_save'}, {'data': 'some_data_to_save'}]
    assert isinstance(data, list)

    data = data_object.get_filtered_upstream_data("recipe_s3_writer", "JUNK")
    assert not data