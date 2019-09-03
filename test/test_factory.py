
import pytest
from primrose.node_factory import NodeFactory
from primrose.writers.abstract_file_writer import AbstractFileWriter
from primrose.configuration.configuration import Configuration
from primrose.readers.csv_reader import CsvReader

def test_init():
    f1 = NodeFactory()
    f2 = NodeFactory()

    class TestWriter(AbstractFileWriter):
        def __init__(self, configuration, instance_name):
            pass
        def write(self, data_dict):
            pass

    f1.register("TestWriter", TestWriter)

    n = len(set(f1.name_dict.keys()))
    assert set(f1.name_dict.keys()) == set(f2.name_dict.keys())
    assert f1 != f2
    assert "TestWriter" in f1.name_dict
    assert "TestWriter" in f2.name_dict

    # we are doing this to cleanup the singleton and maintain state
    f1.unregister("TestWriter")

    # but we as well make into a test
    assert len(set(f1.name_dict.keys())) == n - 1
    assert "TestWriter" not in f1.name_dict

def test_unregister():
    f1 = NodeFactory()
    class TestWriter(AbstractFileWriter):
        def __init__(self, configuration, instance_name):
            pass
        def write(self, data_dict):
            pass

    n_before = len(set(f1.name_dict.keys()))
    f1.register("TestWriter", TestWriter)
    n_after = len(set(f1.name_dict.keys()))
    assert n_after == n_before + 1

    f1.unregister('TestWriter')

    assert len(set(f1.name_dict.keys())) == n_before

    with pytest.raises(Exception) as e:
        f1.unregister('doesnotexist')
    assert 'Key not found doesnotexist' in str(e)

def test_valid_configuration():
    f = NodeFactory()
    class TestWriter(AbstractFileWriter):
        def __init__(self, configuration, instance_name):
            super(TestWriter, self).__init__(configuration, instance_name)
        @staticmethod
        def necessary_config(node_config):
            return set(['key1', 'key2'])
        def write(self, data_dict):
            pass

    f.register('TestWriter', TestWriter)

    config = {
        "class": "TestWriter",
        "key1": "someval1",
        "key2": "someval2",
        "destinations": []
    }

    is_valid = f.valid_configuration(TestWriter, config)
    assert is_valid

    missing_config = {
        "class": "TestWriter",
        "key2": "someval2",
        "destinations": []
    }

    with pytest.raises(Exception) as e:
        f.valid_configuration(TestWriter, missing_config)
    assert 'Configuration missing necessary keys for' in str(e)
