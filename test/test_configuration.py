import pytest
import sys
import os
import json
from primrose.configuration.configuration import Configuration
from primrose.writers.abstract_file_writer import AbstractFileWriter
from primrose.node_factory import NodeFactory

def test_init_error0():
    config = {}
    with pytest.raises(Exception) as e:
        Configuration(config_location=None, is_dict_config=True, dict_config=config)
    assert 'Did not find required top-level key implementation_config' in str(e)

def test_init_error1():
    with pytest.raises(Exception) as e:
        Configuration(None)
    if sys.version_info[:2] == (3, 5):
        assert "stat: can't specify None for path argument" in str(e)
    else:
        assert 'stat: path should be string, bytes, os.PathLike or integer, not NoneType' in str(e)

def test_init_error2():
    with pytest.raises(Exception) as e:
        Configuration(config_location="junky_path_does_not_exist")
    assert 'config file at: junky_path_does_not_exist not found' in str(e)

def test_init_error3():
    with pytest.raises(Exception) as e:
        Configuration(config_location=None, is_dict_config=True, dict_config=None)
    assert 'expected dict_config was None' in str(e)

def test_init_error4():
    with pytest.raises(Exception) as e:
        Configuration(config_location=None, is_dict_config=True, dict_config="1234")
    assert 'did not receive expected dict_config' in str(e)

def test_init_error5():
    config = {  
        "implementation_config": {
            "k1": {"a":1},
            "k2": {"a":2}
        }
    }
    with pytest.raises(Exception) as e:
        Configuration(config_location=None, is_dict_config=True, dict_config=config)
    assert 'Operations must all have unique names in the configuration' in str(e)

def test_init_error6():
    with pytest.raises(Exception) as e:
        filename = "test/test_dupe_toplevel.json"
        Configuration(config_location=filename, is_dict_config=False, dict_config=None)
    assert "duplicate key: 'model_config'" in str(e)

def test_init_error7():
    config = {  
        "junk": {}
    }
    with pytest.raises(Exception) as e:
        Configuration(config_location=None, is_dict_config=True, dict_config=config)
    assert 'Unsupported top-level key: junk. Supported keys are [\'metadata\', \'implementation_config\']' in str(e)

def test_init_error8():
    with pytest.raises(ValueError) as e:
        Configuration('test/tennis.csv')
    assert 'config file at: test/tennis.csv has improper extension type - please use a .json or .yml file' in str(e)

def test_metadata():
    config = {
        "metadata":{"k":"v"},
        "implementation_config":{}
    }
    c = Configuration(config_location=None, is_dict_config=True, dict_config=config)
    assert c.config_metadata["k"] == "v"

def test_init_ok():
    config = {"implementation_config":{}}
    c = Configuration(config_location=None, is_dict_config=True, dict_config=config)
    assert c.config_string
    assert c.config_hash

def test_init_ok2():
    config = {
        "metadata": {
            "traverser": "DepthFirstTraverser"
        },
        "implementation_config": {
            "reader_config": {
                "csv_reader": {
                    "class": "CsvReader",
                    "filename": "some/path/to/file",
                    "destinations": []
                }
            }
        }
    }
    c = Configuration(config_location=None, is_dict_config=True, dict_config=config)
    assert c.config_string
    assert c.config_hash == 'b434870806fe0c61ddf2b417ae62c1be519b069fb6e12572f4ff8143f98086ff'

def test_unrecognized():
    config = {
        "implementation_config": {
            "junky_config": {
                "csv_reader": {
                    "class": "CsvReader",
                    "filename": "some/path/to/file",
                    "destinations": []
                }
            }
        }
    }
    with pytest.raises(Exception) as e:
        Configuration(config_location=None, is_dict_config=True, dict_config=config)
    assert 'Unspported operation: junky_config' in str(e)

def test_nopipelinekey():
    config = {
        "implementation_config":{
            "pipeline_config": {
                "corpus_pipeline": {
                    "destinations": ["recipe_name_model"]
                }
            }
        }
    }
    with pytest.raises(Exception) as e:
        Configuration(config_location=None, is_dict_config=True, dict_config=config)
    assert 'No class key found in pipeline_config.corpus_pipeline' in str(e)

def test_metadata2():
    config = {
        "metadata": {
            "traverser": "doesnotexist"
        },
        "implementation_config":{
        }
    }
    with pytest.raises(Exception) as e:
        Configuration(config_location=None, is_dict_config=True, dict_config=config)
    assert 'doesnotexist is not a valid and/or registered Traverser' in str(e)

def test_check_metadata():
    config = {
        "metadata": {
            "data_object": {
                "read_from_cache": True
            }
        },
        "implementation_config":{
        }
    }
    with pytest.raises(Exception) as e:
        Configuration(config_location=None, is_dict_config=True, dict_config=config)
    assert "metadata.data_object: if read_from_cache==true, you must set 'read_filename'" in str(e)

def test_check_metadata2():
    config = {
        "metadata": {
            "data_object": {
                "read_from_cache": True,
                "read_filename": "path/does/not/exist"
            }
        },
        "implementation_config":{
        }
    }
    with pytest.raises(Exception) as e:
        Configuration(config_location=None, is_dict_config=True, dict_config=config)
    assert "Invalid metadata.data_object.read_filename: path/does/not/exist" in str(e)

def test_check_metadata3():
    config = {
        "metadata": {
            "data_object": {
                "write_to_cache": True
            }
        },
        "implementation_config":{
        }
    }
    with pytest.raises(Exception) as e:
        Configuration(config_location=None, is_dict_config=True, dict_config=config)
    assert "metadata.data_object: if write_to_cache==true, you must set 'write_filename'" in str(e)

def test_check_setions():
    config = {
        "metadata": {
            "section_registry": [
                "phase_1"
            ]
        },
        "implementation_config": {
            "reader_config": {
                "read_data": {
                    "class": "CsvReader",
                    "filename": "test/tennis.csv",
                    "destinations": []
                }
            }
        }
    }
    with pytest.raises(Exception) as e:
        Configuration(config_location=None, is_dict_config=True, dict_config=config)
    assert "Following sections from metadata were not found implementation: {'phase_1'}" in str(e)

def test_check_setions2():
    config = {
        "metadata": {
            "section_registry": [
                "reader_config"
            ]
        },
        "implementation_config": {
            "reader_config": {
                "read_data": {
                    "class": "CsvReader",
                    "filename": "test/tennis.csv",
                    "destinations": []
                }
            },
            "writer_config": {}
        }
    }
    with pytest.raises(Exception) as e:
        Configuration(config_location=None, is_dict_config=True, dict_config=config)
    assert "Following sections from implementation were not found in metadata: {'writer_config'}" in str(e)

def test_check_setions3():
    config = {
        "metadata": {
            "section_registry": [
                "reader_config",
                "writer_config"
            ]
        },
        "implementation_config": {
            "reader_config": {
                "read_data": {
                    "class": "CsvReader",
                    "filename": "test/tennis.csv",
                    "destinations": []
                }
            },
            "writer_config": {}
        }
    }
    # this should not raise an exception
    Configuration(config_location=None, is_dict_config=True, dict_config=config)

def test_check_setions4():
    config = {
        "metadata": {
            "section_registry": [
                "phase1",
                "phase2"
            ]
        },
        "implementation_config": {
            "phase1": {
                "read_data": {
                    "class": "CsvReader",
                    "filename": "test/tennis.csv",
                    "destinations": []
                }
            },
            "phase2": {}
        }
    }
    # this should not raise an exception
    Configuration(config_location=None, is_dict_config=True, dict_config=config)

def test_sections_in_order1():
    config = {
        "metadata": {
            "section_registry": [
                "phase1",
                "phase2"
            ]
        },
        "implementation_config": {
            "phase1": {
                "read_data": {
                    "class": "CsvReader",
                    "filename": "test/tennis.csv",
                    "destinations": []
                }
            },
            "phase2": {}
        }
    }
    config = Configuration(config_location=None, is_dict_config=True, dict_config=config)
    sections, source = config.sections_in_order()
    assert sections == ["phase1", "phase2"]
    assert source == "section_registry"

def test_sections_in_order2():
    config = {
        "metadata": {
        },
        "implementation_config": {
            "reader_config": {
                "read_data": {
                    "class": "CsvReader",
                    "filename": "test/tennis.csv",
                    "destinations": []
                }
            },
            "writer_config": {

            }
        }
    }
    config = Configuration(config_location=None, is_dict_config=True, dict_config=config)
    sections, source = config.sections_in_order()
    assert sections == ["reader_config", "writer_config"]
    assert source == 'default'

def test_sections_in_order3():
    config = {
        "metadata": {
            "section_run": [
                "writer_config"
            ]
        },
        "implementation_config": {
            "reader_config": {
                "read_data": {
                    "class": "CsvReader",
                    "filename": "test/tennis.csv",
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
    config = Configuration(config_location=None, is_dict_config=True, dict_config=config)
    sections, source = config.sections_in_order()
    assert sections == ["writer_config"]
    assert source == "section_run"

def test_registration():
    class TestWriterUnreg(AbstractFileWriter):
        def __init__(self, configuration, instance_name):
            pass
        def write(self, data_dict):
            pass
    
    config = {
        "implementation_config": {
            "reader_config": {
                "read_data": {
                    "class": "TestWriterUnreg",
                    "destinations": []
                }
            }
        }
    }
    with pytest.raises(Exception) as e:
        Configuration(config_location=None, is_dict_config=True, dict_config=config)
    assert 'Cannot register node class TestWriterUnreg' in str(e)


def test_comments_in_json():
    # should not raise exception even though there are comments in the JSON
    config = Configuration(config_location="test/config_with_comments.json")
    assert list(config.config.keys()) == ['reader_config', 'writer_config']
    assert config.config['reader_config']['read_data']['class'] == 'CsvReader'
    assert list(config.config['reader_config']['read_data']['destinations']) == ['write_output']
    #"{'reader_config': {'read_data': {'class': 'CsvReader', 'filename': 'data/tennis.csv', 'destinations': ['write_output']}}, 'writer_config': {'write_output': {'class': 'CsvWriter', 'key': 'data', 'dir': 'cache', 'filename': 'tennis_output.csv'}}}"


def test_perform_any_config_fragment_substitution_bad():
    config_str = """
    {
        {% include "does/not/exist" %}
        "implementation_config": {
        }
    }
    """
    with pytest.raises(Exception) as e:
        Configuration.perform_any_config_fragment_substitution(config_str)
    assert 'Substitution files do not exist: does/not/exist' in str(e)


def test_perform_any_config_fragment_substitution():
    config_str = """
    {
        {% include "test/metadata_fragment.json" %}
        "implementation_config": {
            {% include "test/read_write_fragment.json" %}
        }
    }
    """
    final_str = Configuration.perform_any_config_fragment_substitution(config_str)
    expected = """
    {
        "metadata":{},
        "implementation_config": {

        "reader_config": {
            "read_data": {
                "class": "CsvReader",
                "filename": "data/tennis.csv",
                "destinations": [
                    "write_output"
                ]
            }
        },
        "writer_config": {
            "write_output": {
                "class": "CsvWriter",
                "key": "data",
                "dir": "cache",
                "filename": "tennis_output.csv"
            }
        }
        
        }
    }
    """
    assert json.loads(final_str) == json.loads(expected)

def test_yaml_config1():
    config_yaml = Configuration(config_location='test/hello_world_tennis.yml')
    config_json = Configuration(config_location='test/hello_world_tennis.json')
    assert config_yaml.config_hash == config_json.config_hash

def test_yaml_config2():
    c = Configuration('test/config_substitution.yml')
    assert c.config_string
    assert c.config_hash

def test_yaml_perform_any_config_fragment_substitution():
    config_str = """
{% include "test/metadata_fragment.yml" %}
implementation_config:
{% include "test/read_write_fragment.yml" %}
    """
    final_str = Configuration.perform_any_config_fragment_substitution(config_str)
    expected = """
metadata: {}
implementation_config:
  reader_config:
    read_data:
      class: CsvReader
      destinations:
      - write_output
      filename: data/tennis.csv
  writer_config:
    write_output:
      class: CsvWriter
      dir: cache
      filename: tennis_output.csv
      key: data
    """
    assert final_str == expected

def test_class_prefix():
    config_path = {
        "implementation_config": {
            "reader_config": {
                "read_data": {
                    "class": "TestExtNode",
                    "class_prefix": "test/ext_node_example.py",
                    "destinations": []
                }
            }
        }
    }
    config_dot = {
        "implementation_config": {
            "reader_config": {
                "read_data": {
                    "class": "TestExtNode",
                    "class_prefix": "test.ext_node_example",
                    "destinations": []
                }
            }
        }
    }
    for config in [config_path, config_dot]:
        config = Configuration(config_location=None, is_dict_config=True, dict_config=config)
        assert config.config_string
        assert config.config_hash
        NodeFactory().unregister('TestExtNode')
        

def test_class_package():
    config_path = {
        "metadata": {
            "class_package": "test"
        },
        "implementation_config": {
            "reader_config": {
                "read_data": {
                    "class": "TestExtNode",
                    "destinations": []
                }
            }
        }
    }
    config_full_path = {
        "metadata": {
            "class_package": "test/ext_node_example.py"
        },
        "implementation_config": {
            "reader_config": {
                "read_data": {
                    "class": "TestExtNode",
                    "destinations": []
                }
            }
        }
    }
    config_full_dot = {
        "metadata": {
            "class_package": "test"
        },
        "implementation_config": {
            "reader_config": {
                "read_data": {
                    "class": "TestExtNode",
                    "class_prefix": "ext_node_example",
                    "destinations": []
                }
            }
        }
    }
    for config in [config_full_path, config_path, config_full_dot]:
        config = Configuration(config_location=None, is_dict_config=True, dict_config=config)
        assert config.config_string
        assert config.config_hash
        NodeFactory().unregister('TestExtNode')


def test_env_override_class_package():
    config = {
        "metadata": {
            "class_package": "junk"
        },
        "implementation_config": {
            "reader_config": {
                "read_data": {
                    "class": "TestExtNode",
                    "destinations": []
                }
            }
        }
    }
    os.environ['PRIMROSE_EXT_NODE_PACKAGE'] = 'test'
    config = Configuration(config_location=None, is_dict_config=True, dict_config=config)
    assert config.config_string
    assert config.config_hash
    NodeFactory().unregister('TestExtNode')
    os.environ.pop('PRIMROSE_EXT_NODE_PACKAGE')


def test_incorrect_class_package():
    config = {
        "metadata": {
            "class_package": "junk"
        },
        "implementation_config": {
            "reader_config": {
                "read_data": {
                    "class": "TestExtNode",
                    "destinations": []
                }
            }
        }
    }
    with pytest.raises(Exception) as e:
        config = Configuration(config_location=None, is_dict_config=True, dict_config=config)
    assert "Cannot register node class TestExtNode" in str(e)

def test_incorrect_class_package2():
    config = {
        "metadata": {
            "class_package": "test"
        },
        "implementation_config": {
            "reader_config": {
                "read_data": {
                    "class": "TestExtNode",
                    "class_prefix": "junk",
                    "destinations": []
                }
            }
        }
    }
    with pytest.raises(Exception) as e:
        config = Configuration(config_location=None, is_dict_config=True, dict_config=config)
    assert "Cannot register node class TestExtNode" in str(e)
