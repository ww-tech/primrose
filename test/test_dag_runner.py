
import os
import unittest
import unittest.mock as mock

import pytest

from primrose.configuration.configuration import Configuration
from primrose.readers.csv_reader import CsvReader
from primrose.dag_runner import DagRunner
from testfixtures import LogCapture
from primrose.node_factory import NodeFactory
from primrose.writers.abstract_file_writer import AbstractFileWriter
from primrose.dag.config_layer_traverser import ConfigLayerTraverser
from primrose.data_object import DataObject
from primrose.data_object import DataObjectResponseType
from primrose.dag.dag_traverser import DagTraverser
from primrose.dag.traverser_factory import TraverserFactory
from abc import abstractmethod
from primrose.base.writer import  AbstractWriter


def test_run():
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
    configuration = Configuration(None, is_dict_config=True, dict_config=config)
    runner = DagRunner(configuration)
    runner.run()

def test_run2():
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
    configuration = Configuration(None, is_dict_config=True, dict_config=config)
    runner = DagRunner(configuration)
    with LogCapture() as l:
        runner.run(dry_run=True)
    l.check(('root', 'INFO', 'Taking nodes to run from default'),
            ('root', 'INFO', 'DRY RUN 0: would run node csv_reader of type reader_config and class CsvReader'),
            ('root', 'INFO', 'All done. Bye bye!'))

def test_run3():
    config = {
        "metadata": "ConfigLayerTraverser",
        "implementation_config":{
            "reader_config": {
                "csv_reader": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ["csv_writer"]
                }
            },
            "writer_config": {
                "csv_writer": {
                    "class": "CsvWriter",
                    "key":"test_data",
                    "dir": "cache",
                    "filename": "test/unittest_similar_recipes.csv"
                }
            }
        }
    }
    configuration = Configuration(None, is_dict_config=True, dict_config=config)
    runner = DagRunner(configuration)

    with LogCapture() as l:
        runner.run(dry_run=True)
    l.check(('root', 'INFO', 'Taking nodes to run from default'),
            ('root', 'INFO', 'DRY RUN 0: would run node csv_reader of type reader_config and class CsvReader'),
            ('root', 'INFO', 'DRY RUN 1: would run node csv_writer of type writer_config and class CsvWriter'),
            ('root', 'INFO', 'All done. Bye bye!'))

def test_run4():

    class TestWriter(AbstractFileWriter):
        def __init__(self, configuration, instance_name):
            pass
        @staticmethod
        def necessary_config(node_config):
            return set([])
        def run(self, data_object):
            terminate = True
            return data_object, terminate
    NodeFactory().register("TestWriter", TestWriter)

    config = {
        "implementation_config":{
            "reader_config": {
                "csv_reader": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ["csv_writer"]
                }
            },
            "writer_config": {
                "csv_writer": {
                    "class": "TestWriter"
                }
            }
        }
    }

    configuration = Configuration(None, is_dict_config=True, dict_config=config)
    runner = DagRunner(configuration)

    with LogCapture() as l:
        runner.run(dry_run=False)
    l.check(('root', 'INFO', 'Taking nodes to run from default'),
        ('root','INFO','received node csv_reader of type reader_config and class CsvReader'),
        ('root', 'INFO', 'Reading test/minimal.csv from CSV'),
        ('root', 'INFO', 'received node csv_writer of type writer_config and class TestWriter'),
        ('root', 'INFO', 'Terminating early due to signal from csv_writer'),
        ('root', 'INFO', 'All done. Bye bye!'))

def test_run5():
    config = {
        "metadata": {
            "section_registry":[
                "phase1",
                "phase2"
            ]
        },
        "implementation_config":{
            "phase1": {
                "csv_reader": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ["csv_writer"]
                }
            },
            "phase2": {
                "csv_writer": {
                    "class": "CsvWriter",
                    "key":"test_data",
                    "dir": "cache",
                    "filename": "test/unittest_similar_recipes.csv"
                }
            }
        }
    }
    configuration = Configuration(None, is_dict_config=True, dict_config=config)
    runner = DagRunner(configuration)

    with LogCapture() as l:
        runner.run(dry_run=True)
    l.check(('root', 'INFO', 'Taking nodes to run from section_registry'),
            ('root', 'INFO', 'DRY RUN 0: would run node csv_reader of type phase1 and class CsvReader'),
            ('root', 'INFO', 'DRY RUN 1: would run node csv_writer of type phase2 and class CsvWriter'),
            ('root', 'INFO', 'All done. Bye bye!'))


def test_run6():
    config = {
        "metadata": {
            "section_registry": [
                "phase1",
                "cleanup_config"
            ],
            "notify_on_error": {
                "client": "SlackClient",
                "channel": "some-channel",
                "token": "slack-api-token",
                "member_id": "optional-key"
            }
        },
        "implementation_config": {
            "phase1": {
                "csv_reader": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ["notification"]
                }
            },
            "cleanup_config": {
                "notification": {
                    "class": "ClientNotification",
                    "client": "SlackClient",
                    "channel": "some-channel",
                    "token": "slack-api-token",
                    "member_id": "optional-key",
                    "message": "Yay! Sucess"
                }
            }
        }
    }
    configuration = Configuration(None, is_dict_config=True, dict_config=config)
    runner = DagRunner(configuration)

    with LogCapture() as l:
        runner.run(dry_run=True)
    l.check(('root', 'INFO', 'Taking nodes to run from section_registry'),
            ('root', 'INFO', 'DRY RUN 0: would run node csv_reader of type phase1 and class CsvReader'),
            ('root', 'INFO', 'DRY RUN 1: would run node notification of type cleanup_config and class ClientNotification'),
            ('root', 'INFO', 'All done. Bye bye!'))


def test_run_notification_error():
    config = {
        "metadata": {
            "section_registry": [
                "phase1"
            ],
            "notify_on_error": {
                "client": "SlackClient",
                "channel": "some-channel",
                "token": "slack-api-token",
                "member_id": "optional-key"
            }
        },
        "implementation_config": {
            "phase1": {
                "csv_reader": {
                    "class": "CsvReader",
                    "filename": "bad/path.csv"
                }
            }
        }
    }
    configuration = Configuration(None, is_dict_config=True, dict_config=config)
    runner = DagRunner(configuration)

    mock_client = mock.Mock()
    mock_client.post_message = mock.Mock()
    mock_get_notification_client = mock.Mock(return_value=mock_client)

    path = 'primrose.notification_utils.get_notification_client'
    with mock.patch(path) as mock_get_notification_client:
        with pytest.raises(Exception) as error:
            runner.run()
            assert mock_get_notification_client.post_message.call_count == 1


def test_cache_data_object():
    config = {
        "metadata": {
            "data_object": {
                "write_to_cache": True,
                "write_filename": "dag_runner_test_cache_data_object.pkl"
            }
        },
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
    configuration = Configuration(None, is_dict_config=True, dict_config=config)

    data_object = DataObject(configuration)
    writer = CsvReader(configuration, "csv_reader")
    data_object.add(writer, "some_data")

    runner = DagRunner(configuration)

    filename = "dag_runner_test_cache_data_object.pkl"
    if os.path.exists(filename):
        os.remove(filename)

    cached = runner.cache_data_object(data_object)

    assert cached

    assert os.path.exists(filename)

    if os.path.exists(filename):
        os.remove(filename)

def test_create_data_object():

    filename = "dag_runner_create_data_object.pkl"
    # hack part 1: make sure this filename exists so that checks in Configuration pass
    open(filename, 'w+')

    config = {
        "metadata": {
            "data_object": {
                "read_from_cache": True,
                "read_filename": "dag_runner_create_data_object.pkl"
            }
        },
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
    configuration = Configuration(None, is_dict_config=True, dict_config=config)

    # hack part 2: now get rid of it
    if os.path.exists(filename):
        os.remove(filename)

    # now write the actual object to restore from
    data_object = DataObject(configuration)
    writer = CsvReader(configuration, "csv_reader")
    data_object.add(writer, "some_data")
    data_object.write_to_cache(filename)
    assert os.path.exists(filename)

    # now we get to the code to test
    runner = DagRunner(configuration)
    restored_data_object = runner.create_data_object()

    # run some checks
    assert isinstance(restored_data_object, DataObject)
    assert restored_data_object.get("csv_reader", rtype=DataObjectResponseType.VALUE.value) == "some_data"

    #cleanup
    if os.path.exists(filename):
        os.remove(filename)

def test_init_traverser_from_config():

    class TestTraverser(DagTraverser):
        def traversal_list(self): return []
        def run_section_by_section(self): return False

    TraverserFactory().register("TestTraverser", TestTraverser)

    config = {
        "metadata": {
            "traverser": "TestTraverser"
        },
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
    configuration = Configuration(None, is_dict_config=True, dict_config=config)

    runner = DagRunner(configuration)

    assert isinstance(runner.dag_traverser, TestTraverser)

def test_filter_sequence1():
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
    configuration = Configuration(None, is_dict_config=True, dict_config=config)

    runner = DagRunner(configuration)

    with pytest.raises(Exception) as e:
        runner.filter_sequence(["csv_reader", "csv_reader"])
    assert "You have duplicate nodes from traverser!" in str(e)

def test_filter_sequence2():
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
    configuration = Configuration(None, is_dict_config=True, dict_config=config)

    runner = DagRunner(configuration)

    with pytest.raises(Exception) as e:
        runner.filter_sequence(["csv_reader", "junk!"])
    assert "Unknown key junk!" in str(e)

def test_filter_sequence3():
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
    configuration = Configuration(None, is_dict_config=True, dict_config=config)

    runner = DagRunner(configuration)

    with pytest.raises(Exception) as e:
        runner.filter_sequence([])
    assert "Ran out of nodes for section reader_config. Only received []" in str(e)

def test_filter_sequence4():
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
    configuration = Configuration(None, is_dict_config=True, dict_config=config)

    runner = DagRunner(configuration)

    sequence = runner.filter_sequence(["read_data","recipe_csv_writer"])
    assert sequence == ["recipe_csv_writer"]

def test_filter_sequence5():
    config = {
        "metadata": {
            "section_run": [
                "writer_config",
                "reader_config"
            ]
        },
        "implementation_config": {
            "reader_config": {
                "read_data": {
                    "class": "CsvReader",
                    "filename": "test/tennis.csv",
                    "destinations": ["recipe_csv_writer","recipe_csv_writer2"]
                }
            },
            "writer_config": {
                "recipe_csv_writer": {
                    "class": "CsvWriter",
                    "key":"test_data",
                    "dir": "cache",
                    "filename": "unittest_similar_recipes.csv"
                },
                "recipe_csv_writer2": {
                    "class": "CsvWriter",
                    "key":"test_data",
                    "dir": "cache",
                    "filename": "unittest_similar_recipes.csv"
                }
            }
        }
    }
    configuration = Configuration(None, is_dict_config=True, dict_config=config)

    runner = DagRunner(configuration)
    with pytest.raises(Exception) as e:
        runner.filter_sequence(["recipe_csv_writer", "read_data", "recipe_csv_writer2"])
    assert "Traverser is mismatched with section writer_config. Expecting set ['recipe_csv_writer', 'recipe_csv_writer2']" in str(e)

def test_filter_sequence6():
    #test of dependencies with a section
    config = {
        "metadata": {
            "traverser": "DepthFirstTraverser"
        },
        "implementation_config": {
            "reader_config": {
                "read_data1": {
                    "class": "CsvReader",
                    "filename": "test/tennis.csv",
                    "destinations": ["csv_writer"]
                },
                "read_data2": {
                    "class": "CsvReader",
                    "filename": "test/tennis.csv",
                    "destinations": ["csv_writer"]
                },
                "read_data3": {
                    "class": "CsvReader",
                    "filename": "test/tennis.csv",
                    "destinations": ["read_data2"]
                }
            },
            "writer_config": {
                "csv_writer": {
                    "class": "CsvWriter",
                    "key":"test_data",
                    "dir": "cache",
                    "filename": "unittest_similar_recipes.csv"
                }
            }
        }
    }
    configuration = Configuration(None, is_dict_config=True, dict_config=config)
    runner = DagRunner(configuration)
    s_in = ["read_data3", "read_data2", "read_data1", "csv_writer"]
    s_out = runner.filter_sequence(s_in)
    assert s_out == s_in

    with pytest.raises(Exception) as e:
        runner.filter_sequence([ "read_data1", "csv_writer", "read_data3", "read_data2"])
    assert "Upstream path found, from read_data3 to csv_writer" in str(e.value)

def test_check_upstream():
    config = {
        "metadata": {
            "section_run": [
                "reader_config",
                "writer_config"
            ]
        },
        "implementation_config": {
            "reader_config": {
                "read_data": {
                    "class": "CsvReader",
                    "filename": "test/tennis.csv",
                    "destinations": ["recipe_csv_writer","recipe_csv_writer2"]
                }
            },
            "writer_config": {
                "recipe_csv_writer": {
                    "class": "CsvWriter",
                    "key":"test_data",
                    "dir": "cache",
                    "filename": "unittest_similar_recipes.csv"
                },
                "recipe_csv_writer2": {
                    "class": "CsvWriter",
                    "key":"test_data",
                    "dir": "cache",
                    "filename": "unittest_similar_recipes.csv"
                }
            }
        }
    }
    configuration = Configuration(None, is_dict_config=True, dict_config=config)
    runner = DagRunner(configuration)
    assert runner.check_for_upstream(['read_data', 'recipe_csv_writer', 'recipe_csv_writer2']) == False

def test_check_upstream2():
    config = {
        "metadata": {
            "section_run": [
                "writer_config",
                "reader_config"
            ]
        },
        "implementation_config": {
            "reader_config": {
                "read_data": {
                    "class": "CsvReader",
                    "filename": "test/tennis.csv",
                    "destinations": ["recipe_csv_writer","recipe_csv_writer2"]
                }
            },
            "writer_config": {
                "recipe_csv_writer": {
                    "class": "CsvWriter",
                    "key":"test_data",
                    "dir": "cache",
                    "filename": "unittest_similar_recipes.csv"
                },
                "recipe_csv_writer2": {
                    "class": "CsvWriter",
                    "key":"test_data",
                    "dir": "cache",
                    "filename": "unittest_similar_recipes.csv"
                }
            }
        }
    }
    configuration = Configuration(None, is_dict_config=True, dict_config=config)
    runner = DagRunner(configuration)
    with pytest.raises(Exception) as e:
        runner.check_for_upstream(['recipe_csv_writer2', 'read_data', 'recipe_csv_writer'])
    assert "Upstream path found, from read_data to recipe_csv_writer2" in str(e.value)


def test_run_bad():
    class TestWriterTmp(AbstractWriter):
        @staticmethod
        def necessary_config(node_config):
            return set([])

        def run(self, data_object):
            return data_object, False

    NodeFactory().register("TestWriterTmp", TestWriterTmp)

    config = {
        "implementation_config":{
            "writer_config": {
                "mywriter": {
                    "class": "TestWriterTmp",
                    "destinations": []
                }
            }
        }
    }
    configuration = Configuration(None, is_dict_config=True, dict_config=config)

    runner = DagRunner(configuration)

    # unregister this class
    del NodeFactory().name_dict["TestWriterTmp"]

    with pytest.raises(Exception) as e:
        runner.run()
    assert "Issue instantiating mywriter and class TestWriterTmp" in str(e)

def test_run_bad2():
    class TestWriterTmp(AbstractWriter):
        @staticmethod
        def necessary_config(node_config):
            return set([])

        def run(self, data_object):
            raise Exception("Deliberate error")
            #return data_object, False

    NodeFactory().register("TestWriterTmp", TestWriterTmp)

    config = {
        "implementation_config":{
            "writer_config": {
                "mywriter": {
                    "class": "TestWriterTmp",
                    "destinations": []
                }
            }
        }
    }
    configuration = Configuration(None, is_dict_config=True, dict_config=config)

    runner = DagRunner(configuration)

    with pytest.raises(Exception) as e:
        runner.run()
    assert "Deliberate error" in str(e)

def test_run_pruned():
    config = {
        "implementation_config": {
            "reader_config": {
                "read_data": {
                    "class": "CsvReader",
                    "filename": "data/tennis.csv",
                    "destinations": ["conditional_node"]
                },
                "conditional_node": {
                    "class": "SimpleSwitch",
                    "path_to_travel": "left",
                    "destinations": ['left', 'right']
                }
            },
            "writer_config": {
                "left": {
                    "class": "LoggingSuccess",
                    "msg": "left node!",
                    "level": "INFO",
                },
                "right": {
                    "class": "LoggingSuccess",
                    "msg": "right node!",
                    "level": "INFO",
                    "destinations": ["right2"]
                },
                "right2": {
                    "class": "LoggingSuccess",
                    "msg": "right node2!",
                    "level": "INFO",
                }
            }
        }
    }
    configuration = Configuration(None, is_dict_config=True, dict_config=config)
    runner = DagRunner(configuration)
    with LogCapture() as l:
        runner.run()
    l.check(('root', 'INFO', 'Taking nodes to run from default'),
            ('root', 'INFO', 'received node read_data of type reader_config and class CsvReader'),
            ('root', 'INFO', 'Reading data/tennis.csv from CSV'),
            ('root', 'INFO', 'received node conditional_node of type reader_config and class SimpleSwitch'),
            ('root', 'INFO', 'Skipping pruned node right'),
            ('root', 'INFO', 'Skipping pruned node right2'),
            ('root', 'INFO', 'received node left of type writer_config and class LoggingSuccess'),
            ('root', 'INFO', 'left node!'),
            ('root', 'INFO', 'All done. Bye bye!'))
