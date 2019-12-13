import pytest
import sys
import os
import jstyleson
from primrose.configuration.configuration_dag import ConfigurationDag
from primrose.configuration.configuration import Configuration
from primrose.configuration.configuration import OperationType
from primrose.node_factory import NodeFactory
from primrose.base.node import AbstractNode
from primrose.dag.config_layer_traverser import ConfigLayerTraverser

def test_check_node_exists():
    with pytest.raises(Exception) as e:
        ConfigurationDag.check_node_exists(["a","b","c"],"d")
    assert 'Destination d does not exist' in str(e)

def test_check_cycles_OK():
    config = {
          "implementation_config":{
            "reader_config":{
                "csv_reader": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ['csv_writer']
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

    dag = ConfigurationDag(config['implementation_config'])
    dag.create_dag()
    assert dag.check_for_cycles() is None

def test_check_cycles_bad():
    config = {
        "implementation_config": {
            "reader_config": {
                "csv_reader": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ['csv_writer']
                }
            },
            "writer_config": {
                "csv_writer": {
                    "class": "CsvWriter",
                    "key":"test_data",
                    "dir": "cache",
                    "filename": "unittest_similar_recipes.csv",
                    "destinations": ["csv_reader"]
                }
            }
        }
    }

    dag = ConfigurationDag(config['implementation_config'])
    dag.create_dag()
    with pytest.raises(Exception) as e:
        dag.check_for_cycles()
    assert "Cycle(s) found:" in str(e)
    # it turns out cycle finding code is stochastic so the order of the nodes in the found cycle is not stable. Thus, we look for initial phrase only
    # assert "Cycle(s) found: [('corpus_pipeline', 'recipe_name_model'), ('recipe_name_model', 'corpus_pipeline')]" in str(e)

def test_check_cycles_bad2():
    config = {
        "implementation_config": {
            "reader_config": {
                "csv_reader": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": []
                }
            },
            "writer_config": {
                "csv_writer": {
                    "class": "CsvWriter",
                    "key":"test_data",
                    "dir": "cache",
                    "filename": "unittest_similar_recipes.csv",
                    "destinations": []
                }
            }
        }
    }

    dag = ConfigurationDag(config['implementation_config'])
    dag.create_dag()
    with pytest.raises(Exception) as e:
        dag.check_connected_components()
    assert "Found multiple connected components:" in str(e)

def test_type_bad():
    config = {
        "implementation_config": {
            "reader_config": {
                "csv_reader": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": [1234]
                }
            },
            "writer_config": {
                1234: {
                    "class": "CsvWriter",
                    "key":"test_data",
                    "dir": "cache",
                    "filename": "unittest_similar_recipes.csv",
                    "destinations": []
                }
            }
        }
    }

    dag = ConfigurationDag(config['implementation_config'])
    with pytest.raises(Exception) as e:
        dag.create_dag()
    assert "Unrecognized destination type: 1234" in str(e)

def test_type_bad2():
    config = {
        "implementation_config": {
            "reader_config": {
                "csv_reader": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ['doesnotexist']
                }
            },
            "writer_config": {
                "csv_writer": {
                    "class": "CsvWriter",
                    "key":"test_data",
                    "dir": "cache",
                    "filename": "unittest_similar_recipes.csv",
                    "destinations": []
                }
            }
        }
    }

    dag = ConfigurationDag(config['implementation_config'])
    with pytest.raises(Exception) as e:
        dag.create_dag()
    assert "Did not find doesnotexist destination in" in str(e)

@pytest.mark.optional
@pytest.mark.plotting
def test_plot_dag():

    class TestPostprocess(AbstractNode):
        @staticmethod
        def necessary_config(node_config):
            return set(['key1', 'key2'])
        def run(self, data_object):
            return data_object
    NodeFactory().register("TestPostprocess", TestPostprocess)

    class Testpipeline(AbstractNode):
        #def __init__(self, configuration, instance_name):
        #    pass
        @staticmethod
        def necessary_config(node_config):
            return set([])
        def run(self, data_object):
            return data_object
    NodeFactory().register("Testpipeline", Testpipeline)
    class TestCleanup(AbstractNode):
        @staticmethod
        def necessary_config(node_config):
            return set([])
        def run(self, data_object):
            return data_object
    NodeFactory().register("TestCleanup", TestCleanup)

    config = {
        "implementation_config":{
            "reader_config": {
                "csv_reader": {
                    "class": "CsvReader",
                    "filename": "some/path/to/file",
                    "destinations": ["pipeline1"]
                }
            },
            "pipeline_config": {
                "pipeline1": {
                    "class": "Testpipeline",
                    "destinations": ["decision_tree_model"]
                }
            },
            "model_config": {
                "decision_tree_model": {
                    "class": "SklearnClassifierModel",
                    "model_parameters": {},
                    "sklearn_classifier_name": "tree.DecisionTreeClassifier",
                    "grid_search_scoring": "roc_auc",
                    "cv_folds": 3,
                    "mode": "predict",
                    "destinations": ["nodename"]
                }
            },
            "postprocess_config": {
                "nodename": {
                    "class": "TestPostprocess",
                    "key1":"val1",
                    "key2":"val2",
                    "destinations": ["write_output"]
                }
            },
            "writer_config": {
                "write_output": {
                    "class": "CsvWriter",
                    "key": "read_data",
                    "dir": "cache",
                    "filename": "some/path/to/file.csv",
                    "destinations": ["donothingsuccess"]
                }
            },
            "cleanup_config": {
                "donothingsuccess": {
                    "class": "TestCleanup",
                }
            }
        }
    }

    cfilename = "test/test_dag_plotting.json"
    with open(cfilename, 'w') as f:
        jstyleson.dump(config, f)
    config = Configuration(config_location=cfilename)
    dag = config.dag
    dag.create_dag()

    filename = "test/test_dag_plotting.png"
    if os.path.exists(filename):
        os.remove(filename)

    dag.plot_dag(filename, traverser=ConfigLayerTraverser(config))

    assert os.path.exists(filename)

    if os.path.exists(cfilename):
        os.remove(cfilename)

    if os.path.exists(filename):
        os.remove(filename)

def test_upstream_keys1():
    config = {
        "implementation_config":{
            "reader_config":{
                "csv_reader": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ['csv_writer']
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
    dag = ConfigurationDag(config['implementation_config'])
    dag.create_dag()
    ukeys = dag.upstream_keys('csv_writer')
    assert list(ukeys) == ['csv_reader']

    with pytest.raises(Exception) as e:
        dag.upstream_keys('doesnotexist')
    assert "Node not found in the DAG:" in str(e)

def test_upstream_typed_keys():
    config = {
        "implementation_config":{
            "reader_config":{
                "csv_reader1": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ['csv_writer']
                },
                "csv_reader2": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ['csv_writer']
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
    dag = ConfigurationDag(config['implementation_config'])
    dag.create_dag()
    ukeys = dag.upstream_typed_keys('csv_writer')
    assert ukeys == {'csv_reader1': 'reader_config', 'csv_reader2': 'reader_config'}

def test_starting_nodes():
    #here, we also test descendents()

    config = {
        "metadata": {},

        "implementation_config":{
            "reader_config": {
                "read_data": {
                    "class": "CsvReader",
                    "filename": "test/tennis.csv",
                    "destinations": ["decision_tree_model"]
                },
                "read_data2": {
                    "class": "CsvReader",
                    "filename": "test/tennis.csv",
                    "destinations": ["decision_tree_model"]
                }
            },
            "model_config": {
                "decision_tree_model": {
                    "class": "SklearnClassifierModel",
                    "model_parameters": {},
                    "sklearn_classifier_name": "tree.DecisionTreeClassifier",
                    "grid_search_scoring": "roc_auc",
                    "cv_folds": 3,
                    "mode": "predict",
                    "destinations": ["write_output"]
                }
            },
            "writer_config": {
                "write_output": {
                    "class": "CsvWriter",
                    "key": "predictions",
                    "dir": "cache",
                    "filename": "hello_world_predictions.csv"
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)

    nodes = configuration.dag.starting_nodes()
    assert nodes == set(["read_data", "read_data2"])

    downstream_nodes = configuration.dag.descendents("read_data2")
    assert downstream_nodes == set(["decision_tree_model", "write_output"])

def test_nodes_of_type():
    config = {
        "metadata": {},

        "implementation_config":{
            "reader_config": {
                "read_data": {
                    "class": "CsvReader",
                    "filename": "test/tennis.csv",
                    "destinations": ["decision_tree_model"]
                },
                "read_data2": {
                    "class": "CsvReader",
                    "filename": "test/tennis.csv",
                    "destinations": ["decision_tree_model"]
                }
            },
            "model_config": {
                "decision_tree_model": {
                    "class": "SklearnClassifierModel",
                    "mode": "predict",
                    "sklearn_classifier_name": "tree.DecisionTreeClassifier",
                    "grid_search_scoring": "roc_auc",
                    "cv_folds": 3,
                    "model_parameters": {},
                    "destinations": ["write_output"]
                }
            },
            "writer_config": {
                "write_output": {
                    "class": "CsvWriter",
                    "key": "predictions",
                    "dir": "cache",
                    "filename": "hello_world_predictions.csv"
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)

    nodes = configuration.dag.nodes_of_type(OperationType.reader)
    assert nodes == set(["read_data", "read_data2"])

    nodes = configuration.dag.nodes_of_type(OperationType.pipeline)
    assert nodes == set([])

    nodes = configuration.dag.upstream_nodes_of_type("write_output", OperationType.reader)
    assert nodes == set(["read_data", "read_data2"])

    nodes = configuration.dag.upstream_nodes_of_type("write_output", OperationType.cleanup)
    assert nodes == set([])


def test_paths():
    config = {
        "metadata": {},

        "implementation_config":{
            "reader_config": {
                "read_data": {
                    "class": "CsvReader",
                    "filename": "test/tennis.csv",
                    "destinations": ["read_data2", "decision_tree_model"]
                },
                "read_data2": {
                    "class": "CsvReader",
                    "filename": "test/tennis.csv",
                    "destinations": ["decision_tree_model"]
                }
            },
            "model_config": {
                "decision_tree_model": {
                    "class": "SklearnClassifierModel",
                    "mode": "train",
                    "sklearn_classifier_name": "tree.DecisionTreeClassifier",
                    "grid_search_scoring": "roc_auc",
                    "cv_folds": 3,
                    "model_parameters": {},
                    "destinations": ["write_output"]
                }
            },
            "writer_config": {
                "write_output": {
                    "class": "CsvWriter",
                    "key": "predictions",
                    "dir": "cache",
                    "filename": "hello_world_predictions.csv"
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)

    paths = list(configuration.dag.paths("read_data", "write_output"))
    assert len(paths) == 2
    assert paths[0] == ['read_data', 'read_data2', 'decision_tree_model', 'write_output']
    assert paths[1] == ['read_data', 'decision_tree_model', 'write_output']

    paths = configuration.dag.paths("write_output", "decision_tree_model")
    assert not paths

