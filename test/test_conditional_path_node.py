import pytest
from primrose.base.conditional_path_node import AbstractConditionalPath
from primrose.configuration.configuration import Configuration
from primrose.node_factory import NodeFactory

def test_all_nodes_to_prune():
    class TestConditionalNode(AbstractConditionalPath):
        @staticmethod
        def necessary_config(node_config): return set()
        def destinations_to_prune(self):
            return ["csv_writer2"]
        def run(self, data_object):
            return data_object, False

    NodeFactory().register("TestConditionalNode", TestConditionalNode)

    config = {
        "implementation_config": {
            "reader_config": {
                "conditional_node": {
                    "class": "TestConditionalNode",
                    "destinations": ['csv_writer', 'csv_writer2']
                }
            },
            "writer_config": {
                "csv_writer": {
                    "class": "CsvWriter",
                    "key":"test_data",
                    "dir": "cache",
                    "filename": "unittest_similar_recipes.csv"
                },
                "csv_writer2": {
                    "class": "CsvWriter",
                    "key":"test_data",
                    "dir": "cache",
                    "filename": "unittest_similar_recipes.csv",
                    "destinations": ["csv_writer3"]
                },
                "csv_writer3": {
                    "class": "CsvWriter",
                    "key":"test_data",
                    "dir": "cache",
                    "filename": "unittest_similar_recipes.csv"
                }
            }
        }
    }
    configuration = Configuration(None, is_dict_config=True, dict_config=config)

    node = TestConditionalNode(configuration, "conditional_node")
    assert node.destinations_to_prune() == ["csv_writer2"]
    all_nodes = node.all_nodes_to_prune()
    assert len(all_nodes) == 2
    assert "csv_writer2" in all_nodes
    assert "csv_writer3" in all_nodes

    config = {
        "implementation_config": {
            "reader_config": {
                "conditional_node": {
                    "class": "TestConditionalNode"
                }
            }
        }
    }
    configuration = Configuration(None, is_dict_config=True, dict_config=config)
    node = TestConditionalNode(configuration, "conditional_node")

    with pytest.raises(Exception) as e:
        node.all_nodes_to_prune()
    assert "Destinations key is missing" in str(e)

def test_all_nodes_to_prune2():
    class TestConditionalNode(AbstractConditionalPath):
        @staticmethod
        def necessary_config(node_config): return set()
        def destinations_to_prune(self):
            return ["junk"]
        def run(self, data_object):
            return data_object, False
    NodeFactory().register("TestConditionalNode", TestConditionalNode)

    config = {
        "implementation_config": {
            "reader_config": {
                "conditional_node": {
                    "class": "TestConditionalNode",
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

    configuration = Configuration(None, is_dict_config=True, dict_config=config)
    node = TestConditionalNode(configuration, "conditional_node")

    with pytest.raises(Exception) as e:
        node.all_nodes_to_prune()
    assert "Destination junk is not in destinations list" in str(e)
