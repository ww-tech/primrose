
import pytest
from primrose.configuration.configuration import Configuration
from primrose.conditionalpath.simple_switch import SimpleSwitch

def test_destinations_to_prune():
    config = {
        "implementation_config": {
            "reader_config": {
                "conditional_node": {
                    "class": "SimpleSwitch",
                    "path_to_travel": "left",
                    "destinations": ['left', 'right']
                }
            },
            "writer_config": {
                "left": {
                    "class": "CsvWriter",
                    "key":"test_data",
                    "dir": "cache",
                    "filename": "unittest_similar_recipes.csv"
                },
                "right": {
                    "class": "CsvWriter",
                    "key":"test_data",
                    "dir": "cache",
                    "filename": "unittest_similar_recipes.csv"
                }
            }
        }
    }

    configuration = Configuration(None, is_dict_config=True, dict_config=config)
    node = SimpleSwitch(configuration, "conditional_node")
    to_prune = node.destinations_to_prune()
    assert to_prune == ['right']
