
import pytest
from primrose.configuration.configuration import Configuration
from primrose.dag.config_layer_traverser import ConfigLayerTraverser

def test__sort_by_depth_first():
    config = {
        "implementation_config":{
            "reader_config":{
                "csv_reader": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ['csv_writer']
                },
                "csv_reader2": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ['successlogger']
                }
            },
            "writer_config": {
                "csv_writer": {
                    "class": "CsvWriter",
                    "key":"test_data",
                    "dir": "cache",
                    "filename": "unittest_similar_recipes.csv",
                    "destinations": ["successlogger"]
                }
            },
            "cleanup_config": {
                "successlogger": {
                    "class": "LoggingSuccess",
                    "msg": "woohoo, all done!",
                    "level": "INFO",
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)
    traverser = ConfigLayerTraverser(configuration)

    sequence = traverser._sort_by_depth_first(['successlogger', 'csv_reader', 'csv_writer','csv_reader2'])
    assert sequence == ['csv_reader2', 'csv_reader', 'csv_writer', 'successlogger']

def test_traversal_list():
    config = {
        "implementation_config":{
            "reader_config":{
                "csv_reader": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ['csv_writer']
                },
                "csv_reader2": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ['successlogger']
                }
            },
            "writer_config": {
                "csv_writer": {
                    "class": "CsvWriter",
                    "key":"test_data",
                    "dir": "cache",
                    "filename": "unittest_similar_recipes.csv",
                    "destinations": ["successlogger"]
                }
            },
            "cleanup_config": {
                "successlogger": {
                    "class": "LoggingSuccess",
                    "msg": "woohoo, all done!",
                    "level": "INFO",
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)
    traverser = ConfigLayerTraverser(configuration)
    sequence = traverser.traversal_list()
    assert sequence == ['csv_reader2', 'csv_reader', 'csv_writer', 'successlogger']
