
import pytest
from primrose.configuration.configuration import Configuration
from primrose.dag.depth_first_traverser import DepthFirstTraverser

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
    traverser = DepthFirstTraverser(configuration)
    sequence = traverser.traversal_list()
    print(sequence)
    assert sequence == ['csv_reader2', 'csv_reader', 'csv_writer', 'successlogger']
