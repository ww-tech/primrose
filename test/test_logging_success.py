
import pytest
from primrose.cleanup.logging_success import LoggingSuccess
from primrose.configuration.configuration import Configuration
from testfixtures import LogCapture
from primrose.data_object import DataObject

def test_necessary_config():
    config = {
        "implementation_config":{
            "reader_config": {
                "csv_reader": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ['successlogger']
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
    ls = LoggingSuccess(configuration, "successlogger")
    node_config = {
                    "class": "LoggingSuccess",
                    "msg": "woohoo, all done!",
                    "level": "INFO",
                }
    assert isinstance(ls.necessary_config(node_config), set)
    assert len(ls.necessary_config(node_config)) == 2

    data_object = DataObject(configuration)

    assert ls.run(data_object)

    with LogCapture() as l:
        ls.run(data_object) 
    l.check(('root', 'INFO', 'woohoo, all done!'),)
