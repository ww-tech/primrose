
import pytest
from primrose.pipelines.dataframe_joiner import DataFrameJoiner
from primrose.base.transformer_sequence import TransformerSequence
from primrose.configuration.configuration import Configuration
from primrose.readers.csv_reader import CsvReader
import pandas as pd
from primrose.data_object import DataObject
from primrose.data_object import DataObjectResponseType


def test_init_pipeline():
    config = {
        "implementation_config": {
            "reader_config": {
                "myreader": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ["mypipeline"]
                }
            },
            "pipeline_config": {
                "mypipeline":{
                    "class": "DataFrameJoiner",
                    "join_key": ["first"],
                    "start_table": "myreader"
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)

    pipeline = DataFrameJoiner(configuration, "mypipeline")
    ts = pipeline.init_pipeline()
    assert isinstance(ts, TransformerSequence)

def test_transform():
    config = {
        "implementation_config": {
            "reader_config": {
                "myreader_left": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ["mypipeline"]
                },
                "myreader_right": {
                    "class": "CsvReader",
                    "filename": "test/merge_right3.csv",
                    "destinations": ["mypipeline"]
                }
            },
            "pipeline_config": {
                "mypipeline":{
                    "class": "DataFrameJoiner",
                    "join_key": ["first"],
                    "start_table": "myreader_left",
                    "is_training": True
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)

    data_object = DataObject(configuration)

    left_df = pd.read_csv("test/minimal.csv")
    reader_left = CsvReader(configuration, "myreader_left")
    data_object.add(reader_left, left_df)

    right_df = pd.read_csv("test/merge_right3.csv")
    reader_right = CsvReader(configuration, "myreader_right")
    data_object.add(reader_right, right_df)

    pipeline = DataFrameJoiner(configuration, "mypipeline")

    data_object, terminate = pipeline.run(data_object)

    assert not terminate

    joined_data = data_object.get("mypipeline", pop_data=True, rtype=DataObjectResponseType.VALUE.value)
    assert joined_data.shape[0] == 2

    assert list(joined_data.T.to_dict().values())[0] == {'first': 'joe', 'last': 'doe', 'age': 47}
    assert list(joined_data.T.to_dict().values())[1] == {'first': 'mary', 'last': 'poppins', 'age': 42}


def test_transform2():
    config = {
        "implementation_config": {
            "reader_config": {
                "myreader_left": {
                    "class": "CsvReader",
                    "filename": "test/minimal.csv",
                    "destinations": ["mypipeline"]
                },
                "myreader_right": {
                    "class": "CsvReader",
                    "filename": "test/merge_right3.csv",
                    "destinations": ["mypipeline"]
                }
            },
            "pipeline_config": {
                "mypipeline":{
                    "class": "DataFrameJoiner",
                    "join_key": ["first"],
                    "start_table": "JUNK",
                    "is_training": True
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)

    data_object = DataObject(configuration)

    left_df = pd.read_csv("test/minimal.csv")
    reader_left = CsvReader(configuration, "myreader_left")

    right_df = pd.read_csv("test/merge_right3.csv")
    reader_right = CsvReader(configuration, "myreader_right")

    #note: am deliberately swapping order to right is first
    data_object.add(reader_right, right_df)
    data_object.add(reader_left, left_df)

    pipeline = DataFrameJoiner(configuration, "mypipeline")

    with pytest.raises(Exception) as e:
        pipeline.run(data_object)
    assert "Could not find start_table in upstream keys: JUNK" in str(e)
