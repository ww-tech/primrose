import pytest
from primrose.readers.sklearn_dataset_reader import SklearnDatasetReader
from primrose.configuration.configuration import Configuration
from primrose.data_object import DataObject, DataObjectResponseType

def test_run():
    config = {
        "implementation_config":{
            "reader_config": {
                "dataset_reader": {
                    "class": "SklearnDatasetReader",
                    "dataset": "iris",
                    "destinations": []
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)
    data_object = DataObject(configuration)
    reader = SklearnDatasetReader(configuration, "dataset_reader")
    data_object, terminate = reader.run(data_object)
    assert not terminate
    df = data_object.get("dataset_reader", rtype=DataObjectResponseType.VALUE.value)
    assert len(df.columns) == 5
    assert "target" in df.columns
    assert df.shape == (150, 5)
    assert 'sepal length (cm)' in df.columns