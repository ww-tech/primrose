import pytest
from primrose.base.transformer_sequence import TransformerSequence
from primrose.configuration.configuration import Configuration
from primrose.data_object import DataObject
from primrose.pipelines.transformer_pipeline import TransformerPipeline
from primrose.readers.csv_reader import CsvReader
from primrose.transformers.sklearn_preprocessing_transformer import SklearnPreprocessingTransformer
from primrose.transformers.strings import StringTransformer
def test__instantiate_transformer_1():
    processor = TransformerPipeline._instantiate_transformer(
        {
            "class": "primrose.transformers.sklearn_preprocessing_transformer.SklearnPreprocessingTransformer",
            "preprocessor": "preprocessing.StandardScaler",
            "columns":["test"],
            "kwargs": {"with_mean": False}
        })
    assert isinstance(processor, SklearnPreprocessingTransformer)
    assert processor.preprocessor.with_mean == False

    with pytest.raises(Exception) as e:
        TransformerPipeline._instantiate_transformer({"class": "sklearn.preprocessing.junk"})
    assert "Transformer junk not found in sklearn.preprocessing module" in str(e)


def test_necessary_config():
    assert "transformer_sequence" in TransformerPipeline.necessary_config({})

def test_optional_config():
    assert TransformerPipeline.optional_config({}) == {"training_fraction", "seed", "is_training"}

@pytest.fixture
def config():
    config = {
      "implementation_config": {
        "reader_config": {
          "read_data": {
            "class": "CsvReader",
            "filename": "data/tennis.csv",
            "destinations": [
              "transformers"
            ]
          }
        },
        "pipeline_config": {
          "transformers": {
            "class": "TransformerPipeline",
            "transformer_sequence": [{
                "class": "primrose.transformers.strings.StringTransformer",
                "method": "replace",
                "columns": "outlook",
                "pat": "sunny",
                "repl":"rainy"

            }]
          }
        }
      }
    }
    configuration = Configuration(None, is_dict_config=True, dict_config=config)
    return configuration

@pytest.fixture
def data_object(config):
    data_object = DataObject(config)
    csv_reader = CsvReader(config, "read_data")
    data_object, _ = csv_reader.run(data_object)
    return data_object

@pytest.fixture
def pipeline(config):
    return TransformerPipeline(config, "transformers")

def test__init__(pipeline):
    assert isinstance(pipeline, TransformerPipeline)

def test_init_pipeline(pipeline):
    ts = pipeline.init_pipeline()
    assert isinstance(ts, TransformerSequence)
    assert isinstance(ts.sequence[0], StringTransformer)

def test_transform(pipeline,data_object):
    data_object, _ = pipeline.run(data_object)
    assert data_object.data_dict['transformers']['data_test']['outlook'].loc[0] == "rainy"
