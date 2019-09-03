import pytest
import pandas as pd
from primrose.configuration.configuration import Configuration
from primrose.models.sklearn_classifier_model import SklearnClassifierModel
from primrose.data_object import DataObject, DataObjectResponseType
from primrose.readers.csv_reader import CsvReader
from primrose.pipelines.encode_train_test_split import EncodeTrainTestSplit


@pytest.fixture()
def config():
    return Configuration("test/hello_world_tennis.json")


@pytest.fixture()
def data_obj(config):
    df = pd.read_csv("test/tennis.csv")
    data_object = DataObject(config)
    reader = CsvReader(config, 'read_data')
    data_object.add(reader, df)
    encoder = EncodeTrainTestSplit(config, 'encode_and_split')
    data_object, terminate = encoder.run(data_object)
    return data_object


@pytest.fixture
def model(config):
    return SklearnClassifierModel(config, "decision_tree_model")


def test_necessary_config():
    assert SklearnClassifierModel.necessary_config({}) == set(["model_parameters", "mode", "sklearn_classifier_name",
                                                             "grid_search_scoring", "cv_folds"])


def test_get_data(model, data_obj):
    x_train, y_train, x_test, y_test = model._get_data(data_obj)
    assert x_train.shape[0] == y_train.shape[0]
    assert x_test.shape[0] == y_test.shape[0]  # we removed the ID column by not specifying it in X


def test_train_model(model, data_obj):
    model.train_model(data_obj)
    assert model.model is not None
    x_train, y_train, x_test, y_test = model._get_data(data_obj)
    predictions = model.model.predict(x_test)
    assert list(predictions) == [1, 1, 0, 1, 1]


def test_eval(model, data_obj):
    model.train_model(data_obj)
    model.eval_model(data_obj)
    print(model.scores)
    assert model.scores['recall'] == 1.0
    assert model.scores['precision'] == 0.75
    assert model.scores['predicted_class_fraction'] == 0.8
    assert model.scores['positive_class_fraction'] == 0.6


def test_predict(model, data_obj):
    model.train_model(data_obj)
    data_object = model.predict(data_obj)
    predicted = data_object.get('decision_tree_model', rtype=DataObjectResponseType.KEY_VALUE.value)

    assert predicted['predictions'].shape[0] == 5
    assert list(predicted['predictions'].predictions) == ['yes', 'yes', 'no', 'yes', 'yes']
