import pytest
import sys
import os
import pandas as pd
from primrose.configuration.configuration import Configuration
from primrose.pipelines.encode_train_test_split import EncodeTrainTestSplit
from primrose.data_object import DataObject, DataObjectResponseType
from primrose.readers.csv_reader import CsvReader


@pytest.fixture()
def configuration():
    return Configuration("test/hello_world_tennis.json")


@pytest.fixture
def data_obj_factory(configuration):

    def data_object_factory():
        df = pd.read_csv("test/tennis.csv")

        data_object = DataObject(configuration)

        csv_reader = CsvReader(configuration, "read_data")

        data_object.add(csv_reader, df)

        return data_object

    return data_object_factory


@pytest.fixture()
def pipeline_obj(configuration):
    return EncodeTrainTestSplit(configuration, "encode_and_split")


def test_concatenate_data(pipeline_obj, configuration):

    df1 = pd.read_csv("test/tennis.csv")
    df2 = pd.read_csv("test/tennis.csv")

    data_object = DataObject(configuration)
    csv_reader = CsvReader(configuration, "read_data")

    data_object.add(csv_reader, df1, 'query1')
    data_object.add(csv_reader, df2, 'query2')

    data_object, terminate = pipeline_obj.run(data_object)

    encoded_data = data_object.get('encode_and_split')['data_train']

    assert len(encoded_data) == 18


def test_encode_data(data_obj_factory, pipeline_obj):

    data_obj = data_obj_factory()

    data_object, terminate = pipeline_obj.run(data_obj)

    encoded_data = data_object.get('encode_and_split')['data_train']
    encoded_target = data_object.get('encode_and_split')['target_train']

    assert set(list(encoded_data.outlook.unique())) == set([0, 1, 2])
    assert set(list(encoded_data.temp.unique())) == set([0, 1, 2])
    assert set(list(encoded_data.humidity.unique())) == set([0, 1])
    assert set(list(encoded_data.windy.unique())) == set([True, False])
    assert set(list(encoded_target.unique())) == set([0, 1])

    assert pipeline_obj.transformer_sequence is not None
    assert list(pipeline_obj.first_transformer_in_sequence.target_encoder.inverse_transform([1, 0])) == ['yes', 'no']


def test_transform_after_fit(data_obj_factory, pipeline_obj):

    to_fit_object = data_obj_factory()
    data_obj = data_obj_factory()

    _data_object, _terminate = pipeline_obj.run(to_fit_object)

    data_object = pipeline_obj.transform(data_obj)

    transform_test_data_to_compare = data_object.get('encode_and_split')['data_test']

    assert len(transform_test_data_to_compare) == 14

    for col in transform_test_data_to_compare:
        assert transform_test_data_to_compare[col].dtype != object


def test_train_test_split(pipeline_obj):

    df = pd.read_csv("test/tennis.csv")
    train_data, test_data = pipeline_obj._train_test_split(df)

    assert len(train_data) == 9
    assert len(test_data) == 5
    assert not train_data.equals(test_data)

