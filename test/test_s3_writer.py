
import pytest
import sys
import os
import boto3
from moto import mock_s3
import pandas as pd
from primrose.configuration.configuration import Configuration
from primrose.writers.s3_writer import S3Writer
from primrose.node_factory import NodeFactory
from primrose.data_object import DataObject
from primrose.base.node import AbstractNode

@mock_s3
def test_init_ok():
    config = {
        "implementation_config": {
            "postprocess_config": {
                "nodename": {
                    "class": "TestPostprocess",
                    "key1":"val1",
                    "key2":"val2",
                    "destinations": ["recipe_s3_writer"]
                }
            },
            "writer_config": {
                "recipe_s3_writer": {
                    "class": "S3Writer",
                    "dir": "cache",
                    "key": DataObject.DATA_KEY,
                    "bucket_name": "does_not_exist_bucket_name",
                    "bucket_filename": "does_not_exist.csv"
                }
            }
        }
    }
    class TestPostprocess(AbstractNode):
        @staticmethod
        def necessary_config(node_config):
            return set(['key1', 'key2'])
        def run(self, data_object): return data_object
    NodeFactory().register("TestPostprocess", TestPostprocess)

    #this is to mock out the boto connection
    os.environ["AWS_ACCESS_KEY_ID"] = "fake" 
    os.environ["AWS_SECRET_ACCESS_KEY"] = "fake"
    conn = boto3.resource('s3')
    # We need to create the bucket since this is all in Moto's 'virtual' AWS account
    conn.create_bucket(Bucket='does_not_exist_bucket_name')

    reference_file_path = "test/minimal.csv"

    corpus = pd.read_csv(reference_file_path)

    configuration = Configuration(None, is_dict_config=True, dict_config=config)

    data_object = DataObject(configuration)

    requestor = TestPostprocess(configuration, 'nodename')

    data_object.add(requestor, corpus)

    writer = S3Writer(configuration, 'recipe_s3_writer')
    node_config = {
                    "class": "S3Writer",
                    "dir": "cache",
                    "key": DataObject.DATA_KEY,
                    "bucket_name": "does_not_exist_bucket_name",
                    "bucket_filename": "does_not_exist.csv"
                }
    keys = writer.necessary_config(node_config)
    assert keys is not None
    assert isinstance(keys, set)
    assert len(keys) > 0

    # write to file
    filename = writer._write_locally(data_object)
    assert os.path.exists(filename)

    # check it is same data as expected
    reference = pd.read_csv(reference_file_path)
    just_written = pd.read_csv(filename)

    assert reference.equals(just_written)

    os.remove(filename)

    data_object = writer.run(data_object)
    body = conn.Object('does_not_exist_bucket_name', 'does_not_exist.csv').get()['Body'].read().decode("utf-8")
    assert body == open(reference_file_path).read()