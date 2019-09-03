
import pytest
from primrose.base.pipeline import AbstractPipeline
from primrose.base.pipeline import PipelineModeType
from primrose.configuration.configuration import Configuration
from primrose.data_object import DataObject
from primrose.readers.csv_reader import CsvReader
from primrose.base.transformer_sequence import TransformerSequence
from primrose.base.transformer import AbstractTransformer
from primrose.node_factory import NodeFactory
import pandas as pd
import logging
from testfixtures import LogCapture

def test_names():
    assert PipelineModeType.names() == ['FIT', 'FIT_TRANSFORM', 'TRANSFORM']

def test_values():
    assert PipelineModeType.values() == ['FIT', 'FIT_TRANSFORM', 'TRANSFORM']

def test_run():

    class TestPipeline(AbstractPipeline):
        def transform(self, data_object):
            logging.info("TRANSFORM CALLED")
            return data_object 

        def fit_transform(self, data_object):
            logging.info("FIT_TRANSFORM CALLED")
            return self.transform(data_object)

        @staticmethod
        def necessary_config(node_config): return set(['is_training'])

    NodeFactory().register("TestPipeline", TestPipeline)

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
                    "class": "TestPipeline",
                    "is_training": True
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)

    reference_file_path = "test/minimal.csv"
    corpus = pd.read_csv(reference_file_path)

    reader = CsvReader(configuration, "myreader")

    data_object = DataObject(configuration)
    data_object.add(reader, corpus)

    pipeline = TestPipeline(configuration, "mypipeline")

    with LogCapture() as l:
        pipeline.run(data_object)
    l.check(
        ('root', 'INFO', 'No upstream TransformerSequence found. Creating new TransformerSequence...'),
        ('root', 'INFO', 'FIT_TRANSFORM CALLED'),
        ('root', 'INFO', 'TRANSFORM CALLED')
    )

    data_object.add(reader, TransformerSequence(), "tsequence")
    with LogCapture() as l:
        pipeline.run(data_object)
    l.check(
        ('root', 'INFO', 'Upstream TransformerSequence found, initializing pipeline...'),
        ('root', 'INFO', 'FIT_TRANSFORM CALLED'),
        ('root', 'INFO', 'TRANSFORM CALLED')
    )

    config['implementation_config']['pipeline_config']['mypipeline']['is_training'] = False
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)
    reader = CsvReader(configuration, "myreader")
    data_object = DataObject(configuration)
    data_object.add(reader, corpus)
    pipeline = TestPipeline(configuration, "mypipeline")
    with LogCapture() as l:
        pipeline.run(data_object)
    l.check(
        ('root', 'INFO', 'No upstream TransformerSequence found. Creating new TransformerSequence...'),
        ('root', 'INFO', 'TRANSFORM CALLED')
    )

def test_execute_pipeline():
    class TestTransformer(AbstractTransformer):
        def fit(self, data):
            logging.info("Transfer FIT CALLED")

        def transform(self, data):
            logging.info("Transfer TRANSFORM CALLED")
            return data

        def fit_transform(self, data):
            logging.info("Transfer FIT_TRANSFORM CALLED")
            self.fit(data)
            return self.transform(data)

    class TestPipeline2(AbstractPipeline):
        def transform(self, data_object): return data_object 
        @staticmethod
        def necessary_config(node_config): return set(['is_training'])
    NodeFactory().register("TestPipeline2", TestPipeline2)

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
                    "class": "TestPipeline",
                    "is_training": True
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)

    reference_file_path = "test/minimal.csv"
    corpus = pd.read_csv(reference_file_path)

    reader = CsvReader(configuration, "myreader")

    data_object = DataObject(configuration)
    data_object.add(reader, corpus)

    sequence = TransformerSequence()
    sequence.add(TestTransformer())
    data_object.add(reader, sequence, "tsequence")


    pipeline = TestPipeline2(configuration, "mypipeline")

    with pytest.raises(Exception) as e:
         pipeline.execute_pipeline(corpus, PipelineModeType.FIT)
    assert "run() must be called to extract/create a TransformerSequence" in str(e)

    pipeline.run(data_object)

    with pytest.raises(Exception) as e:
         pipeline.execute_pipeline(corpus,"JUNK")
    assert "mode must be of type PipelineModeType Enum object." in str(e)

    with LogCapture() as l:
        pipeline.execute_pipeline(corpus, PipelineModeType.FIT)
    l.check(
        ('root', 'INFO', 'Transfer FIT CALLED')
    )

    with LogCapture() as l:
        pipeline.execute_pipeline(corpus, PipelineModeType.FIT_TRANSFORM)
    l.check(
        ('root', 'INFO', 'Transfer FIT_TRANSFORM CALLED'),
        ('root', 'INFO', 'Transfer FIT CALLED'),
        ('root', 'INFO', 'Transfer TRANSFORM CALLED')
    )