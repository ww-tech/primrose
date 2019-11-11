import pytest
from primrose.node_factory import NodeFactory
from primrose.base.postprocess import AbstractPostprocess
from primrose.base.node import AbstractNode
from primrose.configuration.configuration import Configuration
from primrose.data_object import DataObject, DataObjectResponseType

def test_init():
    class TestPostprocess(AbstractPostprocess):
#        def __init__(self, configuration, instance_name):
#            super(TestPostprocess, self).__init__(configuration, instance_name)
        @staticmethod
        def necessary_config(node_config):
            return set(['key1'])
        def run(self, data_object):
            data_object.add(self, "some data")
            return data_object, False
        #def process(self, data):
        #    return "some data"
    NodeFactory().register("TestPostprocess", TestPostprocess)

    class TestModel(AbstractNode):
        def __init__(self, configuration, instance_name):
            pass
        @staticmethod
        def necessary_config(node_config):
            return set(['key1'])
        def run(self, data_object):
            return data_object, False
    NodeFactory().register("TestModel", TestModel)

    config = {
        "implementation_config": {
            "model_config": {
                "modelname": {
                    "class": "TestModel",
                    "key1":"val1",
                    "destinations": ["nodename"]
                }
            },
            "postprocess_config": {
                "nodename": {
                    "class": "TestPostprocess",
                    "key1":"val1",
                    "key2":"val2",
                    "destinations": []
                }
            }
        }
    }
    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)

    data_object = DataObject(configuration)

    tp = TestPostprocess(configuration, 'nodename')
    node_config = {
                    "class": "TestPostprocess",
                    "key1":"val1",
                    "key2":"val2",
                    "destinations": []
                }
    assert set(['key1']) == tp.necessary_config(node_config)

    #assert tp.process(None) == "some data"

    data_object, terminate = tp.run(data_object)

    assert not terminate
    
    assert data_object.get("nodename", rtype=DataObjectResponseType.VALUE.value) == "some data"

#    assert tp.upstream_model_keys() == ["modelname"]

#    assert tp.single_upstream_model_key() == "modelname"
