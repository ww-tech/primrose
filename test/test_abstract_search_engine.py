import sys
import math
import pytest
import pandas as pd
from primrose.configuration.configuration import Configuration
from primrose.base.search_engine import AbstractSearchEngine
from primrose.base.node import AbstractNode
from nltk import ngrams
from primrose.node_factory import NodeFactory
from primrose.data_object import DataObject, DataObjectResponseType

'''we can't instatiate AbstractSearchEngine but this will test some of the methods'''

def test_cosine_similarity_matrix():

    class Testpipeline(AbstractNode):
        def __init__(self, configuration, instance_name):
            self.configuration = configuration
            self.instance_name = instance_name
        @staticmethod
        def necessary_config(node_config):
            return set([])
        def run(self, data_object):
            return data_object, False
    NodeFactory().register("Testpipeline", Testpipeline)

    class TestSimpleSearchEngine(AbstractSearchEngine):
        '''
            simple TFIDF search engine
        '''

        def __init__(self, configuration, instance_name):
            AbstractSearchEngine.__init__(self, configuration, instance_name)

        def tokenize(self, s, stopwords=[], add_ngrams=True):
            q = s.lower()
            tokens = q.replace("-", " ").replace(",", "").replace("(", "").replace(")", "").split(" ")
            tokens = [w for w in tokens if w not in stopwords]
            if add_ngrams:
                bigrams = list(ngrams(tokens, 2))
                strbigrams = ["_".join(t) for t in bigrams]
                tokens.extend(strbigrams)
            return tokens

        def eval_model(data_object): return data_object

    NodeFactory().register("TestSimpleSearchEngine", TestSimpleSearchEngine)

    config = {
          "implementation_config":{
            "pipeline_config": {
                "pipeline1": {
                    "class": "Testpipeline",
                    "destinations": ["recipe_name_model"]
                }
            },
            "model_config": {
                "recipe_name_model": {
                    "class": "TestSimpleSearchEngine",
                    "id_key": "id",
                    "doc_key": "name",
                    "mode": "precict",
                    "destinations": []
                }
            }
        }
    }

    configuration = Configuration(None, is_dict_config=True, dict_config=config)

    #set that pipeline provided the corpus
    corpus = [{"id":1,"name":"spinach omelet"},
        {"id":2,"name":"kale omelet"},
        {"id":3,"name":"cherry pie"}]
    data_object = DataObject(configuration)
    pipeline = Testpipeline(configuration, 'pipeline1')
    data_object.add(pipeline, pd.DataFrame(corpus))

    engine = TestSimpleSearchEngine(configuration, 'recipe_name_model')
    engine.predict(data_object)

    m = engine.cosine_similarity_matrix()

    assert math.isclose(m[0, 0], 1., abs_tol=0.001)
    assert math.isclose(m[0, 1], 0.224325, abs_tol=0.001)
    assert math.isclose(m[0, 2], 0., abs_tol=0.001)

    assert math.isclose(m[1, 0], 0.224325, abs_tol=0.001)
    assert math.isclose(m[1, 1], 1., abs_tol=0.001)
    assert math.isclose(m[1, 2], 0., abs_tol=0.001)

    assert math.isclose(m[2, 0], 0., abs_tol=0.001)
    assert math.isclose(m[2, 1], 0., abs_tol=0.001)
    assert math.isclose(m[2, 2], 1., abs_tol=0.001)

    assert engine.ids == [1,2,3]
    assert engine.docs == ["spinach omelet","kale omelet","cherry pie"]
    assert engine.tfidf is not None