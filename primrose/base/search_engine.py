"""Abstract base class that performs TFIDF on some corpus

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
from abc import abstractmethod
from primrose.base.model import AbstractModel
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from primrose.data_object import DataObjectResponseType
import pandas as pd
import logging

class AbstractSearchEngine(AbstractModel):
    """an abstract search engine"""

    def __init__(self, configuration, instance_name):
        """abstract search engine

        Args:
            configuration (Configuration): configuration object
            instance_name (str): name of the instance

        """
        super(AbstractSearchEngine, self).__init__(configuration, instance_name)
        self.ids = None
        self.docs = None
        self.tfidf = None
        self.term_document_matrix = None
        self.corpus_key = self.node_config.get('corpus_key','corpus')

    @staticmethod
    def necessary_config(node_config):
        """Return a list of necessary configuration keys for AbstractSearchEngine

        Args:
            node_config (dict): set of parameters / attributes for the node

        Note:
            id_key: key used in the corpus object for ids
            doc_key: key used in the corpus object for docs

        Returns:
            set of keys necessary to run AbstractSearchEngine

        """
        return set(['id_key', 'doc_key']).union(AbstractModel.necessary_config(node_config))

    def train_model(self, data_object):
        """train the model which means run fit_transform on a TFIDF model

        Args:
            data_object (DataObject): instance of DataObject

        Returns:
            data_object (DataObject): instance of DataObject

        """

        upstream_data = data_object.get_upstream_data(self.instance_name,
            rtype=DataObjectResponseType.VALUE.value)

        if isinstance(upstream_data, pd.DataFrame):
            corpus = upstream_data

        # if a data dict was passed, loop through data objects searching for a corpus dataframe
        elif isinstance(upstream_data, dict):
            for data_key, data in upstream_data.items():

                # check that the data object is a dataframe, if not skip
                if not isinstance(data, pd.DataFrame):
                    logging.info("Upstream data from {}.{} not in necessary format, skipping".format(self.instance_name,
                                                                                                            data_key))
                    continue

                if data_key == self.corpus_key:
                    corpus = data
                else:
                    logging.info("{} key not found for data entry in upstream {} object, skipping".format(
                        self.corpus_key, data_key))
        else:
            raise Exception('Search Engine requires a corpus dataframe'.format())


        self.ids = list(corpus[self.node_config['id_key']])
        self.docs = list(corpus[self.node_config['doc_key']])
        self.tfidf = TfidfVectorizer(tokenizer=self.tokenize,stop_words=None,ngram_range=(1,1))
        self.term_document_matrix = self.tfidf.fit_transform(self.docs)

        return data_object

    def eval_model(self, data_object):
        """evaluate the model

        Args:
            data_object (DataObject): instance of DataObject

        Returns:
            data_object (DataObject): instance of DataObject

        """
        # this is define as abstract in superclass
        # this code is essentially unreachable as it has to be overridden in concrete subclass
        return data_object

    def predict(self, data_object):
        """make predictions from this corpus, meaning create cosine similarity matrix

        Args:
            data_object (DataObject): instance of DataObject

        Returns:
            data_object (DataObject): instance of DataObject

        """
        data_object = self.train_model(data_object)

        data_object.add(self, self.cosine_similarity_matrix())

        return data_object

    @abstractmethod
    def tokenize(self, s):
        """Given some string, tokenize it

        Args:
            s (str): input string

        Returns:
            list of tokens

        """
        # this code is essentially unreachable as it has to be overridden in concrete subclass
        pass # pragma: no cover

    def cosine_similarity_matrix(self):
        """compute the cosine similarities between all document pairs in the corpus

        Returns:
            matrix (numpy): square matrix of cosine similarities where index of matrix is index of corpus IDs

        """
        return cosine_similarity(self.term_document_matrix)
