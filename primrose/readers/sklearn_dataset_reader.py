"""Module to read canned datasets from sklearn

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
from primrose.base.sql_reader import AbstractReader
from sklearn.datasets import *
import pandas as pd

class SklearnDatasetReader(AbstractReader):
    """Read data from sklearn.dataset into a pandas dataframe"""

    @staticmethod
    def necessary_config(node_config):
        """Returns the necessary configuration keys for the SklearnDatasetReader object

        Note:
            dataset (str): name of supported sklearn.dataset. One of "iris", "boston", "diabetes", "breast_cancer", "linnerud", "wine"
            
        Returns: 
            set of necessary keys for the SklearnDatasetReader object

        """
        return set(['dataset'])

    def run(self, data_object):
        """Read sklearn dataset to a pandas dataframe

        Returns:
            data_object (DataObject): DataObject instance
            terminate (bool): should we terminate the DAG? true or false

        """
        dataset = self.node_config['dataset']

        function_map = {
            "iris": load_iris,
            "boston": load_boston,
            "diabetes": load_diabetes,
            "breast_cancer": load_breast_cancer,
            "linnerud": load_linnerud,
            "wine": load_wine
        }

        if not dataset in function_map.keys():
            raise Exception("Dataset not supported " + dataset)

        data = function_map[dataset]()

        df = pd.DataFrame(data['data'], columns = data['feature_names'])
        df['target'] = data['target']

        data_object.add(self, df)

        terminate = False
        return data_object, terminate
