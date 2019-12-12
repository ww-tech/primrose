"""Module to run preprocessing using SKlearn preprocessors

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
import importlib
from primrose.base.transformer_sequence import TransformerSequence
from primrose.transformers.sklearn_preprocessing_transformer import SklearnPreprocessingTransformer
from primrose.data_object import DataObjectResponseType
from primrose.pipelines.train_test_split import TrainTestSplit

class SklearnPreprocessingPipeline(TrainTestSplit):

    @staticmethod
    def necessary_config(node_config):
        """Return the necessary configuration keys for the SklearnPreprocessingPipeline object

        Returns:
            set of keys

        """
        return set(['operations']).union(TrainTestSplit.necessary_config(node_config))

    def init_pipeline(self):
        """create the pipeline's TransformerSequence

        Returns:
            a TransformerSequence

        """
        ts = TransformerSequence()

        for operation in self.node_config["operations"]:

            args = operation.get('args', None)
            columns = operation.get('columns', None)
            
            p = SklearnPreprocessingPipeline._instantiate_preprocessor(operation['class'], args, columns)
            ts.add(p)

        return ts

    @staticmethod
    def _instantiate_preprocessor(classname, args=None, columns=None):
        """Import and validate user-defined sklearn preprocessor
        
        Returns:
            SklearnPreprocessingTransformer
        
        """

        sk_module_name, sk_transformer_name = classname.split('.')
        sk_module = importlib.import_module('sklearn.{}'.format(sk_module_name))

        try:
            t = getattr(sk_module, sk_transformer_name)
        except AttributeError:
            raise Exception('Preprocessor {} not found in {} module'.format(sk_transformer_name, sk_module_name))

        if args:
            return SklearnPreprocessingTransformer(t(**args), columns)

        return SklearnPreprocessingTransformer(t(), columns)


