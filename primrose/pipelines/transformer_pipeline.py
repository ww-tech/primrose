"""Module to run any sequence of transformers, both custom transformers and sklearn preprocessors.

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)
    Brian Graham (brian.graham@ww.com)

"""
import importlib
import inspect
from primrose.base.transformer_sequence import TransformerSequence
from primrose.base.transformer import AbstractTransformer
from primrose.pipelines.train_test_split import TrainTestSplit


class TransformerPipeline(TrainTestSplit):

    def __init__(self,configuration, instance_name):
        super(TransformerPipeline,self).__init__(configuration, instance_name)
        self.training_fraction = self.node_config.get("training_fraction", 0)
        self.seed = self.node_config.get("seed",0)

    @staticmethod
    def necessary_config(node_config):
        """Return the necessary configuration keys for the TransformerPipeline object

        Returns:
            set of keys

        """
        return set(['transformer_sequence'])

    @staticmethod
    def optional_config(node_config):
        """Return the optional configuration keys for the TransformerPipeline object

        Returns:
            set of keys

        """
        return TrainTestSplit.necessary_config(node_config)

    def init_pipeline(self):
        """create the pipeline's TransformerSequence

        Returns:
            a TransformerSequence

        """
        ts = TransformerSequence()

        for transformer in self.node_config["transformer_sequence"]:
            transformer_args = transformer.get('args', None)
            columns = transformer.get('columns', None)

            p = self._instantiate_transformer(transformer)
            ts.add(p)

        return ts

    @staticmethod
    def _instantiate_transformer(transformer):
        """Import and validate user-defined sklearn preprocessor

        Returns:
            AbstractTransformer

        """
        classname = transformer['class']
        path_sequence = classname.split('.')
        target_class_name = path_sequence.pop(-1)
        module = importlib.import_module('.'.join(path_sequence))

        try:
            t = getattr(module, target_class_name)
        except AttributeError:
            raise Exception(f'Transformer {target_class_name} not found in {".".join(path_sequence)} module')

        class_args = {k: v for k,v in transformer.items() if k != "class"}
        params = [p for p in inspect.signature(t).parameters]
        t_args = [class_args.pop(p) for p in params if p in class_args.keys()]

        return t(*t_args, **class_args)


