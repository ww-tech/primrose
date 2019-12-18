"""Module with abstract model class to specify interface needed for future models

Author(s): 
    Michael Skarlinski (michael.skarlinski@weightwatchers.com)

    Carl Anderson (carl.anderson@weightwatchers.com)

"""

from abc import abstractmethod
from primrose.base.node import AbstractNode
from primrose.util import RunModes


class AbstractModel(AbstractNode):
    """Model class should be able to train, evaluate or predict"""

    def run(self, data_object):
        """run the model

        Args:
            data_object (DataObject): instance of DataObject

        Returns:
            data_object (DataObject): instance of DataObject

        """

        mode = self.node_config['mode'].lower()
        assert mode in RunModes.values()

        if mode == RunModes.TRAIN.value:
            data_object = self.train_model(data_object)

        if mode in [RunModes.EVAL.value, RunModes.TRAIN.value]:
            data_object = self.eval_model(data_object)

        data_object = self.predict(data_object)
        terminate = False
        return data_object, terminate

    @staticmethod
    @abstractmethod
    def necessary_config(node_config):
        """Return a list of necessary configuration keys within the implementation

        Args:
            node_config (dict): set of parameters / attributes for the node

        Note:
            After adding this list, validation automatically occurs before instantiation in the pipeline factory.

        Returns:
            set of keys necessary to run implementation

        """
        return set(['mode']) # pragma: no cover

    @abstractmethod
    def train_model(self, data_object):
        """Train an internal model attribute, then save the trained model with the save_model method

            Using the training features in feature_df (pandas dataframe object) and the target_variable (pandas series),
            cross-validation or other training scripts will happen in theis method, according to the parameters
            in parameters

        Args:
            data_object (DataObject): instance of DataObject

        Returns: 
            data_object (DataObject): instance of DataObject

        """
        pass # pragma: no cover

    @abstractmethod
    def eval_model(self, data_object):
        """Evaluate a previously trained model performance using labeled data

            Method should be able to load a serialized model if necessary (load_model method), or work from a model
            trained in-scope with the train_model method. This method should calculate and store some aspect of
            model error into attributes that are serialized with the save_model method

        Args:
            data_object (DataObject): instance of DataObject

        Returns: 
            data_object (DataObject): instance of DataObject

        """
        pass # pragma: no cover

    @abstractmethod
    def predict(self, data_object):
        """Make predictions using a pre-trained model on the features in the feature_df dataframe

            Using a pre-trained model (either from an in-scope run of train_model or a call to the load_model method)
            this method should append predictions to each row of features in the input, feature_df. This method may
            also add feature importance columns, for importance analysis

        Args:
            data_object: DataObject instance

        Returns:
            (tuple): tuple containing:

                data_object (DataObject): instance of DataObject

                terminate (bool): terminate the DAG?

        """
        pass # pragma: no cover
