"""Module with AbstractNode implementation, Example model for primrose user modification

Author(s):
    Mike Skarlinski (michael.skarlinski@ww.com)

"""
from primrose.base.model import AbstractModel


class AwesomeModel(AbstractModel):
    """(EXAMPLE MODEL) Print what is going on for each abstract method implementation

    Notes:
        see AbstractModel class to understand how this model is run in each mode!

    """

    @staticmethod
    def necessary_config(node_config):
        """Returns the necessary configuration keys for the AwesomeModel object

        Put your required configuration keys here, here we pass just the base requirement from the abstract class

        Args:
            node_config (dict): set of parameters / attributes for the node

        Note:
            filename: name of the file

        Returns:
            set of necessary keys for the AwesomeModel object

        """
        return AbstractModel.necessary_config(node_config)

    def train_model(self, data_object):
        """Code to train your model and return a data_object after adding any training or model info"""

        # Example model training code for building your own model
        # ----------------------------------
        print('I am training my model.')

        # Example showing how to get upstream data
        # get some training data if it exists
        try:
            training_data = data_object.get_upstream_data(self.instance_name)

        except:
            print('No upstream data exists')
        # ----------------------------------

        return data_object

    def eval_model(self, data_object):
        """Code to evaluate your models performance"""

        print('My model is doing pretty well.')

        return data_object

    def predict(self, data_object):
        """Make predictions using your model"""

        print('I am predicting!')

        return data_object
