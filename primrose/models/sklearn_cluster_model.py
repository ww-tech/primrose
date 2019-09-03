"""Module to run a basic clustering model

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
import logging
from primrose.models.sklearn_model import SklearnModel

class SklearnClusterModel(SklearnModel):

    @staticmethod
    def necessary_config(node_config):
        """Return a list of necessary configuration keys

        Note:
            X (list): list of columns to use

            While you might expect `model` here, we do not need it when in predict or eval mode as the model is cached, only in train

        Returns:
            set of required keys

        """
        return SklearnModel.necessary_config(node_config)

    def fit_training_data(self):
        """fit training data to model"""
        self.model.fit(self.X_train)

    def get_scores(self):
        """get the scores for X_test
        
        Returns:
            returns a dictionary of scors
        
        """
        return SklearnModel.evaluate_no_ground_truth_classifier_metrics(self.X_test, self.predictions)
