"""Module to run a basic regression model

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
import logging
from primrose.models.sklearn_model import SklearnModel

class SklearnRegressionModel(SklearnModel):

    @staticmethod
    def necessary_config(node_config):
        """Return a list of necessary configuration keys

        Note:
            While you might expect `model` here, we do not need it when in predict or eval mode as the model is cached, only in train

        Returns:
            set of required keys

        """
        return SklearnModel.necessary_config(node_config)

    def fit_training_data(self):
        """fit training data to model"""
        self.model.fit(self.X_train, self.y_train)

    def get_scores(self):
        """get the scores for y_test
        
        Returns:
            dictionary of scores
        
        """
        return SklearnModel.evaluate_regression_metrics(self.y_test, self.predictions)
