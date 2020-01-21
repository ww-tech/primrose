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

    def _make_predictions(self, data_object):
        """Make predictions

        Args:
            data_object (DataObject): instance of DataObject

        Returns:
            nothing. Predictions stored in self.predictions

        """
        if hasattr(self, 'predictions') and self.predictions is not None:
            return

        if self.model is None:
            self.model = self.load_model(data_object)

        self.X_train, self.y_train, self.X_test, self.y_test = self._get_data(data_object)

        logging.info("Making predictions with model")
        to_predict = self.X_test

        if self.X_test is None:
            to_predict = self.X_train
            self.predictions = self.model.labels_
        else:
            self.predictions = self.model.predict(self.X_test)

    def predict(self, data_object, load_model=False, use_serial=False):
        """Predict y_test from X_test

        Args:
            data_object (DataObject): instance of DataObject

            load_model: load model object from gcs or not

        Returns: 
            data_object (DataObject): instance of DataObject

        """
        self._make_predictions(data_object)
        
        if self.X_test is None:
            data = self.X_train
            if self.y_train is not None:
                data['actual'] = self.y_train
        else:
            data = self.X_test
            if self.y_test is not None:
                data['actual'] = self.y_test

        data['predictions'] = self.predictions
        
        data_object.add(self, data)
        return data_object

    def get_scores(self):
        """get the scores for X_test
        
        Returns:
            returns a dictionary of scors
        
        """
        data = self.X_test
        if self.X_test is None:
            data = self.X_train
        return SklearnModel.evaluate_no_ground_truth_classifier_metrics(data, self.predictions)
