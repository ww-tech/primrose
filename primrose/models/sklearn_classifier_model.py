"""Module to run a basic decision tree model

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)
    Mike Skarlinski (michael.skarlinski@weightwatchers.com)

"""
import logging
import datetime
from sklearn.base import is_classifier
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import f1_score
from sklearn.metrics import recall_score
from sklearn.metrics import precision_score
from sklearn.metrics import accuracy_score
from sklearn.metrics import auc
from primrose.readers.dill_reader import DillReader
from primrose.models.sklearn_model import SklearnModel

class SklearnClassifierModel(SklearnModel):

    @staticmethod
    def necessary_config(node_config):
        """Return a list of necessary configuration keys

        Args:
            node_config (dict): set of parameters / attributes for the node

        Notes:
            model_parameters (dict): parameters that mirror the sklearn kwargs for the user's model
            mode: train, eval or predict (see AbstractModel)
            sklearn_classifier_name: sklearn submodule and model name (submodule.model_name) of the user's model
            grid_search_scoring: scoring function name from sklearn CV docs
            cv_folds: number of CV folds

        Returns:
            set of required keys

        """
        return set(["model_parameters", "mode", "sklearn_classifier_name", "grid_search_scoring", "cv_folds"])

    def train_model(self, data_object):
        """train the model using CV, according to user specified options

        Args:
            data_object (DataObject): instance of DataObject

        Returns:
            Nothing

        """
        X_train, y_train, X_test, y_test = self._get_data(data_object)

        logging.info("Fitting model")

        self.model = SklearnModel._instantiate_model(self.node_config['sklearn_classifier_name'], args=None)

        self.model = GridSearchCV(self.model,
                                self.node_config['model_parameters'],
                                n_jobs=-1,
                                scoring=self.node_config['grid_search_scoring'],
                                verbose=2,
                                cv=self.node_config['cv_folds'],
                                refit=True)

        self.model.fit(X_train, y_train)

        data_object.add(self, self.model.best_estimator_, 'model')

        return data_object

    def eval_model(self, data_object):
        """Evaluate model perfomance on a labeled testing dataset
        
        Returns:
            data_object (DataObject): instance of DataObject
        
        """

        if self.model is None:
            self.model = self.load_model(data_object)

        logging.info("Evaluating model performance on testing data")

        X_train, y_train, X_test, y_test = self._get_data(data_object)

        model_predictions = self.model.predict(X_test)
        model_probability = self.model.predict_proba(X_test)

        model_predictions = model_predictions.astype(int)

        model_f1 = f1_score(y_test, model_predictions, labels=None, pos_label=1)
        recall = recall_score(y_test, model_predictions, labels=None, pos_label=1)
        prec = precision_score(y_test, model_predictions, labels=None, pos_label=1)
        accuracy = accuracy_score(y_test, model_predictions)

        logging.info('positive class fraction: {}'.format(float(sum(y_test)) / len(y_test)))
        logging.info('positive class predicted fraction: {}'.format(float(sum(model_predictions)) / len(y_test)))
        logging.info('f1: {}, recall: {}, precision: {}, accuracy: {}'.format(model_f1, recall, prec, accuracy))
        roc_auc = self._get_roc_score(y_test, model_probability)
        logging.info('AUC: {}'.format(roc_auc))

        # write performance and timing information to scores attribute
        self.scores['auc'] = roc_auc
        self.scores['f1'] = model_f1
        self.scores['recall'] = recall  # Recall=TP/(TP+FN)
        self.scores['precision'] = prec  # Precison=TP/(TP+FP)
        self.scores['positive_class_fraction'] = float(sum(y_test)) / len(y_test)
        self.scores['predicted_class_fraction'] = float(sum(model_predictions)) / len(y_test)
        self.scores['eval_time'] = datetime.datetime.now()

        return data_object

    def predict(self, data_object):
        """Make distance-based predictions using the prebuilt matrix

        Args:
            data_object: DataObject instance
            load_model: load model object from gcs or not

        Returns: 
            data_object with prediction data added

        """

        if self.model is None:
            self.model = self.load_model(data_object)

        X_train, y_train, X_test, y_test = self._get_data(data_object)

        predictions = self.model.predict(X_test)

        # get the upstream target_encoder if it exists
        data = data_object.get_filtered_upstream_data(self.instance_name, filter_for_key='target_encoder')

        if 'target_encoder' in data:
            logging.info("Reversing label encoding")
            predictions = data['target_encoder'].inverse_transform(predictions)

        # get original data frame and tack on column of predictions
        data_out = X_test
        data_out['predictions'] = predictions

        data_object.add(self, data_out, 'predictions')

        return data_object
