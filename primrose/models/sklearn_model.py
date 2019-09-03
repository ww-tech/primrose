"""A primrose model based around a sklearn model

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
from primrose.base.model import AbstractModel
import importlib
import logging
from sklearn.metrics import roc_curve, auc
from sklearn import metrics
import datetime

class SklearnModel(AbstractModel):

    def __init__(self, configuration, instance_name):
        """A Sklearn-based model to train, evaluate and predict on dataframe feature data

        Args:
            configuration (Configuration): user defined configuration object
            instance_name (str): name to identify the correct configuration key

        """
        super(SklearnModel, self).__init__(configuration, instance_name)
        self.model = None
        self.scores = {}

    def load_model(self, data_object):
        """finds an upstream sklearn mode

        Args:
            data_object (DataObject): instance of DataObject

        Returns:
            Sklearn model

        """

        # check for a valid pipeline object inside the data object
        model = data_object.get_filtered_upstream_data(self.instance_name, filter_for_key='reader_data')['reader_data']

        if model is None:
            raise Exception('No valid upstream model found in the data object for model.')

        logging.info('Model successfully loaded from upstream data_object.')

        return model

    @staticmethod
    def _instantiate_model(classname, args):
        """instantiates a Sklearn model from class name

        Args:
            classname (str): name of class without initial `sklearn.`, e.g. `cluster.KMeans`

            args (dict): dictionary of arguments/parameters for model instantiation

        Returns:
            instance of a sklearn mode

        """
        sk_module_name, sk_model_name = classname.split('.')
        sk_module = importlib.import_module('sklearn.{}'.format(sk_module_name))

        try:
            m = getattr(sk_module, sk_model_name)
        except AttributeError:
            raise Exception('Sklearn model {} not found in {} module'.format(sk_model_name, sk_module_name))
        logging.info("Instantiating model " + classname)
        if args:
            return m(**args)
        else:
            return m()

    @staticmethod
    def _get_roc_score(actual_values, model_probability):
        """Get (binary) ROC store using actual target variables and predicted probabilities

            Assumes that the model is predicting 1 as the minority class

        Args:
            actual_values: series with correct target variable assignments
            model_probability: probability of target variable assignment from the model

        Returns: AUC for a binary classifier

        """
        fpr, tpr, _ = roc_curve(actual_values, [y[1] for y in model_probability])
        return auc(fpr, tpr)

    def _get_data(self, data_object):
        """get the upstream data, split into X and Y from config, return data frame

        Returns:
            dataframe (dataframe)

        """

        # get upstream data dict which contains the subkey data_test
        data = data_object.get_filtered_upstream_data(self.instance_name, 'data_test')

        X_train = None
        if 'data_train' in data:
            X_train = data['data_train']
            if 'features' in self.node_config:
                X_train = X_train[self.node_config['features']]

        y_train = None
        if 'target_train' in data:
            y_train = data['target_train']

        X_test = None
        if 'data_test' in data:
            X_test = data['data_test']
            if 'features' in self.node_config:
                X_test = X_test[self.node_config['features']]

        y_test = None
        if 'target_test' in data:
            y_test = data['target_test']

        def size(obj):
            if obj is None:
                return None
            return(str(obj.shape))

        logging.info("Get_data, X_train %s, y_train %s, X_test %s, y_test %s", size(X_train), size(y_train), size(X_test), size(y_test))

        return X_train, y_train, X_test, y_test

    def train_model(self, data_object):
        """train the model

        Args:
            data_object (DataObject): instance of DataObject

        Returns:
            data_object (DataObject): instance of DataObject

        """
        self.X_train, self.y_train, self.X_test, self.y_test = self._get_data(data_object)

        args = None
        if 'args' in self.node_config['model']:
            args = self.node_config['model']['args']

        self.model = SklearnModel._instantiate_model(self.node_config['model']['class'], args)

        logging.info("Fitting model")
        self.fit_training_data() #delegate down as args to self.model.fit() vary by model

        data_object.add(self, self.model, 'model')

        return data_object

    def _make_predictions(self, data_object):
        """Make predictions from X_test

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

        data = self.X_test
        data['predictions'] = self.predictions
        if self.y_test is not None:
            data['actual'] = self.y_test

        data_object.add(self, data)
        return data_object

    def eval_model(self, data_object, load_model=False):
        """evalute a model by getting the scores

        Returns:
            data_object (DataObject): instance of DataObject

        """
        self._make_predictions(data_object)

        logging.info("Evaluating metrics")
        scores = self.get_scores() #delegate down as appropriate metrics vary by model
        scores['eval_time'] = datetime.datetime.now()
        logging.info("Scores" + str(scores))

        data_object.add(self, scores, "scores")
        return data_object


    @staticmethod
    def evaluate_no_ground_truth_classifier_metrics(X, labels):
        """Compute a set of metric for a classifier where there is no ground truth

        Args:
            X (datafframe): the data
            labels: the predicted classes

        Returns:
            dictionary of score name: value

        """
        scores = {}
        scores['Silhouette Score']         = metrics.silhouette_score(X, labels, metric='euclidean')
        scores['Calinski Harabasz Score']  = metrics.calinski_harabasz_score(X, labels)
        scores['Davies Bouldin Score']     = metrics.davies_bouldin_score(X, labels)
        return scores

    @staticmethod
    def evaluate_regression_metrics(actual, predictions):
        """compute set of metrics for a regression

        Args:
            actual: vector of actual values
            predictions: vector of predictions

        Returns:
            dictionary of score name: value

        """
        scores = {}
        scores['Explained variance']       = metrics.explained_variance_score(actual, predictions)
        scores['Max error']                = metrics.max_error(actual, predictions)
        scores['Mean absolute error']      = metrics.mean_absolute_error(actual, predictions)
        scores['MSE']                      = metrics.mean_squared_error(actual, predictions)
        scores['Mean squared log error']   = metrics.mean_squared_log_error(actual, predictions)
        scores['Mean absolute error']      = metrics.median_absolute_error(actual, predictions)
        scores['R2']                       = metrics.r2_score(actual, predictions)
        return scores