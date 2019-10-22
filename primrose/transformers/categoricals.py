"""Module to run a basic decision tree model

Author(s):
    Mike Skarlinski (michael.skarlinski@weightwatchers.com)

"""
import pandas as pd
import numpy as np
import logging
from sklearn import preprocessing

from primrose.base.transformer import AbstractTransformer


class ExplicitCategoricalTransform(AbstractTransformer):

    DEFAULT_NUMERIC = -9999

    def __init__(self, categoricals):
        """initialize the ExplicitCategoricalTransform
        Args:
            categoricals: dictionary containing for each column to be transformed:
            - transformations: list of strings to be executed on the data ('x' represents the current categorical variable)
            - rename: if present, rename the current categorical variable to that name
            - to_numeric: if true, attempt to apply to_numeric after previous transformations

        """
        self.categoricals = categoricals

    def fit(self, data):
        pass

    @staticmethod
    def _process_transformations(data, input_data, categorical, x):
        """transform a column

        Args:
            data (dataframe): dataframe

            input configuration (JSON): JSON categorical config for this variable

            categorical (str): varible name

            x (str): transformation string

        Returns:
            data (dataframe)

        """
        if 'transformations' in input_data.keys():
            logging.info("Applying key {} to variable {}".format('transformations', categorical))
            for transformation in input_data['transformations']:
                exec(transformation.format(x=x))

    @staticmethod
    def _process_rename(data, input_data, categorical):
        """rename a field

        Args:
            data (dataframe): dataframe

            input configuration (JSON): JSON categorical config for this variable

            categorical (str): varible name

        Returns:
            (tuple): tuple containing:

                data (dataframe): dataframe

                name (str): original name (if not "to_numeric": True), new_name otherwise
        """
        if 'rename' in input_data.keys():
            logging.info("Applying key {} to variable {}".format('rename', categorical))
            data = data.rename({categorical: input_data['rename']}, axis='columns')
            return data, input_data['rename']
        return data, categorical

    @staticmethod
    def _process_numeric(data, input_data, name):
        """convert column to numeric

        Args:
            data (dataframe): dataframe

            input configuration (JSON): JSON categorical config for this variable

            name (str): field name

        Returns:
            data with the colun converted to numeric

        """
        if 'to_numeric' in input_data.keys():

            logging.info("Applying key {} to variable {}".format('to_numeric', name))

            if input_data['to_numeric']:

                # if there are errors converting to numerical values, we need to sub in a reasonable value
                if sum(pd.to_numeric(data[name], errors='coerce').isnull()) > 0:
                    logging.info("Can't convert these entries in {}. Replacing with {}: {}".format(
                        name, ExplicitCategoricalTransform.DEFAULT_NUMERIC,
                        np.unique(data[name][pd.to_numeric(data[name], errors='coerce').isnull()])))

                    data[name][pd.to_numeric(data[name], errors='coerce').isnull()] = ExplicitCategoricalTransform.DEFAULT_NUMERIC
                try:
                    data[name] = pd.to_numeric(data[name])
                    return data

                except:
                    raise TypeError('Failed to convert feature {} to numeric'.format(name))

        else:
            return data

    def transform(self, data):
        """Transform categorical variables into one or more numeric ones, no need to separate testing & training data

        Args:
            data: dictionary containing dataframe with all categorical columns present

        Returns:
            data with all categorical columns recoded and/or deleted

        """
        for categorical in self.categoricals.keys():

            x = "data['{}']".format(categorical)

            input_data = self.categoricals[categorical]

            ExplicitCategoricalTransform._process_transformations(data, input_data, categorical, x)

            data, new_name = ExplicitCategoricalTransform._process_rename(data, input_data, categorical)

            data = ExplicitCategoricalTransform._process_numeric(data, input_data, new_name)

        return data


class ImplicitCategoricalTransform(AbstractTransformer):
    """Class which implicitly transforms all string columns of a dataframe with sklearn LabelEncoder"""

    def __init__(self, target_variable):
        """initialize this ImplicitCategoricalTransform

        Args:
            target_variable (str): target variable name

        """
        self.target_variable = target_variable
        self._encoder = {}
        self.target_encoder = None

    def fit(self, data):
        """encode the data as categorical labels

        Args:
            data (dataframe)

        Returns:
            dataframe (dataframe)

        """

        logging.info('Fitting LabelEncoders on all string-based dataframe columns...')

        data.is_copy = False

        for column_name in data.columns:

            if data[column_name].dtype == object:

                logging.info('Fitting LabelEncoder for column {}'.format(column_name))

                self._encoder[column_name] = preprocessing.LabelEncoder()
                self._encoder[column_name].fit(data[column_name])

                if column_name == self.target_variable:
                    self.target_encoder = self._encoder[column_name]
            else:
                pass

        return data

    def transform(self, data):
        """Transform data into categorical variables using pre-trained label encoder

        Args:
            data (dataframe)

        Returns:
            dataframe (dataframe)

        """

        data.is_copy = False

        for column_name in data.columns:

            if column_name in self._encoder:

                logging.info('LabelEncoding column {}'.format(column_name))

                data[column_name] = self._encoder[column_name].transform(data[column_name])

        return data
