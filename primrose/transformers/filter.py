"""Transform data by filtering in data using filtering operations

Author(s):
    Michael Skarlinski (michael.skarlinski@weightwatchers.com)

    Carl Anderson (carl.anderson@weightwatchers.com)

"""
import operator
import logging
import numpy as np
import pandas as pd
import itertools


from primrose.base.transformer import AbstractTransformer


class FilterByPandasExpression(AbstractTransformer):
    """Applies filters to data as defined in feature_filters"""

    def __init__(self, feature_filters):
        """initialize filter with a list of feature_filters"""
        self.feature_filters = feature_filters

    def fit(self, data):
        """fit data, here just passing

        Args:
            data (object): some data

        """
        pass

    def transform(self, data):
        """Applies filters to data as defined in feature_filters. This is neccessary so we can filter rows in one reader
        based on information from another, and therefore has to be applied after the combiner step.

        The filters can operate on a single column with a fixed set of operations and a static value:

        fixed operations: `==`, `!=`, `<>`, `<`, `<=`, `>`, `>=`

        The feature_filters object should be structured as a list of lists:

        feature_filters: `[[column, operation, static value], [column, operation, static value]]`

        example: `[["number_of_members", "<", 1000]]` for filtering all rows with number_of_members less than 1000

        Args:
            data (dict): dictionary with dataframes from all readers
            data_key (str): key to pull the dataframe from within the data object
            feature_filters (list): list of lists with columns and operators to filter on
            instance_name (str): name of this pipeline instance

        Returns:
            dataframe with filtered data

        Raises:
            Exception if not a pandas dataframe, operation not supported, or column name not recognized

        """
        if not isinstance(data, pd.DataFrame):
            raise Exception("Data is not a pandas DataFrame")

        ops = {
            "==": operator.eq,
            "!=": operator.ne,
            "<>": operator.ne,
            "<": operator.lt,
            "<=": operator.le,
            ">": operator.gt,
            ">=": operator.ge,
        }

        if len(self.feature_filters) == 0:
            logging.info("no filters found; returning combined_data as filtered_data")
            return data

        else:

            filters = []

            for col, op, val in self.feature_filters:

                if not col in data.columns:
                    raise Exception("Unrecognized filter column '" + str(col) + "'")

                if str(op) not in ops:
                    raise Exception("Unsupported filter operation '" + str(op) + "'")

                filters.append(ops[op](data[col], val))

            filtered_data = data[np.all(filters, axis=0)]
            filtered_data = filtered_data.reset_index(drop=True)

            logging.info("Filtered out %d rows" % (len(data) - len(filtered_data)))

        return filtered_data


class FilterByUnivariateQuantile(AbstractTransformer):
    def __init__(self, features_to_filter, multiplier=2.0, verbose=False):
        """

        features_to_filter: list of str
            cols to perform filtering on

        """
        self.features_to_filter = features_to_filter
        self.multiplier = multiplier
        self.verbose = verbose


    def fit(self, data):
        """fit data, here just passing

        Args:
            data (object): some data

        """
        pass


    def remove_univariate_outliers(self, df, xvarlist, multiplier=2.0, verbose=False):

        """
            Remove outlier, defined as 2.0x interquartile range (IQR) of a single variable.

        Parameters
        ----------
            xvarlist: list of str
                colnames
        """

        df = df.dropna(inplace=False)
        df = df.reset_index(drop=True, inplace=False)
        outliers_lst = []

        for feature in xvarlist:
            Q3 = np.percentile(df[feature], 75)
            Q1 = np.percentile(df[feature], 25)
            step = multiplier * (Q3 - Q1)
            outliers_rows = df.loc[~((df[feature] >= Q1 - step) & (df[feature] <= Q3 + step)), :]
            outliers_lst.append(list(outliers_rows.index))
            if verbose:
                print(feature)
                print(Q1, Q3)
                print(step)
                print(len(outliers_rows))
        outliers = list(itertools.chain.from_iterable(outliers_lst))
        uniq_outliers = list(set(outliers))

        if verbose:
            print('Length of outliers list:\n', len(uniq_outliers))
        good_data = df.drop(df.index[uniq_outliers]).reset_index(drop=True)
        return good_data


    def transform(self, data):
        """

        Args:
            data (dict): dictionary with dataframes from all readers

        Returns:
            dataframe with filtered data

        Raises:
            Exception if not a pandas dataframe, operation not supported, or column name not recognized

        """
        if not isinstance(data, pd.DataFrame):
            raise Exception("Data is not a pandas DataFrame")

        if len(self.features_to_filter) == 0:
            logging.info("no filters found; returning combined_data as filtered_data")
            return data

        else:

            filtered_data = self.remove_univariate_outliers(data, self.features_to_filter, multiplier=self.multiplier, verbose=self.verbose)

            logging.info("Filtered out %d rows" % (len(data) - len(filtered_data)))

        return filtered_data


