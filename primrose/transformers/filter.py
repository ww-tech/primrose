"""Transform data by filtering in data using filtering operations 

Author(s):
    Michael Skarlinski (michael.skarlinski@weightwatchers.com)

    Carl Anderson (carl.anderson@weightwatchers.com)

"""
import operator
import logging
import numpy as np
import pandas as pd

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
            ">=": operator.ge
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

            logging.info('Filtered out %d rows' % (len(data) - len(filtered_data)))

        return filtered_data
