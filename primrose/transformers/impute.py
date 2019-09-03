"""Custom imputer to replace values within pandas dataframe columns with config-specified imputation schemes

Author(s):
    Reka Daniel-Weiner (reka.danielweiner@weightwatchers.com)

    Carl Anderson (carl.anderson@weightwatchers.com)

"""
import pandas as pd
import logging
from primrose.base.transformer import AbstractTransformer

class ColumnSpecificImpute(AbstractTransformer):
    """Transform config specified columns NULL values into zero, mean, median, mode, inf or negative inf"""

    def __init__(self, columns_to_zero, columns_to_mean, columns_to_median, columns_to_mode,
                columns_to_infinity, columns_to_neg_infinity):
        """Transform config specified columns NULL values into zero, mean, median, mode, inf or negative inf

        Args:
            columns_to_zero (list): list of columns to impute zeros
            columns_to_mean (list): list of columns to impute means
            columns_to_median (list): list of columns to impute medians
            columns_to_mode (list): list of columns to impute modes
            columns_to_infinity (list): list of columns to impute to large value (999999999)
            columns_to_neg_infinity (list): list of columns to impute to large negative value (-999999999)

        Returns:
            nothing. Side effect to set list of columns to set to mean, 0, median etc

        """
        self.columns_to_zero = columns_to_zero
        self.columns_to_mean = columns_to_mean
        self.columns_to_median = columns_to_median
        self.columns_to_mode = columns_to_mode
        self.columns_to_infinity = columns_to_infinity
        self.columns_to_neg_infinity = columns_to_neg_infinity
        self.encoder = None

    def fit(self, data):
        """Fit encoder imputation values to dataframe metrics
        Create a dictionary of column name to imputed values. These values might be straight constants or might
        be a function of the complete column's value, such as mode or median

        Args:
            data (pandas data frame): a data frame

        Returns:
            Nothing. Updates internal dictionary of column name to imputed value

        Raises:
            Exception if a column appears in multiple lists, or if column not recognized

        """

        self.encoder = {}
        columns_so_far = set()

        for cols in [self.columns_to_zero,
                    self.columns_to_mean,
                    self.columns_to_median,
                    self.columns_to_mode,
                    self.columns_to_infinity,
                    self.columns_to_neg_infinity]:

            # does column exist?
            for col in cols:
                if not col in data.columns:
                    raise Exception("Unrecognized impute column '" + str(col) + "'")

            # is it in some previous list?
            in_common = columns_so_far.intersection(set(cols))
            if in_common:
                raise Exception("There are columns in multiple lists "+ str(in_common))
            columns_so_far = columns_so_far.union(set(cols))

        logging.info('Specifying columns to impute 0')
        [self.encoder.setdefault(col, 0) for col in self.columns_to_zero]

        logging.info('Specifying columns to impute median')
        [self.encoder.setdefault(col, data[col].median()) for col in self.columns_to_median]

        logging.info('Specifying columns to impute mean')
        [self.encoder.setdefault(col, data[col].mean()) for col in self.columns_to_mean]

        logging.info('Specifying columns to impute large values')
        [self.encoder.setdefault(col, 999999999.) for col in self.columns_to_infinity]

        logging.info('Specifying columns to impute large negative values')
        [self.encoder.setdefault(col, -999999999.) for col in self.columns_to_neg_infinity]

        logging.info('Specifying columns to impute mode')
        for col in self.columns_to_mode:
            logging.info('imputing {}'.format(col))
            try:
                if pd.notnull(data[col].mode().values[0]):
                    col_mode = data[col].mode().values[0]
                else:
                    col_mode = 0
            except Exception:
                col_mode = 0

            self.encoder[col] = col_mode

    def transform(self, data):
        """Impute columns in data according to the imputations fit by self.fit

        Args:
            data (dataframe)

        Returns:
            data (dataframe)

        Raises:
            Exception if train is not called before transfoorm

        """

        if self.encoder is None:
            raise Exception('ColumnSpecificImpute must train imputations with fit before calling transform.')

        for col, val in self.encoder.items():
            data[col] = data[col].fillna(val)

        return data
