"""Transformer that wraps around pandas.Series.str methods

Author(s):
    Brian Graham (brian.graham@weightwatchers.com)

"""

import pandas as pd
import logging
from primrose.base.transformer import AbstractTransformer

class StringTransformer(AbstractTransformer):
    """Transforms Series of strings in a Series or DataFrame."""
    def __init__(self,method,columns,*args,**kwargs):
        """

        Args:
            method (str): pandas.Series.str method
            columns (str or list): single column name str or list of columns to operate on
            *args: args for given string method
            **kwargs: kwargs for given string method

        """
        self.method = method
        self.columns = columns
        self.args = args
        self.kwargs = kwargs

    def fit(self, df):
        """fit data, here just passing.

        Args:
            data (object): some data

        """
        pass

    def transform(self, df):
        """Applies string operation on dataframe columns.

        Args:
            df (pd.DataFrame): pandas dataframe

        Returns:
            df (pd.DataFrame): pandas dataframe

        """
        if isinstance(self.columns,str):
            df[self.columns] = self._execute_str_method(df[self.columns])
        elif isinstance(self.columns,list):
            for col in self.columns:
                df[col] = self._execute_str_method(df[col])
        else:
            logging.info('Column not passed as string or list of columns, returning original dataframe.')
        return df

    def _execute_str_method(self,series):
        """Executes string method on pandas series.

        Args:
            series (pd.Series): pandas series to transform

        Returns:
            series (pd.Series): transformed pandas series

        """
        method = getattr(series.str, self.method)
        return method(*self.args, **self.kwargs)
