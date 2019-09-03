"""Custom combiner object which works via pandas left merges

Author(s):
    Mike Skarlinski (michael.skarlinski@weightwatchers.com)

"""

import pandas as pd
import logging
from primrose.base.transformer import AbstractTransformer


def left_merge_dataframe_on_validated_join_keys(left_df, right_df, join_keys):
    """Merge two dataframes together or return just the left_df if the right df is None

    Args:
        left_df: valid dataframe with join keys of matching data type (will be validated)
        right_df: None or valid dataframe with join keys of matching data type (will be validated)
        join_keys: list of keys to be validated on datatype and existance in left/right

    Returns:
        joined dataframe object
    """
    if right_df is None:

        initial_data_length = len(left_df)
        logging.info("Initial data length: {}".format(initial_data_length))

        return left_df

    else:

        initial_data_length = len(left_df)

        for j in join_keys:

            if not j in left_df.columns:
                raise Exception('Join key {} not in left {}. Aborting merge.'.format(j, left_df.columns))
            elif not j in right_df.columns:
                raise Exception('Join key {} not in right {}. Aborting merge.'.format(j, right_df.columns))

            if not left_df[j].dtype == right_df[j].dtype:
                logging.info('Join key {} is not of type {}. Casting.'.format(j, right_df[j].dtype))
                try:
                    right_df[j] = right_df[j].astype(left_df[j].dtype)
                except:
                    raise Exception('Cannot cast join key {} as {}'.format(j, left_df[j].dtype))

        left_df = left_df.merge(right_df, on=join_keys, how='left')
        left_df.reset_index(inplace=True, drop=True)

        if len(left_df) > initial_data_length:
            logging.warning("Merge increased data size by {} rows.".format(
                len(left_df) - initial_data_length))

        return left_df


class LeftJoinDataCombiner(AbstractTransformer):
    """combine two dataframes doing a left join"""

    def __init__(self, join_key):
        """initialize the class

        Args:
            join_key (list): the columns to perform the left join on

        """
        self.join_key = join_key

    def fit(self, data):
        """fit the data, here doing nothing
        
        Args:
            data (list)L : list of pandas data frames

        Returns:
            nothing
        """
        pass

    def transform(self, data):
        """Applies left joins to combine dataframes from different readers

        Args:
            data (list): list of dataframes

        Returns:
            dataframe

        """
        if not isinstance(data, list):
            raise Exception("In this transformer, data needs to be a list of dataframes")

        if len(data) < 2:

            logging.warning("Combiner needs at least two reader inputs, passing unchanged data.")

            return data

        elif not isinstance(data[0], pd.DataFrame):

            raise Exception('LeftJoinDataCombiner must operate on an iterable of pandas.DataFrame objects.')

        else:

            combined_data = None

            # loop over each reader key and merge into a single combined dataframe
            for d in data:
                combined_data = d if combined_data is None else left_merge_dataframe_on_validated_join_keys(
                    combined_data, d, self.join_key)

            return combined_data

