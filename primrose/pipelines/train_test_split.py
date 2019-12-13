"""Module to run train test splits

Author(s):
    Mike Skarlinski (michael.skarlinski@weightwatchers.com)
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
import logging
import pandas as pd
from sklearn.model_selection import train_test_split

from primrose.base.pipeline import AbstractPipeline, PipelineModeType
from primrose.data_object import DataObject, DataObjectResponseType


class TrainTestSplit(AbstractPipeline):
    """Parent pipeline to split into training / testing data, and run a transform

    Note:
        This class will split data into testing and training portions, then write the objects to the data_object
        if this is the only the pipeline operation needed, then you can use this class directly as a pipeline.
        Otherwise, make a child class and write an init_pipeline method to perform operations on your data.

        This class also handles writing your transformer_sequence into the data_object, so there's no need
        to write in child classes.

    """

    @staticmethod
    def necessary_config(node_config):
        """Return the necessary configuration keys for the DataFrameJoiner object

        Args:
            node_config (dict): set of parameters / attributes for the node

        Note:
            target_variable (string): column name which holds the target variable \
            training_fraction (float): 0->1 float for the fraction of data rows to be used for training \
            seed (int): random number to control the stochastic row selection \

        Returns:
            set of keys

        """
        return set(['training_fraction', 'seed'])

    def features(self, data):
        """Use user-specified features if available, otherwise use all non-target columns
        
        Args:
            data (DataFrame): pandas dataframe

        Returns:
            lsit of feature names
        
        """
        if 'features' in self.node_config:
            return self.node_config['features']

        else:
            if 'target_variable' in self.node_config:
                return [f for f in data.columns if f != self.node_config['target_variable']]
            else:
                return data.columns

    def _train_test_split(self, data):
        """Split data into test/train sets
        
        Returns:
            train_data_to_transform (DataFrame)
            
            test_data_to_transform (DataFrame)
        
        """
        logging.info("Splitting data into testing and training sets.")

        test_size = (1.0 - float(self.node_config['training_fraction']))

        if test_size == 0:
            train_data_to_transform = data
            test_data_to_transform = pd.DataFrame()
        
        else:
            if 'target_variable' in self.node_config:
                data_train, data_test, target_train, target_test = train_test_split(
                    data[self.features(data)],
                    data[self.node_config['target_variable']],
                    test_size=(1.0 - float(self.node_config['training_fraction'])),
                    random_state=self.node_config['seed'])

                # re-merge training and target data into a single dataframe for transforming
                train_data_to_transform = pd.concat([data_train, target_train], axis=1)
                test_data_to_transform = pd.concat([data_test, target_test], axis=1)

            else:
                data_train, data_test = train_test_split(
                    data[self.features(data)],
                    test_size=(1.0 - float(self.node_config['training_fraction'])),
                    random_state=self.node_config['seed'])

                train_data_to_transform = data_train
                test_data_to_transform = data_test

        logging.info('Training data rows: {}, Testing data rows: {}'.format(len(train_data_to_transform),
                                                                            len(test_data_to_transform)))

        return train_data_to_transform, test_data_to_transform

    def final_data_object_additions(self, data_object):
        """DataObject: Template method to be overloaded in child classes

        Note:
            Allows for child class TransformerSequence objects to be added into the data_object

        Args:
            data_object (DataObject): instance of DataObject

        Returns:
            data_object (DataObject): instance of DataObject

        """
        return data_object

    @staticmethod
    def _concatenate_upstream_dataframes(data):
        """Concatenate multiple upstream data sources into a single dataframe

        Note:

            Upstream objects must add dataframes with the same schema, otherwise mismatched columns will have
            all NULL values in the mutual exclusive indexes

        Args:
            data (list): list of dicts keyed to instances

        Returns:
            dataframe (DataFrame): concatenated dataframes from the data list

        """

        dataframes_to_join = []

        for source in data:

            for key in data[source]:

                if isinstance(data[source][key], pd.DataFrame):

                    if dataframes_to_join:
                        if set(data[source][key].columns) != set(dataframes_to_join[0].columns):
                            logging.warning('Concatenated dataframe schemas do not match, subbed with NULL values.')

                    dataframes_to_join.append(data[source][key])

        return pd.concat(dataframes_to_join)

    def fit_transform(self, data_object):
        """Split data into testing and training sets, then applies the categorical transform to each

        Args:
            data_object (DataObject): instance of DataObject

        Returns:
            data_object (DataObject): instance of DataObject

        """

        # we're can expect multiple objects from a reader, so we need to concatenate
        data_list = data_object.get_upstream_data(self.instance_name,
                                            pop_data=False,
                                            rtype=DataObjectResponseType.INSTANCE_KEY_VALUE.value)

        data = self._concatenate_upstream_dataframes(data_list)

        train_data, test_data = self._train_test_split(data)

        if not train_data.empty:
            train_data = self.execute_pipeline(train_data, PipelineModeType.FIT_TRANSFORM)
            data_object.add(self, train_data[self.features(train_data)], key='data_train', overwrite=False)

            if 'target_variable' in self.node_config:
                data_object.add(self, train_data[self.node_config['target_variable']], key='target_train',
                            overwrite=False)

        # run the pre-trained pipeline on the testing data
        if not test_data.empty:
            # run the pipeline in Transform mode since we've already fit the pipeline with training data
            test_data = self.execute_pipeline(test_data, PipelineModeType.TRANSFORM)
            self.data = test_data  # assign the data to the testing data if available
            data_object.add(self, test_data[self.features(train_data)], key='data_test', overwrite=False)

            if 'target_variable' in self.node_config:
                data_object.add(self, test_data[self.node_config['target_variable']], key='target_test',
                            overwrite=False)

        # save pipeline for writing later
        data_object.add(self, self.transformer_sequence, key='transformer_sequence', overwrite=False)
        data_object = self.final_data_object_additions(data_object)

        return data_object

    def transform(self, data_object):
        """Transform the data into label encoded data using the pre-trained transformer sequence

        Args:
            data_object (DataObject): instance of DataObject

        Returns:
            data_object (DataObject): instance of DataObject

        """
        # we're can expect multiple objects from a reader, so we need to concatenate
        data_list = data_object.get_upstream_data(self.instance_name,
                                                pop_data=False,
                                                rtype=DataObjectResponseType.INSTANCE_KEY_VALUE.value)

        data = self._concatenate_upstream_dataframes(data_list)

        data = self.execute_pipeline(data, PipelineModeType.TRANSFORM)

        self.data = data  # keep the data for use in the final_data_object_additions method

        data_object.add(self, data[self.features(data)], key='data_test', overwrite=False)

        if 'target_variable' in self.node_config:
            data_object.add(self, data[self.node_config['target_variable']], key='target_test', overwrite=False)

        data_object = self.final_data_object_additions(data_object)

        return data_object
