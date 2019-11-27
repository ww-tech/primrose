"""Module with abstract pipeline class to specify interface needed for future pipelines

Author(s):
    Michael Skarlinski (michael.skarlinski@weightwatchers.com)

    Carl Anderson (carl.anderson@weightwatchers.com)

"""

from abc import ABC, abstractmethod
from enum import Enum
from primrose.base.node import AbstractNode
import logging
from primrose.base.transformer_sequence import TransformerSequence


class PipelineModeType(Enum):
    """Mode when performing the pipeline

    Note:
        FIT = fit data to transformer object only
        TRANSFORM = transform data only from (previously) fit transformers in a pipeline
        FIT_TRANSFORM = fit data then transform data in a pipeline

    """
    FIT = "FIT"
    FIT_TRANSFORM = "FIT_TRANSFORM"
    TRANSFORM = "TRANSFORM"

    @staticmethod
    def names():
        """list of all the names in the enum

        Returns:
            list of Enum's names

        """
        return list(map(lambda t: t.name, PipelineModeType))

    @staticmethod
    def values():
        """list of the enum's values

        Returns:
            list of (value, PipelineModeType) pairs

        """
        return list(map(lambda t: t.value, PipelineModeType))


class AbstractPipeline(AbstractNode):
    """Pipeline class should have a defined pipeline that it executes and the ability to transform raw data"""

    def __init__(self, configuration, instance_name):
        """Clean/transform/filter data in memory after de-serializing from a reader object

        Args:
            configuration: configuration class specified in src/configuration
            instance_name: name used to find this specific instance's configuration

        """
        super(AbstractPipeline, self).__init__(configuration, instance_name)
        self.transformer_sequence = None
        self.data = None

    def check_for_upstream_transformers(self, data_object):
        """Examine the upstream data_object for any TransformerSequence

        if not found, then initialize a new TransformerSequence

        Args:
            data_object (DataObject): istance of DataObject

        Returns:
            TransformerSequence

        """
        # check for a valid pipeline object inside the data object
        source_dict = data_object.get_upstream_data(self.instance_name, pop_data=False)

        # look for upstream transformer objects
        for source in source_dict:

            # NOTE: some readers in primrose have objects nested TransformerSequences one level deeper than others
            # we will allow for this scenario by checking both the first and second level keys for a TransformerSequence
            if isinstance(source_dict[source], TransformerSequence):
                logging.info('Upstream TransformerSequence found, initializing pipeline...')
                return source_dict[source]
            elif isinstance(source_dict[source], dict):
                for subkey in source_dict[source]:
                    if isinstance(source_dict[source][subkey], TransformerSequence):
                        logging.info('Upstream TransformerSequence found, initializing pipeline...')
                        return source_dict[source][subkey]

        logging.info('No upstream TransformerSequence found. Creating new TransformerSequence...')

        return self.init_pipeline()

    def run(self, data_object):
        """Run pipeline on the data object

        Args:
            data_object (DataObject): instance of DataObject

        Returns:
            (tuple): tuple containing:

                data_object (DataObject): instance of DataObject

                terminate (bool): terminate the DAG?

        """
        is_training = self.node_config.get('is_training', None)

        if is_training is None:
            logging.info('"is_training" key not found in the node_config, assuming production data')
            is_training = False
        else:
            is_training = str(is_training).lower() == "true"

        self.transformer_sequence = self.check_for_upstream_transformers(data_object)

        if is_training:
            data_object = self.fit_transform(data_object)
        else:
            data_object = self.transform(data_object)

        terminate = False
        return data_object, terminate

    @staticmethod
    def necessary_config(node_config):
        """Return a list of necessary configuration keys within the implementation

        Args:
            node_config (dict): set of parameters / attributes for the node

        After adding this list, validation automatically occurs before instantiation in the pipeline factory.

        Returns:
            set of keys necessary to run implementation

        """
        return set(['is_training']) # pragma: no cover

    def fit_transform(self, data_object):
        """Clean/transform or filter data using a pipeline of functions

        The method should also cache results and report on sizing for debugging. fit_transform must store the
        information necessary for data transformations on test data, so any encodings or model-based imputations must be
        cached in this method, to be called when the transform method is used.

        Args:
            data_object (DataObject): DataObject instance

        Returns:
            data_object (DataObject): DataObject instance

        """
        return self.transform(data_object)

    @abstractmethod
    def transform(self, data_object):
        """Clean/transform or filter data using a pipeline of functions and any cached objects from fit_transform

        The method should cache post-transform results and report on sizing for debugging. tranform uses the cached
        objects scored in the fit_tranform call to transform data. It's likely to be used for live predictions or
        test (hold-out) data.

        Args:
            data_object (DataObject): DataObject instance

        Returns:
            data_object (DataObject)

        """
        return data_object # pragma: no cover

    def init_pipeline(self):
        """Initialize the pipeline if no pipeline object is found in the upstream data objects

        Returns:
            TransformerSequence

        """
        return TransformerSequence()

    def execute_pipeline(self, input_, mode):
        """Run a TransformerSequence of functions with chained input and output data

        Args:
            input_ (object): input data (usually a pandas dataframe)
            mode: enum object for fit, transform, or fit_transform

        Returns:
            transformed data (usually a pandas dataframe) after running through all functions in the pipeline

        """
        if not self.transformer_sequence:
            raise Exception("run() must be called to extract/create a TransformerSequence")

        if mode not in PipelineModeType.names() and mode not in PipelineModeType:
            raise Exception('mode must be of type PipelineModeType Enum object.')

        for transformer in self.transformer_sequence.transformers():

            if mode == PipelineModeType.FIT:

                transformer.fit(input_)

            elif mode == PipelineModeType.TRANSFORM:

                input_ = transformer.transform(input_)

            elif mode == PipelineModeType.FIT_TRANSFORM:

                input_ = transformer.fit_transform(input_)

        return input_
