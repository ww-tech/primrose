"""Singleton Factory where one can register objects/classes for instantiation

Author(s):
    Michael Skarlinski (michael.skarlinski@weightwatchers.com)

    Carl Anderson (carl.anderson@weightwatchers.com)

"""

import logging
import inspect
import sys
from primrose.base.node import AbstractNode

from primrose.readers.csv_reader import CsvReader
from primrose.readers.gcs_dill_reader import GcsDillReader
from primrose.readers.dill_reader import DillReader
from primrose.readers.deserializer import Deserializer, GcsDeserializer
from primrose.readers.sklearn_dataset_reader import SklearnDatasetReader
from primrose.readers.mysql_reader import MySQLReader
from primrose.readers.sqlite_reader import SQLiteReader
from primrose.readers.postgres_reader import PostgresReader

from primrose.conditionalpath.simple_switch import SimpleSwitch

from primrose.pipelines.dataframe_joiner import DataFrameJoiner
from primrose.pipelines.encode_train_test_split import EncodeTrainTestSplit
from primrose.pipelines.train_test_split import TrainTestSplit
from primrose.pipelines.sklearn_preprocessing_pipeline import SklearnPreprocessingPipeline

from primrose.models.sklearn_classifier_model import SklearnClassifierModel
from primrose.models.sklearn_cluster_model import SklearnClusterModel
from primrose.models.sklearn_regression_model import SklearnRegressionModel

from primrose.notifications.success_notification import ClientNotification

from primrose.writers.csv_writer import CsvWriter
from primrose.writers.file_writer import FileWriter
from primrose.writers.dill_writer import DillWriter
from primrose.writers.serializer import Serializer
from primrose.writers.s3_writer import S3Writer

from primrose.dataviz.cluster_plotter import ClusterPlotter

from primrose.cleanup.logging_success import LoggingSuccess
from primrose.readers.r_reader import RReader

class NodeFactory:
    """Singleton Factory where one can register objects/classes for instantiation"""
    #https://python-3-patterns-idioms-test.readthedocs.io/en/latest/Singleton.html

    instance = None

    CLASS_KEY = "class"
    CLASS_PREFIX = "class_prefix"

    def __init__(self):
        """instantiate the factory but as a singleton. The guard raails are here
        """
        # where the magic happens, only one instance allowed:
        if not NodeFactory.instance:
            NodeFactory.instance = NodeFactory.__HiddenFactory()

    def __getattr__(self, name):
        """getattr with instance name

        Args:
            name (str): name of the instance

        Returns:
            gettattr

        """
        return getattr(self.instance, name)

    class __HiddenFactory:
        """actual factory where registry and instantiation happens"""

        def __init__(self):
            """instantiate the HiddenFactory"""
            self.name_dict = {'CsvReader': CsvReader,
                'SQLiteReader': SQLiteReader,
                'MySQLReader': MySQLReader,
                'PostgresReader': PostgresReader,
                'GcsDillReader': GcsDillReader,
                'DillReader': DillReader,
                'Deserializer': Deserializer,
                'GcsDeserializer': GcsDeserializer,
                'DataFrameJoiner': DataFrameJoiner,
                'EncodeTrainTestSplit': EncodeTrainTestSplit,
                'TrainTestSplit': TrainTestSplit,
                'CsvWriter': CsvWriter,
                'FileWriter': FileWriter,
                'DillWriter': DillWriter,
                'Serializer': Serializer,
                'S3Writer': S3Writer,
                'SklearnClassifierModel': SklearnClassifierModel,
                'LoggingSuccess': LoggingSuccess,
                'ClusterPlotter': ClusterPlotter,
                'SklearnClusterModel': SklearnClusterModel,
                'SklearnPreprocessingPipeline': SklearnPreprocessingPipeline,
                'SklearnDatasetReader': SklearnDatasetReader,
                'SklearnRegressionModel': SklearnRegressionModel,
                'SimpleSwitch': SimpleSwitch,
                'ClientNotification': ClientNotification,
                'RReader': RReader}

        def register(self, key, class_obj, raise_on_overwrite=False):
            """Registering class_obj with key

            Args:
                key (str): key such as class name, e.g. 'BQWriter'
                class_obj (class obj), e.g. BQWriter

            Returns:
                nothing. Side effect is to register the class

            """
            if raise_on_overwrite and key in self.name_dict:
                raise Exception("Node already exist with the key " + key)

            if not (inspect.isclass(class_obj) and issubclass(class_obj, AbstractNode)):
                raise Exception("NodeFactory can only register classes that implement AbstractNode")

            if key is None:
                try:
                    key = class_obj.__name__
                except AttributeError as e:
                    raise Exception(f"Cannot register {class_obj}, no __name__ attribute found. Please explicity specify a name when registering this class.")

            self.name_dict[key] = class_obj
            logging.debug("Registered %s : %s" % (key, class_obj))

        def is_registered(self, class_key):
            """is this class registered?

            Args:
                class_key (str): key used to register class

            Returns:
                determination (boolean) of whether this is already register

            """
            return class_key in self.name_dict

        def unregister(self, key):
            """unregister an entry

            Arguments:
                key (str): key to unregister

            Returns:
                nothing. Side effect is that the object is unregistered

            """
            if key in self.name_dict:
                del self.name_dict[key]
                logging.info("Unregistered %s", key)
            else:
                raise Exception("Key not found %s" % key)

        def instantiate(self, class_name, configuration, instance_name):
            '''instantiate instances of node, given name key of node (typically a classname but is not required to be)

            Args:
                class_name (str): name key for the class
                configuration (Configuration): Configuration instance

            Returns:
                instance (Traverser): instance of a traverser

            '''
            return self.name_dict[class_name](configuration, instance_name)

        def valid_configuration(self, instance_class, configuration_dict):
            """Check for all the correct configuration keys via the necessary_config method

                For instance, if a CSVWriter has a required filename key,
                check that it exists as a key in the configuration

            Args:
                instance_class: class to validate
                configuration_dict: configuration dictionary from a Configuration object

            Returns:
                True for valid configuration

            Raises:
                Exception if keys not valid

            """
            configuration_requirements = instance_class.necessary_config(configuration_dict)

            if all([key in configuration_dict for key in configuration_requirements]):
                return True
            else:
                raise Exception('Configuration missing necessary keys for '
                                '{}, required keys: {}\n\n{}'.format(instance_class,
                                                                    configuration_requirements,
                                                                    instance_class.necessary_config.__doc__))

        def register_module_classes(self, module):
            """Given some module, find and register any AbstractNode classes it finds in that module

            Args:
                module (str): e.g. __name__, 'src'

            Returns:
                nothing. Side effect is to register the Node classes found

            """
            for name, obj in inspect.getmembers(sys.modules[module]):
                if inspect.isclass(obj) and issubclass(obj, AbstractNode):
                    logging.info("Discovered class " + name + " (" + str(obj) + ")")
                    self.register(name, obj)