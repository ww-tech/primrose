'''Top level notion of a node in the graph

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

'''
from abc import ABC, abstractmethod

class AbstractNode(ABC):

    def __init__(self, configuration, instance_name):
        """

        Args:
            configuration (Configuration): configuration object defined in primrose/Configuration with validated inputs
                from the result of necessary_config, all inputs are described in that method

            instance_name (str): how the code knows where it is from

        """
        self.configuration = configuration
        self.instance_name = instance_name
        self.node_config = configuration.config_for_instance(instance_name)

    @staticmethod
    @abstractmethod
    def necessary_config(node_config):
        """Return a list of necessary configuration keys within the implementation

        Args:
            node_config (dict): set of parameters / attributes for the node

        After adding this list, validation automatically occurs before instantiation in the
        configuration

        Returns:
            set of keys necessary to run implementation

        """
        return set() # pragma: no cover

    @abstractmethod
    def run(self, data_object):
        """
            run the node. For a reader, that means read, for a writer that means write etc.

        Args:
            data_object (DataObject): DataObject instance

        Returns:
            (tuple): tuple containing:

                data_object (DataObject): instance of DataObject

                terminate (bool): terminate the DAG?

        """
        pass # pragma: no cover
