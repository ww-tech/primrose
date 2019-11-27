"""Simple conditional path node that will travel down one destination and prune the rest

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
from primrose.base.conditional_path_node import AbstractConditionalPath

class SimpleSwitch(AbstractConditionalPath):

    @staticmethod
    def necessary_config(node_config): 
        """Return a list of necessary configuration keys within the implementation

        Args:
            node_config (dict): set of parameters / attributes for the node

        Returns:
            set of keys necessary to run implementation

        """
        return set(['path_to_travel'])

    def destinations_to_prune(self):
        """List destinations that the node wishes to be pruned from the DAG
        
        Returns:
            destinations (list): list of destinations

        """
        destinations = self.node_config['destinations']
        destinations.remove(self.node_config['path_to_travel'])
        return destinations

    def run(self, data_object):
        """Run this node. Here, do nothing

        Args:
            data_object (DataObject): DataObject instance

        Returns:
            (tuple): tuple containing:

                data_object (DataObject): instance of DataObject

                terminate (bool): terminate the DAG?

        """
        return data_object, False