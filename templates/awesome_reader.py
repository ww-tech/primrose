"""Module with AbstractNode implementation, Example for primrose user modification

Author(s):
    Mike Skarlinski (michael.skarlinski@ww.com)

"""
from primrose.base.reader import AbstractReader


class AwesomeReader(AbstractReader):
    """(EXAMPLE READER) Read input directly from the configuration file"""

    @staticmethod
    def necessary_config(node_config):
        """Returns the necessary configuration keys for the AwesomeReader object

        Put your required configuration keys here

        Args:
            node_config (dict): set of parameters / attributes for the node

        Note:
            filename: name of the file

        Returns:
            set of necessary keys for the AwesomeReader object

        """
        return set(['data_to_read'])

    def run(self, data_object):
        """Read data from node_config

        Returns:
            data_object (DataObject): DataObject instance
            terminate (bool): should we terminate the DAG? true or false

        """

        # Example showing data being created directly from the config file
        # any valid python can be used here for reading data into the data_object
        # examples of other readers can be found in primrose/readers/*
        # -------------------------------
        data = self.node_config['data_to_read']
        print('Reading in data!: {}'.format(data))
        # -------------------------------
        # add data into data object for use downstream
        data_object.add(self, data)
        terminate = data is None
        return data_object, terminate
