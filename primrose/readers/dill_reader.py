"""Module with AbstractNode implementation, able to read from local dill files

Author(s):
    Mike Skarlinski (michael.skarlinski@weightwatchers.com)

"""

import logging
import dill
import warnings

from primrose.base.reader import AbstractReader


class DillReader(AbstractReader):
    """Read a file from Gcs and un-dills it into memory"""

    DATA_KEY = 'reader_data'
    warnings.warn('Use Deserializer instead. DillReader will be deprecated in a future release.', DeprecationWarning)
    @staticmethod
    def necessary_config(node_config):
        """Returns the necessary configuration keys for the DillReader object

        Args:
            node_config (dict): set of parameters / attributes for the node

        Note:
            filename: local filename to be de-serialized

        Returns:
            set of necessary keys for the DillReader object

        """
        return set(['filename'])

    def run(self, data_object):
        """Read dill object(s) from local filesystem

        Args:
            data_object: DataObject instance

        Returns:
            (tuple): tuple containing:

                data_object (DataObject): instance of DataObject

                terminate (bool): terminate the DAG?

        """
        logging.info('Reading {} from local filesystem'.format(self.node_config['filename']))

        object = dill.load(open(self.node_config['filename'], 'rb'))

        data_object.add(self, object, key=DillReader.DATA_KEY)

        terminate = False

        return data_object, terminate
