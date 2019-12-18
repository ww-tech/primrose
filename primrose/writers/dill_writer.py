"""write some data to a local file, serialized with dill

Author(s):
    Mike Skarlinski (michael.skarlinski@weightwatchers.com)

"""
import os
import logging
import dill
import warnings

from primrose.writers.abstract_file_writer import AbstractFileWriter
from primrose.data_object import DataObjectResponseType


class DillWriter(AbstractFileWriter):
    ''' write some specified data object to a dill file '''
    warnings.warn('Use Serializer instead. DillWriter will be deprecated in a future release.', DeprecationWarning)

    def run(self, data_object):
        """serialize some data to a local filesystem using Dill

        Note:
            this will write object under config['key'] in data_object

        Args:
            data_object (DataObject)

        Returns:
            (tuple): tuple containing:

                data_object (DataObject): instance of DataObject

                terminate (bool): terminate the DAG?

        """
        filename = self.node_config['dir'] + os.path.sep + self.node_config['filename']
        data_key = self.node_config['key']

        logging.info("Saving {} data to {}".format(data_key, filename))

        data_to_write = data_object.get_upstream_data(self.instance_name, pop_data=False, rtype=DataObjectResponseType.KEY_VALUE.value)

        if data_key not in data_to_write:
            raise Exception("Key {} not found inside data_key object".format(data_key))

        with open(filename, 'wb') as f:
            dill.dump(data_to_write[data_key], f)

        terminate = False

        return data_object, terminate
