"""write some data to a local file.

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
import os
import logging
from primrose.writers.abstract_file_writer import AbstractFileWriter
from primrose.data_object import DataObjectResponseType

class FileWriter(AbstractFileWriter):
    ''' write some data object to a local file '''

    def run(self, data_object):
        """write some data to a local file

        Note:
            this will write object under config['key'] in data_object

        Returns:
            (tuple): tuple containing:

                data_object (DataObject): instance of DataObject

                terminate (bool): terminate the DAG?

        """
        filename = self.node_config['dir'] + os.path.sep + self.node_config['filename']
        data_key = self.node_config['key']
        logging.info("Saving %s data to %s", data_key, filename)
        data_to_write = data_object.get_upstream_data(self.instance_name, pop_data=False, rtype=DataObjectResponseType.VALUE.value)

        with open(filename, 'w') as f:
            f.write(data_to_write)
        f.close()

        terminate = False
        return data_object, terminate
