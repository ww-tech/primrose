"""write some dataframe to a local CSV file.

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
import os
import logging
from primrose.data_object import DataObjectResponseType
from primrose.writers.abstract_file_writer import AbstractFileWriter

class CsvWriter(AbstractFileWriter):
    """write some dataframe to a local CSV file."""

    def get_optional_config(self):
        '''
        Optionally get kwargs to pass to pandas csv reader.

        Notes:
            kwargs (dict): dictionary of kwargs key-value pairs

        Example:
            "csv_reader": { \
                "class": "CsvReader", \
                "filename": "data/mydata.csv", \
                "kwargs": { \
                "header": None, \
                "sep": ":" \
                } \
            }

        '''
        if 'kwargs' in self.node_config:
            kwargs = self.node_config['kwargs']
            if 'index' not in kwargs:
                kwargs['index'] = False
            return kwargs
        else:
            return {"index": False}

    def run(self, data_object):
        """write some dataframe to a local CSV file

        Returns:
            (tuple): tuple containing:

                data_object (DataObject): instance of DataObject

                terminate (bool): terminate the DAG?

        """
        filename = self.node_config['dir'] + os.path.sep + self.node_config['filename']
        key = self.node_config['key']
        logging.info("Saving %s data to %s", key, filename)
        data_to_write = data_object.get_upstream_data(self.instance_name, pop_data=False, rtype=DataObjectResponseType.KEY_VALUE.value)
        kwargs = self.get_optional_config()
        data_to_write[key].to_csv(filename, **kwargs)
        terminate = False
        return data_object, terminate

