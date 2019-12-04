"""Module with AbstractNode implementation, able to read from CSV

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""

import logging
import pandas as pd
from primrose.base.reader import AbstractReader

class CsvReader(AbstractReader):
    """Reads CSV file into a pandas dataframe"""

    @staticmethod
    def necessary_config(node_config):
        """Returns the necessary configuration keys for the CsvReader object

        Args:
            node_config (dict): set of parameters / attributes for the node

        Note:
            filename: name of the file

        Returns: 
            set of necessary keys for the CsvReader object

        """
        return set(['filename'])

    def get_optional_config(self):
        """Optionally get kwargs to pass to pandas csv reader.

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
            } \

        """
        if 'kwargs' in self.node_config:
            return self.node_config['kwargs']
        else:
            return {}

    def run(self, data_object):
        """Read CSV to a pandas dataframe

        Returns:
            data_object (DataObject): DataObject instance
            terminate (bool): should we terminate the DAG? true or false

        """
        filename = self.node_config['filename']
        logging.info('Reading {} from CSV'.format(filename))
        kwargs = self.get_optional_config()
        df = pd.read_csv(filename, **kwargs)
        data_object.add(self, df)
        terminate = df.empty
        return data_object, terminate
