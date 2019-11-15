"""write some data to a local or gcs file, serialized with dill or pickle.

Author(s):
    Mike Skarlinski (michael.skarlinski@weightwatchers.com)
    Brian Graham (brian.graham@weightwatchers.com)

"""
import dill
import logging
import os
import pickle
from primrose.data_object import DataObjectResponseType
from primrose.writers.abstract_file_writer import AbstractFileWriter


class Serializer(AbstractFileWriter):
    """Serialize some object to a local file."""

    @staticmethod
    def necessary_config(node_config):
        """Returns the necessary configuration keys for the Serializer object:

        Args:
            node_config (dict): set of parameters / attributes for the node

        Note:
            serializer (str): 'dill' or 'pickle'

        Returns:
            set of necessary configuration keys

        """

        config_params = set(['serializer'])
        return config_params.union(AbstractFileWriter.necessary_config(node_config))

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

            if self.node_config['serializer'] == 'dill':
                dill.dump(data_to_write[data_key], f)
            elif self.node_config['serializer'] == 'pickle':
                pickle.dump(data_to_write[data_key], f)
            else:
                logging.warning(f"{self.node_config['serializer']} serializer not supported.")
                raise Exception(f"Unsupported Serializer: {self.node_config['serializer']}")

        terminate = False

        return data_object, terminate
