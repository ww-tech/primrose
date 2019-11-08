"""Module with AbstractNode implementation, able to read from local and gcs dill and pickle files

Author(s):
    Mike Skarlinski (michael.skarlinski@weightwatchers.com)
    Brian Graham (brian.graham@weightwatchers.com)

"""

import dill
import logging
import pickle
from google.cloud import storage
from primrose.base.reader import AbstractReader



class Deserializer(AbstractReader):
    """Read a local file and de-serialize it into memory."""

    DATA_KEY = 'reader_data'

    @staticmethod
    def necessary_config(node_config):
        """Returns the necessary configuration keys for the Deserializer object

        Args:
            node_config (dict): set of parameters / attributes for the node

        Note:
            filename (str): local filename to be de-serialized
            deserializer (str): 'dill' or 'pickle'

        Returns:
            set of necessary keys for the Deserializer object

        """
        return set(['filename','deserializer'])

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

        if self.node_config['deserializer'] == 'dill':
            object = dill.load(open(self.node_config['filename'], 'rb'))
        elif self.node_config['deserializer'] == 'pickle':
            object = pickle.load(open(self.node_config['filename'], 'rb'))
        else:
            logging.warning(f"{self.node_config['deserializer']} deserializer not supported.")
            raise Exception(f"Unsupported Deserializer: {self.node_config['deserializer']}")

        data_object.add(self, object, key=Deserializer.DATA_KEY)

        terminate = False

        return data_object, terminate

class GcsDeserializer(AbstractReader):
    """Read a file from GCS and de-serialize it into memory."""

    DATA_KEY = 'reader_data'

    @staticmethod
    def necessary_config(node_config):
        """Returns the necessary configuration keys for the GcsDeserializer object

        Args:
            node_config (dict): set of parametera / attributes for the node

        Note:
            bucket_name: name of the GCS bucket
            blob_name: name of the blob
            deserializer (str): 'dill' or 'pickle'

        Returns:
            set of necessary keys for the GcsDeserializer object

        """
        return set(['bucket_name', 'blob_name','deserializer'])

    def download_blobs_as_strings(self):
        """Downloads a blob from the bucket contining the user specified blob_name

        Returns:
            list of strings

        """

        if 'gcs_project' in self.node_config:
            storage_client = storage.Client(project=self.node_config['gcs_project'])
        else:
            storage_client = storage.Client()

        bucket = storage_client.get_bucket(self.node_config['bucket_name'])

        if 'prefix' in self.node_config:
            prefix = self.node_config['prefix']
        else:
            prefix = None

        blob_list = bucket.list_blobs(prefix=prefix)

        valid_blobs = [blob for blob in blob_list if self.node_config['blob_name'] in blob.name]

        if len(valid_blobs) == 0:
            raise Exception('{} not found in GCS bucket.'.format(self.node_config['blob_name']))

        return [blob.download_as_string() for blob in valid_blobs]

    def run(self, data_object):
        """Read serialized object(s) from GCS bucket which contain the blob_name

        Args:
            data_object: DataObject instance

        Returns:
            (tuple): tuple containing:

                data_object (DataObject): instance of DataObject

                terminate (bool): terminate the DAG?

        """
        logging.info('Reading {} from GCS'.format(self.node_config['blob_name']))

        if self.node_config['deserializer'] == 'dill':
            objects = [dill.loads(obj) for obj in self.download_blobs_as_strings()]
        elif self.node_config['deserializer'] == 'pickle':
            objects = [pickle.loads(obj) for obj in self.download_blobs_as_strings()]
        else:
            logging.warning(f"{self.node_config['deserializer']} deserializer not supported.")
            raise Exception(f"Unsupported Deserializer: {self.node_config['deserializer']}")

        terminate = len(objects) == 0

        if len(objects) == 1:
            data_object.add(self, objects[0], key=GcsDeserializer.DATA_KEY)
        else:
            data_object.add(self, objects, key=GcsDeserializer.DATA_KEY)

        return data_object, terminate