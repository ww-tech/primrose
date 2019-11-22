"""Module with AbstractNode implementation, able to read from GCS

Author(s):
    Mike Skarlinski (michael.skarlinski@weightwatchers.com)

"""

import logging
import dill
from google.cloud import storage
import warnings

from primrose.base.reader import AbstractReader


class GcsDillReader(AbstractReader):
    """Read a file from Gcs and un-dills it into memory"""

    DATA_KEY = 'reader_data'
    warnings.warn('Use Deserializer instead. GcsDillReader will be deprecated in a future release.', DeprecationWarning)

    @staticmethod
    def necessary_config(node_config):
        """Returns the necessary configuration keys for the GcsDillReader object

        Args:
            node_config (dict): set of parameters / attributes for the node

        Note:
            bucket_name: name of the GCS bucket
            blob_name: name of the blob

        Returns:
            set of necessary keys for the CsvReader object

        """
        return set(['bucket_name', 'blob_name'])

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
        """Read dill object(s) from GCS bucket which contain the blob_name

        Args:
            data_object: DataObject instance

        Returns:
            (tuple): tuple containing:

                data_object (DataObject): instance of DataObject

                terminate (bool): terminate the DAG?

        """
        logging.info('Reading {} from GCS'.format(self.node_config['blob_name']))
        objects = [dill.loads(obj) for obj in self.download_blobs_as_strings()]

        terminate = len(objects) == 0

        if len(objects) == 1:
            data_object.add(self, objects[0], key=GcsDillReader.DATA_KEY)
        else:
            data_object.add(self, objects, key=GcsDillReader.DATA_KEY)

        return data_object, terminate
