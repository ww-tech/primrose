"""write some data to a S3 bucket, via a local file.

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
import os
import uuid
import logging
import boto3

from primrose.base.writer import AbstractWriter
from primrose.data_object import DataObjectResponseType


class S3Writer(AbstractWriter):
    """a writer that writes a dataframe to disk and from there to S3 bucket"""

    @staticmethod
    def necessary_config(node_config):
        """Necessary keys for S3Writer object

        Args:
            node_config (dict): set of parameters / attributes for the node

        Args:
            node_config (dict): set of parameters / attributes for the node

        Note:
            dir: directory to write, then upload file
            bucket_name: s3 bucket where the file will be written
            bucket_filename: filename for the s3 bucket

        Returns:
            set of necessary config fields

        """
        return set(["dir", "bucket_name", "bucket_filename"])

    def _write_locally(self, data_object):
        """write data to local file

        Args:
            data_object (DataObject): instance of DataObject

        Note:
            generates unique filename using uuid

        Returns:
            filename (str)

        """
        # save dataframe to CSV file first

        unique_filename = str(uuid.uuid4())
        filename = self.node_config['dir'] + os.path.sep + unique_filename
        key = self.node_config['key']
        logging.info("Saving %s data to %s", key, filename)

        data_to_write = data_object.get_upstream_data(self.instance_name, pop_data=False, rtype=DataObjectResponseType.KEY_VALUE.value)
        assert key in data_to_write
        data_to_write[key].to_csv(filename, index=False)

        assert os.path.exists(filename)
        return  filename

    def run(self, data_object):
        """write some data to a local CSV file then from there to S3 bucket

        Returns:
            (tuple): tuple containing:

                data_object (DataObject): instance of DataObject

                terminate (bool): terminate the DAG?

        """
        filename = self._write_locally(data_object)

        # Create an S3 client
        # Boto3 will check these environment variables for credentials:
        # AWS_ACCESS_KEY_ID
        # AWS_SECRET_ACCESS_KEY
        s3 = boto3.client('s3')

        # Uploads the given file using a managed uploader, which will split up large
        # files automatically and upload parts in parallel.
        bucket_name = self.node_config['bucket_name']
        key = self.node_config['bucket_filename']
        logging.info("uploading to %s %s", bucket_name, key)
        s3.upload_file(filename, bucket_name, key)

        # TODO check for success
        # see https://stackoverflow.com/questions/41565766/make-a-unittest-for-a-function-that-uploads-a-file-to-s3-using-boto3
        #s3 = boto3.resource('s3')
        #mytname = nameOfFile
        #obj = s3.Object(bucket_name='cloud-test-cf', key=mytname)
        #response = obj.get()
        #self.assertEqual(response['ContentLength'], len(contentsOfFile))
        #remoteData = response['Body'].read()
        #self.assertEqual(remoteData, contentsOfFile)

        logging.info("upload complete!")
        terminate = False
        return data_object, terminate