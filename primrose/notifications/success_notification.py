"""
    Module with AbstractSuccess implementation.
    Provides slack notification after job completion

    author(s): Parul Laul (parul.laul@ww.com)

"""

import importlib
import os
import logging

from primrose.base.success import AbstractSuccess

from primrose.notification_utils import get_notification_client


class ClientNotification(AbstractSuccess):
    """Outputs success notification using specified client after job completion."""

    def __init__(self, configuration, instance_name):
        """Outputs success message to notify user of DAG completion.

        Args:
            configuration: configuration object from src/configuration
            instance_name: name key to use for this configuration

        Returns:
            None

        """
        super().__init__(configuration, instance_name)
        self.message = self.node_config.get("message", "SUCCESS! DAG Completed")

        self.client = get_notification_client(params=self.node_config)

    @staticmethod
    def necessary_config(node_config):
        """Returns the necessary configuration keys for the ClientNotification object

            Notes:
                client (`str`): The client object to be instantiated (ex. 'SlackClient')
                token (`str`): The token used to initialize the client

        """
        config_params = set(["client", "token"])
        return config_params.union(AbstractSuccess.necessary_config(node_config))

    def run(self, data_object):
        """
            Run job to post slack message

            Args:
                data_object (`DataObject`): primrose data object instance
                terminate (`bool`). Should we terminate the DAG? True or False

        """
        # execute response
        _ = self.client.post_message(message=self.message)

        terminate = False
        return data_object, terminate
