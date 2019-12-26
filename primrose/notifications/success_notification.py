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



def get_client_params(params: dict):
    """
        Creates the parameter dictionary to be read by the
        `get_notification_client` method.

        Args:
            params (`dict`): The parameter dictionary where keys are
                - the primrose `client` (required key),
                - neccessary arguments to instantiate the client
                - the `message` if used in the `implementation` section of the DAG.

                Values can be explicitly or stored as environment variables.
                Environment variables are stored as {CLIENT_NAME}_{CLIENT_KEY}.

                For example, using the built-in SlackClient, environment variables
                will be stored as:
                    SLACKCLIENT_TOKEN="some-token"
                    SLACKCLIENT_CHANNEL="some-channel"
                    SLACKCLIENT_MEMBER_ID="USomeUserID"

        Returns:
            A `dict` with the key-value pairs necessary to be read by the
            `get_notification_client` method.

        Example:
            >>> node_config = {
                    "client": "SlackClient",
                    "message": "starting job...",
                }
            >>> get_client_params(node_config)
            {'client': 'SlackClient',
            'channel': 'some-channel',
            'message': 'starting job...',
            'token': 'some-token',
            'member_id': 'USomeUserID'
            }

    """
    client = params['client'].upper()
    env_var = {
        k.split(f'{client}_')[-1].lower(): v for k, v in os.environ.items()
        if client.upper() in k
        and k.split(f'{client}_')[-1].lower() not in params.keys() # config params take precedence
    }

    # combine environment variables and params
    return {**params, **env_var}


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

        # read from config dict or environment variables
        self.client = get_notification_client(params=get_client_params(self.node_config))

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
