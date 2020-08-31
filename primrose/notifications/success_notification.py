"""
    Module with AbstractSuccess implementation.
    Provides slack notification after job completion

    author(s): Parul Laul (parul.laul@ww.com)

"""

import importlib
import os
import logging

from primrose.base.success import AbstractSuccess
from primrose.data_object import DataObjectResponseType

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
    client = params["client"].upper()
    env_var = {
        k.split(f"{client}_")[-1].lower(): v
        for k, v in os.environ.items()
        if client.upper() in k
        and k.split(f"{client}_")[-1].lower()
        not in params.keys()  # config params take precedence
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
        self.use_configuration_file_message = self.node_config.get(
            "use_configuration_file_message", True
        )
        self.node_name = self.node_config.get("node_name", "")
        self.message_key = self.node_config.get("message_key", "")

        # read from config dict or environment variables
        self.client = get_notification_client(
            params=get_client_params(self.node_config)
        )

    @staticmethod
    def necessary_config(node_config):
        """Returns the necessary configuration keys for the ClientNotification object

        Notes:
            client (`str`): The client object to be instantiated (ex. 'SlackClient')
            token (`str`): The token used to initialize the client

        """
        config_params = set(["client", "token"])
        return config_params.union(AbstractSuccess.necessary_config(node_config))

    @staticmethod
    def optional_config(node_config):
        """Returns the optional configuration keys

        Args:
            node_config (dict): set of parameters for the node

        optional keys:
            use_configuration_file_message(bool): True if you want the slack client to send message from
            configuration file. This is the default behavior. If you set it to false
            pass the next two parameters to send alternate message passed from another node.

            node_name(str): the name of the node where this module will find the message to
            post

            message_key(str): the key in the node that should be used for getting the message
        Returns:
            Set of optional keys for the pipeline object
        """
        return set(
            [
                "use_config_message",
                "node_name",
                "message_key",
            ]
        )

    def run(self, data_object):
        """
        Run job to post slack message

        Args:
            data_object (`DataObject`): primrose data object instance
            terminate (`bool`). Should we terminate the DAG? True or False

        """

        if not self.use_configuration_file_message:
            logging.info("Posting message passed by a node")

            if self.node_name and self.message_key:
                upstream_data = data_object.get_upstream_data(
                    self.instance_name,
                    pop_data=False,
                    rtype=DataObjectResponseType.INSTANCE_KEY_VALUE.value,
                )

                self.message = upstream_data[self.node_name][self.message_key]
            else:
                logging.info(
                    """Do not have adequate information to retrieve message from
                                an upstream node; reverting to default behavior.
                                If you want to post message from an upstream node, provide
                                values for node_name and message_key """
                )

        # execute response
        _ = self.client.post_message(message=self.message)

        terminate = False
        return data_object, terminate
