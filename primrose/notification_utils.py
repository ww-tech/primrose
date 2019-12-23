"""
    Module to establish connection to slack app and post to channel
    Author(s): Parul Laul (parul.laul@ww.com)

"""

import importlib
import logging
import os

import slack
from slack.errors import SlackApiError

from primrose.notifications.abstract_notification import AbstractNotification


class SlackClient(AbstractNotification):
    """Module to establish connection to slack app and post to channel"""

    def __init__(self, channel, token, member_id=None):
        """Outputs messages in a specified slack channel to
        notify user of the status of the DAG after completion.

        Args:
            channel (`str`): Channel to post slack messages
            member_id (`str`): Slack member_id, defaults to None.
                Used if user would like to be pinged with the message output.

        Returns:
            None

        """
        self.channel = channel
        self.member_id = member_id  # this is not userid. See https://medium.com/@moshfeu/how-to-find-my-member-id-in-slack-workspace-d4bba942e38c
        self.client = slack.WebClient(token=token)

    def post_message(self, message: str):
        """Calls slack client api to post message in channel

        Args:
            message (`str`): message to be posted
        Returns:
            slack.web.slack_response.SlackResponse, and message is posted to slack.

        """

        if self.member_id:
            message += f"\n <@{self.member_id}>"

        # post response
        try:
            return self.client.chat_postMessage(channel=self.channel, text=message)

        except SlackApiError as error:
            msg = "Something went wrong when attempting to send message to slack"
            logging.error(msg=msg, exc_info=error)


def get_notification_client(params: dict):

    """Get client given configuration parameters

    Args:
        params (`dict`): parameters for client instantiation
        (example. {'client': 'SlackClient', 'channel': 'my_channel', 'token': 'some-token'})

    Returns:
        instantiated client object

    """
    exclude = ["class", "client", "message", "destinations"]
    client_params = {k: v for k, v in params.items() if k not in exclude}

    # instantiate client
    module = importlib.import_module(__loader__.name)
    try:
        client = getattr(module, params["client"])

        return client(**client_params)

    except AttributeError as error:
        msg = "Are you sure {} is in {}?".format(params["client"], module)
        logging.error(msg, error)
