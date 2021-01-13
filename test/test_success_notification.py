"""
    Module with tests for the success notification implementation
    author(s): Parul Laul (parul.laul@ww.com)

"""

import os
import unittest
import unittest.mock as mock

from primrose.data_object import DataObject
from primrose.node_factory import NodeFactory
from primrose.base.reader import AbstractReader
from primrose.configuration.configuration import Configuration
from primrose.notifications.success_notification import (
    get_client_params,
    ClientNotification,
)


config_dict = {
    "metadata": {
        "section_registry": ["initialize", "cleanup_config"],
        "notify_on_error": {
            "client": "SlackClient",
            "channel": "some-channel",
            "member_id": None,
            "token": "some-token",
        },
    },
    "implementation_config": {
        "initialize": {
            "start_notification": {
                "class": "ClientNotification",
                "client": "SlackClient",
                "channel": "some-channel",
                "message": "starting job...",
                "member_id": "USomeUserID",
                "token": "some-token",
                "destinations": ["notification"],
            }
        },
        "cleanup_config": {
            "notification": {
                "class": "ClientNotification",
                "client": "SlackClient",
                "channel": "some-channel",
                "message": "TEST SUCCESS!",
                "member_id": "USomeUserID",
                "token": "some-token",
            }
        },
    },
}


config_dict_node_message = {
    "metadata": {
        "section_registry": ["reader_config", "cleanup_config"],
        "notify_on_error": {
            "client": "SlackClient",
            "channel": "some-channel",
            "member_id": None,
            "token": "some-token",
        },
    },
    "implementation_config": {
        "reader_config": {
            "test_node": {
                "class": "SlackDataMock",
                "destinations": ["node_notification"],
            }
        },
        "cleanup_config": {
            "node_notification": {
                "class": "ClientNotification",
                "client": "SlackClient",
                "channel": "some-channel",
                "message": "TEST SUCCESS!",
                "member_id": "USomeUserID",
                "token": "some-token",
                "use_configuration_file_message": False,
                "node_name": "test_node",
                "message_key": "test",
            }
        },
    },
}


class SlackDataMock(AbstractReader):
    def __init__(self, configuration, instance_name):
        super().__init__(configuration, instance_name)

    @staticmethod
    def necessary_config(node_config):
        return set()

    def run(self, data_object):

        data_object.add(self, "Node Success!", "test")

        return data_object


class TestClientNotification(unittest.TestCase):
    """Tests for success_notification.py"""

    def test_get_client_params(self):
        os.environ["SLACKCLIENT_CHANNEL"] = "test-channel"
        os.environ["SLACKCLIENT_MEMBER_ID"] = "test-member_id"
        os.environ["SLACKCLIENT_TOKEN"] = "test-token"

        params = {"client": "SlackClient", "message": "starting job..."}

        ans = get_client_params(params)
        expected = {
            "client": "SlackClient",
            "channel": "test-channel",
            "message": "starting job...",
            "member_id": "test-member_id",
            "token": "test-token",
        }
        self.assertDictEqual(ans, expected)

    def test_necessary_config(self):
        self.assertEqual(
            first=ClientNotification.necessary_config(node_config={}),
            second={"client", "token"},
        )

    def test_run(self):

        path = "primrose.notifications.success_notification.get_notification_client"
        with mock.patch(path) as get_client_mock:
            get_client_mock.return_value = mock.Mock()

            configuration = Configuration(
                None, is_dict_config=True, dict_config=config_dict
            )
            success_instance = ClientNotification(
                configuration=configuration, instance_name="notification"
            )
            success_instance.client = get_client_mock.return_value

            success_instance.run("some_data_object")

            success_instance.client.post_message.assert_called_once_with(
                message="TEST SUCCESS!"
            )

    def test_run_node(self):

        path = "primrose.notifications.success_notification.get_notification_client"
        with mock.patch(path) as get_client_mock:
            get_client_mock.return_value = mock.Mock()

            NodeFactory().register("SlackDataMock", SlackDataMock)

            config = Configuration(
                None, is_dict_config=True, dict_config=config_dict_node_message
            )
            data_object = DataObject(config)

            reader = SlackDataMock(config, "test_node")
            data_object = reader.run(data_object)

            success_instance = ClientNotification(
                configuration=config,
                instance_name="node_notification",
            )
            success_instance.client = get_client_mock.return_value

            success_instance.run(data_object)

            success_instance.client.post_message.assert_called_once_with(
                message="Node Success!"
            )


if __name__ == "__main__":
    unittest.main()
