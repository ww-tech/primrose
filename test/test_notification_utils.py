"""e
    Module with all tests for the slack integration implementations
    author(s): Parul Laul (parul.laul@ww.com)
"""

import os
import unittest
import unittest.mock as mock

from primrose.notification_utils import SlackClient, get_notification_client


class TestSlackClient(unittest.TestCase):
    """Tests for SlackClient in notification_utils.py"""

    def test_post(self):
        client_mock = mock.Mock(return_value=mock.Mock())

        # instantiate instance
        slack_instance = SlackClient(channel='some_channel', member_id='USomeUserID', token='some_token')
        slack_instance.client = client_mock.return_value

        slack_instance.post_message(message='test message')

        # check method is called
        slack_instance.client.chat_postMessage.assert_called_once_with(
            channel='some_channel',
            text='test message\n <@USomeUserID>'
        )


class TestGetNotificationClient(unittest.TestCase):
    """Tests for get_notification_client in notification_utils.py"""

    def test_get_notification_client(self):
        importlib_mock = mock.Mock()
        getattr_mock = mock.Mock()

        patches = {
            'importlib': importlib_mock,
            'getattr': getattr_mock
        }
        path = 'primrose.notification_utils'
        with mock.patch.multiple(path, **patches):
            client_params = {'some': 'params', 'client': 'needed', 'token': 'needed'}

            _ = get_notification_client(client_params)

        self.assertEqual(importlib_mock.import_module.call_count, 1)
        self.assertEqual(getattr_mock.call_count, 1)

    def test_get_notification_client_exc(self):
        client_params = {'client': 'DoesNotExist'}
        self.assertRaises(AttributeError, get_notification_client(client_params))


if __name__ == '__main__':
    unittest.main()