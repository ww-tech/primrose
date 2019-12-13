"""
    Module with tests for the success notification implementation
    author(s): Parul Laul (parul.laul@ww.com)

"""

import os
import unittest
import unittest.mock as mock

from primrose.configuration.configuration import Configuration
from primrose.notifications.success_notification import SuccessNotification


config_dict = {
    'metadata': {
        'notify_on_error': {
            'client': 'SlackClient',
            'channel': 'some_channel',
            'member_id': 'None',
            'token': 'some-token'
        }
    },
    'implementation_config': {
        'cleanup_config': {
            'notification': {
                'class': 'SuccessNotification',
                'client': 'SlackClient',
                'channel': 'some-channel',
                'message': 'TEST SUCCESS!',
                'member_id': 'USomeUserID',
                'token': 'some-token'
            }
        }
    }
}


class TestSuccessNotification(unittest.TestCase):
    """Tests for slack_integration.py"""

    def test_necessary_config(self):
        self.assertEqual(
            first=SuccessNotification.necessary_config(node_config={}),
            second={'client', 'token'}
        )

    def test_run(self):

        path = 'primrose.notifications.success_notification.get_notification_client'
        with mock.patch(path) as get_client_mock:
            get_client_mock.return_value = mock.Mock()

            configuration = Configuration(None, is_dict_config=True, dict_config=config_dict)
            success_instance = SuccessNotification(
                configuration=configuration,
                instance_name='notification'
            )
            success_instance.client = get_client_mock.return_value

            success_instance.run('some_data_object')

            success_instance.client.post_message.assert_called_once_with(message='TEST SUCCESS!')


if __name__ == '__main__':
    unittest.main()