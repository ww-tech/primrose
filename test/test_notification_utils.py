"""e
    Module with all tests for the slack integration implementations
    author(s): Parul Laul (parul.laul@ww.com)
"""
import unittest.mock as mock
import pytest
import logging

from primrose.notification_utils import SlackClient, get_notification_client

@pytest.mark.basic
def test_post():
    client_mock = mock.Mock(return_value=mock.Mock())

    # instantiate instance
    slack_instance = SlackClient(
        channel="some_channel", member_id="USomeUserID", token="some_token"
    )
    slack_instance.client = client_mock.return_value

    slack_instance.post_message(message="test message")

    # check method is called
    slack_instance.client.chat_postMessage.assert_called_once_with(
        channel="some_channel", text="test message\n <@USomeUserID>"
    )

@pytest.mark.basic
def test_get_notification_client():
    importlib_mock = mock.Mock()
    getattr_mock = mock.Mock()

    patches = {"importlib": importlib_mock, "getattr": getattr_mock}
    path = "primrose.notification_utils"
    with mock.patch.multiple(path, **patches):
        client_params = {"some": "params", "client": "needed", "token": "needed"}

        _ = get_notification_client(client_params)

    assert importlib_mock.import_module.call_count == 1
    assert getattr_mock.call_count == 1

@pytest.mark.basic
def test_get_notification_client_exc(caplog):
    client_params = {"client": "DoesNotExist"}
    with caplog.at_level(logging.ERROR):
        get_notification_client(client_params)
    assert (
        "Are you sure DoesNotExist is in <module 'primrose.notification_utils'"
        in caplog.text
    )
