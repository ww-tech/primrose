"""
    Abstract class to notify upon exception

    Author(s):
        Parul Laul (parul.laul@ww.com)

"""


from abc import ABC, abstractmethod


class AbstractNotification(ABC):

    @abstractmethod
    def post_message(self, message):
        """
        Sends message to the notification client.

        Args:
            message (str): Message to be sent

        """
        pass