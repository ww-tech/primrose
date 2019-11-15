"""Simple success node: log a message at specified log level

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
import logging
from primrose.base.success import AbstractSuccess

class LoggingSuccess(AbstractSuccess):
    """simple success node: log a message"""

    @staticmethod
    def necessary_config(node_config):
        """Return a list of necessary configuration keys within the implementation

        Args:
            node_config (dict): set of parameters / attributes for the node

        Note:
            msg: message you want logged
            level: one of 'DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'

        Returns:
            set of keys necessary to run implementation

        """
        return set(['msg', 'level']) 

    def run(self, data_object):
        """Signal success by logging a message at specified log level

        Args:
            data_object (DataObject): DataObject instance

        Returns:
            nothing. Side effect is to signal success via logging

        """
        msg = self.node_config['msg']

        level = str(self.node_config['level']).upper()

        # check whether valid level: will throw KeyError if level not recognized
        logging._nameToLevel[level]

        level = logging.getLevelName(level)

        logging.getLogger("")._log(level, msg, None, None)

        terminate = False

        return data_object, terminate
