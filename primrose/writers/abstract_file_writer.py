"""Abstract class to write some data to a some file

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
from primrose.base.writer import AbstractWriter

class AbstractFileWriter(AbstractWriter):
    """write some data to a some file"""

    @staticmethod
    def necessary_config(node_config):
        """Necessary inputs for file writers:

        Args:
            node_config (dict): set of parametera / attributes for the node

        Note:
            key: key that identifies object to write
            dir: directory to write csv (must already exist)
            filename: name of file to be written

        Returns:
            set of necessary configuration keys

        """
        return set(["key", "dir", "filename"])
