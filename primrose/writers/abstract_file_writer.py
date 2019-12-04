"""Abstract class to write some data to a some file

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
import pathlib
from primrose.base.writer import AbstractWriter

class AbstractFileWriter(AbstractWriter):
    """write some data to a some file"""

    def __init__(self,configuration, instance_name):
        """

        Args:
            configuration (Configuration): configuration object defined in primrose/Configuration with validated inputs
                from the result of necessary_config, all inputs are described in that method

            instance_name (str): how the code knows where it is from

        """
        super().__init__(configuration, instance_name)
        self._check_directory()

    def _check_directory(self):
        """Create directory or directory structure if it does not already exist."""
        pathlib.Path(self.node_config['dir']).mkdir(parents=True, exist_ok=True)

    @staticmethod
    def necessary_config(node_config):
        """Necessary inputs for file writers:

        Args:
            node_config (dict): set of parameters / attributes for the node

        Note:
            key: key that identifies object to write
            dir: directory to write csv (must already exist)
            filename: name of file to be written

        Returns:
            set of necessary configuration keys

        """
        return set(["key", "dir", "filename"])
