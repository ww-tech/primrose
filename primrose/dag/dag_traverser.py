"""Abstract module to traverse a DAG

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
from abc import ABC, abstractmethod

class DagTraverser(ABC):
    """Abstract module to traverse a DAG"""

    def __init__(self, configuration):
        """instantiate the traverser

        Args:
            configuration (Configuration): Configuration instance

        """
        self.configuration = configuration

    @abstractmethod
    def run_section_by_section(self):
        """Do we want go through section by section?

        Note:
            suppose we had 5 sections and we asked to run just 2: section1 and section2.
            Is it important that we run section1 and then section 2 (if so, return True)
            or just that we run all the nodes from section1 and section2 together in some
            desired order (return False)

        Returns:
            result (Boolean)

        """

    @abstractmethod
    def traversal_list(self):
        """get list of nodes to traverse

        Returns:
            sequence (list): list of node name in the order that they should be run

        """
        pass # pragma: no cover
