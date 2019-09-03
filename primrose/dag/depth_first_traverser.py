"""Module to traverse DAG by running depth first algorithm

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
import networkx as nx
from primrose.dag.dag_traverser import DagTraverser

class DepthFirstTraverser(DagTraverser):
    """a traverser that traverses in depth first manner"""

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
        return False

    def traversal_list(self):
        """get list of nodes to traverse doing a depth first traversal

        Returns:
            sequence (list): list of node name in the order that they will be run

        """
        G = self.configuration.dag.G2
        sequence = list(nx.dfs_postorder_nodes(G))
        sequence.reverse()
        return sequence
