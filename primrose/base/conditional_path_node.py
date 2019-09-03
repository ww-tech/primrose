"""Module with abstract pipeline class to specify interface needed for future pipelines

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
from abc import abstractmethod
from primrose.base.node import AbstractNode

class AbstractConditionalPath(AbstractNode):
    """A class that supports conditional pathing through the DAG. 
    After running, prune() can provide a list of destination nodes, representing start of paths to prune. 
    DAGRunner can then prune those nodes, and all paths downstream of those nodes, from the DAG"""

    def all_nodes_to_prune(self):
        """What are all the nodes we should prune from the DAG?

        Note:
            this call destinations_to_prune() and then uses the DAG to identify 
            the complete subgraphs starting with those destinations

        Returns:
            set of all nodes to prune

        """
        if not 'destinations' in self.node_config:
            raise Exception("Destinations key is missing")

        destinations_to_prune = self.destinations_to_prune()

        if destinations_to_prune:

            all_nodes_to_prune = set()

            for destination in destinations_to_prune:
                if not destination in self.node_config['destinations']:
                    raise Exception("Destination " + destination + " is not in destinations list")

                all_nodes_to_prune.add(destination)
                all_nodes_to_prune.update(self.configuration.dag.descendents(destination))

            return all_nodes_to_prune

        return None

    @abstractmethod
    def destinations_to_prune(self):
        """Which destinations, if any, should we prune from DAG?

        Returns:
            list of destinations nodes to prune from DAG, None otherwise

        """
        return None # pragma: no cover