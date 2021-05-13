from primrose.base.node import AbstractNode

class DoNothingNode(AbstractNode):
    '''A node that does absolutely nothing.

    Why? If you have a DAG with two independent chains of nodes (A->B->C, D->E), the code will complain
    about multiple connect components. While those could be run as 2 DAGS, perhaps there are times that 
    you want to combine those into one job. This node provides the ability to hang those independent
    chains off of the destinations
    '''

    @staticmethod
    def necessary_config(node_config):
        """Returns the necessary configuration keys for the DoNothingNode object

        Args:
            node_config (dict): set of parameters / attributes for the node

        Returns:
            set of necessary keys for the DoNothingNode object

        """
        return set([])

    def run(self, data_object):
        """Pass through, doing nothing
        Returns:
            (tuple): tuple containing:

                data_object (DataObject): instance of DataObject

                terminate (bool): terminate the DAG?

        """
        terminate = False
        return data_object, terminate
