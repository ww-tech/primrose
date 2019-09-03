"""Module to traverse DAG by running all readers, then all pipelines, then all models, 
then all postprocess, then all cleanup

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
from primrose.dag.dag_traverser import DagTraverser
from primrose.dag.depth_first_traverser import DepthFirstTraverser

class ConfigLayerTraverser(DagTraverser):
    """Module to traverse DAG by running all readers, then all pipelines,
    then all models, then all postprocess, then all cleanup

    """

    def _sort_by_depth_first(self, candidates):
        depth_first_order = DepthFirstTraverser(self.configuration).traversal_list()
        return [n for n in depth_first_order if n in candidates]

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
        return True

    def traversal_list(self):
        """get list of nodes to traverse doing all readers, then all pipelines, then all models etc,
        or from user-defined list from metadata.section_registry

        Returns:
            sequence (list): list of node names in the order that they will be run

        """
        sequence = []
        sections, source = self.configuration.sections_in_order()
        for section_name in sections:

            section_keys = self.configuration.config[section_name].keys()

            sorted_keys = self._sort_by_depth_first(section_keys)

            sequence.extend(sorted_keys)

        return sequence
