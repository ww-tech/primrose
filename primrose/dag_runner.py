"""Run the DAG: gets list of nodes to traverse then calls run(data_object) on each

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
from primrose.data_object import DataObject
import networkx as nx
import os
import logging
import collections
from primrose.node_factory import NodeFactory
from primrose.configuration.configuration import OperationType
from primrose.notification_utils import get_notification_client
from primrose.dag.traverser_factory import TraverserFactory
from primrose.base.conditional_path_node import AbstractConditionalPath


class DagRunner():
    """class that runs the DAG: gets the list of nodes to traverse and then asks them to run"""

    def __init__(self, configuration):
        """instantiate the DagRunner

        Args:
            configuration (Configuration): configuration object defined in primrose/Configuration with validated inputs
                from the result of necessary_config, all inputs are described in that method

            instance_name (str): how the code knows where it is from

        """
        self.configuration = configuration
        self.dag = self.configuration.dag

        if configuration.config_metadata and 'traverser' in configuration.config_metadata:
            self.dag_traverser = TraverserFactory().instantiate(configuration.config_metadata['traverser'], configuration)
        else:
            self.dag_traverser = TraverserFactory().default_traverser(configuration)

        logging.info("Traverser is of class %s", self.dag_traverser.__class__)

    def create_data_object(self):
        """restore data_object from cache

        Returns:
            data_object (DataObject)

        """
        if self.configuration.config_metadata and 'data_object' in self.configuration.config_metadata:

            cfg = self.configuration.config_metadata['data_object']

            if "read_from_cache" in cfg and cfg['read_from_cache']:
                # we can assume that read_filename exists due to configuration checks
                filename = cfg['read_filename']
                assert os.path.exists(filename)

                logging.info("Reading DataObject from cache " + filename)
                return DataObject.read_from_cache(filename)

        data_object = DataObject(self.configuration)

        return data_object

    def cache_data_object(self, data_object):
        """cache the data object

        Args:
            data_object (DataObject): instance of DataObject

        Returns:
            whether it was cached (bool)

        """

        if self.configuration.config_metadata and 'data_object' in self.configuration.config_metadata:

            cfg = self.configuration.config_metadata['data_object']

            if "write_to_cache" in cfg and cfg['write_to_cache']:

                assert "write_filename" in cfg
                filename = cfg["write_filename"]
                data_object.write_to_cache(filename)
                return True

        return False

    def initial_check_sequence(self, sequence):
        """Some checks on the incoming sequence

        Args:
            sequence (list): list of nodes to run in given order

        Returns:
            nothing

        Raises:
            Exception if there are dupes in the sequence, or if nodes are not in config, or we have nodes from other sections.
            The latter can happen if we mix up nodes from sections. That is, suppose we have section1 (1 node) and section 2 (2 nodes) and
            we want to run section2 and then section1 and we receive sequence [section2_node1, section1_node1, section2_node2], it will
            complain about the partition [section2_node1, section1_node1] [section2_node2] as they are mixed from sections.

        """
        dupes = [item for item, count in collections.Counter(sequence).items() if count > 1]

        if len(dupes) > 0:
            raise Exception("You have duplicate nodes from traverser! " + str(dupes))

        # explictly check that each of these is a known node in config, raising exception if not
        [self.configuration.config_for_instance(instance_name) for instance_name in sequence]

    def filter_sequence(self, sequence):
        """The user may have specified some subset of sections to run in metadata.section_run
        Let's assume we can't trust traverser to limit themselves to those sections, so here
        we limit the sequence, if necessary

        Args:
            sequence (list): list of nodes to run in given order

        Returns:
            sequence (list): complete or subset of input sequence

        Raises:
            Exception if there are dupes in the sequence, or if nodes are not in config, or we have nodes from other sections.
            The latter can happen if we mix up nodes from sections. That is, suppose we have section1 (1 node) and section 2 (2 nodes) and
            we want to run section2 and then section1 and we receive sequence [section2_node1, section1_node1, section2_node2], it will
            complain about the partition [section2_node1, section1_node1] [section2_node2] as they are mixed from sections.

        """
        self. initial_check_sequence(sequence)

        # it might be that traverser provides a sequence of all config nodes and we only want to run those in section2
        # Thus, first, let's only consider the nodes in that section
        nodes_to_run = set()
        sections, source = self.configuration.sections_in_order()

        logging.info("Taking nodes to run from " + source)

        for section_name in sections:
            nodes_to_run = nodes_to_run.union(set(self.configuration.config[section_name].keys()))
        sequence = [n for n in sequence if n in nodes_to_run]

        if not self.dag_traverser.run_section_by_section():
            self.check_for_upstream(sequence)
            return sequence

        filtered_sequence = []

        for section_name in sections:

            #get a set of all nodes in this section
            this_section = set(self.configuration.config[section_name].keys())

            n = len(this_section)

            # do we have enough nodes in sequence to compare against?
            if len(sequence) >= n:

                subset = sequence[:n]

                #logic: the set of first n nodes of sequence should match this set of this_section
                if this_section == set(subset):

                    #these nodes passed the checks
                    filtered_sequence.extend(sequence[:n])

                    #pop off the first n nodes and repeat until we've gone through each section
                    sequence = sequence[n:]
                else:
                    # We can't get here if there is only 1 section to run as we raise on dupes and importantly, we now filter on sections earlier.
                    # This means that we cannot pull in nodes from a later section. That is, suppose the sequence was
                    # [section2_node1, section1_node1, section2_node2] and we wanted to run section 2 only (which has two nodes).
                    # We wouldn't grab first two nodes of sequence [section2_node1, section1_node1] which is what code below was
                    # meant to detect, but instead we would get [section2_node1, section2_node2] from the section filter.
                    # So, it would either match exactly or we would run out of nodes.
                    #
                    # For 2 or more nodes, however, it is different. If we wanted to run section 2 and then section 1, we would
                    # grab first 2 nodes [section2_node1, section1_node1] and fall into this section.
                    msg = "Traverser is mismatched with section " + section_name + "."
                    msg += " Expecting set " + str(sorted(this_section)) + ".\n"
                    msg += " Received list " + str(sorted(subset))
                    raise Exception(msg)
            else:
                raise Exception("Ran out of nodes for section " + section_name + ". Only received " + str(sequence))

        self.check_for_upstream(filtered_sequence)

        return filtered_sequence

    def check_for_upstream(self, sequence):
        """check for any upstream paths with the input sequence.
        That is, suppose we had a reader flowing to writer. It would not make sense
        to run writer before the reader.

        Args:
            sequence (list): list of node names

        Raises:
            Exception if any upstream paths found

        """

        for idx_from in range(len(sequence)):
            for idx_to in range(len(sequence)):
                if idx_from > idx_to:
                    if self.configuration.dag.paths(sequence[idx_from],  sequence[idx_to]):
                        msg = "Upstream path found, from %s to %s" % (sequence[idx_from], sequence[idx_to])
                        raise Exception(msg)
        return False

    def run(self, dry_run=False):
        """run the whole DAG. Optonally, you can call dry_run=True
            which will log what would be run and in what order
            but not actually run it

        Args:
            dry_run: Boolean. Want to do a dry run?

        Returns:
            data_object: DataObject instance
            node (Node): last node run

        """
        data_object = self.create_data_object()

        candidate_sequence = self.dag_traverser.traversal_list()

        sequence = self.filter_sequence(candidate_sequence)

        if len(candidate_sequence) > len(sequence):
            logging.info("Sequence of nodes to be run: %s", sequence)

        pruned_nodes = set()

        if (self.configuration.config_metadata and
            'notify_on_error' in self.configuration.config_metadata):
            try:
                params = self.configuration.config_metadata['notify_on_error']
                client = get_notification_client(params)

            except Exception as error:
                msg = (
                    'Error trying to instantiate notification client.'
                    'Check class name and parameters"'
                )
                logging.error(error)
                raise(msg)
        else:
            client = None

        for i, node in enumerate(sequence):

            if node in pruned_nodes:
                logging.info("Skipping pruned node " + node)
                continue

            section = self.dag.node_map[node]
            class_name = self.configuration.nodename_to_classname[node]

            if dry_run:
                logging.info("DRY RUN %s: would run node %s of type %s and class %s", i, node, section, class_name)
                continue
            else:
                logging.info("received node %s of type %s and class %s", node, section, class_name)

            try:
                node_instance = NodeFactory().instantiate(class_name, self.configuration, node)
            except Exception as e:
                msg = "Issue instantiating %s and class %s" % (node, class_name)
                logging.error(msg)
                if client:
                    client.post_message(msg)
                raise Exception(msg)

            try:
                data_object, terminate = node_instance.run(data_object)

                if isinstance(node_instance, AbstractConditionalPath):
                    to_prune = node_instance.all_nodes_to_prune()
                    if to_prune:
                        pruned_nodes.update(to_prune)

            except Exception as e:
                msg = "Issue with %s" % node
                logging.error(msg)
                if client:
                    client.post_message(msg)
                raise e

            if terminate:
                logging.info("Terminating early due to signal from %s", node)
                if client:
                    client.post_message(msg)
                break

        self.cache_data_object(data_object)

        logging.info("All done. Bye bye!")
        return data_object
