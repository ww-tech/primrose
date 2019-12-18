'''A class that creates a directed acyclic graph (DAG) and perhaps a number of checks,
such as detecting cycles, orphans, and unrecognized nodes

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

'''
import logging
import networkx as nx
from primrose.configuration.util import OperationType, ConfigurationError
import matplotlib.pyplot as plt
from networkx.algorithms.shortest_paths.generic import has_path
from primrose.node_factory import NodeFactory
from primrose.base.conditional_path_node import AbstractConditionalPath


class ConfigurationDag():

    def __init__(self, config):
        """instantiate ComfigurationDAG instance

        Args:
            config (Configuration): Configuration instance

        """
        self.config = config

    @staticmethod
    def check_node_exists(node_names, key):
        """check that some specified destination is node on graph

        Args:
            nodes_names (list): list of node names
            key (str): name of node to check

        Raises:
            ConfigurationError

        """
        if not key in node_names:
            raise ConfigurationError("Destination %s does not exist" % key)

    @staticmethod
    def add_edge(G, G2, node_names, key, destination):
        """add an edge to the DAG

        Args:
            G (networkx bidirectional graph): bidirectional graph instance
            G2 (networkx directional graph): bidrectional graph instance
            key (str): starting node name
            destination (str): destination node name

        Returns:
            nothing. Side effect is to add the edge

        """
        ConfigurationDag.check_node_exists(node_names, destination)
        logging.debug("Adding edge %s -> %s" % (key, destination))
        G.add_edge(key, destination)
        G2.add_edge(key, destination)

    def starting_nodes(self):
        """Where does the DAG start? Compute list of starting (level 0) nodes

        Returns:
            list of node name

        """
        # Level 0 nodes in a directed graph will have 1 or more out_edges but no in_edges
        nodes_with_outs = set(e[0] for e in self.G2.out_edges())
        nodes_with_ins = set(e[1] for e in self.G2.in_edges())
        return nodes_with_outs - nodes_with_ins

    def nodes_of_type(self, operation_type):
        """get set of nodes of a given operation type (OperationType.reader, OperationType.writer etc)

        Args:
            operation_type (OperationType): OperationType

        Returns:
            set of keys, if any, of the given operation type

        """
        return set([k for k in self.node_map.keys() if self.node_map[k] == operation_type.value])

    def upstream_nodes_of_type(self, target_node_name, operation_type):
        """get set of nodes of a given operation type (OperationType.reader, OperationType.writer etc)
            upstream of some given target node

        Args:
            operation_type (OperationType): OperationType

        Returns:
            set of keys, if any, of the given operation type

        """
        assert target_node_name in self.node_map
        nodes = self.nodes_of_type(operation_type)
        nodes = [node for node in nodes if has_path(self.G2, node, target_node_name)]
        return set(nodes)

    def descendents(self, source):
        """Get the list of descendents from source, i.e. subgraph below source

        Args:
            source (str): name of source

        Returns:
            list of descendents of source node

        """
        assert source in self.node_map
        return nx.descendants(self.G2, source)

    def paths(self, source, target):
        """return the paths, if any, from a given source node to a given target node

        Args:
            source (str): name of node which is starting point of path
            target (str): name of node which is end point of path

        Returns:
            list of list of nodes (in order) forming the paths, or None if no path

        """
        assert source in self.node_map
        assert target in self.node_map
        if has_path(self.G2, source, target):
            return nx.all_simple_paths(self.G2, source=source, target=target)
        return None

    def check_for_cycles(self):
        """check for cycles

        Raise:
            ConfigurationError if cycles found

        """
        cycles = None
        try:
            cycles = nx.find_cycle(self.G2)
            logging.info("cycles:" + str(cycles))
        except nx.exception.NetworkXNoCycle as e:
            # it throws an error if there are no cycles, the opposite of what we want
            if str(e) != "No cycle found.":
                raise e # pragma: no cover
            else:
                logging.info("OK: no cycles found")
        #to get here, we must have cycles!
        if cycles:
            raise ConfigurationError("Cycle(s) found: %s" % str(cycles))

    def check_connected_components(self):
        """now we can count the number of connected components. >1 is problem

        Raises:
            ConfigurationError if multiuple connected components

        """
        connected_components = nx.connected_components(self.G)
        n = sum([1 for c in connected_components])
        if n > 1:
            raise ConfigurationError("Found multiple connected components: %s" % str(list( nx.connected_components(self.G) )))
        else:
            logging.info("OK: 1 connected component")

    def upstream_keys(self, instance_name):
        """get list of keys (names of nodes in the DAG) that feed into instance_name node

        Args:
            instance_name (str): name of instance

        Returns:
            list of nodes

        """
        if not self.G2.has_node(instance_name):
            raise ConfigurationError("Node not found in the DAG: %s" % instance_name)
        return list(self.G2.predecessors(instance_name))

    def upstream_typed_keys(self, instance_name):
        """get dictionary of the upstream keys with Operation types as values

        Args:
            instance_name (str): name of instance

        Returns:
            dictionary of {name : node type}

        """
        keys = self.upstream_keys(instance_name)
        d = {}
        for k in keys:
            d[k] = self.node_map[k]
        return d

    def create_dag(self):
        """Create the DAG

        Returns:
            nothing. Side effect is to set up graphs and node map

        """
        logging.info("Checking configuration DAG")
        G = nx.Graph()
        G2 = nx.DiGraph()
        node_names = set()
        cleanup_nodes = set()

        some_postprocess_node = None

        # key to section type
        node_map = {}

        self.conditional_nodes = set()

        #add the nodes to the graph:
        for section_key in self.config.keys():

            for key in self.config[section_key].keys():
                logging.debug("Adding node '%s'" % key)
                G.add_node(key)
                G2.add_node(key)
                node_names.add(key)

                node_map[key] = section_key

                # root out conditional nodes...
                node_config = self.config[section_key][key]
                node_class = node_config['class']
                class_obj = NodeFactory().name_dict[node_class]
                if issubclass(class_obj, AbstractConditionalPath):
                    self.conditional_nodes.add(key)

                # cleanup section can be disconnected from rest of graph so let's keep track of these nodes
                if section_key == OperationType.cleanup.value:
                    cleanup_nodes.add(key)

                # hack: we are going to add an edge from a postprcocess step (any one) to cleanup nodes so they
                # are not a separate connected component
                if section_key == OperationType.postprocess.value and some_postprocess_node is None:
                    some_postprocess_node = key

        # add the edges
        for section_key in self.config.keys():

            for key in self.config[section_key].keys():
                d = self.config[section_key][key]

                if 'destinations' in d:
                    for destination in d['destinations']:

                        if not isinstance(destination, str):
                            raise ConfigurationError("Unrecognized destination type: %s" % destination)

                        if destination in node_map:
                            ConfigurationDag.add_edge(G,G2,node_names,key,destination)
                        else:
                            raise ConfigurationError("Did not find %s destination in %s.%s" % (destination, section_key, key))

        logging.info("OK: good referential integrity")

        self.G = G
        self.G2 = G2
        self.node_map = node_map

    def check_dag(self):
        """check that it is a DAG

        Note:
            check for cycles
            check only 1 connected component, no orphans
            check that all edges point to known nodes

        Raises:
            Excetions if cycles found or multiple connected components

        """
        self.create_dag()

        self.check_connected_components()

        self.check_for_cycles()

    def plot_dag(self, filename, traverser, node_size=500, label_font_size=12, text_angle=0, image_width=16, image_height=12):
        """plot the DAG to image file

        Args:
            filename (str): path to write image to
            title (str): title to add to chart
            node_size (int): node size
            label_font_size (int): font size
            text_angle (int): angle to rotate. This is angle in degrees counter clockwise from east
            image_width (int): width of image in inches
            image_height (int): heightof image in inches

        Returns:
            nothing. Saves image to file

        """
        # map nodes to a color for their operation type
        # https://stackoverflow.com/questions/27030473/how-to-set-colors-for-nodes-in-networkx-python
        color_map = []
        colors = ['#fbb4ae','#b3cde3','#ccebc5','#decbe4','#fed9a6']
        for node in self.G2:
            if self.node_map[node] == OperationType.reader.value:
                color_map.append(colors[0])
            elif self.node_map[node] == OperationType.pipeline.value:
                color_map.append(colors[1])
            elif self.node_map[node] == OperationType.model.value:
                color_map.append(colors[2])
            elif self.node_map[node] == OperationType.writer.value:
                color_map.append(colors[3])
            else:
                color_map.append(colors[4])

        fig = plt.figure(figsize=(image_width,image_height))
        ax = plt.subplot(111)
        ax.set_title(filename, fontsize=10)

        try:
            import pydot
            from networkx.drawing.nx_pydot import graphviz_layout
        except ImportError: # pragma: no cover
            raise ImportError(
                "This example needs Graphviz and pydot."
                "Please refer to the Plotting requirements in the README"
            )

        # pos = nx.spring_layout(G)
        # pos = nx.circular_layout(G)
        # pos = nx.kamada_kawai_layout(G)
        # pos = nx.shell_layout(G)
        # pos = nx.spectral_layout(G)
        pos = graphviz_layout(self.G2, prog='dot') #, prog='twopi', args='')

        nx.draw(self.G2, pos, node_size=node_size,node_color = color_map, edge_color='#939393', font_size=8, font_weight='bold')
        # nx.draw_networkx_nodes(G, pos, node_color='b', node_size=500, alpha=0.8)

        if len(self.conditional_nodes) > 0:
            cnodes = nx.draw_networkx_nodes(self.G2, pos, node_color='#e6b655', node_size=1.5*node_size, alpha=0.8, node_shape="D", nodelist=list(self.conditional_nodes))
            cnodes.set_edgecolor('red')

#        nx.draw_networkx_labels(self.G2,pos, font_size=9)

        text = nx.draw_networkx_labels(self.G2,pos,with_labels=False, font_size=label_font_size) #, bbox=Bbox.from_bounds(x0, y0, 20,30)) #, wrap=True)

        if traverser:
            #map node name to sequence number
            sequence = traverser.traversal_list()
            idx = list(range(1,len(sequence) + 1))
            d = dict(zip(sequence,idx))

            # let's plot the sequence numner above the node. How far above it?
            ys = [t._y for _,t in text.items()]
            ysrange = max(ys) - min(ys)
            offset = 0.02 * abs(ysrange)

        for _,t in text.items():
            t.set_rotation(text_angle)

            if traverser:
                plt.text(t._x, t._y + offset, d[t._text], fontsize=24, color='red')

        plt.axis('off')
        plt.tight_layout()
        plt.savefig(filename, format="PNG")
        logging.info("Graph written to %s" % filename)
