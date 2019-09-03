"""Singleton Factory where one can register traversers for instantiation

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""

import logging
from primrose.dag.config_layer_traverser import ConfigLayerTraverser
from primrose.dag.depth_first_traverser import DepthFirstTraverser

class TraverserFactory:
    """Singleton Factory where one can register traversers for instantiation"""

    instance = None

    def __init__(self):
        """instantiate the factory but as a singleton. The guard raails are here
        """
        # where the magic happens, only one instance allowed:
        if not TraverserFactory.instance:
            TraverserFactory.instance = TraverserFactory.__HiddenFactory()

    def __getattr__(self, name):
        """getattr with instance name

        Returns:
            gettattr

        """
        return getattr(self.instance, name)

    class __HiddenFactory():
        """actual factory where registry and instantiation happens"""

        
        def __init__(self):
            """instantiate the HiddenFactory"""
            self.name_dict = {}
            self.register('ConfigLayerTraverser', ConfigLayerTraverser)
            self.register('DepthFirstTraverser', DepthFirstTraverser)
            self.DEFAULT_TRAVERSER = 'ConfigLayerTraverser'

        def register(self, key, class_obj, raise_on_overwrite=False):
            """Registering class_obj with key

            Args:
                key (str): key such as class name, e.g. 'ConfigLayerTraverser'
                class_obj (class obj), e.g. ConfigLayerTraverser

            Returns:
                nothing. Side effect is to register the class

            """
            if raise_on_overwrite and key in self.name_dict:
                raise Error("Traverser already exist with the key " + key)
            self.name_dict[key] = class_obj
            logging.debug("Registered %s : %s", key, class_obj)

        def default_traverser(self, configuration):
            """instantiate the default traverser class

            Args:
                configuration (Configuration): Configuration instance

            Returns:
                Traverser instance

            """
            return self.instantiate(self.DEFAULT_TRAVERSER, configuration)

        def instantiate(self, class_name, configuration):
            '''instantiate instances of rule, given name of rule class

            Args:
                class_name (str): name of the class
                configuration (Configuration): Configuration instance

            Returns:
                instance (Traverser): instance of a traverser

            '''
            return self.name_dict[class_name](configuration)
