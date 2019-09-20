"""Module to implement a Configuration parser which enhances parsing functionality of configparser

Author(s): 
    Michael Skarlinski (michael.skarlinski@weightwatchers.com)

    Carl Anderson (carl.anderson@weightwatchers.com)

"""
import re
import datetime
import jstyleson
import yaml
import json
import hashlib
import os
import logging
from primrose.node_factory import NodeFactory
from primrose.configuration.util import OperationType, ConfigurationError, ConfigurationSectionType
from primrose.configuration.configuration_dag import ConfigurationDag
from primrose.dag.traverser_factory import TraverserFactory

SUPPORTED_EXTS = frozenset(['.json', '.yaml', '.yml'])

class Configuration:
    """Stores user defined configuration for primrose job"""

    def __init__(self, config_location, is_dict_config=False, dict_config=None):
        """Read in configuration file and parse into specified values

        Args:
            config_location (str): valid filepath for file
            is_dict_config (bool): are we passing in a dictionary configuration directly
            dict_config (dict): dictionary object, if is_dict_config

        """
        if is_dict_config:
            ext = None

            if dict_config is None:
                raise Exception('expected dict_config was None')

            if not isinstance(dict_config, dict):
                raise Exception('did not receive expected dict_config')

            dict_str = jstyleson.dumps(dict_config)

            config_str = Configuration.perform_any_config_fragment_substitution(dict_str)

        else:
            logging.info('Loading config file at {}'.format(config_location))
            self.config_location = config_location
            
            if os.path.exists(config_location):
                ext = os.path.splitext(config_location)[1].lower()
                if ext not in SUPPORTED_EXTS:
                    raise ValueError('config file at: {} has improper extension type - please use a .json or .yml file'.format(config_location))

                with open(config_location, 'r') as f:
                    config_str = f.read()

                config_str = Configuration.perform_any_config_fragment_substitution(config_str)

            else:
                raise Exception('config file at: {} not found'.format(config_location))

        if ext is None or ext == '.json':
            self.config = jstyleson.loads(config_str, object_pairs_hook=self.dict_raise_on_duplicates)
        elif ext in ['.yaml', '.yml']:
            self.config = yaml.load(config_str, Loader=yaml.FullLoader)

        assert isinstance(self.config, dict)

        # check top-level keys
        for k in self.config:
            if k not in ConfigurationSectionType.values():
                msg = "Unsupported top-level key: %s. " % k
                msg += "Supported keys are %s" % str(ConfigurationSectionType.values())
                raise ConfigurationError(msg)

        # metadata section can be optional
        self.config_metadata = None
        if ConfigurationSectionType.METADATA.value in self.config:
            self.config_metadata = self.config[ConfigurationSectionType.METADATA.value]

        # implemetation_config section is required
        if not ConfigurationSectionType.IMPLEMENTATION_CONFIG.value in self.config:
            raise ConfigurationError("Did not find required top-level key %s" % ConfigurationSectionType.IMPLEMENTATION_CONFIG.value)

        # keep a copy of the complete configuration
        self.complete_config = self.config.copy()

        # note: config is now just the implementation component of the dictionary
        self.config = self.config[ConfigurationSectionType.IMPLEMENTATION_CONFIG.value]

        # store the dag object
        self.dag = ConfigurationDag(self.config)

        # populate configuration file string and hash
        self.config_string, self.config_hash = self._get_configuration_hash()

        # get the formatted time this file was instantiated
        self.config_time = datetime.datetime.now().strftime('%Y%m%d_%H%M')

        # parse the file into an internal config object
        self._parse_config()

        self.check_config()

    @staticmethod
    def perform_any_config_fragment_substitution(config_str):
        """Given some configuration file content string, look for \
        subtitutions given by `$$FILE=path/to/config/file/fragment.json$$` and make the \
        replacements using the filenames provided\
        For example: \
        { \
            $$FILE=/tmp/metadata.json$$ \
            "implementation_config": { \
                $$FILE= config/read_write_fragment.json $$ \
            } \
        } \
        will inject /tmp/metadata.json into the 2nd line of that config.

        Args:
            config_str (str): content of some configuration file that may or may not contain substition variables

        Returns:
            config_str (str): the post-substituted configuration string

        """
        matches = re.findall(r'.*?\$\$FILE=.*?\$\$.*?', config_str)

        for match in matches:
            filename = match.split("$$FILE=")[1].split("$$")[0].strip()

            if not os.path.exists(filename):
                raise ConfigurationError("Substitution files does not exist: " + filename)

            with(open(filename)) as file:
                fragment = file.read()

            config_str = config_str.replace(match, fragment)

        return config_str

    def dict_raise_on_duplicates(self, ordered_pairs):
        """Reject duplicate keys in JSON string, ie. sections and node names.

        Args:
            ordered_pairs (list): list of key:values from the config \
            Example: ordered_pairs `[('class', 'CsvReader'), ('filename', 'data/tennis.csv'), ('destinations', ['write_output'])]` \
            ordered_pairs `[('read_data', {'class': 'CsvReader', 'filename': 'data/tennis.csv', 'destinations': ['write_output']})]`

        Returns:
            dictionary (dict): dictionary of key (node type) and value (node name)

        """
        #https://stackoverflow.com/questions/14902299/json-loads-allows-duplicate-keys-in-a-dictionary-overwriting-the-first-value

        d = {}
        for k, v in ordered_pairs:
            if k in d:
                raise ConfigurationError("duplicate key: %r" % (k,))
            else:
                d[k] = v
        return d

    def _get_configuration_hash(self):
        """Get configuration file string and hash

        Returns:
            (tuple): tuple containing:

                configuration_string (str):  configuration_string

                configuration_file_hashname (str): terminate the DAG?

        """
        configuration_string = json.dumps(self.complete_config, sort_keys=True)
        configuration_file_hashname = hashlib.sha256(configuration_string.encode('utf-8')).hexdigest()
        return configuration_string, configuration_file_hashname

    def check_metadata(self):
        """checks some dependencies among metadata keys
        
        Raises:
            ConfigurationError is issues found
        
        """
        if self.config_metadata:
        
            if 'traverser' in self.config_metadata:
                classname = self.config_metadata['traverser']
                try:
                    TraverserFactory().instantiate(classname, self)
                except KeyError:
                    raise Exception(classname + " is not a valid and/or registered Traverser")

            if 'data_object' in self.config_metadata:

                cfg = self.config_metadata['data_object']

                if 'read_from_cache' in cfg and (cfg['read_from_cache'] or str(cfg['read_from_cache']).lower() == "true"):

                    if not 'read_filename' in cfg:
                        raise ConfigurationError("metadata.data_object: if read_from_cache==true, you must set 'read_filename'")

                    #just check path exists but not that one can read into a DataObject
                    if not os.path.exists(cfg['read_filename']):
                        raise ConfigurationError("Invalid metadata.data_object.read_filename: " + str(cfg['read_filename']))

                if 'write_to_cache' in cfg and (cfg['write_to_cache'] or str(cfg['write_to_cache']).lower() == "true"):

                    if not 'write_filename' in cfg:
                        raise ConfigurationError("metadata.data_object: if write_to_cache==true, you must set 'write_filename'")

    def check_sections(self):
        """Check that all the sections in implementation are supported ones.
        Either the user supplied metata.section_registry, or they are using default sections

        Raises:
            ConfigurationError if declaring metadata.section_registry and sections from implementation were not found in metadata
            or vice versa, or if using default operations but sections found that were not supported

        """
        if self.config_metadata and 'section_registry' in self.config_metadata and len(self.config_metadata['section_registry']) > 0:
            actual_set = set(self.config.keys())
            user_set = set(self.config_metadata['section_registry'])

            if actual_set != user_set:

                diff = user_set.difference(actual_set)
                if len(diff) > 0:
                    msg = "Following sections from metadata were not found implementation: " + str(diff)
                    raise ConfigurationError(msg)

                diff = actual_set.difference(user_set)
                if len(diff) > 0:
                    msg = "Following sections from implementation were not found in metadata: " + str(diff)
                    raise ConfigurationError(msg)

            logging.info("OK: section_registry sections match implementation sections")
            return

        # otherwise, let's check for default operations
        supported = OperationType.values()
        for section_key in self.config.keys():
            if section_key not in supported:
                raise ConfigurationError("Unspported operation: %s" % section_key)
        logging.info("OK: all sections are supported operations")

    def config_for_instance(self, instance_name):
        """get the configuration for a given node / instance_name

        Returns:
            JSON chunk for this instance

        """
        if not instance_name in self.instance_to_config:
            raise Exception("Unknown key " + instance_name)
        return self.instance_to_config[instance_name]

    def sections_in_order(self):
        """Return list of section names in order, either explicitly from metadata or from default Enum order
        
        Note:
            If there is a non-empty section_run list in metadata return that
            elif there is a non-empty section_registry in metadata return that 
            otherwise return sections present from default OperationType enum. 

            We need this method because the config sections are a dictionary not a list so we can't 
            guarantee order of keys. This method imposes an expected order.

        Returns:
            (tuple): tuple containing:

                section names (list): list of sections

                source (str): where did the list come from? section_run, section_registry, or default?

        """
        if self.config_metadata and 'section_run' in self.config_metadata and len(self.config_metadata['section_run']) > 0:
            return self.config_metadata['section_run'], 'section_run'

        if self.config_metadata and 'section_registry' in self.config_metadata and len(self.config_metadata['section_registry']) > 0:
            return self.config_metadata['section_registry'], 'section_registry'

        return [k for k in OperationType.values() if k in self.config.keys()], 'default'

    def check_config(self):
        """check the configuration as much as we can as early as we can

        Raises:
            various exceptions if any checks fail
        
        """
        self.check_metadata()

        self.check_sections()

        self.nodename_to_classname = {}

        unique_class_keys = set()

        self.instance_to_config = {}

        # check that all child nodes of each section have a Factory.CLASS_KEY field
        for section_key in self.config.keys():

            for child_key in self.config[section_key].keys():

                child = self.config[section_key][child_key]

                self.instance_to_config[child_key] = child

                if not NodeFactory.CLASS_KEY in child:
                    raise ConfigurationError("No class key found in %s.%s" % (section_key,child_key))

                self.nodename_to_classname[child_key] = child[NodeFactory.CLASS_KEY]

                unique_class_keys.add(child[NodeFactory.CLASS_KEY])

                for k in ['destination_pipeline', 'destination_models', 'destination_postprocesses', 'destination_writer']:
                    if k in child:
                        raise Exception("Do you have a old config file? You have %s. Nodes just have 'destinations':[] now", k)

        logging.info("OK: all class keys are present")

        # check that each referenced class is registered in NodeFactory
        for class_key in unique_class_keys:
            if not NodeFactory().is_registered(class_key):
                raise ConfigurationError("Node class " + class_key + " is not registered")

        #check necessary_configs
        for instance_name in self.nodename_to_classname:
            class_key = self.nodename_to_classname[instance_name]
            #section_key = instance_to_section[instance_name]

            configuration_dict = self.instance_to_config[instance_name]

            instance = NodeFactory().instantiate(class_key, self, instance_name)

            NodeFactory().valid_configuration(instance, configuration_dict)

        logging.info("OK: all classes recognized")
        logging.info("OK: good necessary_configs")

        # run our DAG checks. Throws error if not OK
        self.dag.check_dag()

    def _parse_config(self):
        """Assign top level keys to config attributes

        Note:
            Assign top level keys to config attributes. The method then assigns the inner dict to the top level key object

        Returns:
            Nothing, side effect is that top level keys are added as config attributes

        """
        # __init__ json.load checks that implementation_config top-level keys are unique.
        # Here, we check next-level down and error out on first duplicate found
        all_keys = set()
        for key in self.config.keys():
            section_dict = self.config[key]
            self.__setattr__(key, section_dict)
            for k2 in section_dict.keys():
                if k2 in all_keys:
                    raise ConfigurationError("Operations must all have unique names in the configuration. Duplicate key: '%s'" % k2)
                else:
                    all_keys.add(k2)
