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
from jinja2 import Environment, FileSystemLoader
from jinja2.exceptions import TemplateNotFound
import hashlib
import os
import logging
import importlib
import glob
from primrose.node_factory import NodeFactory
from primrose.configuration.util import OperationType, ConfigurationError, ConfigurationSectionType
from primrose.configuration.configuration_dag import ConfigurationDag
from primrose.dag.traverser_factory import TraverserFactory


SUPPORTED_EXTS = frozenset(['.json', '.yaml', '.yml'])
CLASS_ENV_PACKAGE_KEY = 'PRIMROSE_EXT_NODE_PACKAGE'

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
        jinja_env = Environment(loader=FileSystemLoader(['.', '/']))
        try:
            config_str_template = jinja_env.from_string(config_str)
            config_str = config_str_template.render()
        except(TemplateNotFound) as error:
            filenames = str(error)
            raise ConfigurationError(f"Substitution files do not exist: {filenames}")
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

                unique_class_keys.add((child[NodeFactory.CLASS_KEY], child.get(NodeFactory.CLASS_PREFIX)))

                for k in ['destination_pipeline', 'destination_models', 'destination_postprocesses', 'destination_writer']:
                    if k in child:
                        raise Exception("Do you have a old config file? You have %s. Nodes just have 'destinations':[] now", k)

        logging.info("OK: all class keys are present")

        # get class_prefixes by traversing node package
        unique_class_keys = self._traverse_node_package(unique_class_keys)

        # check that each referenced class is registered in NodeFactory
        for class_key, class_prefix in unique_class_keys:
            if not NodeFactory().is_registered(class_key):
                try:
                    logging.info(f'attempting to register {class_key}')
                    self._register_class(class_key, class_prefix)
                except:
                    raise ConfigurationError(f"Cannot register node class {class_key}")

        #check necessary_configs
        for instance_name in self.nodename_to_classname:
            class_key = self.nodename_to_classname[instance_name]

            configuration_dict = self.instance_to_config[instance_name]

            instance = NodeFactory().instantiate(class_key, self, instance_name)

            NodeFactory().valid_configuration(instance, configuration_dict)

        logging.info("OK: all classes recognized")
        logging.info("OK: good necessary_configs")

        # run our DAG checks. Throws error if not OK
        self.dag.check_dag()

    def _register_class(self, class_key, class_prefix):
        """Register a class specified in the config file.

        Args:
            class_key (str): class key to register
            class_prefix(str): the prefix of the class to register. Can be in `path.to.module` format,
                or a full path `path/to/module`.

        Returns:
            None - attempts to register the class with it's default name
        """
        # convert to string before checking if file
        if class_prefix is None:
            class_prefix = ''

        if os.path.isfile(class_prefix):
            modulename = self._import_file(class_key, class_prefix)

        # loading from module
        else:
            if self.config_metadata:
                if 'class_package' in self.config_metadata:
                    class_package = self.config_metadata['class_package']
                    prefix = '.'.join(filter(None, [class_package, class_prefix]))
            else:
                prefix = class_prefix

            modulename = importlib.import_module(prefix)

        clz = getattr(modulename, class_key)
        NodeFactory().register(None, clz)

    @staticmethod
    def _import_file(full_name, path):
        """Import module from given path

        Args:
            full_name (str): full module name to import
            path (str): full path to python module

        Returns:
            module imported from the path with given name
        """
        spec = importlib.util.spec_from_file_location(full_name, path)
        mod = importlib.util.module_from_spec(spec)

        spec.loader.exec_module(mod)
        return mod

    def _get_file_candidates(self):
        """Get file candidates to search through when specifying a class package.

        Priority will first consider environment variable PRIMROSE_EXT_NODE_PACKAGE. If unset, will
        search the configuration metadata for key `class_package`. If nothing is specified, in either
        location, an empty list is returned.

        Returns:
            list of potential files to search for classes to register
        """
        # for now assume packages/top level only
        if CLASS_ENV_PACKAGE_KEY in os.environ:
            pkg_name = os.environ[CLASS_ENV_PACKAGE_KEY]
        elif self.config_metadata:
            if 'class_package' in self.config_metadata:
                pkg_name = self.config_metadata['class_package']
            else:
                return []
        else:
            return []
        # look for path to module to find potential file candidates
        try:
            # if we are passed something like __init__.py, grab the package
            if os.path.isfile(pkg_name):
                pkg_name = os.path.dirname(pkg_name)
            # if we have an actual package from pip install
            if not os.path.isdir(pkg_name):
                pkg_name = os.path.dirname(importlib.import_module(pkg_name).__file__)
        except ModuleNotFoundError:
            logging.warning("Could not find module specified for external node configuration")
            return []

        candidates = glob.glob(os.path.join(pkg_name, '**', '*.py'), recursive=True)

        return candidates

    def _traverse_node_package(self, unique_class_keys, overwrite=False):
        """Traverse node package to find classes in the DAG to register.

        Args:
            unique_class_keys (tuple(str, str)): a tuple of class names and prefixes.
            overwrite (boolean, Optional): If a prefix is already set from the configuration, do we overwrite?

        Returns:
            (class_name, class_prefix) tuples
        """
        class_keys_prefix = []
        candidates = self._get_file_candidates()
        for filename in candidates:
            with open(filename, 'r') as f:
                src_str = f.read()
                for class_key, class_key_prefix in unique_class_keys:
                    if (class_key_prefix is None) or (overwrite == True):
                        pattern = "class\s" + class_key + "\(?.*\)?:\s"
                        if re.search(pattern, src_str) is not None:
                            class_keys_prefix.append((class_key, filename))
                            continue
        for class_key, class_key_prefix in unique_class_keys:
            if class_key not in [x[0] for x in class_keys_prefix]:
                class_keys_prefix.append((class_key, class_key_prefix))
        return class_keys_prefix

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
