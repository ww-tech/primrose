"""Module to handle book keeping of data

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
import os
from collections import defaultdict
import logging
from enum import Enum
import dill

class DataObjectResponseType(Enum):
    """Type of object when getting data from DataObject

    INSTANCE_KEY_VALUE = dictionary of instance_name keys and their data dictionaries: 
        {'instance_name': {'key':value}, 'instance_name2': {'key2':value2}, ... }
        e.g. {'corpus_reader': {'data': dataframe}}
        this is useful if there is a set of upstream data arriving from mulitple sources

    KEY_VALUE = dictonary of data for given instance name: {'key':value}
        e.g. {'data': dataframe} or {'data': dataframe, 'query': 'select * from table'}
        this is useful if there are multiple keys for a given instance_name
        or if you want to explicitly check against expected keys

    VALUE = value only (for 1st or only instance name and for only key): 
        e.g. dataframe
        this is useful if you know a node in DAG has only a single upsteam source and only a single
        value. Readers are often a good example as they typically read in and provide a single data frame

    """
    INSTANCE_KEY_VALUE = "ikv"
    KEY_VALUE = "kv"
    VALUE = "v"

    @staticmethod
    def values():
        ''' list of all the values in the enum'''
        return list(map(lambda t: t.value, DataObjectResponseType))

class DataObject():
    """DataObject: a container for "data" (strings, dicts, arbitrary objects etc)"""

    # when we are storing some basic data, what is key we use?
    DATA_KEY = 'data'

    DEFAULT_RESPONSE_TYPE = DataObjectResponseType.KEY_VALUE.value

    def __init__(self, config):
        """instantiate the DataObject

        Args:
            config (Configuration): Configuration instance

        """
        #assert isinstance(config, Configuration)
        self.config = config
        self.data_dict = defaultdict(dict)

    @staticmethod
    def read_from_cache(filename):
        """restore DatObject from dill-cached file

        Args:
            filename (str): cache filename

        Returns:
            data_object (DataObject): DataObject instance from cache

        """
        assert os.path.exists(filename)
        with open(filename, 'rb') as f:
            data_object = dill.load(f)
            assert isinstance(data_object, DataObject)
            return data_object

    def write_to_cache(self, filename):
        """write data_object (self) to dill-cache

        Returns:
            nothing. Side effect is to cache object to file

        """
        with open(filename, 'wb') as f:
            logging.info("Cache DataObect to " + filename) 
            dill.dump(self, f)

    def __repr__(self):
        """string representation of the class
        
        Returns:
            string representation

        """
        return self.__class__.__name__ + ":" + str(self.data_dict)

    def add(self, requestor, data, key=DATA_KEY, overwrite=False):
        """for requestor's instance_name, set key:data in storage

        Args:
            requestor (Node): is object (model, pipeline, writer etc) that has instance_name attribute
            data (object): some object
            key (string): if not supplied default data key is used

        Returns:
            nothing.
        """
        assert isinstance(key, str)
        if not overwrite and requestor.instance_name in self.data_dict and key in self.data_dict[requestor.instance_name]:
            raise Exception("Key already exists for %s:%s" % (requestor.instance_name, key))

        # as this is  defaultdict(dict) it should update existing dict with new key
        self.data_dict[requestor.instance_name][key] = data

    def get(self, instance_name, pop_data=False, rtype=DEFAULT_RESPONSE_TYPE):
        """get some data from storage, optionally popping it off.

        Args:
            instance_name (str): name of node in DAG
            pop_data (bool): boolean, whether to pop data from storage
            rtype (DataObjectResponseType): DataObjectResponseType value, specifying response type

        Returns:
            data of desired DataObjectResponseType, selected with rtype

        Raises:
            Exception if unrecognixzed rtype or keys

        """
        assert instance_name
        assert isinstance(instance_name, str)

        assert rtype
        if rtype not in DataObjectResponseType.values():
            raise Exception("Unrecognized rtype: %s" % rtype)

        if not instance_name in self.data_dict.keys():
            raise Exception("Key not found: %s" % instance_name)

        d = None
        if pop_data:
            d = self.data_dict.pop(instance_name)
        else:
            d = self.data_dict[instance_name]

        if rtype == DataObjectResponseType.INSTANCE_KEY_VALUE.value:
            return {instance_name: d}
        elif rtype == DataObjectResponseType.KEY_VALUE.value:
            return d
        elif rtype == DataObjectResponseType.VALUE.value:
            # only if there is a dict with single key can we return value
            if isinstance(d, dict):
                keys = list(d.keys())
                if len(keys) == 1:
                    return d[keys[0]]
                else:
                    logging.info("Multiple keys found: %s" % str(keys))
            # otherwise return whatever d is
            return d

    def upstream_keys(self, instance_name, operation_type_filter=None):
        """get list of upstream node names for a given input requestor node

        Args:
            instance_name (str): name of requestor
            operation_type_filter (optional): type of operation type to filter in

        Returns:
            list of keys, if any

        """
        assert instance_name
        keys = self.config.dag.upstream_keys(instance_name)

        if operation_type_filter:
            keys = [k for k in keys if self.config.dag.node_map[k] == operation_type_filter.value]

        return keys

    def get_upstream_data(self, instance_name, pop_data=False, rtype=DEFAULT_RESPONSE_TYPE, operation_type_filter=None):
        """Return data from upstream source(s), choose to pop or not from the dict

        Note:
            returns dictionary, where keys are instance_names and each value is a dictionary.
            However, if
            i) there is only 1 upstream key
            and
            ii) value_only=True
            then return the value only.
            
            This option is useful if you expect 1 upstream source only and it returns
            a single artifact, such as a single dataframe. In that case just the dataframe
            is returned

        Returns:
            object (type depends on DEFAULT_RESPONSE_TYPE)

        Raises:
            Exception if no upstream data found

        """
        assert instance_name
        upstream_keys = self.upstream_keys(instance_name, operation_type_filter=operation_type_filter)

        # While upstream_keys list the upstream sources, it doesn't mean any data were set for them.
        # Thus, we need to see which of these we have data for
        upstream_keys_with_data = set(upstream_keys).intersection(set(self.data_dict.keys()))

        if not upstream_keys_with_data:
            raise Exception("No upstream keys with data found for %s" % instance_name)

        # if there are multiple keys, returning key:value or value makes no sense so ignore rtype
        if len(upstream_keys) > 1:
            return {iname_key: self.get(iname_key, pop_data) for iname_key in upstream_keys}

        # we must have only 1 key now so this is now simple:
        return self.get(upstream_keys[0], pop_data, rtype)

    def get_filtered_upstream_data(self, instance_name, filter_for_key):
        """Upstream data where first level dict keys are first checked for the presence of a filter key
        
        Args:
            instance_name (str): name of instance to look upstream from

            filter_for_key (str): the key data was saved with (not instance name but the data value key)

        Returns:
            dictionary of stored data for that instance if only one matching dict,
            if more than one valid dictionary then return list of dicts,
            None otherwise

        """
        data = self.get_upstream_data(instance_name, pop_data=False,
                                        rtype=DataObjectResponseType.INSTANCE_KEY_VALUE.value)

        data_to_return = []

        for ikey in data:
            if filter_for_key in data[ikey]:
                data_to_return.append(data[ikey])

        if len(data_to_return) == 1:
            return data_to_return[0]

        elif not data_to_return:  # pep8 for empty list checks
            return None

        else:
            return data_to_return
