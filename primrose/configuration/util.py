"""set of utility methods and enum for configurations

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
from enum import Enum

class ConfigurationError(Exception):
    ''' named error specifically for configuration errors'''
    pass

########################################################################
# note: am adding this Enum in file separate from configuration.py 
# to fend off circular dependencies:
#   configuration -> factory -> individual operation classes -> configuration
########################################################################

class ConfigurationSectionType(Enum):
    """set of top-level sections in config"""
    METADATA = 'metadata'
    IMPLEMENTATION_CONFIG = 'implementation_config'

    @staticmethod
    def values():
        '''list of the enum's values'''
        return list(map(lambda t: t.value, ConfigurationSectionType))

class OperationType(Enum):
    """set of operation type identifiers"""
    reader = 'reader_config'
    pipeline = 'pipeline_config'
    model = 'model_config'
    postprocess = 'postprocess_config'
    writer = 'writer_config'
    dataviz = 'dataviz_config'
    cleanup = 'cleanup_config'

    @staticmethod
    def names():
        """list of the enum's names

        Returns:
            list of (name, OperationType) pairs

        """
        return list(map(lambda t: t.name, OperationType))

    @staticmethod
    def values():
        """list of the enum's values

        Returns:
            list of (value, OperationType) pairs

        """
        return list(map(lambda t: t.value, OperationType))

    @staticmethod
    def values_to_names():
        """value:name dictionary

        Returns:
            dict of {value: name}

        """
        return dict(zip(OperationType.values(), OperationType.names()))
