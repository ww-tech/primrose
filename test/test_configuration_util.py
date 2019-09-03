
import pytest
from primrose.configuration.util import *

def test_ConfigurationSectionType():
    assert ConfigurationSectionType.values() == ['metadata', 'implementation_config']

def test_OperationType_names():
    assert OperationType.names() == ['reader', 'pipeline', 'model', 'postprocess', 'writer', 'dataviz', 'cleanup']

def test_OperationType_values():
    assert OperationType.values() == ['reader_config', 'pipeline_config', 'model_config', 'postprocess_config', 'writer_config', 'dataviz_config', 'cleanup_config']

def test_values_to_names():
    assert OperationType.values_to_names() == {'reader_config': 'reader', 'pipeline_config': 'pipeline', 'model_config':'model', 'postprocess_config':'postprocess', 'writer_config':'writer', 'dataviz_config':'dataviz', 'cleanup_config':'cleanup'}
