
import pytest
from primrose.util import RunModes

def test_values():
    values = RunModes.values()
    assert len(values) == 3
    assert set(values) == set(['train', 'predict' ,'eval'])

def test_names():
    names = RunModes.names()
    assert len(names) == 3
    assert set(names) == set(['TRAIN', 'PREDICT' ,'EVAL'])