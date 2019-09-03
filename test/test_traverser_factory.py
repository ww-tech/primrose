import pytest
from primrose.dag.depth_first_traverser import DepthFirstTraverser
from primrose.dag.config_layer_traverser import ConfigLayerTraverser
from primrose.dag.traverser_factory import TraverserFactory

def test_instantiate():
    assert isinstance(TraverserFactory().instantiate('DepthFirstTraverser', None), DepthFirstTraverser)
    assert isinstance(TraverserFactory().instantiate('ConfigLayerTraverser', None), ConfigLayerTraverser)
    assert isinstance(TraverserFactory().default_traverser(None), ConfigLayerTraverser)