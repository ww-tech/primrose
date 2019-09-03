
import pytest
from primrose.base.transformer_sequence import TransformerSequence
from primrose.base.transformer import AbstractTransformer

@pytest.fixture
def transformer_class():
    class TestTransformer(AbstractTransformer):
        def fit(self, data):
            return data
        def transform(self, data):
            return data
    return TestTransformer()

def test_init(transformer_class):
    t1 = transformer_class
    t2 = transformer_class
    ts = TransformerSequence([t1, t2])
    array = list(ts.transformers())
    assert len(array) == 2
    assert array[0] == t1

def test_add(transformer_class):
    ts = TransformerSequence()
    assert len(ts.sequence) == 0
    t = transformer_class
    ts.add(t)
    assert len(ts.sequence) == 1
    
    with pytest.raises(Exception) as e:
        ts.add(0)
    assert 'Transformer needs to extend AbstractTransformer' in str(e)

    ts.add(transformer_class)
    ts.add(transformer_class)

    array = list(ts.transformers())
    assert len(array) == 3
    assert array[0] == t