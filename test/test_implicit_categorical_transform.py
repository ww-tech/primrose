import pytest
import pandas as pd
from sklearn import preprocessing

from primrose.transformers.categoricals import ImplicitCategoricalTransform


@pytest.fixture()
def data():
    return pd.DataFrame({'one': ['a', 'b'], 'two': ['c', 'c']})


def test_implicit_cat_fit(data):

    ict = ImplicitCategoricalTransform('two')

    out = ict.fit(data)

    assert isinstance(ict.target_encoder, preprocessing.LabelEncoder)
    assert isinstance(ict._encoder['one'], preprocessing.LabelEncoder)
    assert isinstance(ict._encoder['two'], preprocessing.LabelEncoder)


def test_implicit_cat_transform(data):

    ict = ImplicitCategoricalTransform('two')

    out = ict.fit(data)

    out = ict.transform(out)

    assert set(out['one'].unique()) == set([0, 1])
