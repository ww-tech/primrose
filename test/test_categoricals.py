
import pytest
import pandas as pd
from primrose.transformers.categoricals import ExplicitCategoricalTransform

def test__process_transformations():
    categoricals = {
        "currency": {
            "transformations": [
                "{x}[{x}=='USD'] = 1",
                "{x}[{x}=='EUR'] = 2"
            ],
            "rename": "money",
            "to_numeric": True
        }
    }
    x = "data['{}']".format("currency")
    input_data = categoricals["currency"]

    data = pd.DataFrame(data={"currency": ['EUR', 'USD', 'USD']})

    ExplicitCategoricalTransform._process_transformations(data, input_data, "currency", x)

    assert list(data.currency) == [2, 1, 1]
    assert "money" not in list(data.columns)

    data, new_name = ExplicitCategoricalTransform._process_rename(data, input_data, "currency")

    assert "money" in list(data.columns)
    assert list(data.money) == [2, 1, 1]

    assert str(data.dtypes['money']) == "object"

    data = ExplicitCategoricalTransform._process_numeric(data, input_data, new_name)

    assert str(data.dtypes['money']) == "int64"

def test__process_numeric():
    input_data = {
        "to_numeric": True
    }
    data = pd.DataFrame(data={"currency": ['EUR', 'USD', 'USD']})
    data = ExplicitCategoricalTransform._process_numeric(data, input_data, "currency")

    assert list(data.currency) == 3*[ExplicitCategoricalTransform.DEFAULT_NUMERIC]

def test__process_numeric_no_config_key():
    input_data = {}
    data = pd.DataFrame(data={"currency": ['EUR', 'USD', 'USD']})
    data = ExplicitCategoricalTransform._process_numeric(data, input_data, "currency")

    assert data is not None

def test__process_rename():
    input_data = {
        "transformations": [
            "{x}[{x}=='USD'] = 1",
            "{x}[{x}=='EUR'] = 2"
        ]
    }
    data = pd.DataFrame(data={"currency": ['EUR', 'USD', 'USD']})
    data, new_name = ExplicitCategoricalTransform._process_rename(data, input_data, "currency")
    assert new_name == "currency"

def test_transform():
    categoricals = {
        "currency": {
            "transformations": [
                "{x}[{x}=='USD'] = 1",
                "{x}[{x}=='EUR'] = 2"
            ],
            "rename": "money",
            "to_numeric": True
        }
    }
    data = pd.DataFrame(data={"currency": ['EUR', 'USD', 'USD']})
    t = ExplicitCategoricalTransform(categoricals)

    #does nothing
    t.fit(data)

    data_out = t.transform(data)
    assert list(data_out.money) == [2, 1, 1]
    assert str(data_out.dtypes['money']) == "int64"
