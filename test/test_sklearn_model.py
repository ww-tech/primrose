import pytest
from primrose.models.sklearn_model import SklearnModel
from sklearn.linear_model import LinearRegression

def test__instantiate_model():
    model = SklearnModel._instantiate_model("linear_model.LinearRegression", args=None)
    assert isinstance(model, LinearRegression)

    with pytest.raises(Exception) as e:
        SklearnModel._instantiate_model("linear_model.junk", args=None)
    assert "Sklearn model junk not found in linear_model module" in str(e)


