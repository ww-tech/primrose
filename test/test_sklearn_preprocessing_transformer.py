
from primrose.transformers.sklearn_preprocessing_transformer import SklearnPreprocessingTransformer
from sklearn.preprocessing import StandardScaler
import pytest
import pandas as pd
import math

def test_fit():
    scaler = StandardScaler(with_mean=True, with_std=True)
    t = SklearnPreprocessingTransformer(scaler, columns=['x','y'])

    df = pd.DataFrame({'x':[1,2,3,4,5], 'y': [5,20,30,40,90]})
    t.fit(df)

    transformed_data = t.transform(df)

    assert list(t.preprocessor.mean_) == [3.0, 37.0]
    assert list(t.preprocessor.var_) == [2., 836.]
    assert math.isclose(transformed_data.x.mean(), 0.0, abs_tol = 0.0001)
    #note setting this default degrees of freedom=0 is important as it won't match numpy's std for same data otherwise
    assert math.isclose(transformed_data.x.std(ddof=0), 1.0, abs_tol = 0.0001)
    assert math.isclose(transformed_data.y.mean(), 0.0, abs_tol = 0.0001)
    assert math.isclose(transformed_data.y.std(ddof=0), 1.0, abs_tol = 0.0001)
