
import pytest
import pandas as pd

from primrose.transformers.filter import FilterByPandasExpression

def test_transform():
    df = pd.read_csv("test/tennis.csv")

    ft = FilterByPandasExpression([["outlook", "==", "sunny"]])

    assert not ft.fit(df)

    assert df.shape == (14, 6)

    filtered_df = ft.transform(df)

    assert filtered_df.shape == (5, 6)

def test_transform2():
    df = pd.read_csv("test/tennis.csv")

    ft = FilterByPandasExpression([])

    assert df.shape == (14, 6)

    filtered_df = ft.transform(df)

    assert filtered_df.shape == df.shape

def test_transform3():
    ft = FilterByPandasExpression([["outlook", "==", "sunny"]])
    with pytest.raises(Exception) as e:
        ft.transform(42)
    assert "Data is not a pandas DataFrame" in str(e)

def test_transform4():
    df = pd.read_csv("test/tennis.csv")
    ft = FilterByPandasExpression([["JUNK", "==", "sunny"]])
    with pytest.raises(Exception) as e:
        ft.transform(df)
    assert "Unrecognized filter column 'JUNK'" in str(e)

def test_transform5():
    df = pd.read_csv("test/tennis.csv")
    ft = FilterByPandasExpression([["outlook", "JUNK", "sunny"]])
    with pytest.raises(Exception) as e:
        ft.transform(df)
    assert "Unsupported filter operation 'JUNK'" in str(e)

