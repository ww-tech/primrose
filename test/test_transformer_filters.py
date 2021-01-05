import pytest
import pandas as pd

from primrose.transformers.filter import FilterByPandasExpression, FilterByUnivariateQuantile


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



def test_univariate_transform():

    df = pd.DataFrame([[1000],[2000],[3000],[4000],[5000],[6000],[7000],[8000],[9000],[20000], [1e6], [-1e5]], columns=['tmp'])

    good_data = FilterByUnivariateQuantile(features_to_filter=['tmp'], multiplier=2.0).transform(df)
    assert good_data.shape == (9, 1)
