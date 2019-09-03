
import math
import pytest
import pandas as pd
import numpy as np

from primrose.transformers.impute import ColumnSpecificImpute

def test_fit():
    d = {'col1': [1, 2, 3],
         'col2': [4, 6, 11.2],
         'col3': [1, 1, 1],
         'col4': [3, 2, 3],
         'col5': [1, 2, 3],
         'col6': [1, 2, 3],
         'col7': [None, None, None],
         'col8': np.nan}
    df = pd.DataFrame(data=d)

    imputer = ColumnSpecificImpute(columns_to_zero=['col1'],columns_to_mean=['col2'],columns_to_median=['col3'],columns_to_mode=['col4','col7', 'col8'], columns_to_infinity=['col5'],columns_to_neg_infinity=['col6'])

    imputer.fit(df)

    encoder = imputer.encoder
    assert encoder['col1'] == 0
    assert math.isclose(encoder['col2'], 21.2/3, abs_tol=0.001)
    assert encoder['col3'] == 1
    assert encoder['col4'] == 3
    assert math.isclose(encoder['col5'], 999999999, abs_tol=0.001)
    assert math.isclose(encoder['col6'], -999999999, abs_tol=0.001)
    assert encoder['col7'] == 0
    assert encoder['col8'] == 0

def test_fit2():
    d = {'col1': [1, 2, 3],
         'col2': [4, 6, 11.2],
         'col3': [1, 1, 1],
         'col4': [3, 2, 3],
         'col5': [1, 2, 3],
         'col6': [1, 2, 3]}
    df = pd.DataFrame(data=d)

    imputer = ColumnSpecificImpute(columns_to_zero=['col1'],columns_to_mean=['col2', 'col1'],columns_to_median=['col3'],columns_to_mode=['col4'], columns_to_infinity=['col5'],columns_to_neg_infinity=['col6'])

    with pytest.raises(Exception) as e:
        imputer.fit(df)
    assert "There are columns in multiple lists {'col1'}" in str(e)

def test_fit3():
    d = {'col1': [1, 2, 3],
         'col2': [4, 6, 11.2],
         'col3': [1, 1, 1],
         'col4': [3, 2, 3],
         'col5': [1, 2, 3],
         'col6': [1, 2, 3]}
    df = pd.DataFrame(data=d)

    imputer = ColumnSpecificImpute(columns_to_zero=['col1'],columns_to_mean=['JUNK'],columns_to_median=['col3'],columns_to_mode=['col4'], columns_to_infinity=['col5'],columns_to_neg_infinity=['col6'])

    with pytest.raises(Exception) as e:
        imputer.fit(df)
    assert "Unrecognized impute column 'JUNK'" in str(e)

def test_transform():
    df = pd.DataFrame()
    imputer = ColumnSpecificImpute(columns_to_zero=['col1'],columns_to_mean=['col2'],columns_to_median=['col3'],columns_to_mode=['col4'], columns_to_infinity=['col5'],columns_to_neg_infinity=['col6'])
    with pytest.raises(Exception) as e:
        imputer.transform(df)
    assert 'ColumnSpecificImpute must train imputations with fit before calling transform' in str(e)

def test_transform2():
    d = {'col1': [1, np.nan, 3],
         'col2': [4, 6, np.nan]}
    df = pd.DataFrame(data=d)

    imputer = ColumnSpecificImpute(columns_to_zero=['col1'],columns_to_mean=['col2'],columns_to_median=[],columns_to_mode=[], columns_to_infinity=[],columns_to_neg_infinity=[])

    imputer.fit(df)

    transormed_df = imputer.transform(df)

    assert list(transormed_df.col1) == [1, 0, 3]
    assert list(transormed_df.col2) == [4, 6, 5]

