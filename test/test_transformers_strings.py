import pytest
import pandas as pd

from primrose.transformers.strings import StringTransformer

@pytest.fixture
def dataframe():
    df = pd.DataFrame({
        "caps":['TEST!','TEST@'],
        "splits":['a,b,c','def'],
        "strips":['aaa,,,,',',,,bbb'],
        "caps2":['TEST#','TEST$']})
    return df

def test_StringTransformer_single_column_str(dataframe):
    df_test = dataframe.copy()
    df_test['splits'] = df_test['splits'].str.split(',')
    t = StringTransformer('split','splits',',')
    df_out = t.transform(dataframe)
    pd.testing.assert_frame_equal(df_test,df_out)

def test_StringTransformer_multiple_column_list(dataframe):
    df_test = dataframe.copy()
    df_test['caps'] = df_test['caps'].str.lower()
    df_test['caps2'] = df_test['caps2'].str.lower()
    t = StringTransformer('lower',['caps','caps2'])
    df_out = t.transform(dataframe)
    pd.testing.assert_frame_equal(df_test,df_out)

def test_StringTransformer_kwargs(dataframe):
    df_test = dataframe.copy()
    df_test['strips'] = df_test['strips'].str.rstrip(to_strip=',')
    t = StringTransformer('rstrip', ['strips'],to_strip=',')
    df_out = t.transform(dataframe)
    pd.testing.assert_frame_equal(df_test, df_out)

def test__execute_str_method(dataframe):
    df_test = dataframe.copy()
    test_series = df_test['splits'].str.split(',')
    t = StringTransformer('split', 'splits', ',')
    result_series = t._execute_str_method(df_test['splits'])
    pd.testing.assert_series_equal(result_series,test_series)
