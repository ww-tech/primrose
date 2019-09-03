
import pytest
import pandas as pd
from primrose.transformers.combine import left_merge_dataframe_on_validated_join_keys
from testfixtures import LogCapture
from primrose.transformers.combine import LeftJoinDataCombiner

def test_left_merge_dataframe_on_validated_join_keys():
    corpus = pd.read_csv("test/minimal.csv")
    data_out = left_merge_dataframe_on_validated_join_keys(corpus, right_df=None, join_keys=None)
    assert data_out is corpus

def test_left_merge_dataframe_on_validated_join_keys2():
    corpus = pd.read_csv("test/minimal.csv")

    with pytest.raises(Exception) as e:
        left_merge_dataframe_on_validated_join_keys(corpus, right_df=corpus, join_keys=['JUNK'])
    assert "Join key JUNK not in left Index(['first', 'last'], dtype='object'). Aborting merge." in str(e)

def test_left_merge_dataframe_on_validated_join_keys3():
    corpus = pd.read_csv("test/minimal.csv")
    right_df = pd.read_csv("test/merge_right.csv")

    with pytest.raises(Exception) as e:
        left_merge_dataframe_on_validated_join_keys(corpus, right_df=right_df, join_keys=['first'])
    assert "Join key first not in right Index(['col1', 'col2'], dtype='object'). Aborting merge." in str(e)

def test_left_merge_dataframe_on_validated_join_keys4():
    right_df = pd.read_csv("test/minimal.csv")
    left_df = pd.read_csv("test/merge_right2.csv")

    with pytest.raises(Exception) as e:
        left_merge_dataframe_on_validated_join_keys(left_df, right_df=right_df, join_keys=['first'])
    assert "Cannot cast join key first as int64" in str(e)

def test_left_merge_dataframe_on_validated_join_keys5():
    left_df = pd.read_csv("test/minimal.csv")
    right_df = pd.read_csv("test/merge_right3.csv")

    out = left_merge_dataframe_on_validated_join_keys(left_df, right_df=right_df, join_keys=['first'])
    assert out.shape[0] == 2

    assert list(out.T.to_dict().values())[0] == {'first': 'joe', 'last': 'doe', 'age': 47}
    assert list(out.T.to_dict().values())[1] == {'first': 'mary', 'last': 'poppins', 'age': 42}

def test_left_merge_dataframe_on_validated_join_keys_fanout():
    left_df = pd.read_csv("test/minimal.csv")
    right_df = pd.read_csv("test/merge_right4.csv")


    with LogCapture() as l:
        out = left_merge_dataframe_on_validated_join_keys(left_df, right_df=right_df, join_keys=['first'])
    l.check(
        ('root', 'WARNING', 'Merge increased data size by 1 rows.')
    )

    assert out.shape[0] == 3

    assert list(out.T.to_dict().values())[0] == {'first': 'joe', 'last': 'doe', 'age': 47}
    assert list(out.T.to_dict().values())[1] == {'first': 'joe', 'last': 'doe', 'age': 48}
    assert list(out.T.to_dict().values())[2] == {'first': 'mary', 'last': 'poppins', 'age': 42}

def test_LeftJoinDataCombiner():
    combiner = LeftJoinDataCombiner(["first"])
    # does nothing
    combiner.fit(None)

    corpus = pd.read_csv("test/minimal.csv")

    with LogCapture() as l:
        out = combiner.transform([corpus])
    l.check(
        ('root', 'WARNING', 'Combiner needs at least two reader inputs, passing unchanged data.')
    )
    assert out == [corpus]

def test_LeftJoinDataCombiner2():
    with pytest.raises(Exception) as e:
         LeftJoinDataCombiner(["first"]).transform("JUNK")
    assert "In this transformer, data needs to be a list of dataframes" in str(e)

def test_LeftJoinDataCombiner3():
    with pytest.raises(Exception) as e:
         LeftJoinDataCombiner(["first"]).transform(["JUNK", "JUNK2"])
    assert "LeftJoinDataCombiner must operate on an iterable of pandas.DataFrame objects." in str(e)

def test_LeftJoinDataCombiner4():
    left_df = pd.read_csv("test/minimal.csv")
    right_df = pd.read_csv("test/merge_right3.csv")
    data_list = [left_df, right_df]
    out = LeftJoinDataCombiner(["first"]).transform(data_list)
    assert out.shape[0] == 2

    assert list(out.T.to_dict().values())[0] == {'first': 'joe', 'last': 'doe', 'age': 47}
    assert list(out.T.to_dict().values())[1] == {'first': 'mary', 'last': 'poppins', 'age': 42}


