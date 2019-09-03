
import pytest
import os
import pandas as pd

from primrose.readers.mysql_reader import MySQLReader
from primrose.configuration.configuration import Configuration
import mysql.connector
from primrose.data_object import DataObject, DataObjectResponseType
from unittest.mock import patch

def test_necessary_config():
    assert len(MySQLReader.necessary_config({})) == 1

def test_run(monkeypatch):
    config = {
        "implementation_config": {
            "reader_config": {
                "mynode": {
                    "class": "MySQLReader",
                    "query_json": [
                        {"query": "test/test_mysql.sql"}
                    ],
                    "destinations": []
                }
            }
        }
    }
    configuration = Configuration(None, is_dict_config=True, dict_config=config)

    data_object = DataObject(configuration)

    keys = ["MYSQL_HOST",
            "MYSQL_PORT",
            "MYSQL_DB",
            "MYSQL_USER",
            "MYSQL_PASS"]
    for i, k in enumerate(keys):
        os.environ[k] = str(i)

    reader = MySQLReader(configuration, 'mynode')

    #FIXME need to test reader.run(data_object)

    with patch(target='mysql.connector.connect') as mock:

        def fake_df(query, con):
            return pd.DataFrame({'Name':['Tom', 'nick', 'krish', 'jack'], 'Age':[20, 21, 19, 18]})

        monkeypatch.setattr(pd,'read_sql',fake_df)

        data_object, terminate = reader.run(data_object)

        assert not terminate

        dd = data_object.get('mynode', rtype=DataObjectResponseType.KEY_VALUE.value)
        assert 'query_0' in dd
        df = dd['query_0']
        assert list(df.T.to_dict().values())[0] == {'Name': "Tom", "Age": 20}
