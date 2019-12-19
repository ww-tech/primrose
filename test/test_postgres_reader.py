
import pytest
import os
import pandas as pd

from primrose.readers.postgres_reader import PostgresReader
from primrose.configuration.configuration import Configuration
from primrose.data_object import DataObject, DataObjectResponseType
from unittest.mock import patch

def test_necessary_config():
    assert len(PostgresReader.necessary_config({})) == 1


@pytest.mark.optional
@pytest.mark.postgres
def test_run(monkeypatch):
    config = {
        "implementation_config": {
            "reader_config": {
                "mynode": {
                    "class": "PostgresReader",
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

    keys = ["POSTGRES_HOST",
            "POSTGRES_PORT",
            "POSTGRES_DB",
            "POSTGRES_USER",
            "POSTGRES_PASS"]
    for i, k in enumerate(keys):
        os.environ[k] = str(i)

    reader = PostgresReader(configuration, 'mynode')

    with patch('psycopg2.connect') as mock_connect:

        def fake_df(query, con):
            return pd.DataFrame({'Name':['Tom', 'nick', 'krish', 'jack'], 'Age':[20, 21, 19, 18]})
        monkeypatch.setattr(pd,'read_sql',fake_df)

        data_object, terminate = reader.run(data_object)

        assert not terminate

        dd = data_object.get('mynode', rtype=DataObjectResponseType.KEY_VALUE.value)
        assert 'query_0' in dd
        df = dd['query_0']
        assert list(df.T.to_dict().values())[0] == {'Name': "Tom", "Age": 20}
