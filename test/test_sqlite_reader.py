import pytest
import os
import sys
import sqlite3
from primrose.configuration.configuration import Configuration
from primrose.readers.sqlite_reader import SQLiteReader
from primrose.data_object import DataObject, DataObjectResponseType

def test_necessary_config():
    assert isinstance(SQLiteReader.necessary_config({}), set)
    assert len(SQLiteReader.necessary_config({})) == 2

def test_read():
    config = {
        "implementation_config":{
            "reader_config": {
                "mynode": {
                    "class": "SQLiteReader",
                    "filename": "test/test_sqlite.db",
                    "query_json": [
                        {"query": "test/test_sqlite.sql"}
                    ],
                    "destinations": []
                }
            }
        }
    }

    filename = "test/test_sqlite.db"
    if os.path.exists(filename):
        os.remove(filename)

    conn = sqlite3.connect(filename)
    c = conn.cursor()
    c.execute("create table test(firstname text, lastname text);")
    c.execute("insert into test(firstname, lastname) values('joe', 'doe'), ('mary','poppins');")
    conn.commit()
    conn.close()

    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)
    reader = SQLiteReader(configuration, "mynode")

    data_object = DataObject(configuration)

    data_object, terminate = reader.run(data_object)
    assert not terminate
    dd = data_object.get('mynode', rtype=DataObjectResponseType.KEY_VALUE.value)
    assert "query_0" in dd
    df = dd['query_0']
    assert df is not None
    assert df.shape == (2, 2)

    if os.path.exists(filename):
        os.remove(filename)
