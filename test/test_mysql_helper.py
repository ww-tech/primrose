
import pytest
import os
from primrose.readers.mysql_helper import MySQLHelper

def test_extract_mongo_credentials():
    keys=["MYSQL_HOST",
            "MYSQL_PORT",
            "MYSQL_DB",
            "MYSQL_USER",
            "MYSQL_PASS"]
    for k in keys:
        if k in os.environ:
            del os.environ[k]

    with pytest.raises(Exception) as e:
        MySQLHelper.extract_mysql_credentials()
    assert 'Did not find env variable for MYSQL_HOST' in str(e)

def test_extract_mongo_credentials2():
    keys=["MYSQL_HOST",
            "MYSQL_PORT",
            "MYSQL_DB",
            "MYSQL_USER",
            "MYSQL_PASS"]
    for i,k in enumerate(keys):
        os.environ[k] = str(i)

    host, port, username, password, database = MySQLHelper.extract_mysql_credentials()
    assert host == "0"
    assert port == 1
    assert username == "3"
    assert password == "4"
    assert database == "2" 