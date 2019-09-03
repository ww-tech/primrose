
import pytest
import os
from primrose.base.sql_reader import AbstractSqlReader
from primrose.configuration.configuration import Configuration
from primrose.readers.sqlite_reader import SQLiteReader
import sqlite3

def test__substitute_query():

    query = """SELECT * FROM TABLE where field={x} and otherfield='{y}'"""

    filename = "test/test_substitute.sql"
    if os.path.exists(filename):
        os.remove(filename)

    with open(filename, 'w') as f:
        f.write(query)

    individual_query_json = {
        "query": "test/test_substitute.sql",
        "parameters": {
            "x": 3,
            "y": "somestring"
        }
    }
    class TestSqlReader(AbstractSqlReader):
        pass   
    
    transformed_query = TestSqlReader._substitute_query(individual_query_json)

    assert transformed_query == "SELECT * FROM TABLE where field=3 and otherfield='somestring'"

    if os.path.exists(filename):
        os.remove(filename)

def test__generate_queries():

    config = {
        "implementation_config": {
            "reader_config": {
                "myreader": {
                    "class": "SQLiteReader",
                    "filename": "test/test_abstract_sqlite.db",
                    "destinations": [],
                    "query_json": [
                        {
                            "query": "test/test_substitute1.sql",
                            "parameters": {
                                "firstname": "joe",
                                "lastname": "doe2"
                            }
                        },
                        {
                            "query": "test/test_substitute2.sql",
                            "parameters": {
                                "firstname": 'mary',
                                "lastname": "poppins"
                            }
                        }
                    ]
                }
            }
        }
    }

    filenames = ["test/test_substitute1.sql", "test/test_substitute2.sql"]

    for filename in filenames:

        query = """SELECT * FROM test where firstname='{firstname}' and lastname='{lastname}'"""

        if os.path.exists(filename):
            os.remove(filename)

        with open(filename, 'w') as f:
            f.write(query)

    db_filename = "test/test_abstract_sqlite.db"
    if os.path.exists(db_filename):
        os.remove(db_filename)

    conn = sqlite3.connect(db_filename)
    c = conn.cursor()
    c.execute("create table test(firstname text, lastname text);")
    c.execute("insert into test(firstname, lastname) values('joe', 'doe'), ('joe', 'doe2'), ('mary','poppins');")
    conn.commit()
    conn.close()

    configuration = Configuration(config_location=None, is_dict_config=True, dict_config=config)

    reader = SQLiteReader(configuration, "myreader")
    queries = [q for q in reader._generate_queries()]

    assert len(queries) == 2
    assert queries[0] == "SELECT * FROM test where firstname='joe' and lastname='doe2'"
    assert queries[1] == "SELECT * FROM test where firstname='mary' and lastname='poppins'"

    for filename in filenames:
        if os.path.exists(filename):
            os.remove(filename)
    
    if os.path.exists(db_filename):
        os.remove(db_filename)