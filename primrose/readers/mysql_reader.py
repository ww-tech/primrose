"""Module with AbstractReader implementation, able to read from MySQL

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
from primrose.base.sql_reader import AbstractSqlReader
from primrose.readers.mysql_helper import MySQLHelper

class MySQLReader(AbstractSqlReader):
    """Runs MySQL queries into pandas dataframes"""

    def get_connection(self):
        """return connection to MySQL DB

        Returns:
            connection to MySQL DB

        """
        return MySQLHelper.create_db_connection()
