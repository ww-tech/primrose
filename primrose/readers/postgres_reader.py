"""Module with AbstractReader implementation, able to read from PostgreSQL

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
from primrose.base.sql_reader import AbstractSqlReader
from primrose.readers.postgres_helper import PostgresHelper

class PostgresReader(AbstractSqlReader):
    """Runs PostgreSQL queries into pandas dataframes"""

    def get_connection(self):
        """return connection to PostgreSQL DB

        Returns:
            connection to PostgreSQL DB

        """
        return PostgresHelper.create_db_connection()
