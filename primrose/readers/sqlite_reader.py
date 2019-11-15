"""Module with AbstractReader implementation, able to read from SQLite

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
import sqlite3
from primrose.base.sql_reader import AbstractSqlReader

class SQLiteReader(AbstractSqlReader):
    """Runs SQLite queries into pandas dataframes"""

    @staticmethod
    def necessary_config(node_config):
        """Return a list of necessary configuration keys within the implementation

        Args:
            node_config (dict): set of parameters / attributes for the node

        Note:
            After adding this list, validation automatically occurs before instantiation in the pipeline factory.

        Returns:
            set of keys necessary to run implementation

        """
        return set(['filename', 'query_json'])

    def get_connection(self):
        """return connection to SQLite DB

        Returns:
            connection to SQLite DB file

        """
        return sqlite3.connect(self.node_config['filename'])
