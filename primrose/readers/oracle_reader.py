"""Module with AbstractReader implementation, able to read from Oracle

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
from primrose.base.sql_reader import AbstractSqlReader

class OracleReader(AbstractSqlReader):
    """Runs Oracle queries into pandas dataframes"""

    def get_connection(self):
        """return connection to Oracle DB

        Returns:
            connection to Oracle DB

        """
        raise NotImplementedError("Implement connection creation")
