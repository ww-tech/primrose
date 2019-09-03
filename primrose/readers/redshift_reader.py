"""Module with AbstractReader implementation, able to read from AWS Redshift

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
from primrose.base.sql_reader import AbstractSqlReader

class OracleReader(AbstractSqlReader):
    """Runs Redshift queries into pandas dataframes"""

    def get_connection(self):
        """return connection to Redshift DB

        Returns:
            connection to Redshift DB

        """
        raise NotImplementedError("Implement connection creation")
