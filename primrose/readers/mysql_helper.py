"""MySQL helper

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
import mysql.connector
from primrose.readers.database_helper import get_env_val

class MySQLHelper():
    """"some utility methods for connecting to MySQL"""

    @staticmethod
    def extract_mysql_credentials():
        """extract MySQL credentials from config

        Returns:
            (tuple): tuple containing: \
                host (str): host \
                port (int): port \
                database (str): database name \
                username (str): username \
                password (str): password \

        """
        host = get_env_val("MYSQL_HOST")
        port = int(get_env_val("MYSQL_PORT"))
        database = get_env_val("MYSQL_DB")
        username = get_env_val("MYSQL_USER")
        password = get_env_val("MYSQL_PASS")
        return (host, port, username, password, database)

    @staticmethod
    def create_db_connection():
        '''authenticate with MySQL database

        Returns:
            db (connection object): MySQL db object

        '''

        host, port, username, password, database = MySQLHelper.extract_mysql_credentials()
        conn = mysql.connector.connect(database=database, user=username, passwd=password, host=host, port=port, auth_plugin='mysql_native_password')
        return conn
