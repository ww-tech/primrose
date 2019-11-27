"""Postgres helper

Author(s):
    Wassym Kalouache (wassym.kalouache@weightwatchers.com)

    Carl Anderson (carl.anderson@weightwatchers.com)

"""
import os
from primrose.readers.database_helper import get_env_val

try:
    import psycopg2
    HAS_PSYCOPG2 = True

except ImportError:
    HAS_PSYCOPG2 = False


class PostgresHelper():
    '''
        some utility methods for connecting to postgres
    '''

    @staticmethod
    def extract_postgres_credentials():
        """extract PostgreSQL credentials from config

        Returns:
            (tuple): tuple containing: \
                host (str): host \
                port (int): port \
                database (str): database name \
                username (str): username \
                password (str): password \

        """
        host = get_env_val("POSTGRES_HOST")
        port = int(get_env_val("POSTGRES_PORT"))
        database = get_env_val("POSTGRES_DB")
        username = get_env_val("POSTGRES_USER")
        password = get_env_val("POSTGRES_PASS")
        return (host, port, username, password, database)

    @staticmethod
    def create_db_connection():
        """authenticate with postgres database

            Returns:
                db (connection): postgres db object

        """
        if not HAS_PSYCOPG2:
            raise ImportError("psycopg2 is necessary to establish connection")

        host, port, username, password, database = PostgresHelper.extract_postgres_credentials()
        conn = psycopg2.connect(dbname=database, user=username, password=password, host=host, port=port, sslmode='require')
        return conn

