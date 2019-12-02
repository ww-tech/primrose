"""Abstract reader that uses SQL

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
import os
from primrose.base.reader import AbstractReader
from abc import abstractmethod
import pandas as pd
import logging
import hashlib
from jinja2 import Environment, FileSystemLoader

class AbstractSqlReader(AbstractReader):
    """A reader that explicitly reads from relational DB using SQL and is able to run pd.read_sql."""

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
        return set(['query_json']) # pragma: no cover

    @staticmethod
    def _substitute_query(query_json):
        """Substitue our paramters in a query string. Renders the file as a jinja template to allow for advanced
        query nesting and logic. Uses a jinja environment which searches for templates (sql queries) using relative 
        path and absolute path.
        
        Note:
            given JSON:

            {
                "query": "somequery.sql",
                "parameters": {
                    "x": 3,
                    "y": "somestring"
                }
            }

            and `somequery.sql`:
            ```
            SELECT * from table_01 where x = {x} and y = {y}
            ```

            This function will read in the SQL file via jinja, and if parameters is present, substitue out the parameters, returning the final query string

        Returns:
            query_json (JSON): JSON

        """
        assert 'query' in query_json
        assert os.path.exists(query_json['query'])

        sql_input = {}
        if 'parameters' in query_json:
            sql_input = query_json['parameters']

        jinja_env = Environment(loader=FileSystemLoader(['.', '/']), 
                                variable_start_string='{',
                                variable_end_string='}')
        
        query = jinja_env.get_template(query_json['query'])
        return query.render(**sql_input)

    def _generate_queries(self):
        """generate final queries, using parameter substitution, if requested

        Note:
            query_json should be structured as: \
                "query_json": [ \
                    { \
                        "query": "somequery.sql", \
                        "parameters": { \
                            "x": 1, \
                            "y": 2 \
                        } \
                    }, \
                    { \
                        "query": "someotherquery.sql", \
                        "parameters": { \
                            "x": 3, \
                            "y": "somestring" \
                        } \
                    } \
                ] \
            where parameters is an optional key.

            This generator will return each query string with variables substituted from parameters (if any) 

        Yields:
            query (str): query

        """
        for individual_query_json in self.node_config['query_json']:
            yield self._substitute_query(individual_query_json)

    def _get_key(self, i):
        """Returns the key name for the ith query in the query_json list. This is the key used when adding the dataframe to the dataobject.
        If none is provided, default to `query_i`

        Args:
            i (int): the index of the query
        
        Returns:
            key (str): key name for placement in a `DataObject`
        """
        key = "query_" + str(i)
        this_query_config = self.node_config['query_json'][i]
        if "key" in this_query_config.keys():
            key = this_query_config['key']
        return key

    @abstractmethod
    def get_connection(self):
        """return a database connection, one that is compatible with pd.read_sql"""
        pass # pragma: no cover

    def run(self, data_object):
        """run SQL queries into pandas dataframes

        Args:
            data_object (DataObject): instance of DataObject

        Returns:
            (tuple): tuple containing:

                data_object (DataObject): instance of DataObject

                terminate (bool): terminate the DAG?

        """
        conn = self.get_connection()
        debug = self.node_config.setdefault('debug',False)
        for i, query in enumerate(self._generate_queries()):

            if debug:
                with open('debug_query_' + str(i) + '.sql', 'w') as qfile:
                    qfile.write(os.path.join(query))
            df = self.query_db(query,conn)
            key = self._get_key(i)
            logging.info("Adding df with key %s", key)
            data_object.add(self, df, key)
            if df.empty:
                return data_object, True

        terminate = False
        return data_object, terminate

    def query_db(self, query, conn):
        """Query the db using Bigquery logic if specified.

        Args:
            query (str): SQL query string
            conn (DB connection): database connection

        Returns:
            dataframe (DataFrame): dataframe

        """
        return pd.read_sql(query, con=conn)
