"""Module with AbstractReader implementation, able to read datasets from R

    This is a simple example to show that primrose can work with other languages.

Author(s):
    Carl Anderson (carl.anderson@ww.com)

"""
from primrose.base.reader import AbstractReader
import logging
import pandas as pd

class RReader(AbstractReader):
    """Reads in canned dataset from R"""

    @staticmethod
    def necessary_config(node_config):
        """Return a list of necessary configuration keys within the implementation

        Args:
            dataset (string): name of dataset in R

        Returns:
            set of keys necessary to run implementation

        """
        return set(['dataset'])

    def run(self, data_object):
        """Read canned dataset from R to a pandas dataframe

        Returns:
            data_object (DataObject): DataObject instance
            terminate (bool): should we terminate the DAG? true or false

        """
        dataset = self.node_config['dataset']
        logging.info('Reading {} from R'.format(dataset))

        from rpy2.robjects.packages import importr, data
        datasets = importr('datasets')
        r_env = data(datasets).fetch(dataset)
        data = r_env[dataset]

        # at time of writing, rpy2's R dataframe to pandas dataframe was not fully supported
        # However, as python list() seems to work for FloatVector, StrVector, and FactorVector, let's use it
        from rpy2.robjects import r
        colnames = r.colnames(data)
        pandas_data = {}
        # convert each column of the R dataframe in turn
        for i, colname in enumerate(colnames):
            pandas_data[colname] = list(data[i])
        # Unfortunately, some datasets have rownames that should be an ID column (e.g., see mtcars where rownames=names of the cars). 
        # This is the best we can do: pull it out as an additional column for each and every dataset
        pandas_data['row_names'] = list(data.rownames)

        df = pd.DataFrame(pandas_data)
        data_object.add(self, df)
        terminate = df.empty
        return data_object, terminate

#conda install -c r rpy2 
