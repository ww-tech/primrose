"""Module to plot scatter plot of clusters

Author(s): 
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
from primrose.base.node import AbstractNode
from primrose.data_object import DataObjectResponseType
import matplotlib.pyplot as plt
import logging

class ClusterPlotter(AbstractNode):
    """Plot clusters on a 2D scatter plot"""

    @staticmethod
    def necessary_config(node_config):
        """Returns the necessary configuration keys for the ClusterPlotter object

        Note:
            id_col (str): name of column with cluster IDs
            filename (str): name of the file

        Returns: 
            set of necessary keys for the CsvReader object

        """
        return set(['id_col', 'filename'])

    def run(self, data_object):
        """Create a PNG image of the clustered data

        Note:
            saves image to file specified in filename key
            optional key: `title` which is title of plot

        Returns:
            data_object (DataObject): DataObject instance. Here, it is unmodified
            terminate (bool): should we terminate the DAG? true or false. Terminate if empty data

        """
        dat = data_object.get_upstream_data(self.instance_name, pop_data=False, rtype=DataObjectResponseType.KEY_VALUE.value)
        X = dat['data']
        cluster_ids = X[self.node_config['id_col']]
        centers = X.groupby(self.node_config['id_col']).mean()
        del X[self.node_config['id_col']]

        plt.scatter(X.iloc[:,0],X.iloc[:,1], c=cluster_ids, s=50, cmap='viridis')
        plt.scatter(centers.iloc[:,0], centers.iloc[:,1], c='black', s=200, alpha=0.5)

        if self.node_config['title']:
            plt.title(self.node_config['title'])

        plt.savefig(self.node_config['filename'])
        logging.info("Saved cluster plot to " + self.node_config['filename'])

        terminate = X.empty
        return data_object, terminate
