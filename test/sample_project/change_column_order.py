"""Module to reorder columns in a dataframe

Author(s): 
    Parul Laul (parul.laul@ww.com)

"""
import logging
from primrose.base.pipeline import AbstractPipeline


class ColumnReorder(AbstractPipeline):
    """Reorder columns in a dataframe"""

    @staticmethod
    def necessary_config(node_config):
        """Return the necessary configuration keys for the DataFrameJoiner object

        Args:
            node_config (dict): set of parameters / attributes for the node

        Note:
            cols_order: list of column names in desired order
            
        Returns:
            set of keys

        """
        return set(["cols_order"])

    def transform(self, data_object):
        """Get DataFrames from the data object and reorder columns

        Args:
            data_object (DataObject): instance of DataObject

        Returns:
            data_object (DataObject): instance of DataObject

        """

        upstream_data = data_object.get_upstream_data(
            self.instance_name, pop_data=False
        )
        logging.info("Reordering columns")
        data = upstream_data['data'][self.node_config["cols_order"]]
  
        data_object.add(self, data, overwrite=False)

        return data_object