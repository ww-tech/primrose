"""Module toJoin upstream dataframes

Author(s): 
    Michael Skarlinski (michael.skarlinski@weightwatchers.com)

"""
import logging
from primrose.base.pipeline import AbstractPipeline, PipelineModeType
from primrose.transformers.combine import LeftJoinDataCombiner
from primrose.base.transformer_sequence import TransformerSequence
from primrose.data_object import DataObject

class DataFrameJoiner(AbstractPipeline):
    """Join upstream dataframes"""

    @staticmethod
    def necessary_config(node_config):
        """Return the necessary configuration keys for the DataFrameJoiner object

        Args:
            node_config (dict): set of parameters / attributes for the node

        Note:
            start_table: first table index in alpha order which defines who is eligible for this analysis (all other tables will be left joined to this)
            join_key: list of column names to join dataframes from different readers on

        Returns:
            set of keys

        """
        return set(['start_table', 'join_key'])

    def init_pipeline(self):
        """create the pipeline's TransformerSequence

        Returns:
            a TransformerSequence

        """
        ts = TransformerSequence()
        # Note: this is a trasnformer that does not striclty adhere to Transformer interface
        # It takes a in *list* of data frames, not a single data, and returns a single dataframe
        ts.add(LeftJoinDataCombiner(self.node_config['join_key']))
        return ts

    def transform(self, data_object):
        """Get DataFrames from the data object then put them into a alphabetical list for constant join order

        Args:
            data_object (DataObject): instance of DataObject

        Returns:
            data_object (DataObject): instance of DataObject

        """

        upstream_data = data_object.get_upstream_data(self.instance_name,
                                                pop_data=False)

        upstream_keys = sorted([k for k in upstream_data.keys()])

        start_key = self.node_config['start_table']

        if start_key not in upstream_keys:
            raise Exception("Could not find start_table in upstream keys: " + start_key)

        upstream_keys = [start_key] + [k for k in upstream_keys if k != start_key]

        logging.info('Joining dataframes from {} sources'.format(len(upstream_keys)))

        # note we are passing in a list of dataframes here
        # note: we assume default DATA_KEY
        data = self.execute_pipeline([upstream_data[k][DataObject.DATA_KEY] for k in upstream_keys], PipelineModeType.TRANSFORM)

        logging.info('Final size of joined data is {} rows.'.format(len(data)))

        data_object.add(self, data, overwrite=False)

        return data_object
