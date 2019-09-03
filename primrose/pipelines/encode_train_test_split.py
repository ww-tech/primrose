""""Encode all string df columns to numeric labels, inherit test/train split from parent

Author(s):
    Mike Skarlinski (michael.skarlinski@weightwatchers.com)

"""
from primrose.pipelines.train_test_split import TrainTestSplit
from primrose.base.transformer_sequence import TransformerSequence
from primrose.transformers.categoricals import ImplicitCategoricalTransform


class EncodeTrainTestSplit(TrainTestSplit):
    """Encode all string df columns to numeric labels, inherit test/train split from parent"""

    def init_pipeline(self):
        """create the pipeline's TransformerSequence

        Returns:
            a TransformerSequence

        """
        ts = TransformerSequence()
        ts.add(ImplicitCategoricalTransform(self.node_config['target_variable']))
        return ts

    @property
    def first_transformer_in_sequence(self):
        """returns 1st transformer in a sequence

        Returns:
            a transformer

        """
        return self.transformer_sequence.sequence[0]

    def final_data_object_additions(self, data_object):
        """Overload function which adds the label encoder after running fit_transform or transform
        
        Returns:
            data_object (DataObject): instance of DataObject
        
        """
        # add target label encoder to the data object if it's needed downstream
        target_label_encoder = self.first_transformer_in_sequence.target_encoder
        data_object.add(self, target_label_encoder, key='target_encoder')
        return data_object
