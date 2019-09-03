"""primrose wrapper around sklearn preprocessor

Author(s):
    Carl Anderson (carl.anderson@weightwatchers.com)

"""
from primrose.base.transformer import AbstractTransformer
import pandas as pd

class SklearnPreprocessingTransformer(AbstractTransformer):

    def __init__(self, preprocessor, columns):
        """initialize the proprocessor

        Args:
            preprocessor (SKlearn preprocessor): preprocesor from Sklearn
            columns (list of str): list of columns

        """
        #Ideally, would check that this is a preprocessor but there is no good class to check against
        self.preprocessor = preprocessor
        self.columns = columns

    def fit(self, data):
        """User implements fit operation on a single data element from a data_object
        
        Args:
            data (object): some data

        Returns:
            data

        """
        if isinstance(data, pd.DataFrame):
            if self.columns:
                self.preprocessor.fit(data[self.columns].values)
            else:
                self.preprocessor.fit(data.values)
        else:
            self.preprocessor.fit(data)

    def transform(self, data):
        """User implements internal transform function which operates on a single data element from a data_object
        
        Args:
            data (object): input data

        Returns:
            data, transformed

        """
        if isinstance(data, pd.DataFrame):
            if self.columns:
                scaled_features_df = data.copy()

                # transform select columns
                scaled_features = self.preprocessor.transform(data[self.columns].values)
                for idx, colname in enumerate(self.columns):
                    scaled_features_df[colname] = scaled_features[:,idx]

            else:
                scaled_features = self.preprocessor.transform(data.values)
                scaled_features_df = pd.DataFrame(scaled_features, index=data.index, columns=data.columns)
            return scaled_features_df

        return self.preprocessor.transform(data)

    def fit_transform(self, data):
        """fit then transform data

        Args:
            data (data): input data

        Returns:
            data, transformed

        """
        self.fit(data)
        return self.transform(data)