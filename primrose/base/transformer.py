"""Module with abstract transformer class to specify interface needed for transformers

Author(s):
    Michael Skarlinski (michael.skarlinski@weightwatchers.com)

    Carl Anderson (carl.anderson@weightwatchers.com)

"""

from abc import ABC, abstractmethod

class AbstractTransformer(ABC):
    """Serializable object that can be string together within a pipeline"""

    @abstractmethod
    def fit(self, data):
        """User implements fit operation on a single data element from a data_object
        
        Args:
            data (object): some data

        Returns:
            data

        """
        pass # pragma: no cover

    @abstractmethod
    def transform(self, data):
        """User implements internal transform function which operates on a single data element from a data_object
        
        Args:
            data (object): input data

        Returns:
            data, transformed

        """
        return data # pragma: no cover

    def fit_transform(self, data):
        """fit then transform data

        Args:
            data (object): input data

        Returns:
            data, transformed

        """
        self.fit(data)
        return self.transform(data)