"""Enum to speacify type of run mode: train, predict, and eval

Author(s):
    Michael Skarlinski (michael.skarlinski@weightwatchers.com)

    Carl Anderson (carl.anderson@weightwatchers.com)

"""
from enum import Enum

class RunModes(Enum):
    """set of operation type identifiers"""
    TRAIN = 'train'
    PREDICT = 'predict'
    EVAL = 'eval'

    @staticmethod
    def names():
        """list of all the names in the enum

        Returns:
            list of Enum's names

        """
        return list(map(lambda t: t.name, RunModes))

    @staticmethod
    def values():
        """list of all the values in the enum

        Returns:
            list of Enum's values

        """
        return list(map(lambda t: t.value, RunModes))
