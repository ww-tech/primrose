"""Abstract base class for a clean up node, such as signalling success

Author(s):
    Michael Skarlinski (michael.skarlinski@weightwatchers.com)

    Carl Anderson (carl.anderson@weightwatchers.com)

"""

from abc import abstractmethod
from primrose.base.node import AbstractNode

class AbstractSuccess(AbstractNode):
    """ Ability to cleanup, such as notify success"""
    pass
