"""Module with abstract postprocess class to specify interface needed for future postprocesses

Author(s):
    Michael Skarlinski (michael.skarlinski@weightwatchers.com)

    Carl Anderson (carl.anderson@weightwatchers.com)

"""
from primrose.base.node import AbstractNode

class AbstractPostprocess(AbstractNode):
    """Postprocess module which must have an postprocess method to send data to an external source"""
    pass
