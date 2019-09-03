
import pytest
from primrose.node_factory import NodeFactory
from testfixtures import LogCapture

from primrose.readers.csv_reader import CsvReader
from primrose.writers.dill_writer import DillWriter

def test_register_module_classes():

    with LogCapture() as l:
        NodeFactory().register_module_classes(__name__)
    l.check(
        ('root','INFO','Discovered class CsvReader (<class '
            "'primrose.readers.csv_reader.CsvReader'>)"),
        ('root',
            'DEBUG',
            "Registered CsvReader : <class 'primrose.readers.csv_reader.CsvReader'>"),
        ('root',
            'INFO',
            'Discovered class DillWriter (<class '
            "'primrose.writers.dill_writer.DillWriter'>)"),
        ('root',
            'DEBUG',
            "Registered DillWriter : <class 'primrose.writers.dill_writer.DillWriter'>")
    )