
import pytest
from primrose.base.transformer import AbstractTransformer
import logging
from testfixtures import LogCapture

def test_fit_transform():
    class TestTransformer(AbstractTransformer):
        def fit(self, data):
            logging.info("Transfer FIT CALLED")

        def transform(self, data):
            logging.info("Transfer TRANSFORM CALLED")
            return data

    t = TestTransformer()

    with LogCapture() as l:
        t.fit_transform(None)
    l.check(
        ('root', 'INFO', 'Transfer FIT CALLED'),
        ('root', 'INFO', 'Transfer TRANSFORM CALLED')
    )

