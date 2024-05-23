import logging
import os
import unittest

from py_tests.internal import utils

_logging_configured = False


class TestBase(unittest.TestCase):
    @staticmethod
    def _setup_logging(debug=False, verbose=False):
        log_level = logging.WARNING
        if verbose:
            log_level = logging.INFO
        if debug:
            log_level = logging.DEBUG
        utils.setup_logging(log_level)

    @classmethod
    def setUpClass(cls):
        global _logging_configured
        if not _logging_configured:
            cls._setup_logging(
                debug=utils.str2bool(os.getenv('CEPHTEST_DEBUG')),
                verbose=utils.str2bool(os.getenv('CEPHTEST_VERBOSE')))
            _logging_configured = True
