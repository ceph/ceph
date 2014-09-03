import logging
import unittest
from teuthology.task import interactive


log = logging.getLogger(__name__)


class CephFSTestCase(unittest.TestCase):
    fs = None

    def assert_session_count(self, expected, ls_data=None):
        if ls_data is None:
            ls_data = self.fs.mds_asok(['session', 'ls'])

        self.assertEqual(expected, len(ls_data), "Expected {0} sessions, found {1}".format(
            expected, len(ls_data)
        ))

    def assert_session_state(self, client_id,  expected_state):
        self.assertEqual(
            self._session_by_id(
                self.fs.mds_asok(['session', 'ls'])).get(client_id, {'state': None})['state'],
            expected_state)

    def get_session_data(self, client_id):
        return self._session_by_id(client_id)

    def _session_list(self):
        ls_data = self.fs.mds_asok(['session', 'ls'])
        ls_data = [s for s in ls_data if s['state'] not in ['stale', 'closed']]
        return ls_data

    def get_session(self, client_id, session_ls=None):
        if session_ls is None:
            session_ls = self.fs.mds_asok(['session', 'ls'])

        return self._session_by_id(session_ls)[client_id]

    def _session_by_id(self, session_ls):
        return dict([(s['id'], s) for s in session_ls])


class LogStream(object):
    def __init__(self):
        self.buffer = ""

    def write(self, data):
        self.buffer += data
        if "\n" in self.buffer:
            lines = self.buffer.split("\n")
            for line in lines[:-1]:
                log.info(line)
            self.buffer = lines[-1]

    def flush(self):
        pass


class InteractiveFailureResult(unittest.TextTestResult):
    """
    Specialization that implements interactive-on-error style
    behavior.
    """
    ctx = None

    def addFailure(self, test, err):
        log.error(self._exc_info_to_string(err, test))
        log.error("Failure in test '{0}', going interactive".format(
            self.getDescription(test)
        ))
        interactive.task(ctx=self.ctx, config=None)

    def addError(self, test, err):
        log.error(self._exc_info_to_string(err, test))
        log.error("Error in test '{0}', going interactive".format(
            self.getDescription(test)
        ))
        interactive.task(ctx=self.ctx, config=None)


def run_tests(ctx, config, test_klass, params):
    for k, v in params.items():
        setattr(test_klass, k, v)

    # Execute test suite
    # ==================
    if config and 'test_name' in config:
        # Test names like TestCase.this_test
        suite = unittest.TestLoader().loadTestsFromName(
            "{0}.{1}".format(test_klass.__module__, config['test_name']))
    else:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_klass)

    if ctx.config.get("interactive-on-error", False):
        InteractiveFailureResult.ctx = ctx
        result_class = InteractiveFailureResult
    else:
        result_class = unittest.TextTestResult
    result = unittest.TextTestRunner(
        stream=LogStream(),
        resultclass=result_class,
        verbosity=2,
        failfast=True).run(suite)

    if not result.wasSuccessful():
        result.printErrors()  # duplicate output at end for convenience
        raise RuntimeError("Test failure.")
