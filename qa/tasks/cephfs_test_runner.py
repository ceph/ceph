import contextlib
import logging
import os
import unittest
from unittest import suite, loader, case
from teuthology.task import interactive
from teuthology import misc
from tasks.cephfs.filesystem import Filesystem, MDSCluster, CephCluster
from tasks.mgr.mgr_test_case import MgrCluster

log = logging.getLogger(__name__)


class DecoratingLoader(loader.TestLoader):
    """
    A specialization of TestLoader that tags some extra attributes
    onto test classes as they are loaded.
    """
    def __init__(self, params):
        self._params = params
        super(DecoratingLoader, self).__init__()

    def _apply_params(self, obj):
        for k, v in self._params.items():
            setattr(obj, k, v)

    def loadTestsFromTestCase(self, testCaseClass):
        self._apply_params(testCaseClass)
        return super(DecoratingLoader, self).loadTestsFromTestCase(testCaseClass)

    def loadTestsFromName(self, name, module=None):
        result = super(DecoratingLoader, self).loadTestsFromName(name, module)

        # Special case for when we were called with the name of a method, we get
        # a suite with one TestCase
        tests_in_result = list(result)
        if len(tests_in_result) == 1 and isinstance(tests_in_result[0], case.TestCase):
            self._apply_params(tests_in_result[0])

        return result


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


@contextlib.contextmanager
def task(ctx, config):
    """
    Run the CephFS test cases.

    Run everything in tasks/cephfs/test_*.py:

    ::

        tasks:
          - install:
          - ceph:
          - ceph-fuse:
          - cephfs_test_runner:

    `modules` argument allows running only some specific modules:

    ::

        tasks:
            ...
          - cephfs_test_runner:
              modules:
                - tasks.cephfs.test_sessionmap
                - tasks.cephfs.test_auto_repair

    By default, any cases that can't be run on the current cluster configuration
    will generate a failure.  When the optional `fail_on_skip` argument is set
    to false, any tests that can't be run on the current configuration will
    simply be skipped:

    ::
        tasks:
            ...
         - cephfs_test_runner:
           fail_on_skip: false

    """

    ceph_cluster = CephCluster(ctx)

    if len(list(misc.all_roles_of_type(ctx.cluster, 'mds'))):
        mds_cluster = MDSCluster(ctx)
        fs = Filesystem(ctx)
    else:
        mds_cluster = None
        fs = None

    if len(list(misc.all_roles_of_type(ctx.cluster, 'mgr'))):
        mgr_cluster = MgrCluster(ctx)
    else:
        mgr_cluster = None

    # Mount objects, sorted by ID
    if hasattr(ctx, 'mounts'):
        mounts = [v for k, v in sorted(ctx.mounts.items(), key=lambda mount: mount[0])]
    else:
        # The test configuration has a filesystem but no fuse/kclient mounts
        mounts = []

    decorating_loader = DecoratingLoader({
        "ctx": ctx,
        "mounts": mounts,
        "fs": fs,
        "ceph_cluster": ceph_cluster,
        "mds_cluster": mds_cluster,
        "mgr_cluster": mgr_cluster,
    })

    fail_on_skip = config.get('fail_on_skip', True)

    # Put useful things onto ctx for interactive debugging
    ctx.fs = fs
    ctx.mds_cluster = mds_cluster
    ctx.mgr_cluster = mgr_cluster

    # Depending on config, either load specific modules, or scan for moduless
    if config and 'modules' in config and config['modules']:
        module_suites = []
        for mod_name in config['modules']:
            # Test names like cephfs.test_auto_repair
            module_suites.append(decorating_loader.loadTestsFromName(mod_name))
        overall_suite = suite.TestSuite(module_suites)
    else:
        # Default, run all tests
        overall_suite = decorating_loader.discover(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "cephfs/"
            )
        )

    if ctx.config.get("interactive-on-error", False):
        InteractiveFailureResult.ctx = ctx
        result_class = InteractiveFailureResult
    else:
        result_class = unittest.TextTestResult

    class LoggingResult(result_class):
        def startTest(self, test):
            log.info("Starting test: {0}".format(self.getDescription(test)))
            return super(LoggingResult, self).startTest(test)

        def addSkip(self, test, reason):
            if fail_on_skip:
                # Don't just call addFailure because that requires a traceback
                self.failures.append((test, reason))
            else:
                super(LoggingResult, self).addSkip(test, reason)

    # Execute!
    result = unittest.TextTestRunner(
        stream=LogStream(),
        resultclass=LoggingResult,
        verbosity=2,
        failfast=True).run(overall_suite)

    if not result.wasSuccessful():
        result.printErrors()  # duplicate output at end for convenience

        bad_tests = []
        for test, error in result.errors:
            bad_tests.append(str(test))
        for test, failure in result.failures:
            bad_tests.append(str(test))

        raise RuntimeError("Test failure: {0}".format(", ".join(bad_tests)))

    yield
