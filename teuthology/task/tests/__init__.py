"""
This task is used to integration test teuthology. Including this
task in your yaml config will execute pytest which finds any tests in
the current directory.  Each test that is discovered will be passed the
teuthology ctx and config args that each teuthology task usually gets.
This allows the tests to operate against the cluster.

An example:

tasks
  - tests:

"""
import logging
import pytest


log = logging.getLogger(__name__)


@pytest.fixture
def ctx():
    return {}


@pytest.fixture
def config():
    return []


class TeuthologyContextPlugin(object):
    def __init__(self, ctx, config):
        self.ctx = ctx
        self.config = config

    # this is pytest hook for generating tests with custom parameters
    def pytest_generate_tests(self, metafunc):
        # pass the teuthology ctx and config to each test method
        metafunc.parametrize(["ctx", "config"], [(self.ctx, self.config),])

    # log the outcome of each test
    def pytest_runtest_makereport(self, __multicall__, item, call):
        report = __multicall__.execute()

        # after the test has been called, get it's report and log it
        if call.when == 'call':
            # item.location[0] is a slash delimeted path to the test file
            # being ran. We only want the portion after teuthology.task.tests
            test_path = item.location[0].replace("/", ".").split(".")
            test_path = ".".join(test_path[4:-1])
            # removes the string '[ctx0, config0]' after the test name
            test_name = item.location[2].split("[")[0]
            msg = "{path}:{name}".format(path=test_path, name=test_name)
            if report.passed:
                log.info("{msg} PASSED!".format(msg=msg))
            else:
                log.error("{msg} FAILED!".format(msg=msg))
                log.error("{msg} failed with: '{info}'".format(
                    msg=msg,
                    info=call.excinfo
                ))

        return report


def task(ctx, config):
    """
    Use pytest to recurse through this directory, finding any tests
    and then executing them with the teuthology ctx and config args.
    Your tests must follow standard pytest conventions to be discovered.
    """
    status = pytest.main(
        args=[
            '-s', '-q',
            '--pyargs', __name__
        ],
        plugins=[TeuthologyContextPlugin(ctx, config)]
    )
    if status == 0:
        log.info("OK. All tests passed!")
    else:
        log.error("FAIL. Saw test failures...")
        ctx.summary["status"] = "fail"
