"""
A place to put various testing functions for teuthology. Maybe at some point we
can turn this into a proper test suite of sorts.
"""
import logging
import pytest

from functools import wraps

from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)


def log_test_results(f):
    """
    Use this to decorate test functions to log the result of
    the test in the teuthology.log.
    """
    # TODO: it'd be nice to have the output from pytest use our logger
    # I tried a few different ways, but couldn't get it to work so I settled
    # on using this decorator instead.
    @wraps(f)
    def func(ctx, config):
        name = f.func_name
        try:
            log.info("running {name}...".format(name=name))
            f(ctx, config)
        except AssertionError:
            log.error("***** FAILED {name}".format(name=name))
            ctx.summary["status"] = "fail"
            ctx.summary["failure_reason"] = "failed: {0}".format(name)
            raise
        except Exception:
            log.error("***** ERROR {name}".format(name=name))
            ctx.summary["status"] = "fail"
            ctx.summary["failure_reason"] = "error: {0}".format(name)
            raise
        else:
            log.info("***** PASSED {name}".format(name=name))

    return func


@log_test_results
def test_command_failed_label(ctx, config):
    result = ""
    try:
        force_command_failure(ctx, config)
    except CommandFailedError as e:
        result = str(e)

    assert "working as expected" in result


def force_command_failure(ctx, config):
    log.info("forcing a command failure...")
    ctx.cluster.run(
        args=["python", "-c", "assert False"],
        label="working as expected, nothing to see here"
    )


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


def task(ctx, config):
    # use pytest to find any test_* methods in this file
    # and execute them with the teuthology ctx and config args
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
