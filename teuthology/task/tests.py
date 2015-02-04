"""
A place to put various testing functions for teuthology. Maybe at some point we
can turn this into a proper test suite of sorts.
"""
import logging

from functools import wraps

from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)


def test(f):
    @wraps(f)
    def func(*args, **kwargs):
        try:
            log.info("running {name}...".format(name=f.func_name))
            f(*args, **kwargs)
        except AssertionError:
            log.error("***** FAILED")
            args[0].summary["status"] = "fail"
        except Exception:
            log.error("***** ERROR")
            args[0].summary["status"] = "fail"
        else:
            log.info("***** PASSED")

    return func


@test
def test_command_failed_label(ctx, config):
    result = ""
    try:
        force_command_failure(ctx, config)
    except CommandFailedError as e:
        result = str(e)

    assert "working as expected" in result


def force_command_failure(ctx, config):
    ctx.cluster.run(
        args=["python", "-c", "assert False"],
        label="working as expected, nothing to see here"
    )


def task(ctx, config):
    # TODO: find a way to auto discover test functions in this file
    # and execute them
    test_command_failed_label(ctx, config)
