import logging
import pytest

from StringIO import StringIO

from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)


class TestRun(object):
    """
    Tests to see if we can make remote procedure calls to the current cluster
    """

    def test_command_failed_label(self, ctx, config):
        result = ""
        try:
            ctx.cluster.run(
                args=["python", "-c", "assert False"],
                label="working as expected, nothing to see here"
            )
        except CommandFailedError as e:
            result = str(e)

        assert "working as expected" in result

    def test_command_failed_no_label(self, ctx, config):
        with pytest.raises(CommandFailedError):
            ctx.cluster.run(
                args=["python", "-c", "assert False"],
            )

    def test_command_success(self, ctx, config):
        result = StringIO()
        ctx.cluster.run(
            args=["python", "-c", "print('hi')"],
            stdout=result
        )
        assert result.getvalue().strip() == "hi"
