from collections import namedtuple

from .generated_api import MonCommandApi

class CommandResult(namedtuple('CommandResult', ['retval', 'stdout', 'stderr'])):
    def __new__(cls, retval=0, stdout="", stderr=""):
        """
        Tuple containing the result of `handle_command()`

        Only write to stderr if there is an error, or in extraordinary circumstances

        Avoid having `ceph foo bar` commands say "did foo bar" on success unless there
        is critical information to include there.

        Everything programmatically consumable should be put on stdout

        :param retval: return code. E.g. 0 or -errno.EINVAL
        :type retval: int
        :param stdout: data of this result.
        :type stdout: str
        :param stderr: Typically used for error messages.
        :type stderr: str
        """
        return super(CommandResult, cls).__new__(cls, retval, stdout, stderr)
