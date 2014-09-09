
class BranchNotFoundError(ValueError):
    def __init__(self, branch, repo=None):
        self.branch = branch
        self.repo = repo

    def __str__(self):
        if self.repo:
            repo_str = " in repo: %s" % self.repo
        else:
            repo_str = ""
        return "Branch '{branch}' not found{repo_str}!".format(
            branch=self.branch, repo_str=repo_str)


class GitError(RuntimeError):
    pass


class BootstrapError(RuntimeError):
    pass


class CommandFailedError(Exception):

    """
    Exception thrown on command failure
    """
    def __init__(self, command, exitstatus, node=None):
        self.command = command
        self.exitstatus = exitstatus
        self.node = node

    def __str__(self):
        return "Command failed on {node} with status {status}: {cmd!r}".format(
            node=self.node,
            status=self.exitstatus,
            cmd=self.command,
            )


class CommandCrashedError(Exception):

    """
    Exception thrown on crash
    """
    def __init__(self, command):
        self.command = command

    def __str__(self):
        return "Command crashed: {command!r}".format(
            command=self.command,
            )


class ConnectionLostError(Exception):

    """
    Exception thrown when the connection is lost
    """
    def __init__(self, command):
        self.command = command

    def __str__(self):
        return "SSH connection was lost: {command!r}".format(
            command=self.command,
            )
