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


class CommitNotFoundError(ValueError):
    def __init__(self, commit, repo=None):
        self.commit = commit
        self.repo = repo

    def __str__(self):
        if self.repo:
            repo_str = " in repo: %s" % self.repo
        else:
            repo_str = ""
        return "'{commit}' not found{repo_str}!".format(
            commit=self.commit, repo_str=repo_str)


class GitError(RuntimeError):
    pass


class BootstrapError(RuntimeError):
    pass


class ConfigError(RuntimeError):
    """
    Meant to be used when an invalid config entry is found.
    """
    pass


class ParseError(Exception):
    pass


class CommandFailedError(Exception):

    """
    Exception thrown on command failure
    """
    def __init__(self, command, exitstatus, node=None, label=None):
        self.command = command
        self.exitstatus = exitstatus
        self.node = node
        self.label = label

    def __str__(self):
        prefix = "Command failed"
        if self.label:
            prefix += " ({label})".format(label=self.label)
        if self.node:
            prefix += " on {node}".format(node=self.node)
        return "{prefix} with status {status}: {cmd!r}".format(
            status=self.exitstatus,
            cmd=self.command,
            prefix=prefix,
            )

    def fingerprint(self):
        """
        Returns a list of strings to group failures with.
        Used by sentry instead of grouping by backtrace.
        """
        return [
            self.label or self.command,
            'exit status {}'.format(self.exitstatus),
            '{{ type }}',
        ]


class AnsibleFailedError(Exception):

    """
    Exception thrown when an ansible playbook fails
    """
    def __init__(self, failures):
        self.failures = failures

    def __str__(self):
        return "{failures}".format(
            failures=self.failures,
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
    def __init__(self, command, node=None):
        self.command = command
        self.node = node

    def __str__(self):
        node_str = 'to %s ' % self.node if self.node else ''
        return "SSH connection {node_str}was lost: {command!r}".format(
            node_str=node_str,
            command=self.command,
            )


class ScheduleFailError(RuntimeError):
    def __init__(self, message, name=None):
        self.message = message
        self.name = name

    def __str__(self):
        return "Scheduling {name} failed: {msg}".format(
            name=self.name,
            msg=self.message,
        ).replace('  ', ' ')


class VersionNotFoundError(Exception):
    def __init__(self, url):
        self.url = url

    def __str__(self):
        return "Failed to fetch package version from %s" % self.url


class UnsupportedPackageTypeError(Exception):
    def __init__(self, node):
        self.node = node

    def __str__(self):
        return "os.package_type {pkg_type!r} on {node}".format(
            node=self.node, pkg_type=self.node.os.package_type)


class SELinuxError(Exception):
    def __init__(self, node, denials):
        self.node = node
        self.denials = denials

    def __str__(self):
        return "SELinux denials found on {node}: {denials}".format(
            node=self.node, denials=self.denials)


class QuotaExceededError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class SkipJob(Exception):
    """
    Used by teuthology.worker when it notices that a job is broken and should
    be skipped.
    """
    pass


class MaxWhileTries(Exception):
    pass


class ConsoleError(Exception):
    pass


class NoRemoteError(Exception):
    message = "This operation requires a remote"

    def __str__(self):
        return self.message
