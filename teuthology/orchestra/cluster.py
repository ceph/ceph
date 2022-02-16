"""
Cluster definition
part of context, Cluster is used to save connection information.
"""
from teuthology.orchestra import run

class Cluster(object):
    """
    Manage SSH connections to a cluster of machines.
    """

    def __init__(self, remotes=None):
        """
        :param remotes: A sequence of 2-tuples of this format:
                            (Remote, [role_1, role_2 ...])
        """
        self.remotes = {}
        if remotes is not None:
            for remote, roles in remotes:
                self.add(remote, roles)

    def __repr__(self):
        remotes = [(k, v) for k, v in self.remotes.items()]
        remotes.sort(key=lambda tup: tup[0].name)
        remotes = '[' + ', '.join('[{remote!r}, {roles!r}]'.format(
            remote=k, roles=v) for k, v in remotes) + ']'
        return '{classname}(remotes={remotes})'.format(
            classname=self.__class__.__name__,
            remotes=remotes,
            )

    def __str__(self):
        remotes = list(self.remotes.items())
        remotes.sort(key=lambda tup: tup[0].name)
        remotes = ((k, ','.join(v)) for k, v in remotes)
        remotes = ('{k}[{v}]'.format(k=k, v=v) for k, v in remotes)
        return ' '.join(remotes)

    def add(self, remote, roles):
        """
        Add roles to the list of remotes.
        """
        if remote in self.remotes:
            raise RuntimeError(
                'Remote {new!r} already found in remotes: {old!r}'.format(
                    new=remote,
                    old=self.remotes[remote],
                    ),
                )
        self.remotes[remote] = list(roles)

    def run(self, wait=True, parallel=False, **kwargs):
        """
        Run a command on all the nodes in this cluster.

        Goes through nodes in alphabetical order.

        The default usage is when parallel=False and wait=True,
        which is a sequential run for each node one by one.

        If you specify parallel=True, it will be in parallel.

        If you specify wait=False, it returns immediately.
        Since it is not possible to run sequentially and
        do not wait each command run finished, the parallel value
        is ignored and treated as True.

        Returns a list of `RemoteProcess`.
        """
        # -+-------+----------+----------+------------+---------------
        #  | wait  | parallel | run.wait | remote.run | comments
        # -+-------+----------+----------+------------+---------------
        # 1|*True  |*False    | no       | wait=True  | sequentially
        # 2| True  | True     | yes      | wait=False | parallel
        # 3| False | True     | no       | wait=False | parallel
        # 4| False | False    | no       | wait=False | same as above

        # We always run in parallel if wait=False,
        # that is why (4) is equivalent to (3).

        # We wait from remote.run only if run sequentially.
        _wait = (parallel == False and wait == True)

        remotes = sorted(self.remotes.keys(), key=lambda rem: rem.name)
        procs = [remote.run(**kwargs, wait=_wait) for remote in remotes]

        # We do run.wait only if parallel=True, because if parallel=False,
        # we have run sequentially and all processes are complete.

        if parallel and wait:
            run.wait(procs)
        return procs

    def sh(self, script, **kwargs):
        """
        Run a command on all the nodes in this cluster.

        Goes through nodes in alphabetical order.

        Returns a list of the command outputs correspondingly.
        """
        remotes = sorted(self.remotes.keys(), key=lambda rem: rem.name)
        return [remote.sh(script, **kwargs) for remote in remotes]

    def write_file(self, file_name, content, sudo=False, perms=None, owner=None):
        """
        Write text to a file on each node.

        :param file_name: file name
        :param content: file content
        :param sudo: use sudo
        :param perms: file permissions (passed to chmod) ONLY if sudo is True
        """
        remotes = sorted(self.remotes.keys(), key=lambda rem: rem.name)
        for remote in remotes:
            if sudo:
                remote.write_file(file_name, content,
                                  sudo=True, mode=perms, owner=owner)
            else:
                if perms is not None or owner is not None:
                    raise ValueError("To specify perms or owner, sudo must be True")
                remote.write_file(file_name, content)

    def only(self, *roles):
        """
        Return a cluster with only the remotes that have all of given roles.

        For roles given as strings, they are matched against the roles
        on a remote, and the remote passes the check only if all the
        roles listed are present.

        Argument can be callable, and will act as a match on roles of
        the remote. The matcher will be evaluated one role at a time,
        but a match on any role is good enough. Note that this is
        subtly diffent from the behavior of string roles, but is
        logical if you consider a callable to be similar to passing a
        non-string object with an `__eq__` method.

        For example::

	    web = mycluster.only(lambda role: role.startswith('web-'))
        """
        c = self.__class__()
        want = frozenset(r for r in roles if not callable(r))
        matchers = [r for r in roles if callable(r)]

        for remote, has_roles in self.remotes.items():
            # strings given as roles must all match
            if frozenset(has_roles) & want != want:
                # not a match
                continue

            # every matcher given must match at least one role
            if not all(
                any(matcher(role) for role in has_roles)
                for matcher in matchers
                ):
                continue

            c.add(remote, has_roles)

        return c

    def exclude(self, *roles):
        """
        Return a cluster *without* remotes that have all of given roles.

        This is the opposite of `only`.
        """
        matches = self.only(*roles)
        c = self.__class__()
        for remote, has_roles in self.remotes.items():
            if remote not in matches.remotes:
                c.add(remote, has_roles)
        return c

    def filter(self, func):
        """
        Return a cluster whose remotes are filtered by `func`.

        Example::
            cluster = ctx.cluster.filter(lambda r: r.is_online)
        """
        result = self.__class__()
        for rem, roles in self.remotes.items():
            if func(rem):
                result.add(rem, roles)
        return result
