import teuthology.misc

class Cluster(object):
    """
    Manage SSH connections to a cluster of machines.
    """

    def __init__(self, remotes=None):
        """
        Initialize
        """
        self.remotes = {}
        if remotes is not None:
            for remote, roles in remotes:
                self.add(remote, roles)

    def __repr__(self):
        remotes = [(k, v) for k,v in self.remotes.items()]
        remotes.sort(key=lambda tup: tup[0].name)
        remotes = '{' + ', '.join('{remote!r}: {roles!r}'.format(remote=k, roles=v) for k,v in remotes) + '}'
        return '{classname}(remotes={remotes})'.format(
            classname=self.__class__.__name__,
            remotes=remotes,
            )

    def __str__(self):
        remotes = list(self.remotes.items())
        remotes.sort(key=lambda tup: tup[0].name)
        remotes = ((k, ','.join(v)) for k,v in remotes)
        remotes = ('{k}[{v}]'.format(k=k, v=v) for k,v in remotes)
        return ' '.join(remotes)

    def add(self, remote, roles):
        if remote in self.remotes:
            raise RuntimeError(
                'Remote {new!r} already found in remotes: {old!r}'.format(
                    new=remote,
                    old=self.remotes[remote],
                    ),
                )
        self.remotes[remote] = list(roles)

    def run(self, **kwargs):
        """
        Run a command on all the nodes in this cluster.

        Goes through nodes in alphabetical order.

        If you don't specify wait=False, this will be sequentially.

        Returns a list of `RemoteProcess`.
        """
        remotes = sorted(self.remotes.iterkeys(), key=lambda rem: rem.name)
        return [remote.run(**kwargs) for remote in remotes]

    def write_file(self, file_name, content, sudo=False, perms=None):
        """
        Write text to a file on each node.

        :param file_name: file name
        :param content: file content
        :param sudo: use sudo
        :param perms: file permissions (passed to chmod) ONLY if sudo is True
        """
        remotes = sorted(self.remotes.iterkeys(), key=lambda rem: rem.name)
        for remote in remotes:
            if sudo:
                teuthology.misc.sudo_write_file(remote, file_name, content, perms)
            else:
                if perms is not None:
                    raise ValueError("To specify perms, sudo must be True")
                teuthology.misc.write_file(remote, file_name, content, perms)

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

        for remote, has_roles in self.remotes.iteritems():
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
        for remote, has_roles in self.remotes.iteritems():
            if remote not in matches.remotes:
                c.add(remote, has_roles)
        return c
