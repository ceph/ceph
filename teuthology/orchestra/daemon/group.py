from teuthology import misc
from teuthology.orchestra.daemon.state import DaemonState
from teuthology.orchestra.daemon.systemd import SystemDState
from teuthology.orchestra.daemon.cephadmunit import CephadmUnit


class DaemonGroup(object):
    """
    Collection of daemon state instances
    """
    def __init__(self, use_systemd=False, use_cephadm=None):
        """
        self.daemons is a dictionary indexed by role.  Each entry is a
        dictionary of DaemonState values indexed by an id parameter.

        :param use_systemd: Whether or not to use systemd when appropriate
                            (default: False) Note: This option may be removed
                            in the future.
        """
        self.daemons = {}
        self.use_systemd = use_systemd
        self.use_cephadm = use_cephadm

    def add_daemon(self, remote, type_, id_, *args, **kwargs):
        """
        Add a daemon.  If there already is a daemon for this id_ and role, stop
        that daemon.  (Re)start the daemon once the new value is set.

        :param remote: Remote site
        :param type_: type of daemon (osd, mds, mon, rgw,  for example)
        :param id_: Id (index into role dictionary)
        :param args: Daemonstate positional parameters
        :param kwargs: Daemonstate keyword parameters
        """
        # for backwards compatibility with older ceph-qa-suite branches,
        # we can only get optional args from unused kwargs entries
        self.register_daemon(remote, type_, id_, *args, **kwargs)
        cluster = kwargs.pop('cluster', 'ceph')
        role = cluster + '.' + type_
        self.daemons[role][id_].restart()

    def register_daemon(self, remote, type_, id_, *args, **kwargs):
        """
        Add a daemon.  If there already is a daemon for this id_ and role, stop
        that daemon.

        :param remote: Remote site
        :param type_: type of daemon (osd, mds, mon, rgw,  for example)
        :param id_: Id (index into role dictionary)
        :param args: Daemonstate positional parameters
        :param kwargs: Daemonstate keyword parameters
        """
        # for backwards compatibility with older ceph-qa-suite branches,
        # we can only get optional args from unused kwargs entries
        cluster = kwargs.pop('cluster', 'ceph')
        role = cluster + '.' + type_
        if role not in self.daemons:
            self.daemons[role] = {}
        if id_ in self.daemons[role]:
            self.daemons[role][id_].stop()
            self.daemons[role][id_] = None

        klass = DaemonState
        if self.use_cephadm:
            klass = CephadmUnit
            kwargs['use_cephadm'] = self.use_cephadm
        elif self.use_systemd and \
             not any(i == 'valgrind' for i in args) and \
             remote.init_system == 'systemd':
            # We currently cannot use systemd and valgrind together because
            # it would require rewriting the unit files
            klass = SystemDState
        self.daemons[role][id_] = klass(
            remote, role, id_, *args, **kwargs)

    def get_daemon(self, type_, id_, cluster='ceph'):
        """
        get the daemon associated with this id_ for this role.

        :param type_: type of daemon (osd, mds, mon, rgw,  for example)
        :param id_: Id (index into role dictionary)
        """
        role = cluster + '.' + type_
        if role not in self.daemons:
            return None
        return self.daemons[role].get(str(id_), None)

    def iter_daemons_of_role(self, type_, cluster='ceph'):
        """
        Iterate through all daemon instances for this role.  Return dictionary
        of daemon values.

        :param type_: type of daemon (osd, mds, mon, rgw,  for example)
        """
        role = cluster + '.' + type_
        return self.daemons.get(role, {}).values()

    def resolve_role_list(self, roles, types, cluster_aware=False):
        """
        Resolve a configuration setting that may be None or contain wildcards
        into a list of roles (where a role is e.g. 'mds.a' or 'osd.0').  This
        is useful for tasks that take user input specifying a flexible subset
        of the available roles.

        The task calling this must specify what kinds of roles it can can
        handle using the ``types`` argument, where a role type is 'osd' or
        'mds' for example.  When selecting roles this is used as a filter, or
        when an explicit list of roles is passed, the an exception is raised if
        any are not of a suitable type.

        Examples:

        ::

            # Passing None (i.e. user left config blank) defaults to all roles
            # (filtered by ``types``)
            None, types=['osd', 'mds', 'mon'] ->
              ['osd.0', 'osd.1', 'osd.2', 'mds.a', mds.b', 'mon.a']
            # Wildcards are expanded
            roles=['mds.*', 'osd.0'], types=['osd', 'mds', 'mon'] ->
              ['mds.a', 'mds.b', 'osd.0']
            # Boring lists are unaltered
            roles=['osd.0', 'mds.a'], types=['osd', 'mds', 'mon'] ->
              ['osd.0', 'mds.a']
            # Entries in role list that don't match types result in an
            # exception
            roles=['osd.0', 'mds.a'], types=['osd'] -> RuntimeError

        :param roles: List (of roles or wildcards) or None (select all suitable
                      roles)
        :param types: List of acceptable role types, for example
                      ['osd', 'mds'].
        :param cluster_aware: bool to determine whether to consider include
                              cluster in the returned roles - just for
                              backwards compatibility with pre-jewel versions
                              of ceph-qa-suite
        :return: List of strings like ["mds.0", "osd.2"]
        """
        assert (isinstance(roles, list) or roles is None)

        resolved = []
        if roles is None:
            # Handle default: all roles available
            for type_ in types:
                for role, daemons in self.daemons.items():
                    if not role.endswith('.' + type_):
                        continue
                    for daemon in daemons.values():
                        prefix = type_
                        if cluster_aware:
                            prefix = daemon.role
                        resolved.append(prefix + '.' + daemon.id_)
        else:
            # Handle explicit list of roles or wildcards
            for raw_role in roles:
                try:
                    cluster, role_type, role_id = misc.split_role(raw_role)
                except ValueError:
                    msg = ("Invalid role '{0}', roles must be of format "
                           "[<cluster>.]<type>.<id>").format(raw_role)
                    raise RuntimeError(msg)

                if role_type not in types:
                    msg = "Invalid role type '{0}' in role '{1}'".format(
                        role_type, raw_role)
                    raise RuntimeError(msg)

                if role_id == "*":
                    # Handle wildcard, all roles of the type
                    for daemon in self.iter_daemons_of_role(role_type,
                                                            cluster=cluster):
                        prefix = role_type
                        if cluster_aware:
                            prefix = daemon.role
                        resolved.append(prefix + '.' + daemon.id_)
                else:
                    # Handle explicit role
                    resolved.append(raw_role)

        return resolved
