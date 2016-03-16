import logging
import struct

from . import run

log = logging.getLogger(__name__)


class DaemonState(object):
    """
    Daemon State.  A daemon exists for each instance of each role.
    """
    def __init__(self, remote, role, id_, *command_args, **command_kwargs):
        """
        Pass remote command information as parameters to remote site

        :param remote: Remote site
        :param role: Role (osd, rgw, mon, mds)
        :param id_: Id within role (osd.1, osd.2, for eaxmple)
        :param command_args: positional arguments (used in restart commands)
        :param command_kwargs: keyword arguments (used in restart commands)
        """
        self.remote = remote
        self.command_args = command_args
        self.command_kwargs = command_kwargs
        self.role = role
        self.id_ = id_
        self.log = command_kwargs.get('logger', log)
        self.proc = None

    def stop(self, timeout=300):
        """
        Stop this daemon instance.

        Note: this can raise a CommandFailedError,
        CommandCrashedError, or ConnectionLostError.

        :param timeout: timeout to pass to orchestra.run.wait()
        """
        if not self.running():
            self.log.error('tried to stop a non-running daemon')
            return
        self.proc.stdin.close()
        self.log.debug('waiting for process to exit')
        run.wait([self.proc], timeout=timeout)
        self.proc = None
        self.log.info('Stopped')

        """
        Wait for daemon to exit

        Wait for daemon to stop (but don't trigger the stop).  Pass up
        any exception.  Mark the daemon as not running.
        """
    def wait(self, timeout=300):
        self.log.debug('waiting for process to exit')
        try:
            run.wait([self.proc], timeout=timeout)
            self.log.info('Stopped')
        except:
            self.log.info('Failed')
            raise
        finally:
            self.proc = None

    def restart(self, *args, **kwargs):
        """
        Restart with a new command passed in the arguments

        :param args: positional arguments passed to remote.run
        :param kwargs: keyword arguments passed to remote.run
        """
        self.log.info('Restarting daemon')
        if self.proc is not None:
            self.log.info('Stopping old one...')
            self.stop()
        cmd_args = list(self.command_args)
        cmd_args.extend(args)
        cmd_kwargs = self.command_kwargs
        cmd_kwargs.update(kwargs)
        self.proc = self.remote.run(*cmd_args, **cmd_kwargs)
        self.log.info('Started')

    def restart_with_args(self, extra_args):
        """
        Restart, adding new paramaters to the current command.

        :param extra_args: Extra keyword arguments to be added.
        """
        self.log.info('Restarting daemon')
        if self.proc is not None:
            self.log.info('Stopping old one...')
            self.stop()
        cmd_args = list(self.command_args)
        # we only want to make a temporary mod of the args list
        # so we shallow copy the dict, and deepcopy the args list
        cmd_kwargs = self.command_kwargs.copy()
        from copy import deepcopy
        cmd_kwargs['args'] = deepcopy(self.command_kwargs['args'])
        cmd_kwargs['args'].extend(extra_args)
        self.proc = self.remote.run(*cmd_args, **cmd_kwargs)
        self.log.info('Started')

    def signal(self, sig, silent=False):
        """
        Send a signal to associated remote commnad

        :param sig: signal to send
        """
        self.proc.stdin.write(struct.pack('!b', sig))
        if not silent:
            self.log.info('Sent signal %d', sig)

    def running(self):
        """
        Are we running?
        :return: True if remote run command value is set, False otherwise.
        """
        return self.proc is not None

    def reset(self):
        """
        clear remote run command value.
        """
        self.proc = None

    def wait_for_exit(self):
        """
        clear remote run command value after waiting for exit.
        """
        if self.proc:
            try:
                run.wait([self.proc])
            finally:
                self.proc = None


class DaemonGroup(object):
    """
    Collection of daemon state instances
    """
    def __init__(self):
        """
        self.daemons is a dictionary indexed by role.  Each entry is a
        dictionary of DaemonState values indexed by an id parameter.
        """
        self.daemons = {}

    def add_daemon(self, remote, type_, id_, *args, **kwargs):
        """
        Add a daemon.  If there already is a daemon for this id_ and role, stop
        that daemon and.  Restart the damon once the new value is set.

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
        self.daemons[role][id_] = DaemonState(remote, role, id_, *args,
                                              **kwargs)
        self.daemons[role][id_].restart()

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

    def resolve_role_list(self, roles, types):
        """
        Resolve a configuration setting that may be None or contain wildcards
        into a list of roles (where a role is e.g. 'mds.a' or 'osd.0').  This
        is useful for tasks that take user input specifying a flexible subset
        of the available roles.

        The task calling this must specify what kinds of roles it can can handle using the ``types`` argument, where
        a role type is 'osd' or 'mds' for example.  When selecting roles this is used as a filter, or when
        an explicit list of roles is passed, the an exception is raised if any are not of a suitable type.

        Examples:

        ::

            # Passing None (i.e. user left config blank) defaults to all roles (filtered by ``types``)
            None, types=['osd', 'mds', 'mon'] -> ['osd.0', 'osd.1', 'osd.2', 'mds.a', mds.b', 'mon.a']
            # Wildcards are expanded
            roles=['mds.*', 'osd.0'], types=['osd', 'mds', 'mon'] -> ['mds.a', 'mds.b', 'osd.0']
            # Boring lists are unaltered
            roles=['osd.0', 'mds.a'], types=['osd', 'mds', 'mon'] -> ['osd.0', 'mds.a']
            # Entries in role list that don't match types result in an exception
            roles=['osd.0', 'mds.a'], types=['osd'] -> RuntimeError

        :param roles: List (of roles or wildcards) or None (select all suitable roles)
        :param types: List of acceptable role types, for example ['osd', 'mds'].
        :return: List of strings like ["mds.0", "osd.2"]
        """
        assert (isinstance(roles, list) or roles is None)

        resolved = []
        if roles is None:
            # Handle default: all roles available
            for type_ in types:
                for daemon in self.iter_daemons_of_role(type_):
                    resolved.append(type_ + '.' + daemon.id_)
        else:
            # Handle explicit list of roles or wildcards
            for raw_role in roles:
                try:
                    role_type, role_id = raw_role.split(".")
                except ValueError:
                    raise RuntimeError("Invalid role '{0}', roles must be of format <type>.<id>".format(raw_role))

                if role_type not in types:
                    raise RuntimeError("Invalid role type '{0}' in role '{1}'".format(role_type, raw_role))

                if role_id == "*":
                    # Handle wildcard, all roles of the type
                    for daemon in self.iter_daemons_of_role(role_type):
                        resolved.append(role_type + '.' + daemon.id_)
                else:
                    # Handle explicit role
                    resolved.append(raw_role)

        return resolved
