import logging

from teuthology.misc import deep_merge
from teuthology.orchestra.cluster import Cluster

log = logging.getLogger(__name__)


class Task(object):
    """
    A base-class for "new-style" teuthology tasks.

    Can be used as a drop-in replacement for the old-style task functions with
    @contextmanager decorators.

    Note: While looking up overrides, we use the lowercase name of the class by
          default. While this works well for the main task in a module, other
          tasks or 'subtasks' may want to override that name using a class
          variable called 'name' e.g.:

              class MyTask(Task):
                  pass
              class MySubtask(MyTask):
                  name = 'mytask.mysubtask'
    """

    def __init__(self, ctx=None, config=None):
        if not hasattr(self, 'name'):
            self.name = self.__class__.__name__.lower()
        self.log = log
        self.ctx = ctx
        self.config = config or dict()
        if not isinstance(self.config, dict):
            raise TypeError("config must be a dict")
        self.apply_overrides()
        self.filter_hosts()

    def apply_overrides(self):
        """
        Look for an 'overrides' dict in self.ctx.config; look inside that for a
        dict with the same name as this task. Override any settings in
        self.config with those overrides
        """
        if not hasattr(self.ctx, 'config'):
            return
        all_overrides = self.ctx.config.get('overrides', dict())
        if not all_overrides:
            return
        task_overrides = all_overrides.get(self.name)
        if task_overrides:
            self.log.debug(
                "Applying overrides for task {name}: {overrides}".format(
                    name=self.name, overrides=task_overrides)
            )
            deep_merge(self.config, task_overrides)

    def filter_hosts(self):
        """
        Look for a 'hosts' list in self.config. Each item in the list may
        either be a role or a hostname. Builds a new Cluster object containing
        only those hosts which match one (or more) of the roles or hostnames
        specified. The filtered Cluster object is stored as self.cluster so
        that the task may only run against those hosts.
        """
        if not hasattr(self.ctx, 'cluster'):
            return
        elif 'hosts' not in self.config:
            self.cluster = self.ctx.cluster
            return self.cluster
        host_specs = self.config.get('hosts', list())
        cluster = Cluster()
        for host_spec in host_specs:
            role_matches = self.ctx.cluster.only(host_spec)
            if len(role_matches.remotes) > 0:
                for (remote, roles) in role_matches.remotes.items():
                    cluster.add(remote, roles)
            elif isinstance(host_spec, str):
                for (remote, roles) in self.ctx.cluster.remotes.items():
                    if remote.name.split('@')[-1] == host_spec or \
                            remote.shortname == host_spec:
                        cluster.add(remote, roles)
        if not cluster.remotes:
            raise RuntimeError("All target hosts were excluded!")
        self.cluster = cluster
        hostnames = [h.shortname for h in self.cluster.remotes.keys()]
        self.log.debug("Restricting task {name} to hosts: {hosts}".format(
            name=self.name, hosts=' '.join(hostnames))
        )
        return self.cluster

    def setup(self):
        """
        Perform any setup that is needed by the task before it executes
        """
        pass

    def begin(self):
        """
        Execute the main functionality of the task
        """
        pass

    def end(self):
        """
        Perform any work needed to stop processes started in begin()
        """
        pass

    def teardown(self):
        """
        Perform any work needed to restore configuration to a previous state.

        Can be skipped by setting 'skip_teardown' to True in self.config
        """
        pass

    def __enter__(self):
        """
        When using an instance of the class as a context manager, this method
        calls self.setup(), then calls self.begin() and returns self.
        """
        self.setup()
        self.begin()
        return self

    def __exit__(self, type_, value, traceback):
        """
        When using an instance of the class as a context manager, this method
        calls self.end() and self.teardown() - unless
        self.config['skip_teardown'] is True
        """
        self.end()
        if self.config.get('skip_teardown', False):
            self.log.info("Skipping teardown")
        else:
            self.teardown()
