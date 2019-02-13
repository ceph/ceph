import json
import re
import os
import threading
import functools
import uuid
from subprocess import check_output

from mgr_module import MgrModule

import orchestrator


all_completions = []


class TestReadCompletion(orchestrator.ReadCompletion):

    def __init__(self, cb):
        super(TestReadCompletion, self).__init__()
        self.cb = cb
        self._result = None
        self._complete = False

        self.message = "<read op>"

        global all_completions
        all_completions.append(self)

    def __str__(self):
        return "TestReadCompletion(result={} message={})".format(self.result, self.message)


    @property
    def result(self):
        return self._result

    @property
    def is_complete(self):
        return self._complete

    def execute(self):
        self._result = self.cb()
        self._complete = True


class TestWriteCompletion(orchestrator.WriteCompletion):
    def __init__(self, execute_cb, message):
        super(TestWriteCompletion, self).__init__()
        self.execute_cb = execute_cb

        # Executed means I executed my API call, it may or may
        # not have succeeded
        self.executed = False

        self._result = None

        self.effective = False

        self.id = str(uuid.uuid4())

        self.message = message

        self.error = None

        # XXX hacky global
        global all_completions
        all_completions.append(self)

    def __str__(self):
        return "TestWriteCompletion(executed={} result={} id={} message={} error={})".format(self.executed, self._result, self.id, self.message,  self.error)

    @property
    def result(self):
        return self._result

    @property
    def is_persistent(self):
        return (not self.is_errored) and self.executed

    @property
    def is_effective(self):
        return self.effective

    @property
    def is_errored(self):
        return self.error is not None

    def execute(self):
        if not self.executed:
            self._result = self.execute_cb()
            self.executed = True
            self.effective = True


def deferred_write(message):
    def wrapper(f):
        @functools.wraps(f)
        def inner(*args, **kwargs):
            return TestWriteCompletion(lambda: f(*args, **kwargs),
                                       '{}, args={}, kwargs={}'.format(message, args, kwargs))
        return inner
    return wrapper


def deferred_read(f):
    """
    Decorator to make TestOrchestrator methods return
    a completion object that executes themselves.
    """

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        return TestReadCompletion(lambda: f(*args, **kwargs))

    return wrapper


class TestOrchestrator(MgrModule, orchestrator.Orchestrator):
    """
    This is an orchestrator implementation used for internal testing. It's meant for
    development environments and integration testing.

    It does not actually do anything.

    The implementation is similar to the Rook orchestrator, but simpler.
    """
    def _progress(self, *args, **kwargs):
        try:
            self.remote("progress", *args, **kwargs)
        except ImportError:
            # If the progress module is disabled that's fine,
            # they just won't see the output.
            pass

    def wait(self, completions):
        self.log.info("wait: completions={0}".format(completions))

        incomplete = False

        # Our `wait` implementation is very simple because everything's
        # just an API call.
        for c in completions:
            if not isinstance(c, TestReadCompletion) and \
                    not isinstance(c, TestWriteCompletion):
                raise TypeError(
                    "wait() requires list of completions, not {0}".format(
                        c.__class__
                    ))

            if c.is_complete:
                continue

            if not c.is_read:
                self._progress("update", c.id, c.message, 0.5)

            try:
                c.execute()
            except Exception as e:
                self.log.exception("Completion {0} threw an exception:".format(
                    c.message
                ))
                c.error = e
                c._complete = True
                if not c.is_read:
                    self._progress("complete", c.id)
            else:
                if c.is_complete:
                    if not c.is_read:
                        self._progress("complete", c.id)

            if not c.is_complete:
                incomplete = True

        return not incomplete

    def available(self):
        return True, ""

    def __init__(self, *args, **kwargs):
        super(TestOrchestrator, self).__init__(*args, **kwargs)

        self._initialized = threading.Event()
        self._shutdown = threading.Event()

    def shutdown(self):
        self._shutdown.set()

    def serve(self):

        self._initialized.set()

        while not self._shutdown.is_set():
            # XXX hack (or is it?) to kick all completions periodically,
            # in case we had a caller that wait()'ed on them long enough
            # to get persistence but not long enough to get completion

            global all_completions
            self.wait(all_completions)
            all_completions = [c for c in all_completions if not c.is_complete]

            self._shutdown.wait(5)

    @deferred_read
    def get_inventory(self, node_filter=None, refresh=False):
        """
        There is no guarantee which devices are returned by get_inventory.
        """
        if node_filter and node_filter.nodes is not None:
            assert isinstance(node_filter.nodes, list)
        try:
            c_v_out = check_output(['ceph-volume', 'inventory', '--format', 'json'])
        except OSError:
            cmd = """
            . {tmpdir}/ceph-volume-virtualenv/bin/activate
            ceph-volume inventory --format json
            """.format(tmpdir=os.environ.get('TMPDIR', '/tmp'))
            c_v_out = check_output(cmd, shell=True)

        for out in c_v_out.splitlines():
            if not out.startswith(b'-->') and not out.startswith(b' stderr'):
                self.log.error(out)
                devs = []
                for device in json.loads(out):
                    dev = orchestrator.InventoryDevice.from_ceph_volume_inventory(device)
                    devs.append(dev)
                return [orchestrator.InventoryNode('localhost', devs)]
        self.log.error('c-v failed: ' + str(c_v_out))
        raise Exception('c-v failed')

    @deferred_read
    def describe_service(self, service_type=None, service_id=None, node_name=None):
        """
        There is no guarantee which daemons are returned by describe_service, except that
        it returns the mgr we're running in.
        """
        if service_type:
            assert service_type in ("mds", "osd", "mon", "rgw", "mgr"), service_type + " unsupported"

        out = map(str, check_output(['ps', 'aux']).splitlines())
        types = [service_type] if service_type else ("mds", "osd", "mon", "rgw", "mgr")
        processes = [p for p in out if any([('ceph-' + t in p) for t in types])]

        result = []
        for p in processes:
            sd = orchestrator.ServiceDescription()
            sd.nodename = 'localhost'
            sd.service_instance = re.search('ceph-[^ ]+', p).group()
            result.append(sd)

        return result

    @deferred_write("Adding stateless service")
    def add_stateless_service(self, service_type, spec):
        pass

    @deferred_write("create_osds")
    def create_osds(self, drive_group, all_hosts):
        drive_group.validate(all_hosts)

    @deferred_write("remove_osds")
    def remove_osds(self, osd_ids):
        assert isinstance(osd_ids, list)

    @deferred_write("service_action")
    def service_action(self, action, service_type, service_name=None, service_id=None):
        pass

    @deferred_write("remove_stateless_service")
    def remove_stateless_service(self, service_type, id_):
        pass

    @deferred_read
    def get_hosts(self):
        return [orchestrator.InventoryNode('localhost', [])]

    @deferred_write("add_host")
    def add_host(self, host):
        assert isinstance(host, str)

    @deferred_write("remove_host")
    def remove_host(self, host):
        assert isinstance(host, str)

    @deferred_write("update_mgrs")
    def update_mgrs(self, num, hosts):
        assert not hosts or len(hosts) == num
        assert all([isinstance(h, str) for h in hosts])

    @deferred_write("update_mons")
    def update_mons(self, num, hosts):
        assert not hosts or len(hosts) == num
        assert all([isinstance(h[0], str) for h in hosts])
        assert all([isinstance(h[1], str) or h[1] is None for h in hosts])
