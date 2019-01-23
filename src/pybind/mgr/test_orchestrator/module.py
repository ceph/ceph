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
    def __init__(self, execute_cb, complete_cb, message):
        super(TestWriteCompletion, self).__init__()
        self.execute_cb = execute_cb
        self.complete_cb = complete_cb

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

        if not self.effective:
            # TODO: check self.result for API errors
            if self.complete_cb is None:
                self.effective = True
            else:
                self.effective = self.complete_cb()


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
    def get_inventory(self, node_filter=None):
        """
        There is no guarantee which devices are returned by get_inventory.
        """
        try:
            c_v_out = check_output(['ceph-volume', 'inventory', '--format', 'json'])
        except OSError:
            cmd = """
            . {}/ceph-volume-virtualenv/bin/activate
            ceph-volume inventory --format json
            """.format(tmpdir=os.environ.get('TMPDIR', '/tmp'))
            c_v_out = check_output(cmd, shell=True)

        for out in c_v_out.splitlines():
            if not out.startswith(b'-->') and not out.startswith(b' stderr'):
                self.log.error(out)
                devs = []
                for device in json.loads(out):
                    dev = orchestrator.InventoryDevice()
                    if device["sys_api"]["rotational"] == "1":
                        dev.type = 'hdd'  # 'ssd', 'hdd', 'nvme'
                    elif 'nvme' in device["path"]:
                        dev.type = 'nvme'
                    else:
                        dev.type = 'ssd'
                    dev.size = device['sys_api']['size']
                    dev.id = device['path']
                    dev.extended = device
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

    def add_stateless_service(self, service_type, spec):
        raise NotImplementedError(service_type)

    def create_osds(self, drive_group, all_hosts):
        raise NotImplementedError(str(drive_group))

    def service_action(self, action, service_type, service_name=None, service_id=None):
        return TestWriteCompletion(
            lambda: True, None,
            "Pretending to {} service {} (name={}, id={})".format(
                action, service_type, service_name, service_id))

