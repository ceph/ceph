import errno
import json
import re
import os
import threading
import functools
import itertools
from subprocess import check_output, CalledProcessError

from ceph.deployment.service_spec import ServiceSpec, NFSServiceSpec, IscsiServiceSpec

try:
    from typing import Callable, List, Sequence, Tuple
except ImportError:
    pass  # type checking

from ceph.deployment import inventory
from ceph.deployment.drive_group import DriveGroupSpec
from mgr_module import CLICommand, HandleCommandResult
from mgr_module import MgrModule

import orchestrator
from orchestrator import handle_orch_error, raise_if_exception


class TestOrchestrator(MgrModule, orchestrator.Orchestrator):
    """
    This is an orchestrator implementation used for internal testing. It's meant for
    development environments and integration testing.

    It does not actually do anything.

    The implementation is similar to the Rook orchestrator, but simpler.
    """

    @CLICommand('test_orchestrator load_data', perm='w')
    def _load_data(self, inbuf):
        """
        load dummy data into test orchestrator
        """
        try:
            data = json.loads(inbuf)
            self._init_data(data)
            return HandleCommandResult()
        except json.decoder.JSONDecodeError as e:
            msg = 'Invalid JSON file: {}'.format(e)
            return HandleCommandResult(retval=-errno.EINVAL, stderr=msg)
        except orchestrator.OrchestratorValidationError as e:
            return HandleCommandResult(retval=-errno.EINVAL, stderr=str(e))

    def available(self):
        return True, "", {}

    def __init__(self, *args, **kwargs):
        super(TestOrchestrator, self).__init__(*args, **kwargs)

        self._initialized = threading.Event()
        self._shutdown = threading.Event()
        self._init_data({})

    def shutdown(self):
        self._shutdown.set()

    def serve(self):

        self._initialized.set()

        while not self._shutdown.is_set():
            self._shutdown.wait(5)

    def _init_data(self, data=None):
        self._inventory = [orchestrator.InventoryHost.from_json(inventory_host)
                           for inventory_host in data.get('inventory', [])]
        self._services = [orchestrator.ServiceDescription.from_json(service)
                           for service in data.get('services', [])]
        self._daemons = [orchestrator.DaemonDescription.from_json(daemon)
                          for daemon in data.get('daemons', [])]

    @handle_orch_error
    def get_inventory(self, host_filter=None, refresh=False):
        """
        There is no guarantee which devices are returned by get_inventory.
        """
        if host_filter and host_filter.hosts is not None:
            assert isinstance(host_filter.hosts, list)

        if self._inventory:
            if host_filter:
                return list(filter(lambda host: host.name in host_filter.hosts,
                                   self._inventory))
            return self._inventory

        try:
            c_v_out = check_output(['ceph-volume', 'inventory', '--format', 'json'])
        except OSError:
            cmd = """
            . {tmpdir}/ceph-volume-virtualenv/bin/activate
            ceph-volume inventory --format json
            """
            try:
                c_v_out = check_output(cmd.format(tmpdir=os.environ.get('TMPDIR', '/tmp')), shell=True)
            except (OSError, CalledProcessError):
                c_v_out = check_output(cmd.format(tmpdir='.'),shell=True)

        for out in c_v_out.splitlines():
            self.log.error(out)
            devs = inventory.Devices.from_json(json.loads(out))
            return [orchestrator.InventoryHost('localhost', devs)]
        self.log.error('c-v failed: ' + str(c_v_out))
        raise Exception('c-v failed')

    def _get_ceph_daemons(self):
        # type: () -> List[orchestrator.DaemonDescription]
        """ Return ceph daemons on the running host."""
        types = ("mds", "osd", "mon", "rgw", "mgr", "nfs", "iscsi")
        out = map(str, check_output(['ps', 'aux']).splitlines())
        processes = [p for p in out if any(
            [('ceph-{} '.format(t) in p) for t in types])]

        daemons = []
        for p in processes:
            # parse daemon type
            m = re.search('ceph-([^ ]+)', p)
            if m:
                _daemon_type = m.group(1)
            else:
                raise AssertionError('Fail to determine daemon type from {}'.format(p))

            # parse daemon ID. Possible options: `-i <id>`, `--id=<id>`, `--id <id>`
            patterns = [r'-i\s(\w+)', r'--id[\s=](\w+)']
            for pattern in patterns:
                m = re.search(pattern, p)
                if m:
                    daemon_id = m.group(1)
                    break
            else:
                raise AssertionError('Fail to determine daemon ID from {}'.format(p))
            daemon = orchestrator.DaemonDescription(
                daemon_type=_daemon_type, daemon_id=daemon_id, hostname='localhost')
            daemons.append(daemon)
        return daemons

    @handle_orch_error
    def describe_service(self, service_type=None, service_name=None, refresh=False):
        if self._services:
            # Dummy data
            services = self._services
            if service_type is not None:
                services = list(filter(lambda s: s.spec.service_type == service_type, services))
        else:
            # Deduce services from daemons running on localhost
            all_daemons = self._get_ceph_daemons()
            services = []
            for daemon_type, daemons in itertools.groupby(all_daemons, key=lambda d: d.daemon_type):
                if service_type is not None and service_type != daemon_type:
                    continue
                daemon_size = len(list(daemons))
                services.append(orchestrator.ServiceDescription(
                    spec=ServiceSpec(
                        service_type=daemon_type,  # type: ignore
                    ),
                    size=daemon_size, running=daemon_size))
        
        def _filter_func(svc):
            if service_name is not None and service_name != svc.spec.service_name():
                return False
            return True

        return list(filter(_filter_func, services))

    @handle_orch_error
    def list_daemons(self, service_name=None, daemon_type=None, daemon_id=None, host=None, refresh=False):
        """
        There is no guarantee which daemons are returned by describe_service, except that
        it returns the mgr we're running in.
        """
        if daemon_type:
            daemon_types = ("mds", "osd", "mon", "rgw", "mgr", "iscsi", "crash", "nfs")
            assert daemon_type in daemon_types, daemon_type + " unsupported"

        daemons = self._daemons if self._daemons else self._get_ceph_daemons()

        def _filter_func(d):
            if service_name is not None and service_name != d.service_name():
                return False
            if daemon_type is not None and daemon_type != d.daemon_type:
                return False
            if daemon_id is not None and daemon_id != d.daemon_id:
                return False
            if host is not None and host != d.hostname:
                return False
            return True

        return list(filter(_filter_func, daemons))

    def preview_drivegroups(self, drive_group_name=None, dg_specs=None):
        return [{}]

    @handle_orch_error
    def create_osds(self, drive_group):
        # type: (DriveGroupSpec) -> str
        """ Creates OSDs from a drive group specification.

        $: ceph orch osd create -i <dg.file>

        The drivegroup file must only contain one spec at a time.
        """
        return self._create_osds(drive_group)

    def _create_osds(self, drive_group):
        # type: (DriveGroupSpec) -> str

        drive_group.validate()
        all_hosts = raise_if_exception(self.get_hosts())
        if not drive_group.placement.filter_matching_hostspecs(all_hosts):
            raise orchestrator.OrchestratorValidationError('failed to match')
        return ''

    @handle_orch_error
    def apply_drivegroups(self, specs):
        # type: (List[DriveGroupSpec]) -> List[str]
        return [self._create_osds(dg) for dg in specs]

    @handle_orch_error
    def remove_daemons(self, names):
        assert isinstance(names, list)
        return 'done'

    @handle_orch_error
    def remove_service(self, service_name, force = False):
        assert isinstance(service_name, str)
        return 'done'

    @handle_orch_error
    def blink_device_light(self, ident_fault, on, locations):
        assert ident_fault in ("ident", "fault")
        assert len(locations)
        return ''

    @handle_orch_error
    def service_action(self, action, service_name):
        return 'done'

    @handle_orch_error
    def daemon_action(self, action, daemon_name, image=None):
        return 'done'

    @handle_orch_error
    def add_daemon(self, spec: ServiceSpec):
        return [spec.one_line_str()]

    @handle_orch_error
    def apply_nfs(self, spec):
        return spec.one_line_str()

    @handle_orch_error
    def apply_iscsi(self, spec):
        # type: (IscsiServiceSpec) -> str
        return spec.one_line_str()

    @handle_orch_error
    def get_hosts(self):
        if self._inventory:
            return [orchestrator.HostSpec(i.name, i.addr, i.labels) for i in self._inventory]
        return [orchestrator.HostSpec('localhost')]

    @handle_orch_error
    def add_host(self, spec):
        # type: (orchestrator.HostSpec) -> str
        host = spec.hostname
        if host == 'raise_validation_error':
            raise orchestrator.OrchestratorValidationError("MON count must be either 1, 3 or 5")
        if host == 'raise_error':
            raise orchestrator.OrchestratorError("host address is empty")
        if host == 'raise_bug':
            raise ZeroDivisionError()
        if host == 'raise_not_implemented':
            raise NotImplementedError()
        if host == 'raise_no_orchestrator':
            raise orchestrator.NoOrchestrator()
        if host == 'raise_import_error':
            raise ImportError("test_orchestrator not enabled")
        assert isinstance(host, str)
        return ''

    @handle_orch_error
    def remove_host(self, host, force: bool, offline: bool, rm_crush_entry: bool):
        assert isinstance(host, str)
        return 'done'

    @handle_orch_error
    def apply_mgr(self, spec):
        # type: (ServiceSpec) -> str

        assert not spec.placement.hosts or len(spec.placement.hosts) == spec.placement.count
        assert all([isinstance(h, str) for h in spec.placement.hosts])
        return spec.one_line_str()

    @handle_orch_error
    def apply_mon(self, spec):
        # type: (ServiceSpec) -> str

        assert not spec.placement.hosts or len(spec.placement.hosts) == spec.placement.count
        assert all([isinstance(h[0], str) for h in spec.placement.hosts])
        assert all([isinstance(h[1], str) or h[1] is None for h in spec.placement.hosts])
        return spec.one_line_str()
