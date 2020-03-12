import datetime
import errno
import json
import yaml

import six

from ceph.deployment.inventory import Device
from prettytable import PrettyTable

from mgr_util import format_bytes, to_pretty_timedelta

try:
    from typing import List, Set, Optional, Dict
except ImportError:
    pass  # just for type checking.


from ceph.deployment.drive_group import DriveGroupSpec, DeviceSelection, \
    DriveGroupSpecs
from ceph.deployment.service_spec import PlacementSpec, ServiceSpec
from mgr_module import MgrModule, HandleCommandResult

from ._interface import OrchestratorClientMixin, DeviceLightLoc, _cli_read_command, \
    raise_if_exception, _cli_write_command, TrivialReadCompletion, OrchestratorError, \
    NoOrchestrator, OrchestratorValidationError, NFSServiceSpec, \
    RGWSpec, InventoryFilter, InventoryHost, HostSpec, CLICommandMeta

def nice_delta(now, t, suffix=''):
    if t:
        return to_pretty_timedelta(now - t) + suffix
    else:
        return '-'

@six.add_metaclass(CLICommandMeta)
class OrchestratorCli(OrchestratorClientMixin, MgrModule):
    MODULE_OPTIONS = [
        {
            'name': 'orchestrator',
            'type': 'str',
            'default': None,
            'desc': 'Orchestrator backend',
            'enum_allowed': ['cephadm', 'rook',
                             'test_orchestrator'],
            'runtime': True,
        },
    ]
    NATIVE_OPTIONS = []  # type: List[dict]

    def __init__(self, *args, **kwargs):
        super(OrchestratorCli, self).__init__(*args, **kwargs)
        self.ident = set()  # type: Set[str]
        self.fault = set()  # type: Set[str]
        self._load()
        self._refresh_health()

    def _load(self):
        active = self.get_store('active_devices')
        if active:
            decoded = json.loads(active)
            self.ident = set(decoded.get('ident', []))
            self.fault = set(decoded.get('fault', []))
        self.log.debug('ident {}, fault {}'.format(self.ident, self.fault))

    def _save(self):
        encoded = json.dumps({
            'ident': list(self.ident),
            'fault': list(self.fault),
            })
        self.set_store('active_devices', encoded)

    def _refresh_health(self):
        h = {}
        if self.ident:
            h['DEVICE_IDENT_ON'] = {
                'severity': 'warning',
                'summary': '%d devices have ident light turned on' % len(
                    self.ident),
                'detail': ['{} ident light enabled'.format(d) for d in self.ident]
            }
        if self.fault:
            h['DEVICE_FAULT_ON'] = {
                'severity': 'warning',
                'summary': '%d devices have fault light turned on' % len(
                    self.fault),
                'detail': ['{} fault light enabled'.format(d) for d in self.ident]
            }
        self.set_health_checks(h)

    def _get_device_locations(self, dev_id):
        # type: (str) -> List[DeviceLightLoc]
        locs = [d['location'] for d in self.get('devices')['devices'] if d['devid'] == dev_id]
        return [DeviceLightLoc(**l) for l in sum(locs, [])]

    @_cli_read_command(
        prefix='device ls-lights',
        desc='List currently active device indicator lights')
    def _device_ls(self):
        return HandleCommandResult(
            stdout=json.dumps({
                'ident': list(self.ident),
                'fault': list(self.fault)
                }, indent=4, sort_keys=True))

    def light_on(self, fault_ident, devid):
        # type: (str, str) -> HandleCommandResult
        assert fault_ident in ("fault", "ident")
        locs = self._get_device_locations(devid)
        if locs is None:
            return HandleCommandResult(stderr='device {} not found'.format(devid),
                                       retval=-errno.ENOENT)

        getattr(self, fault_ident).add(devid)
        self._save()
        self._refresh_health()
        completion = self.blink_device_light(fault_ident, True, locs)
        self._orchestrator_wait([completion])
        return HandleCommandResult(stdout=str(completion.result))

    def light_off(self, fault_ident, devid, force):
        # type: (str, str, bool) -> HandleCommandResult
        assert fault_ident in ("fault", "ident")
        locs = self._get_device_locations(devid)
        if locs is None:
            return HandleCommandResult(stderr='device {} not found'.format(devid),
                                       retval=-errno.ENOENT)

        try:
            completion = self.blink_device_light(fault_ident, False, locs)
            self._orchestrator_wait([completion])

            if devid in getattr(self, fault_ident):
                getattr(self, fault_ident).remove(devid)
                self._save()
                self._refresh_health()
            return HandleCommandResult(stdout=str(completion.result))

        except:
            # There are several reasons the try: block might fail:
            # 1. the device no longer exist
            # 2. the device is no longer known to Ceph
            # 3. the host is not reachable
            if force and devid in getattr(self, fault_ident):
                getattr(self, fault_ident).remove(devid)
                self._save()
                self._refresh_health()
            raise

    @_cli_write_command(
        prefix='device light',
        cmd_args='name=enable,type=CephChoices,strings=on|off '
                 'name=devid,type=CephString '
                 'name=light_type,type=CephChoices,strings=ident|fault,req=false '
                 'name=force,type=CephBool,req=false',
        desc='Enable or disable the device light. Default type is `ident`\n'
             'Usage: device light (on|off) <devid> [ident|fault] [--force]')
    def _device_light(self, enable, devid, light_type=None, force=False):
        # type: (str, str, Optional[str], bool) -> HandleCommandResult
        light_type = light_type or 'ident'
        on = enable == 'on'
        if on:
            return self.light_on(light_type, devid)
        else:
            return self.light_off(light_type, devid, force)

    def _select_orchestrator(self):
        return self.get_module_option("orchestrator")

    @_cli_write_command(
        'orch host add',
        'name=hostname,type=CephString,req=true '
        'name=addr,type=CephString,req=false '
        'name=labels,type=CephString,n=N,req=false',
        'Add a host')
    def _add_host(self, hostname:str, addr: Optional[str]=None, labels: Optional[List[str]]=None):
        s = HostSpec(hostname=hostname, addr=addr, labels=labels)
        completion = self.add_host(s)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch host rm',
        "name=hostname,type=CephString,req=true",
        'Remove a host')
    def _remove_host(self, hostname):
        completion = self.remove_host(hostname)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch host set-addr',
        'name=hostname,type=CephString '
        'name=addr,type=CephString',
        'Update a host address')
    def _update_set_addr(self, hostname, addr):
        completion = self.update_host_addr(hostname, addr)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_read_command(
        'orch host ls',
        'name=format,type=CephChoices,strings=json|plain,req=false',
        'List hosts')
    def _get_hosts(self, format='plain'):
        completion = self.get_hosts()
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        if format == 'json':
            hosts = [host.to_json()
                     for host in completion.result]
            output = json.dumps(hosts, sort_keys=True)
        else:
            table = PrettyTable(
                ['HOST', 'ADDR', 'LABELS', 'STATUS'],
                border=False)
            table.align = 'l'
            table.left_padding_width = 0
            table.right_padding_width = 2
            for host in sorted(completion.result, key=lambda h: h.hostname):
                table.add_row((host.hostname, host.addr, ' '.join(host.labels), host.status))
            output = table.get_string()
        return HandleCommandResult(stdout=output)

    @_cli_write_command(
        'orch host label add',
        'name=hostname,type=CephString '
        'name=label,type=CephString',
        'Add a host label')
    def _host_label_add(self, hostname, label):
        completion = self.add_host_label(hostname, label)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch host label rm',
        'name=hostname,type=CephString '
        'name=label,type=CephString',
        'Remove a host label')
    def _host_label_rm(self, hostname, label):
        completion = self.remove_host_label(hostname, label)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_read_command(
        'orch device ls',
        "name=hostname,type=CephString,n=N,req=false "
        "name=format,type=CephChoices,strings=json|plain,req=false "
        "name=refresh,type=CephBool,req=false",
        'List devices on a host')
    def _list_devices(self, hostname=None, format='plain', refresh=False):
        # type: (Optional[List[str]], str, bool) -> HandleCommandResult
        """
        Provide information about storage devices present in cluster hosts

        Note: this does not have to be completely synchronous. Slightly out of
        date hardware inventory is fine as long as hardware ultimately appears
        in the output of this command.
        """
        nf = InventoryFilter(hosts=hostname) if hostname else None

        completion = self.get_inventory(host_filter=nf, refresh=refresh)

        self._orchestrator_wait([completion])
        raise_if_exception(completion)

        if format == 'json':
            data = [n.to_json() for n in completion.result]
            return HandleCommandResult(stdout=json.dumps(data))
        else:
            out = []

            table = PrettyTable(
                ['HOST', 'PATH', 'TYPE', 'SIZE', 'DEVICE', 'AVAIL',
                 'REJECT REASONS'],
                border=False)
            table.align = 'l'
            table._align['SIZE'] = 'r'
            table.left_padding_width = 0
            table.right_padding_width = 2
            for host_ in completion.result: # type: InventoryHost
                for d in host_.devices.devices:  # type: Device
                    table.add_row(
                        (
                            host_.name,
                            d.path,
                            d.human_readable_type,
                            format_bytes(d.sys_api.get('size', 0), 5),
                            d.device_id,
                            d.available,
                            ', '.join(d.rejected_reasons)
                        )
                    )
            out.append(table.get_string())
            return HandleCommandResult(stdout='\n'.join(out))

    @_cli_write_command(
        'orch device zap',
        'name=hostname,type=CephString '
        'name=path,type=CephString '
        'name=force,type=CephBool,req=false',
        'Zap (erase!) a device so it can be re-used')
    def _zap_device(self, hostname, path, force=False):
        if not force:
            raise OrchestratorError('must pass --force to PERMANENTLY ERASE DEVICE DATA')
        completion = self.zap_device(hostname, path)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_read_command(
        'orch ls',
        "name=service_type,type=CephString,req=false "
        "name=service_name,type=CephString,req=false "
        "name=format,type=CephChoices,strings=json|plain,req=false "
        "name=refresh,type=CephBool,req=false",
        'List services known to orchestrator')
    def _list_services(self, host=None, service_type=None, service_name=None, format='plain', refresh=False):
        completion = self.describe_service(service_type,
                                           service_name,
                                           refresh=refresh)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        services = completion.result

        def ukn(s):
            return '<unknown>' if s is None else s

        # Sort the list for display
        services.sort(key=lambda s: (ukn(s.service_name)))

        if len(services) == 0:
            return HandleCommandResult(stdout="No services reported")
        elif format == 'json':
            data = [s.to_json() for s in services]
            return HandleCommandResult(stdout=json.dumps(data))
        else:
            now = datetime.datetime.utcnow()
            table = PrettyTable(
                ['NAME', 'RUNNING', 'REFRESHED', 'AGE',
                 'SPEC',
                 'IMAGE NAME', 'IMAGE ID',
                ],
                border=False)
            table.align['NAME'] = 'l'
            table.align['RUNNING'] = 'r'
            table.align['REFRESHED'] = 'l'
            table.align['AGE'] = 'l'
            table.align['IMAGE NAME'] = 'l'
            table.align['IMAGE ID'] = 'l'
            table.align['SPEC'] = 'l'
            table.left_padding_width = 0
            table.right_padding_width = 2
            for s in sorted(services, key=lambda s: s.service_name):
                table.add_row((
                    s.service_name,
                    '%d/%d' % (s.running, s.size),
                    nice_delta(now, s.last_refresh, ' age'),
                    nice_delta(now, s.created),
                    s.spec.placement.pretty_str() if s.spec else '-',
                    ukn(s.container_image_name),
                    ukn(s.container_image_id)[0:12],
                ))

            return HandleCommandResult(stdout=table.get_string())

    @_cli_read_command(
        'orch ps',
        "name=hostname,type=CephString,req=false "
        "name=daemon_type,type=CephString,req=false "
        "name=daemon_id,type=CephString,req=false "
        "name=format,type=CephChoices,strings=json|plain,req=false "
        "name=refresh,type=CephBool,req=false",
        'List daemons known to orchestrator')
    def _list_daemons(self, hostname=None, daemon_type=None, daemon_id=None, format='plain', refresh=False):
        completion = self.list_daemons(daemon_type,
                                       daemon_id=daemon_id,
                                       host=hostname,
                                       refresh=refresh)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        daemons = completion.result

        def ukn(s):
            return '<unknown>' if s is None else s
        # Sort the list for display
        daemons.sort(key=lambda s: (ukn(s.daemon_type), ukn(s.hostname), ukn(s.daemon_id)))

        if len(daemons) == 0:
            return HandleCommandResult(stdout="No daemons reported")
        elif format == 'json':
            data = [s.to_json() for s in daemons]
            return HandleCommandResult(stdout=json.dumps(data))
        else:
            now = datetime.datetime.utcnow()
            table = PrettyTable(
                ['NAME', 'HOST', 'STATUS', 'REFRESHED', 'AGE',
                 'VERSION', 'IMAGE NAME', 'IMAGE ID', 'CONTAINER ID'],
                border=False)
            table.align = 'l'
            table.left_padding_width = 0
            table.right_padding_width = 2
            for s in sorted(daemons, key=lambda s: s.name()):
                status = {
                    -1: 'error',
                    0: 'stopped',
                    1: 'running',
                    None: '<unknown>'
                }[s.status]
                if s.status == 1 and s.started:
                    status += ' (%s)' % to_pretty_timedelta(now - s.started)

                table.add_row((
                    s.name(),
                    ukn(s.hostname),
                    status,
                    nice_delta(now, s.last_refresh, ' ago'),
                    nice_delta(now, s.created),
                    ukn(s.version),
                    ukn(s.container_image_name),
                    ukn(s.container_image_id)[0:12],
                    ukn(s.container_id)[0:12]))

            return HandleCommandResult(stdout=table.get_string())

    @_cli_write_command(
        'orch osd create',
        "name=svc_arg,type=CephString,req=false",
        'Create an OSD service. Either --svc_arg=host:drives or -i <drive_group>')
    def _create_osd(self, svc_arg=None, inbuf=None):
        # type: (Optional[str], Optional[str]) -> HandleCommandResult
        """Create one or more OSDs"""

        usage = """
Usage:
  ceph orch osd create -i <json_file/yaml_file>
  ceph orch osd create host:device1,device2,...
"""

        if inbuf:
            try:
                dgs = DriveGroupSpecs(yaml.load(inbuf))
                drive_groups = dgs.drive_groups
            except ValueError as e:
                msg = 'Failed to read JSON input: {}'.format(str(e)) + usage
                return HandleCommandResult(-errno.EINVAL, stderr=msg)

        elif svc_arg:
            try:
                host_name, block_device = svc_arg.split(":")
                block_devices = block_device.split(',')
            except (TypeError, KeyError, ValueError):
                msg = "Invalid host:device spec: '{}'".format(svc_arg) + usage
                return HandleCommandResult(-errno.EINVAL, stderr=msg)

            devs = DeviceSelection(paths=block_devices)
            drive_groups = [DriveGroupSpec(placement=PlacementSpec(host_pattern=host_name), data_devices=devs)]
        else:
            return HandleCommandResult(-errno.EINVAL, stderr=usage)

        completion = self.create_osds(drive_groups)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())
    
    @_cli_write_command(
        'orch osd rm',
        "name=svc_id,type=CephString,n=N "
        "name=replace,type=CephBool,req=false "
        "name=force,type=CephBool,req=false",
        'Remove OSD services')
    def _osd_rm(self, svc_id: List[str],
                replace: bool = False,
                force: bool = False) -> HandleCommandResult:
        completion = self.remove_osds(svc_id, replace, force)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch osd rm status',
        desc='status of OSD removal operation')
    def _osd_rm_status(self) -> HandleCommandResult:
        completion = self.remove_osds_status()
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        report = completion.result
        if not report:
            return HandleCommandResult(stdout="No OSD remove/replace operations reported")
        table = PrettyTable(
            ['NAME', 'HOST', 'PGS', 'STARTED_AT'],
            border=False)
        table.align = 'l'
        table.left_padding_width = 0
        table.right_padding_width = 1
        # TODO: re-add sorted and sort by pg_count
        for osd in report:
            table.add_row((osd.fullname, osd.nodename, osd.pg_count_str, osd.started_at))

        return HandleCommandResult(stdout=table.get_string())

    @_cli_write_command(
        'orch daemon add mon',
        'name=placement,type=CephString,req=false',
        'Start monitor daemon(s)')
    def _daemon_add_mon(self, placement=None):
        placement = PlacementSpec.from_string(placement)
        placement.validate()

        spec = ServiceSpec('mon', placement=placement)

        completion = self.add_mon(spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch daemon add mgr',
        'name=placement,type=CephString,req=false',
        'Start rbd-mirror daemon(s)')
    def _daemon_add_mgr(self, placement=None):
        spec = ServiceSpec(
            'mgr',
            placement=PlacementSpec.from_string(placement),
        )
        completion = self.add_mgr(spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch apply',
        desc='Applies a Service Specification from a file. ceph orch apply -i $file')
    def _apply_services(self, inbuf):
        completion = self.apply_service_config(inbuf)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch daemon add rbd-mirror',
        'name=placement,type=CephString,req=false',
        'Start rbd-mirror daemon(s)')
    def _rbd_mirror_add(self, placement=None):
        spec = ServiceSpec(
            'rbd-mirror',
            placement=PlacementSpec.from_string(placement),
        )
        completion = self.add_rbd_mirror(spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch daemon add mds',
        'name=fs_name,type=CephString '
        'name=placement,type=CephString,req=false',
        'Start MDS daemon(s)')
    def _mds_add(self, fs_name, placement=None):
        spec = ServiceSpec(
            'mds', fs_name,
            placement=PlacementSpec.from_string(placement),
        )
        completion = self.add_mds(spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch daemon add rgw',
        'name=realm_name,type=CephString '
        'name=zone_name,type=CephString '
        'name=placement,type=CephString,req=false',
        'Start RGW daemon(s)')
    def _rgw_add(self, realm_name, zone_name, placement=None, inbuf=None):
        usage = """
Usage:
  ceph orch daemon rgw add -i <json_file>
  ceph orch daemon rgw add <realm_name> <zone_name>
        """
        if inbuf:
            try:
                rgw_spec = RGWSpec.from_json(json.loads(inbuf))
            except ValueError as e:
                msg = 'Failed to read JSON input: {}'.format(str(e)) + usage
                return HandleCommandResult(-errno.EINVAL, stderr=msg)
        rgw_spec = RGWSpec(
            rgw_realm=realm_name,
            rgw_zone=zone_name,
            placement=PlacementSpec.from_string(placement),
        )

        completion = self.add_rgw(rgw_spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch daemon add nfs',
        "name=svc_arg,type=CephString "
        "name=pool,type=CephString "
        "name=namespace,type=CephString,req=false "
        'name=placement,type=CephString,req=false',
        'Start NFS daemon(s)')
    def _nfs_add(self, svc_arg, pool, namespace=None, placement=None):
        spec = NFSServiceSpec(
            svc_arg,
            pool=pool,
            namespace=namespace,
            placement=PlacementSpec.from_string(placement),
        )
        spec.validate_add()
        completion = self.add_nfs(spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch daemon add prometheus',
        'name=placement,type=CephString,req=false',
        'Add prometheus daemon(s)')
    def _daemon_add_prometheus(self, placement=None):
        spec = ServiceSpec(
            'prometheus',
            placement=PlacementSpec.from_string(placement),
        )
        completion = self.add_prometheus(spec)
        self._orchestrator_wait([completion])
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch daemon add node-exporter',
        'name=placement,type=CephString,req=false',
        'Add node-exporter daemon(s)')
    def _daemon_add_node_exporter(self, placement=None):
        spec = ServiceSpec(
            'node-exporter',
            placement=PlacementSpec.from_string(placement),
        )
        completion = self.add_node_exporter(spec)
        self._orchestrator_wait([completion])
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch daemon add crash',
        'name=placement,type=CephString,req=false',
        'Add node-exporter daemon(s)')
    def _daemon_add_crash(self, placement=None):
        spec = ServiceSpec(
            'crash',
            placement=PlacementSpec.from_string(placement),
        )
        completion = self.add_crash(spec)
        self._orchestrator_wait([completion])
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch daemon add grafana',
        'name=placement,type=CephString,req=false',
        'Add grafana daemon(s)')
    def _daemon_add_grafana(self, placement=None):
        # type: (Optional[str]) -> HandleCommandResult
        spec = ServiceSpec(
            'grafana',
            placement=PlacementSpec.from_string(placement),
        )
        completion = self.add_grafana(spec)
        self._orchestrator_wait([completion])
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch daemon add alertmanager',
        'name=placement,type=CephString,req=false',
        'Add alertmanager daemon(s)')
    def _daemon_add_alertmanager(self, placement=None):
        # type: (Optional[str]) -> HandleCommandResult
        spec = ServiceSpec(
            'alertmanager',
            placement=PlacementSpec.from_string(placement),
        )
        completion = self.add_alertmanager(spec)
        self._orchestrator_wait([completion])
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch',
        "name=action,type=CephChoices,strings=start|stop|restart|redeploy|reconfig "
        "name=service_name,type=CephString",
        'Start, stop, restart, redeploy, or reconfig an entire service (i.e. all daemons)')
    def _service_action(self, action, service_name):
        completion = self.service_action(action, service_name)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch daemon',
        "name=action,type=CephChoices,strings=start|stop|restart|redeploy|reconfig "
        "name=name,type=CephString",
        'Start, stop, restart, redeploy, or reconfig a specific daemon')
    def _daemon_action(self, action, name):
        if '.' not in name:
            raise OrchestratorError('%s is not a valid daemon name' % name)
        (daemon_type, daemon_id) = name.split('.', 1)
        completion = self.daemon_action(action, daemon_type, daemon_id)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch daemon rm',
        "name=names,type=CephString,n=N "
        'name=force,type=CephBool,req=false',
        'Remove specific daemon(s)')
    def _daemon_rm(self, names, force=False):
        for name in names:
            if '.' not in name:
                raise OrchestratorError('%s is not a valid daemon name' % name)
        completion = self.remove_daemons(names, force)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch rm',
        'name=service_name,type=CephString '
        'name=force,type=CephBool,req=false',
        'Remove a service')
    def _service_rm(self, service_name, force=False):
        if service_name in ['mon', 'mgr'] and not force:
            raise OrchestratorError('The mon and mgr services cannot be removed')
        completion = self.remove_service(service_name)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch spec dump',
        desc='List all Service specs')
    def _get_service_specs(self):
        completion = self.list_specs()
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        specs = completion.result_str()
        return HandleCommandResult(stdout=specs)

    @_cli_write_command(
        'orch apply mgr',
        'name=placement,type=CephString,req=false',
        'Update the size or placement of managers')
    def _apply_mgr(self, placement=None):
        placement = PlacementSpec.from_string(placement)
        placement.validate()

        spec = ServiceSpec('mgr', placement=placement)

        completion = self.apply_mgr(spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch apply mon',
        'name=placement,type=CephString,req=false',
        'Update the number of monitor instances')
    def _apply_mon(self, placement=None):
        placement = PlacementSpec.from_string(placement)
        placement.validate()

        spec = ServiceSpec('mon', placement=placement)

        completion = self.apply_mon(spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch apply mds',
        'name=fs_name,type=CephString '
        'name=placement,type=CephString,req=false',
        'Update the number of MDS instances for the given fs_name')
    def _apply_mds(self, fs_name, placement=None):
        placement = PlacementSpec.from_string(placement)
        placement.validate()
        spec = ServiceSpec(
            'mds', fs_name,
            placement=placement)
        completion = self.apply_mds(spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch apply rbd-mirror',
        'name=placement,type=CephString,req=false',
        'Update the number of rbd-mirror instances')
    def _apply_rbd_mirror(self, placement=None):
        spec = ServiceSpec(
            'rbd-mirror',
            placement=PlacementSpec.from_string(placement),
        )
        completion = self.apply_rbd_mirror(spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch apply rgw',
        'name=realm_name,type=CephString '
        'name=zone_name,type=CephString '
        'name=placement,type=CephString,req=false',
        'Update the number of RGW instances for the given zone')
    def _apply_rgw(self, zone_name, realm_name, placement=None):
        spec = RGWSpec(
            rgw_realm=realm_name,
            rgw_zone=zone_name,
            placement=PlacementSpec.from_string(placement),
        )
        completion = self.apply_rgw(spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch apply nfs',
        "name=svc_id,type=CephString "
        'name=placement,type=CephString,req=false',
        'Scale an NFS service')
    def _apply_nfs(self, svc_id, placement=None):
        spec = NFSServiceSpec(
            svc_id,
            placement=PlacementSpec.from_string(placement),
        )
        completion = self.apply_nfs(spec)
        self._orchestrator_wait([completion])
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch apply prometheus',
        'name=placement,type=CephString,req=false',
        'Scale prometheus service')
    def _apply_prometheus(self, placement=None):
        spec = ServiceSpec(
            'prometheus',
            placement=PlacementSpec.from_string(placement),
        )
        completion = self.apply_prometheus(spec)
        self._orchestrator_wait([completion])
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch apply grafana',
        'name=placement,type=CephString,req=false',
        'Scale grafana service')
    def _apply_grafana(self, placement=None):
        spec = ServiceSpec(
            'grafana',
            placement=PlacementSpec.from_string(placement),
        )
        completion = self.apply_grafana(spec)
        self._orchestrator_wait([completion])
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch apply alertmanager',
        'name=placement,type=CephString,req=false',
        'Scale alertmanager service')
    def _apply_alertmanager(self, placement=None):
        spec = ServiceSpec(
            'alertmanager',
            placement=PlacementSpec.from_string(placement),
        )
        completion = self.apply_alertmanager(spec)
        self._orchestrator_wait([completion])
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch apply node-exporter',
        'name=placement,type=CephString,req=false',
        'Update node_exporter service')
    def _apply_node_exporter(self, placement=None):
        spec = ServiceSpec(
            'node-exporter',
            placement=PlacementSpec.from_string(placement),
        )
        completion = self.apply_node_exporter(spec)
        self._orchestrator_wait([completion])
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch apply crash',
        'name=placement,type=CephString,req=false',
        'Update node_exporter service')
    def _apply_crash(self, placement=None):
        spec = ServiceSpec(
            'crash',
            placement=PlacementSpec.from_string(placement),
        )
        completion = self.apply_crash(spec)
        self._orchestrator_wait([completion])
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch set backend',
        "name=module_name,type=CephString,req=true",
        'Select orchestrator module backend')
    def _set_backend(self, module_name):
        """
        We implement a setter command instead of just having the user
        modify the setting directly, so that we can validate they're setting
        it to a module that really exists and is enabled.

        There isn't a mechanism for ensuring they don't *disable* the module
        later, but this is better than nothing.
        """
        mgr_map = self.get("mgr_map")

        if module_name is None or module_name == "":
            self.set_module_option("orchestrator", None)
            return HandleCommandResult()

        for module in mgr_map['available_modules']:
            if module['name'] != module_name:
                continue

            if not module['can_run']:
                continue

            enabled = module['name'] in mgr_map['modules']
            if not enabled:
                return HandleCommandResult(-errno.EINVAL,
                                           stderr="Module '{module_name}' is not enabled. \n Run "
                                                  "`ceph mgr module enable {module_name}` "
                                                  "to enable.".format(module_name=module_name))

            try:
                is_orchestrator = self.remote(module_name,
                                              "is_orchestrator_module")
            except NameError:
                is_orchestrator = False

            if not is_orchestrator:
                return HandleCommandResult(-errno.EINVAL,
                                           stderr="'{0}' is not an orchestrator module".format(module_name))

            self.set_module_option("orchestrator", module_name)

            return HandleCommandResult()

        return HandleCommandResult(-errno.EINVAL, stderr="Module '{0}' not found".format(module_name))

    @_cli_write_command(
        'orch cancel',
        desc='cancels ongoing operations')
    def _cancel(self):
        """
        ProgressReferences might get stuck. Let's unstuck them.
        """
        self.cancel_completions()
        return HandleCommandResult()

    @_cli_read_command(
        'orch status',
        desc='Report configured backend and its status')
    def _status(self):
        o = self._select_orchestrator()
        if o is None:
            raise NoOrchestrator()

        avail, why = self.available()
        if avail is None:
            # The module does not report its availability
            return HandleCommandResult(stdout="Backend: {0}".format(o))
        else:
            return HandleCommandResult(stdout="Backend: {0}\nAvailable: {1}{2}".format(
                                           o, avail,
                                           " ({0})".format(why) if not avail else ""
                                       ))

    def self_test(self):
        old_orch = self._select_orchestrator()
        self._set_backend('')
        assert self._select_orchestrator() is None
        self._set_backend(old_orch)

        e1 = self.remote('selftest', 'remote_from_orchestrator_cli_self_test', "ZeroDivisionError")
        try:
            raise_if_exception(e1)
            assert False
        except ZeroDivisionError as e:
            assert e.args == ('hello', 'world')

        e2 = self.remote('selftest', 'remote_from_orchestrator_cli_self_test', "OrchestratorError")
        try:
            raise_if_exception(e2)
            assert False
        except OrchestratorError as e:
            assert e.args == ('hello', 'world')

        c = TrivialReadCompletion(result=True)
        assert c.has_result

    @_cli_write_command(
        'upgrade check',
        'name=image,type=CephString,req=false '
        'name=ceph_version,type=CephString,req=false',
        desc='Check service versions vs available and target containers')
    def _upgrade_check(self, image=None, ceph_version=None):
        completion = self.upgrade_check(image=image, version=ceph_version)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'upgrade status',
        desc='Check service versions vs available and target containers')
    def _upgrade_status(self):
        completion = self.upgrade_status()
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        r = {
            'target_image': completion.result.target_image,
            'in_progress': completion.result.in_progress,
            'services_complete': completion.result.services_complete,
            'message': completion.result.message,
        }
        out = json.dumps(r, indent=4)
        return HandleCommandResult(stdout=out)

    @_cli_write_command(
        'upgrade start',
        'name=image,type=CephString,req=false '
        'name=ceph_version,type=CephString,req=false',
        desc='Initiate upgrade')
    def _upgrade_start(self, image=None, ceph_version=None):
        completion = self.upgrade_start(image, ceph_version)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'upgrade pause',
        desc='Pause an in-progress upgrade')
    def _upgrade_pause(self):
        completion = self.upgrade_pause()
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'upgrade resume',
        desc='Resume paused upgrade')
    def _upgrade_resume(self):
        completion = self.upgrade_resume()
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'upgrade stop',
        desc='Stop an in-progress upgrade')
    def _upgrade_stop(self):
        completion = self.upgrade_stop()
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())
