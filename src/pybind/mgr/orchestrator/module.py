import enum
import errno
import json
from typing import List, Set, Optional, Iterator, cast, Dict, Any, Union, Sequence
import re
import datetime

import yaml
from prettytable import PrettyTable

from ceph.deployment.inventory import Device
from ceph.deployment.drive_group import DriveGroupSpec, DeviceSelection, OSDMethod
from ceph.deployment.service_spec import PlacementSpec, ServiceSpec, service_spec_allow_invalid_from_json
from ceph.deployment.hostspec import SpecValidationError
from ceph.utils import datetime_now

from mgr_util import to_pretty_timedelta, format_dimless, format_bytes
from mgr_module import MgrModule, HandleCommandResult, Option

from ._interface import OrchestratorClientMixin, DeviceLightLoc, _cli_read_command, \
    raise_if_exception, _cli_write_command, OrchestratorError, \
    NoOrchestrator, OrchestratorValidationError, NFSServiceSpec, \
    RGWSpec, InventoryFilter, InventoryHost, HostSpec, CLICommandMeta, \
    ServiceDescription, DaemonDescription, IscsiServiceSpec, json_to_generic_spec, \
    GenericSpec, DaemonDescriptionStatus, SNMPGatewaySpec, MDSSpec


def nice_delta(now: datetime.datetime, t: Optional[datetime.datetime], suffix: str = '') -> str:
    if t:
        return to_pretty_timedelta(now - t) + suffix
    else:
        return '-'


def nice_bytes(v: Optional[int]) -> str:
    if not v:
        return '-'
    return format_bytes(v, 5)


class Format(enum.Enum):
    plain = 'plain'
    json = 'json'
    json_pretty = 'json-pretty'
    yaml = 'yaml'


class ServiceType(enum.Enum):
    mon = 'mon'
    mgr = 'mgr'
    rbd_mirror = 'rbd-mirror'
    cephfs_mirror = 'cephfs-mirror'
    crash = 'crash'
    alertmanager = 'alertmanager'
    grafana = 'grafana'
    node_exporter = 'node-exporter'
    prometheus = 'prometheus'
    mds = 'mds'
    rgw = 'rgw'
    nfs = 'nfs'
    iscsi = 'iscsi'
    snmp_gateway = 'snmp-gateway'


class ServiceAction(enum.Enum):
    start = 'start'
    stop = 'stop'
    restart = 'restart'
    redeploy = 'redeploy'
    reconfig = 'reconfig'


class DaemonAction(enum.Enum):
    start = 'start'
    stop = 'stop'
    restart = 'restart'
    reconfig = 'reconfig'


def to_format(what: Any, format: Format, many: bool, cls: Any) -> Any:
    def to_json_1(obj: Any) -> Any:
        if hasattr(obj, 'to_json'):
            return obj.to_json()
        return obj

    def to_json_n(objs: List) -> List:
        return [to_json_1(o) for o in objs]

    to_json = to_json_n if many else to_json_1

    if format == Format.json:
        return json.dumps(to_json(what), sort_keys=True)
    elif format == Format.json_pretty:
        return json.dumps(to_json(what), indent=2, sort_keys=True)
    elif format == Format.yaml:
        # fun with subinterpreters again. pyyaml depends on object identity.
        # as what originates from a different subinterpreter we have to copy things here.
        if cls:
            flat = to_json(what)
            copy = [cls.from_json(o) for o in flat] if many else cls.from_json(flat)
        else:
            copy = what

        def to_yaml_1(obj: Any) -> Any:
            if hasattr(obj, 'yaml_representer'):
                return obj
            return to_json_1(obj)

        def to_yaml_n(objs: list) -> list:
            return [to_yaml_1(o) for o in objs]

        to_yaml = to_yaml_n if many else to_yaml_1

        if many:
            return yaml.dump_all(to_yaml(copy), default_flow_style=False)
        return yaml.dump(to_yaml(copy), default_flow_style=False)
    else:
        raise OrchestratorError(f'unsupported format type: {format}')


def generate_preview_tables(data: Any, osd_only: bool = False) -> str:
    error = [x.get('error') for x in data if x.get('error')]
    if error:
        return json.dumps(error)
    warning = [x.get('warning') for x in data if x.get('warning')]
    osd_table = preview_table_osd(data)
    service_table = preview_table_services(data)

    if osd_only:
        tables = f"""
{''.join(warning)}

################
OSDSPEC PREVIEWS
################
{osd_table}
"""
        return tables
    else:
        tables = f"""
{''.join(warning)}

####################
SERVICESPEC PREVIEWS
####################
{service_table}

################
OSDSPEC PREVIEWS
################
{osd_table}
"""
        return tables


def preview_table_osd(data: List) -> str:
    table = PrettyTable(header_style='upper', title='OSDSPEC PREVIEWS', border=True)
    table.field_names = "service name host data db wal".split()
    table.align = 'l'
    table.left_padding_width = 0
    table.right_padding_width = 2
    notes = ''
    for osd_data in data:
        if osd_data.get('service_type') != 'osd':
            continue
        for host, specs in osd_data.get('data').items():
            for spec in specs:
                if spec.get('error'):
                    return spec.get('message')
                dg_name = spec.get('osdspec')
                if spec.get('notes', []):
                    notes += '\n'.join(spec.get('notes')) + '\n'
                for osd in spec.get('data', []):
                    db_path = osd.get('block_db', '-')
                    wal_path = osd.get('block_wal', '-')
                    block_data = osd.get('data', '')
                    if not block_data:
                        continue
                    table.add_row(('osd', dg_name, host, block_data, db_path, wal_path))
    return notes + table.get_string()


def preview_table_services(data: List) -> str:
    table = PrettyTable(header_style='upper', title="SERVICESPEC PREVIEW", border=True)
    table.field_names = 'SERVICE NAME ADD_TO REMOVE_FROM'.split()
    table.align = 'l'
    table.left_padding_width = 0
    table.right_padding_width = 2
    for item in data:
        if item.get('warning'):
            continue
        if item.get('service_type') != 'osd':
            table.add_row((item.get('service_type'), item.get('service_name'),
                           " ".join(item.get('add')), " ".join(item.get('remove'))))
    return table.get_string()


class OrchestratorCli(OrchestratorClientMixin, MgrModule,
                      metaclass=CLICommandMeta):
    MODULE_OPTIONS = [
        Option(
            'orchestrator',
            type='str',
            default=None,
            desc='Orchestrator backend',
            enum_allowed=['cephadm', 'rook', 'test_orchestrator'],
            runtime=True,
        )
    ]
    NATIVE_OPTIONS = []  # type: List[dict]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(OrchestratorCli, self).__init__(*args, **kwargs)
        self.ident = set()  # type: Set[str]
        self.fault = set()  # type: Set[str]
        self._load()
        self._refresh_health()

    def _load(self) -> None:
        active = self.get_store('active_devices')
        if active:
            decoded = json.loads(active)
            self.ident = set(decoded.get('ident', []))
            self.fault = set(decoded.get('fault', []))
        self.log.debug('ident {}, fault {}'.format(self.ident, self.fault))

    def _save(self) -> None:
        encoded = json.dumps({
            'ident': list(self.ident),
            'fault': list(self.fault),
        })
        self.set_store('active_devices', encoded)

    def _refresh_health(self) -> None:
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
        return [DeviceLightLoc(**loc) for loc in sum(locs, [])]

    @_cli_read_command(prefix='device ls-lights')
    def _device_ls(self) -> HandleCommandResult:
        """List currently active device indicator lights"""
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

            if devid in getattr(self, fault_ident):
                getattr(self, fault_ident).remove(devid)
                self._save()
                self._refresh_health()
            return HandleCommandResult(stdout=str(completion.result))

        except Exception:
            # There are several reasons the try: block might fail:
            # 1. the device no longer exist
            # 2. the device is no longer known to Ceph
            # 3. the host is not reachable
            if force and devid in getattr(self, fault_ident):
                getattr(self, fault_ident).remove(devid)
                self._save()
                self._refresh_health()
            raise

    class DeviceLightEnable(enum.Enum):
        on = 'on'
        off = 'off'

    class DeviceLightType(enum.Enum):
        ident = 'ident'
        fault = 'fault'

    @_cli_write_command(prefix='device light')
    def _device_light(self,
                      enable: DeviceLightEnable,
                      devid: str,
                      light_type: DeviceLightType = DeviceLightType.ident,
                      force: bool = False) -> HandleCommandResult:
        """
        Enable or disable the device light. Default type is `ident`
        'Usage: device light (on|off) <devid> [ident|fault] [--force]'
        """""
        if enable == self.DeviceLightEnable.on:
            return self.light_on(light_type.value, devid)
        else:
            return self.light_off(light_type.value, devid, force)

    def _select_orchestrator(self) -> str:
        return cast(str, self.get_module_option("orchestrator"))

    @_cli_write_command('orch host add')
    def _add_host(self,
                  hostname: str,
                  addr: Optional[str] = None,
                  labels: Optional[List[str]] = None,
                  maintenance: Optional[bool] = False) -> HandleCommandResult:
        """Add a host"""
        _status = 'maintenance' if maintenance else ''

        # split multiple labels passed in with --labels=label1,label2
        if labels and len(labels) == 1:
            labels = labels[0].split(',')

        s = HostSpec(hostname=hostname, addr=addr, labels=labels, status=_status)

        return self._apply_misc([s], False, Format.plain)

    @_cli_write_command('orch host rm')
    def _remove_host(self, hostname: str, force: bool = False, offline: bool = False) -> HandleCommandResult:
        """Remove a host"""
        completion = self.remove_host(hostname, force, offline)
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command('orch host drain')
    def _drain_host(self, hostname: str) -> HandleCommandResult:
        """drain all daemons from a host"""
        completion = self.drain_host(hostname)
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command('orch host set-addr')
    def _update_set_addr(self, hostname: str, addr: str) -> HandleCommandResult:
        """Update a host address"""
        completion = self.update_host_addr(hostname, addr)
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_read_command('orch host ls')
    def _get_hosts(self, format: Format = Format.plain, host_pattern: str = '', label: str = '', host_status: str = '') -> HandleCommandResult:
        """List hosts"""
        completion = self.get_hosts()
        hosts = raise_if_exception(completion)

        filter_spec = PlacementSpec(
            host_pattern=host_pattern,
            label=label
        )
        filtered_hosts: List[str] = filter_spec.filter_matching_hostspecs(hosts)
        hosts = [h for h in hosts if h.hostname in filtered_hosts]

        if host_status:
            hosts = [h for h in hosts if h.status.lower() == host_status]

        if format != Format.plain:
            output = to_format(hosts, format, many=True, cls=HostSpec)
        else:
            table = PrettyTable(
                ['HOST', 'ADDR', 'LABELS', 'STATUS'],
                border=False)
            table.align = 'l'
            table.left_padding_width = 0
            table.right_padding_width = 2
            for host in sorted(hosts, key=lambda h: h.hostname):
                table.add_row((host.hostname, host.addr, ' '.join(
                    host.labels), host.status.capitalize()))
            output = table.get_string()
        if format == Format.plain:
            output += f'\n{len(hosts)} hosts in cluster'
            if label:
                output += f' who had label {label}'
            if host_pattern:
                output += f' whose hostname matched {host_pattern}'
            if host_status:
                output += f' with status {host_status}'
        return HandleCommandResult(stdout=output)

    @_cli_write_command('orch host label add')
    def _host_label_add(self, hostname: str, label: str) -> HandleCommandResult:
        """Add a host label"""
        completion = self.add_host_label(hostname, label)
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command('orch host label rm')
    def _host_label_rm(self, hostname: str, label: str) -> HandleCommandResult:
        """Remove a host label"""
        completion = self.remove_host_label(hostname, label)
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command('orch host ok-to-stop')
    def _host_ok_to_stop(self, hostname: str) -> HandleCommandResult:
        """Check if the specified host can be safely stopped without reducing availability"""""
        completion = self.host_ok_to_stop(hostname)
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch host maintenance enter')
    def _host_maintenance_enter(self, hostname: str, force: bool = False) -> HandleCommandResult:
        """
        Prepare a host for maintenance by shutting down and disabling all Ceph daemons (cephadm only)
        """
        completion = self.enter_host_maintenance(hostname, force=force)
        raise_if_exception(completion)

        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch host maintenance exit')
    def _host_maintenance_exit(self, hostname: str) -> HandleCommandResult:
        """
        Return a host from maintenance, restarting all Ceph daemons (cephadm only)
        """
        completion = self.exit_host_maintenance(hostname)
        raise_if_exception(completion)

        return HandleCommandResult(stdout=completion.result_str())

    @_cli_read_command('orch device ls')
    def _list_devices(self,
                      hostname: Optional[List[str]] = None,
                      format: Format = Format.plain,
                      refresh: bool = False,
                      wide: bool = False) -> HandleCommandResult:
        """
        List devices on a host
        """
        # Provide information about storage devices present in cluster hosts
        #
        # Note: this does not have to be completely synchronous. Slightly out of
        # date hardware inventory is fine as long as hardware ultimately appears
        # in the output of this command.
        nf = InventoryFilter(hosts=hostname) if hostname else None

        completion = self.get_inventory(host_filter=nf, refresh=refresh)

        inv_hosts = raise_if_exception(completion)

        if format != Format.plain:
            return HandleCommandResult(stdout=to_format(inv_hosts,
                                                        format,
                                                        many=True,
                                                        cls=InventoryHost))
        else:
            display_map = {
                "Unsupported": "N/A",
                "N/A": "N/A",
                "On": "On",
                "Off": "Off",
                True: "Yes",
                False: "",
            }

            out = []
            if wide:
                table = PrettyTable(
                    ['HOST', 'PATH', 'TYPE', 'TRANSPORT', 'RPM', 'DEVICE ID', 'SIZE',
                     'HEALTH', 'IDENT', 'FAULT',
                     'AVAILABLE', 'REFRESHED', 'REJECT REASONS'],
                    border=False)
            else:
                table = PrettyTable(
                    ['HOST', 'PATH', 'TYPE', 'DEVICE ID', 'SIZE',
                     'AVAILABLE', 'REFRESHED', 'REJECT REASONS'],
                    border=False)
            table.align = 'l'
            table._align['SIZE'] = 'r'
            table.left_padding_width = 0
            table.right_padding_width = 2
            now = datetime_now()
            for host_ in sorted(inv_hosts, key=lambda h: h.name):  # type: InventoryHost
                for d in sorted(host_.devices.devices, key=lambda d: d.path):  # type: Device

                    led_ident = 'N/A'
                    led_fail = 'N/A'
                    if d.lsm_data.get('ledSupport', None):
                        led_ident = d.lsm_data['ledSupport']['IDENTstatus']
                        led_fail = d.lsm_data['ledSupport']['FAILstatus']

                    if wide:
                        table.add_row(
                            (
                                host_.name,
                                d.path,
                                d.human_readable_type,
                                d.lsm_data.get('transport', ''),
                                d.lsm_data.get('rpm', ''),
                                d.device_id,
                                format_dimless(d.sys_api.get('size', 0), 5),
                                d.lsm_data.get('health', ''),
                                display_map[led_ident],
                                display_map[led_fail],
                                display_map[d.available],
                                nice_delta(now, d.created, ' ago'),
                                ', '.join(d.rejected_reasons)
                            )
                        )
                    else:
                        table.add_row(
                            (
                                host_.name,
                                d.path,
                                d.human_readable_type,
                                d.device_id,
                                format_dimless(d.sys_api.get('size', 0), 5),
                                display_map[d.available],
                                nice_delta(now, d.created, ' ago'),
                                ', '.join(d.rejected_reasons)
                            )
                        )
            out.append(table.get_string())
            return HandleCommandResult(stdout='\n'.join(out))

    @_cli_write_command('orch device zap')
    def _zap_device(self, hostname: str, path: str, force: bool = False) -> HandleCommandResult:
        """
        Zap (erase!) a device so it can be re-used
        """
        if not force:
            raise OrchestratorError('must pass --force to PERMANENTLY ERASE DEVICE DATA')
        completion = self.zap_device(hostname, path)
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_read_command('orch ls')
    def _list_services(self,
                       service_type: Optional[str] = None,
                       service_name: Optional[str] = None,
                       export: bool = False,
                       format: Format = Format.plain,
                       refresh: bool = False) -> HandleCommandResult:
        """
        List services known to orchestrator
        """
        if export and format == Format.plain:
            format = Format.yaml

        completion = self.describe_service(service_type,
                                           service_name,
                                           refresh=refresh)

        services = raise_if_exception(completion)

        def ukn(s: Optional[str]) -> str:
            return '<unknown>' if s is None else s

        # Sort the list for display
        services.sort(key=lambda s: (ukn(s.spec.service_name())))

        if len(services) == 0:
            return HandleCommandResult(stdout="No services reported")
        elif format != Format.plain:
            with service_spec_allow_invalid_from_json():
                if export:
                    data = [s.spec for s in services if s.deleted is None]
                    return HandleCommandResult(stdout=to_format(data, format, many=True, cls=ServiceSpec))
                else:
                    return HandleCommandResult(stdout=to_format(services, format, many=True, cls=ServiceDescription))
        else:
            now = datetime_now()
            table = PrettyTable(
                [
                    'NAME', 'PORTS',
                    'RUNNING', 'REFRESHED', 'AGE',
                    'PLACEMENT',
                ],
                border=False)
            table.align['NAME'] = 'l'
            table.align['PORTS'] = 'l'
            table.align['RUNNING'] = 'r'
            table.align['REFRESHED'] = 'l'
            table.align['AGE'] = 'l'
            table.align['PLACEMENT'] = 'l'
            table.left_padding_width = 0
            table.right_padding_width = 2
            for s in services:
                if not s.spec:
                    pl = '<no spec>'
                elif s.spec.unmanaged:
                    pl = '<unmanaged>'
                else:
                    pl = s.spec.placement.pretty_str()
                if s.deleted:
                    refreshed = '<deleting>'
                else:
                    refreshed = nice_delta(now, s.last_refresh, ' ago')

                if s.spec.service_type == 'osd':
                    running = str(s.running)
                else:
                    running = '{}/{}'.format(s.running, s.size)

                table.add_row((
                    s.spec.service_name(),
                    s.get_port_summary(),
                    running,
                    refreshed,
                    nice_delta(now, s.created),
                    pl,
                ))

            return HandleCommandResult(stdout=table.get_string())

    @_cli_read_command('orch ps')
    def _list_daemons(self,
                      hostname: Optional[str] = None,
                      _end_positional_: int = 0,
                      service_name: Optional[str] = None,
                      daemon_type: Optional[str] = None,
                      daemon_id: Optional[str] = None,
                      format: Format = Format.plain,
                      refresh: bool = False) -> HandleCommandResult:
        """
        List daemons known to orchestrator
        """
        completion = self.list_daemons(service_name,
                                       daemon_type,
                                       daemon_id=daemon_id,
                                       host=hostname,
                                       refresh=refresh)

        daemons = raise_if_exception(completion)

        def ukn(s: Optional[str]) -> str:
            return '<unknown>' if s is None else s
        # Sort the list for display
        daemons.sort(key=lambda s: (ukn(s.daemon_type), ukn(s.hostname), ukn(s.daemon_id)))

        if format != Format.plain:
            return HandleCommandResult(stdout=to_format(daemons, format, many=True, cls=DaemonDescription))
        else:
            if len(daemons) == 0:
                return HandleCommandResult(stdout="No daemons reported")

            now = datetime_now()
            table = PrettyTable(
                ['NAME', 'HOST', 'PORTS',
                 'STATUS', 'REFRESHED', 'AGE',
                 'MEM USE', 'MEM LIM',
                 'VERSION', 'IMAGE ID', 'CONTAINER ID'],
                border=False)
            table.align = 'l'
            table._align['REFRESHED'] = 'r'
            table._align['AGE'] = 'r'
            table._align['MEM USE'] = 'r'
            table._align['MEM LIM'] = 'r'
            table.left_padding_width = 0
            table.right_padding_width = 2
            for s in sorted(daemons, key=lambda s: s.name()):
                if s.status_desc:
                    status = s.status_desc
                else:
                    status = DaemonDescriptionStatus.to_str(s.status)
                if s.status == DaemonDescriptionStatus.running and s.started:  # See DDS.starting
                    status += ' (%s)' % to_pretty_timedelta(now - s.started)

                table.add_row((
                    s.name(),
                    ukn(s.hostname),
                    s.get_port_summary(),
                    status,
                    nice_delta(now, s.last_refresh, ' ago'),
                    nice_delta(now, s.created),
                    nice_bytes(s.memory_usage),
                    nice_bytes(s.memory_request),
                    ukn(s.version),
                    ukn(s.container_image_id)[0:12],
                    ukn(s.container_id)))

            remove_column = 'CONTAINER ID'
            if table.get_string(fields=[remove_column], border=False,
                                header=False).count('<unknown>') == len(daemons):
                try:
                    table.del_column(remove_column)
                except AttributeError as e:
                    # del_column method was introduced in prettytable 2.0
                    if str(e) != "del_column":
                        raise
                    table.field_names.remove(remove_column)
                    table._rows = [row[:-1] for row in table._rows]

            return HandleCommandResult(stdout=table.get_string())

    @_cli_write_command('orch apply osd')
    def _apply_osd(self,
                   all_available_devices: bool = False,
                   format: Format = Format.plain,
                   unmanaged: Optional[bool] = None,
                   dry_run: bool = False,
                   no_overwrite: bool = False,
                   inbuf: Optional[str] = None  # deprecated. Was deprecated before Quincy
                   ) -> HandleCommandResult:
        """
        Create OSD daemon(s) on all available devices
        """

        if inbuf and all_available_devices:
            return HandleCommandResult(-errno.EINVAL, '-i infile and --all-available-devices are mutually exclusive')

        if not inbuf and not all_available_devices:
            # one parameter must be present
            return HandleCommandResult(-errno.EINVAL, '--all-available-devices is required')

        if inbuf:
            if unmanaged is not None:
                return HandleCommandResult(-errno.EINVAL, stderr='-i infile and --unmanaged are mutually exclusive')

            try:
                drivegroups = [_dg for _dg in yaml.safe_load_all(inbuf)]
            except yaml.scanner.ScannerError as e:
                msg = f"Invalid YAML received : {str(e)}"
                self.log.exception(e)
                return HandleCommandResult(-errno.EINVAL, stderr=msg)

            dg_specs = []
            for dg in drivegroups:
                spec = DriveGroupSpec.from_json(dg)
                if dry_run:
                    spec.preview_only = True
                dg_specs.append(spec)

            return self._apply_misc(dg_specs, dry_run, format, no_overwrite)

        if all_available_devices:
            if unmanaged is None:
                unmanaged = False
            dg_specs = [
                DriveGroupSpec(
                    service_id='all-available-devices',
                    placement=PlacementSpec(host_pattern='*'),
                    data_devices=DeviceSelection(all=True),
                    unmanaged=unmanaged,
                    preview_only=dry_run
                )
            ]
            return self._apply_misc(dg_specs, dry_run, format, no_overwrite)

        return HandleCommandResult(-errno.EINVAL, stderr='--all-available-devices is required')

    @_cli_write_command('orch daemon add osd')
    def _daemon_add_osd(self,
                        svc_arg: Optional[str] = None,
                        method: Optional[OSDMethod] = None) -> HandleCommandResult:
        """Create OSD daemon(s) on specified host and device(s) (e.g., ceph orch daemon add osd myhost:/dev/sdb)"""
        # Create one or more OSDs"""

        usage = """
Usage:
  ceph orch daemon add osd host:device1,device2,...
"""
        if not svc_arg:
            return HandleCommandResult(-errno.EINVAL, stderr=usage)
        try:
            host_name, block_device = svc_arg.split(":")
            block_devices = block_device.split(',')
            devs = DeviceSelection(paths=block_devices)
            drive_group = DriveGroupSpec(
                placement=PlacementSpec(host_pattern=host_name),
                data_devices=devs,
                method=method,
            )
        except (TypeError, KeyError, ValueError) as e:
            msg = f"Invalid host:device spec: '{svc_arg}': {e}" + usage
            return HandleCommandResult(-errno.EINVAL, stderr=msg)

        completion = self.create_osds(drive_group)
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command('orch osd rm')
    def _osd_rm_start(self,
                      osd_id: List[str],
                      replace: bool = False,
                      force: bool = False,
                      zap: bool = False) -> HandleCommandResult:
        """Remove OSD daemons"""
        completion = self.remove_osds(osd_id, replace=replace, force=force, zap=zap)
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command('orch osd rm stop')
    def _osd_rm_stop(self, osd_id: List[str]) -> HandleCommandResult:
        """Cancel ongoing OSD removal operation"""
        completion = self.stop_remove_osds(osd_id)
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command('orch osd rm status')
    def _osd_rm_status(self, format: Format = Format.plain) -> HandleCommandResult:
        """Status of OSD removal operation"""
        completion = self.remove_osds_status()
        raise_if_exception(completion)
        report = completion.result

        if not report:
            return HandleCommandResult(stdout="No OSD remove/replace operations reported")

        if format != Format.plain:
            out = to_format(report, format, many=True, cls=None)
        else:
            table = PrettyTable(
                ['OSD', 'HOST', 'STATE', 'PGS', 'REPLACE', 'FORCE', 'ZAP',
                 'DRAIN STARTED AT'],
                border=False)
            table.align = 'l'
            table._align['PGS'] = 'r'
            table.left_padding_width = 0
            table.right_padding_width = 2
            for osd in sorted(report, key=lambda o: o.osd_id):
                table.add_row([osd.osd_id, osd.hostname, osd.drain_status_human(),
                               osd.get_pg_count(), osd.replace, osd.force, osd.zap,
                               osd.drain_started_at or ''])
            out = table.get_string()

        return HandleCommandResult(stdout=out)

    @_cli_write_command('orch daemon add')
    def daemon_add_misc(self,
                        daemon_type: Optional[ServiceType] = None,
                        placement: Optional[str] = None,
                        inbuf: Optional[str] = None) -> HandleCommandResult:
        """Add daemon(s)"""
        usage = f"""Usage:
    ceph orch daemon add -i <json_file>
    ceph orch daemon add {daemon_type or '<daemon_type>'} <placement>"""
        if inbuf:
            if daemon_type or placement:
                raise OrchestratorValidationError(usage)
            spec = ServiceSpec.from_json(yaml.safe_load(inbuf))
        else:
            if not placement or not daemon_type:
                raise OrchestratorValidationError(usage)
            placement_spec = PlacementSpec.from_string(placement)
            spec = ServiceSpec(daemon_type.value, placement=placement_spec)

        return self._daemon_add_misc(spec)

    def _daemon_add_misc(self, spec: ServiceSpec) -> HandleCommandResult:
        completion = self.add_daemon(spec)
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command('orch daemon add mds')
    def _mds_add(self,
                 fs_name: str,
                 placement: Optional[str] = None,
                 inbuf: Optional[str] = None) -> HandleCommandResult:
        """Start MDS daemon(s)"""
        if inbuf:
            raise OrchestratorValidationError('unrecognized command -i; -h or --help for usage')

        spec = ServiceSpec(
            service_type='mds',
            service_id=fs_name,
            placement=PlacementSpec.from_string(placement),
        )
        return self._daemon_add_misc(spec)

    @_cli_write_command('orch daemon add rgw')
    def _rgw_add(self,
                 svc_id: str,
                 placement: Optional[str] = None,
                 _end_positional_: int = 0,
                 port: Optional[int] = None,
                 ssl: bool = False,
                 inbuf: Optional[str] = None) -> HandleCommandResult:
        """Start RGW daemon(s)"""
        if inbuf:
            raise OrchestratorValidationError('unrecognized command -i; -h or --help for usage')

        spec = RGWSpec(
            service_id=svc_id,
            rgw_frontend_port=port,
            ssl=ssl,
            placement=PlacementSpec.from_string(placement),
        )
        return self._daemon_add_misc(spec)

    @_cli_write_command('orch daemon add nfs')
    def _nfs_add(self,
                 svc_id: str,
                 placement: Optional[str] = None,
                 inbuf: Optional[str] = None) -> HandleCommandResult:
        """Start NFS daemon(s)"""
        if inbuf:
            raise OrchestratorValidationError('unrecognized command -i; -h or --help for usage')

        spec = NFSServiceSpec(
            service_id=svc_id,
            placement=PlacementSpec.from_string(placement),
        )
        return self._daemon_add_misc(spec)

    @_cli_write_command('orch daemon add iscsi')
    def _iscsi_add(self,
                   pool: str,
                   api_user: str,
                   api_password: str,
                   trusted_ip_list: Optional[str] = None,
                   placement: Optional[str] = None,
                   inbuf: Optional[str] = None) -> HandleCommandResult:
        """Start iscsi daemon(s)"""
        if inbuf:
            raise OrchestratorValidationError('unrecognized command -i; -h or --help for usage')

        spec = IscsiServiceSpec(
            service_id='iscsi',
            pool=pool,
            api_user=api_user,
            api_password=api_password,
            trusted_ip_list=trusted_ip_list,
            placement=PlacementSpec.from_string(placement),
        )
        return self._daemon_add_misc(spec)

    @_cli_write_command('orch')
    def _service_action(self, action: ServiceAction, service_name: str) -> HandleCommandResult:
        """Start, stop, restart, redeploy, or reconfig an entire service (i.e. all daemons)"""
        completion = self.service_action(action.value, service_name)
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command('orch daemon')
    def _daemon_action(self, action: DaemonAction, name: str) -> HandleCommandResult:
        """Start, stop, restart, (redeploy,) or reconfig a specific daemon"""
        if '.' not in name:
            raise OrchestratorError('%s is not a valid daemon name' % name)
        completion = self.daemon_action(action.value, name)
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command('orch daemon redeploy')
    def _daemon_action_redeploy(self,
                                name: str,
                                image: Optional[str] = None) -> HandleCommandResult:
        """Redeploy a daemon (with a specifc image)"""
        if '.' not in name:
            raise OrchestratorError('%s is not a valid daemon name' % name)
        completion = self.daemon_action("redeploy", name, image=image)
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command('orch daemon rm')
    def _daemon_rm(self,
                   names: List[str],
                   force: Optional[bool] = False) -> HandleCommandResult:
        """Remove specific daemon(s)"""
        for name in names:
            if '.' not in name:
                raise OrchestratorError('%s is not a valid daemon name' % name)
            (daemon_type) = name.split('.')[0]
            if not force and daemon_type in ['osd', 'mon', 'prometheus']:
                raise OrchestratorError(
                    'must pass --force to REMOVE daemon with potentially PRECIOUS DATA for %s' % name)
        completion = self.remove_daemons(names)
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command('orch rm')
    def _service_rm(self,
                    service_name: str,
                    force: bool = False) -> HandleCommandResult:
        """Remove a service"""
        if service_name in ['mon', 'mgr'] and not force:
            raise OrchestratorError('The mon and mgr services cannot be removed')
        completion = self.remove_service(service_name, force=force)
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command('orch apply')
    def apply_misc(self,
                   service_type: Optional[ServiceType] = None,
                   placement: Optional[str] = None,
                   dry_run: bool = False,
                   format: Format = Format.plain,
                   unmanaged: bool = False,
                   no_overwrite: bool = False,
                   inbuf: Optional[str] = None) -> HandleCommandResult:
        """Update the size or placement for a service or apply a large yaml spec"""
        usage = """Usage:
  ceph orch apply -i <yaml spec> [--dry-run]
  ceph orch apply <service_type> [--placement=<placement_string>] [--unmanaged]
        """
        if inbuf:
            if service_type or placement or unmanaged:
                raise OrchestratorValidationError(usage)
            content: Iterator = yaml.safe_load_all(inbuf)
            specs: List[Union[ServiceSpec, HostSpec]] = []
            for s in content:
                spec = json_to_generic_spec(s)

                # validate the config (we need MgrModule for that)
                if isinstance(spec, ServiceSpec) and spec.config:
                    for k, v in spec.config.items():
                        try:
                            self.get_foreign_ceph_option('mon', k)
                        except KeyError:
                            raise SpecValidationError(f'Invalid config option {k} in spec')

                if dry_run and not isinstance(spec, HostSpec):
                    spec.preview_only = dry_run
                specs.append(spec)
        else:
            placementspec = PlacementSpec.from_string(placement)
            if not service_type:
                raise OrchestratorValidationError(usage)
            specs = [ServiceSpec(service_type.value, placement=placementspec,
                                 unmanaged=unmanaged, preview_only=dry_run)]
        return self._apply_misc(specs, dry_run, format, no_overwrite)

    def _apply_misc(self, specs: Sequence[GenericSpec], dry_run: bool, format: Format, no_overwrite: bool = False) -> HandleCommandResult:
        completion = self.apply(specs, no_overwrite)
        raise_if_exception(completion)
        out = completion.result_str()
        if dry_run:
            completion = self.plan(specs)
            raise_if_exception(completion)
            data = completion.result
            if format == Format.plain:
                out = generate_preview_tables(data)
            else:
                out = to_format(data, format, many=True, cls=None)
        return HandleCommandResult(stdout=out)

    @_cli_write_command('orch apply mds')
    def _apply_mds(self,
                   fs_name: str,
                   placement: Optional[str] = None,
                   dry_run: bool = False,
                   unmanaged: bool = False,
                   format: Format = Format.plain,
                   no_overwrite: bool = False,
                   inbuf: Optional[str] = None) -> HandleCommandResult:
        """Update the number of MDS instances for the given fs_name"""
        if inbuf:
            raise OrchestratorValidationError('unrecognized command -i; -h or --help for usage')

        spec = MDSSpec(
            service_type='mds',
            service_id=fs_name,
            placement=PlacementSpec.from_string(placement),
            unmanaged=unmanaged,
            preview_only=dry_run)

        spec.validate()  # force any validation exceptions to be caught correctly

        return self._apply_misc([spec], dry_run, format, no_overwrite)

    @_cli_write_command('orch apply rgw')
    def _apply_rgw(self,
                   svc_id: str,
                   placement: Optional[str] = None,
                   _end_positional_: int = 0,
                   realm: Optional[str] = None,
                   zone: Optional[str] = None,
                   port: Optional[int] = None,
                   ssl: bool = False,
                   dry_run: bool = False,
                   format: Format = Format.plain,
                   unmanaged: bool = False,
                   no_overwrite: bool = False,
                   inbuf: Optional[str] = None) -> HandleCommandResult:
        """Update the number of RGW instances for the given zone"""
        if inbuf:
            raise OrchestratorValidationError('unrecognized command -i; -h or --help for usage')

        if realm and not zone:
            raise OrchestratorValidationError(
                'Cannot add RGW: Realm specified but no zone specified')
        if zone and not realm:
            raise OrchestratorValidationError(
                'Cannot add RGW: Zone specified but no realm specified')

        spec = RGWSpec(
            service_id=svc_id,
            rgw_realm=realm,
            rgw_zone=zone,
            rgw_frontend_port=port,
            ssl=ssl,
            placement=PlacementSpec.from_string(placement),
            unmanaged=unmanaged,
            preview_only=dry_run
        )

        spec.validate()  # force any validation exceptions to be caught correctly

        return self._apply_misc([spec], dry_run, format, no_overwrite)

    @_cli_write_command('orch apply nfs')
    def _apply_nfs(self,
                   svc_id: str,
                   placement: Optional[str] = None,
                   format: Format = Format.plain,
                   port: Optional[int] = None,
                   dry_run: bool = False,
                   unmanaged: bool = False,
                   no_overwrite: bool = False,
                   inbuf: Optional[str] = None) -> HandleCommandResult:
        """Scale an NFS service"""
        if inbuf:
            raise OrchestratorValidationError('unrecognized command -i; -h or --help for usage')

        spec = NFSServiceSpec(
            service_id=svc_id,
            port=port,
            placement=PlacementSpec.from_string(placement),
            unmanaged=unmanaged,
            preview_only=dry_run
        )

        spec.validate()  # force any validation exceptions to be caught correctly

        return self._apply_misc([spec], dry_run, format, no_overwrite)

    @_cli_write_command('orch apply iscsi')
    def _apply_iscsi(self,
                     pool: str,
                     api_user: str,
                     api_password: str,
                     trusted_ip_list: Optional[str] = None,
                     placement: Optional[str] = None,
                     unmanaged: bool = False,
                     dry_run: bool = False,
                     format: Format = Format.plain,
                     no_overwrite: bool = False,
                     inbuf: Optional[str] = None) -> HandleCommandResult:
        """Scale an iSCSI service"""
        if inbuf:
            raise OrchestratorValidationError('unrecognized command -i; -h or --help for usage')

        spec = IscsiServiceSpec(
            service_id=pool,
            pool=pool,
            api_user=api_user,
            api_password=api_password,
            trusted_ip_list=trusted_ip_list,
            placement=PlacementSpec.from_string(placement),
            unmanaged=unmanaged,
            preview_only=dry_run
        )

        spec.validate()  # force any validation exceptions to be caught correctly

        return self._apply_misc([spec], dry_run, format, no_overwrite)

    @_cli_write_command('orch apply snmp-gateway')
    def _apply_snmp_gateway(self,
                            snmp_version: SNMPGatewaySpec.SNMPVersion,
                            destination: str,
                            port: Optional[int] = None,
                            engine_id: Optional[str] = None,
                            auth_protocol: Optional[SNMPGatewaySpec.SNMPAuthType] = None,
                            privacy_protocol: Optional[SNMPGatewaySpec.SNMPPrivacyType] = None,
                            placement: Optional[str] = None,
                            unmanaged: bool = False,
                            dry_run: bool = False,
                            format: Format = Format.plain,
                            no_overwrite: bool = False,
                            inbuf: Optional[str] = None) -> HandleCommandResult:
        """Add a Prometheus to SNMP gateway service (cephadm only)"""

        if not inbuf:
            raise OrchestratorValidationError(
                'missing credential configuration file. Retry with -i <filename>')

        try:
            # load inbuf
            credentials = yaml.safe_load(inbuf)
        except (OSError, yaml.YAMLError):
            raise OrchestratorValidationError('credentials file must be valid YAML')

        spec = SNMPGatewaySpec(
            snmp_version=snmp_version,
            port=port,
            credentials=credentials,
            snmp_destination=destination,
            engine_id=engine_id,
            auth_protocol=auth_protocol,
            privacy_protocol=privacy_protocol,
            placement=PlacementSpec.from_string(placement),
            unmanaged=unmanaged,
            preview_only=dry_run
        )

        spec.validate()  # force any validation exceptions to be caught correctly

        return self._apply_misc([spec], dry_run, format, no_overwrite)

    @_cli_write_command('orch set backend')
    def _set_backend(self, module_name: Optional[str] = None) -> HandleCommandResult:
        """
        Select orchestrator module backend
        """
        # We implement a setter command instead of just having the user
        # modify the setting directly, so that we can validate they're setting
        # it to a module that really exists and is enabled.

        # There isn't a mechanism for ensuring they don't *disable* the module
        # later, but this is better than nothing.
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

    @_cli_write_command('orch pause')
    def _pause(self) -> HandleCommandResult:
        """Pause orchestrator background work"""
        self.pause()
        return HandleCommandResult()

    @_cli_write_command('orch resume')
    def _resume(self) -> HandleCommandResult:
        """Resume orchestrator background work (if paused)"""
        self.resume()
        return HandleCommandResult()

    @_cli_write_command('orch cancel')
    def _cancel(self) -> HandleCommandResult:
        """
        Cancel ongoing background operations
        """
        self.cancel_completions()
        return HandleCommandResult()

    @_cli_read_command('orch status')
    def _status(self,
                detail: bool = False,
                format: Format = Format.plain) -> HandleCommandResult:
        """Report configured backend and its status"""
        o = self._select_orchestrator()
        if o is None:
            raise NoOrchestrator()

        avail, why, module_details = self.available()
        result: Dict[str, Any] = {
            "available": avail,
            "backend": o,
        }

        if avail:
            result.update(module_details)
        else:
            result['reason'] = why

        if format != Format.plain:
            output = to_format(result, format, many=False, cls=None)
        else:
            output = "Backend: {0}".format(result['backend'])
            output += f"\nAvailable: {'Yes' if result['available'] else 'No'}"
            if 'reason' in result:
                output += ' ({0})'.format(result['reason'])
            if 'paused' in result:
                output += f"\nPaused: {'Yes' if result['paused'] else 'No'}"
            if 'workers' in result and detail:
                output += f"\nHost Parallelism: {result['workers']}"
        return HandleCommandResult(stdout=output)

    def self_test(self) -> None:
        old_orch = self._select_orchestrator()
        self._set_backend('')
        assert self._select_orchestrator() is None
        self._set_backend(old_orch)

        e1 = self.remote('selftest', 'remote_from_orchestrator_cli_self_test', "ZeroDivisionError")
        try:
            raise_if_exception(e1)
            assert False
        except ZeroDivisionError as e:
            assert e.args == ('hello, world',)

        e2 = self.remote('selftest', 'remote_from_orchestrator_cli_self_test', "OrchestratorError")
        try:
            raise_if_exception(e2)
            assert False
        except OrchestratorError as e:
            assert e.args == ('hello, world',)

    @staticmethod
    def _upgrade_check_image_name(image: Optional[str], ceph_version: Optional[str]) -> None:
        """
        >>> OrchestratorCli._upgrade_check_image_name('v15.2.0', None)
        Traceback (most recent call last):
        orchestrator._interface.OrchestratorValidationError: Error: unable to pull image name `v15.2.0`.
          Maybe you meant `--ceph-version 15.2.0`?

        """
        if image and re.match(r'^v?\d+\.\d+\.\d+$', image) and ceph_version is None:
            ver = image[1:] if image.startswith('v') else image
            s = f"Error: unable to pull image name `{image}`.\n" \
                f"  Maybe you meant `--ceph-version {ver}`?"
            raise OrchestratorValidationError(s)

    @_cli_write_command('orch upgrade check')
    def _upgrade_check(self,
                       image: Optional[str] = None,
                       ceph_version: Optional[str] = None) -> HandleCommandResult:
        """Check service versions vs available and target containers"""
        self._upgrade_check_image_name(image, ceph_version)
        completion = self.upgrade_check(image=image, version=ceph_version)
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_read_command('orch upgrade ls')
    def _upgrade_ls(self,
                    image: Optional[str] = None,
                    tags: bool = False) -> HandleCommandResult:
        """Check for available versions (or tags) we can upgrade to"""
        completion = self.upgrade_ls(image, tags)
        r = raise_if_exception(completion)
        out = json.dumps(r, indent=4)
        return HandleCommandResult(stdout=out)

    @_cli_write_command('orch upgrade status')
    def _upgrade_status(self) -> HandleCommandResult:
        """Check service versions vs available and target containers"""
        completion = self.upgrade_status()
        status = raise_if_exception(completion)
        r = {
            'target_image': status.target_image,
            'in_progress': status.in_progress,
            'services_complete': status.services_complete,
            'progress': status.progress,
            'message': status.message,
        }
        out = json.dumps(r, indent=4)
        return HandleCommandResult(stdout=out)

    @_cli_write_command('orch upgrade start')
    def _upgrade_start(self,
                       image: Optional[str] = None,
                       _end_positional_: int = 0,
                       ceph_version: Optional[str] = None) -> HandleCommandResult:
        """Initiate upgrade"""
        self._upgrade_check_image_name(image, ceph_version)
        completion = self.upgrade_start(image, ceph_version)
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command('orch upgrade pause')
    def _upgrade_pause(self) -> HandleCommandResult:
        """Pause an in-progress upgrade"""
        completion = self.upgrade_pause()
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command('orch upgrade resume')
    def _upgrade_resume(self) -> HandleCommandResult:
        """Resume paused upgrade"""
        completion = self.upgrade_resume()
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command('orch upgrade stop')
    def _upgrade_stop(self) -> HandleCommandResult:
        """Stop an in-progress upgrade"""
        completion = self.upgrade_stop()
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())
