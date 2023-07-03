import enum
import errno
import json
from typing import List, Set, Optional, Iterator, cast, Dict, Any, Union, Sequence, Mapping
import re
import datetime
import math

import yaml
from prettytable import PrettyTable

try:
    from natsort import natsorted
except ImportError:
    # fallback to normal sort
    natsorted = sorted  # type: ignore

from ceph.deployment.inventory import Device  # noqa: F401; pylint: disable=unused-variable
from ceph.deployment.drive_group import DriveGroupSpec, DeviceSelection, OSDMethod
from ceph.deployment.service_spec import PlacementSpec, ServiceSpec, service_spec_allow_invalid_from_json
from ceph.deployment.hostspec import SpecValidationError
from ceph.deployment.utils import unwrap_ipv6
from ceph.utils import datetime_now

from mgr_util import to_pretty_timedelta, format_bytes
from mgr_module import MgrModule, HandleCommandResult, Option
from object_format import Format

from ._interface import OrchestratorClientMixin, DeviceLightLoc, _cli_read_command, \
    raise_if_exception, _cli_write_command, OrchestratorError, \
    NoOrchestrator, OrchestratorValidationError, NFSServiceSpec, \
    RGWSpec, InventoryFilter, InventoryHost, HostSpec, CLICommandMeta, \
    ServiceDescription, DaemonDescription, IscsiServiceSpec, json_to_generic_spec, \
    GenericSpec, DaemonDescriptionStatus, SNMPGatewaySpec, MDSSpec, TunedProfileSpec


def nice_delta(now: datetime.datetime, t: Optional[datetime.datetime], suffix: str = '') -> str:
    if t:
        return to_pretty_timedelta(now - t) + suffix
    else:
        return '-'


def nice_bytes(v: Optional[int]) -> str:
    if not v:
        return '-'
    return format_bytes(v, 5)


class HostDetails:
    def __init__(self,
                 host: Optional[HostSpec] = None,
                 facts: Optional[Dict[str, Any]] = None,
                 object_dump: Optional[Dict[str, Any]] = None):
        self._hostspec = host
        self._facts = facts
        self.hdd_summary = 'N/A'
        self.ram = 'N/A'
        self.cpu_summary = 'N/A'
        self.server = 'N/A'
        self.os = 'N/A'
        self.ssd_summary = 'N/A'
        self.nic_count = 'N/A'

        assert host or object_dump
        if object_dump:
            self._load(object_dump)
        else:
            self._build()

    def _load(self, object_dump: Dict[str, Any]) -> None:
        """Build the object from predefined dictionary"""
        self.addr = object_dump.get('addr')
        self.hostname = object_dump.get('hostname')
        self.labels = object_dump.get('labels')
        self.status = object_dump.get('status')
        self.location = object_dump.get('location')
        self.server = object_dump.get('server', 'N/A')
        self.hdd_summary = object_dump.get('hdd_summary', 'N/A')
        self.ssd_summary = object_dump.get('ssd_summary', 'N/A')
        self.os = object_dump.get('os', 'N/A')
        self.cpu_summary = object_dump.get('cpu_summary', 'N/A')
        self.ram = object_dump.get('ram', 'N/A')
        self.nic_count = object_dump.get('nic_count', 'N/A')

    def _build(self) -> None:
        """build host details from the HostSpec and facts"""
        for a in self._hostspec.__dict__:
            setattr(self, a, getattr(self._hostspec, a))

        if self._facts:
            self.server = f"{self._facts.get('vendor', '').strip()} {self._facts.get('model', '').strip()}"
            _cores = self._facts.get('cpu_cores', 0) * self._facts.get('cpu_count', 0)
            _threads = self._facts.get('cpu_threads', 0) * _cores
            self.os = self._facts.get('operating_system', 'N/A')
            self.cpu_summary = f"{_cores}C/{_threads}T" if _cores > 0 else 'N/A'

            _total_bytes = self._facts.get('memory_total_kb', 0) * 1024
            divisor, suffix = (1073741824, 'GiB') if _total_bytes > 1073741824 else (1048576, 'MiB')
            self.ram = f'{math.ceil(_total_bytes / divisor)} {suffix}'
            _hdd_capacity = self._facts.get('hdd_capacity', '')
            _ssd_capacity = self._facts.get('flash_capacity', '')
            if _hdd_capacity:
                if self._facts.get('hdd_count', 0) == 0:
                    self.hdd_summary = '-'
                else:
                    self.hdd_summary = f"{self._facts.get('hdd_count', 0)}/{self._facts.get('hdd_capacity', 0)}"

            if _ssd_capacity:
                if self._facts.get('flash_count', 0) == 0:
                    self.ssd_summary = '-'
                else:
                    self.ssd_summary = f"{self._facts.get('flash_count', 0)}/{self._facts.get('flash_capacity', 0)}"

            self.nic_count = self._facts.get('nic_count', '')

    def to_json(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if not k.startswith('_')}

    @classmethod
    def from_json(cls, host_details: dict) -> 'HostDetails':
        _cls = cls(object_dump=host_details)
        return _cls

    @staticmethod
    def yaml_representer(dumper: 'yaml.SafeDumper', data: 'HostDetails') -> Any:
        return dumper.represent_dict(cast(Mapping, data.to_json().items()))


yaml.add_representer(HostDetails, HostDetails.yaml_representer)


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
    loki = 'loki'
    promtail = 'promtail'
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
    rotate_key = 'rotate-key'


class DaemonAction(enum.Enum):
    start = 'start'
    stop = 'stop'
    restart = 'restart'
    reconfig = 'reconfig'
    rotate_key = 'rotate-key'


class IngressType(enum.Enum):
    default = 'default'
    keepalive_only = 'keepalive-only'


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
    elif format == Format.xml or format == Format.xml_pretty:
        raise OrchestratorError(f"format '{format.name}' is not implemented.")
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
        self.ident: Set[str] = set()
        self.fault: Set[str] = set()
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

        if addr is not None:
            addr = unwrap_ipv6(addr)

        s = HostSpec(hostname=hostname, addr=addr, labels=labels, status=_status)

        return self._apply_misc([s], False, Format.plain)

    @_cli_write_command('orch host rm')
    def _remove_host(self, hostname: str, force: bool = False, offline: bool = False) -> HandleCommandResult:
        """Remove a host"""
        completion = self.remove_host(hostname, force, offline)
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command('orch host drain')
    def _drain_host(self, hostname: str, force: bool = False) -> HandleCommandResult:
        """drain all daemons from a host"""
        completion = self.drain_host(hostname, force)
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command('orch host set-addr')
    def _update_set_addr(self, hostname: str, addr: str) -> HandleCommandResult:
        """Update a host address"""
        completion = self.update_host_addr(hostname, addr)
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_read_command('orch host ls')
    def _get_hosts(self,
                   format: Format = Format.plain,
                   host_pattern: str = '',
                   label: str = '',
                   host_status: str = '',
                   detail: bool = False) -> HandleCommandResult:
        """List high level host information"""
        completion = self.get_hosts()
        hosts = raise_if_exception(completion)

        cephadm_active = True if self._select_orchestrator() == "cephadm" else False
        show_detail = cephadm_active and detail

        filter_spec = PlacementSpec(
            host_pattern=host_pattern,
            label=label
        )
        filtered_hosts: List[str] = filter_spec.filter_matching_hostspecs(hosts)
        hosts = [h for h in hosts if h.hostname in filtered_hosts]

        if host_status:
            hosts = [h for h in hosts if h.status.lower() == host_status]

        if show_detail:
            # switch to a HostDetails based representation
            _hosts = []
            for h in hosts:
                facts_completion = self.get_facts(h.hostname)
                host_facts = raise_if_exception(facts_completion)
                _hosts.append(HostDetails(host=h, facts=host_facts[0]))
            hosts: List[HostDetails] = _hosts  # type: ignore [no-redef]

        if format != Format.plain:
            if show_detail:
                output = to_format(hosts, format, many=True, cls=HostDetails)
            else:
                output = to_format(hosts, format, many=True, cls=HostSpec)
        else:
            if show_detail:
                table_headings = ['HOST', 'ADDR', 'LABELS', 'STATUS',
                                  'VENDOR/MODEL', 'CPU', 'RAM', 'HDD', 'SSD', 'NIC']
            else:
                table_headings = ['HOST', 'ADDR', 'LABELS', 'STATUS']

            table = PrettyTable(
                table_headings,
                border=False)
            table.align = 'l'
            table.left_padding_width = 0
            table.right_padding_width = 2
            for host in natsorted(hosts, key=lambda h: h.hostname):
                row = (host.hostname, host.addr, ','.join(
                    host.labels), host.status.capitalize())

                if show_detail and isinstance(host, HostDetails):
                    row += (host.server, host.cpu_summary, host.ram,
                            host.hdd_summary, host.ssd_summary, host.nic_count)

                table.add_row(row)
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
    def _host_label_rm(self, hostname: str, label: str, force: bool = False) -> HandleCommandResult:
        """Remove a host label"""
        completion = self.remove_host_label(hostname, label, force)
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command('orch host ok-to-stop')
    def _host_ok_to_stop(self, hostname: str) -> HandleCommandResult:
        """Check if the specified host can be safely stopped without reducing availability"""""
        completion = self.host_ok_to_stop(hostname)
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command('orch host maintenance enter')
    def _host_maintenance_enter(self, hostname: str, force: bool = False, yes_i_really_mean_it: bool = False) -> HandleCommandResult:
        """
        Prepare a host for maintenance by shutting down and disabling all Ceph daemons (cephadm only)
        """
        completion = self.enter_host_maintenance(hostname, force=force, yes_i_really_mean_it=yes_i_really_mean_it)
        raise_if_exception(completion)

        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command('orch host maintenance exit')
    def _host_maintenance_exit(self, hostname: str) -> HandleCommandResult:
        """
        Return a host from maintenance, restarting all Ceph daemons (cephadm only)
        """
        completion = self.exit_host_maintenance(hostname)
        raise_if_exception(completion)

        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command('orch host rescan')
    def _host_rescan(self, hostname: str, with_summary: bool = False) -> HandleCommandResult:
        """Perform a disk rescan on a host"""
        completion = self.rescan_host(hostname)
        raise_if_exception(completion)

        if with_summary:
            return HandleCommandResult(stdout=completion.result_str())
        return HandleCommandResult(stdout=completion.result_str().split('.')[0])

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
                False: "No",
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
            for host_ in natsorted(inv_hosts, key=lambda h: h.name):  # type: InventoryHost
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
                                format_bytes(d.sys_api.get('size', 0), 5),
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
                                format_bytes(d.sys_api.get('size', 0), 5),
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
            for s in natsorted(daemons, key=lambda d: d.name()):
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
                   method: Optional[OSDMethod] = None,
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
                    preview_only=dry_run,
                    method=method
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
  ceph orch daemon add osd host:data_devices=device1,device2,db_devices=device3,osds_per_device=2,...
"""
        if not svc_arg:
            return HandleCommandResult(-errno.EINVAL, stderr=usage)
        try:
            host_name, raw = svc_arg.split(":")
            drive_group_spec = {
                'data_devices': []
            }  # type: Dict
            drv_grp_spec_arg = None
            values = raw.split(',')
            while values:
                v = values[0].split(',', 1)[0]
                if '=' in v:
                    drv_grp_spec_arg, value = v.split('=')
                    if drv_grp_spec_arg in ['data_devices',
                                            'db_devices',
                                            'wal_devices',
                                            'journal_devices']:
                        drive_group_spec[drv_grp_spec_arg] = []
                        drive_group_spec[drv_grp_spec_arg].append(value)
                    else:
                        drive_group_spec[drv_grp_spec_arg] = value
                elif drv_grp_spec_arg is not None:
                    drive_group_spec[drv_grp_spec_arg].append(v)
                else:
                    drive_group_spec['data_devices'].append(v)
                values.remove(v)

            for dev_type in ['data_devices', 'db_devices', 'wal_devices', 'journal_devices']:
                drive_group_spec[dev_type] = DeviceSelection(
                    paths=drive_group_spec[dev_type]) if drive_group_spec.get(dev_type) else None

            drive_group = DriveGroupSpec(
                placement=PlacementSpec(host_pattern=host_name),
                method=method,
                **drive_group_spec,
            )
        except (TypeError, KeyError, ValueError) as e:
            msg = f"Invalid 'host:device' spec: '{svc_arg}': {e}" + usage
            return HandleCommandResult(-errno.EINVAL, stderr=msg)

        completion = self.create_osds(drive_group)
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command('orch osd rm')
    def _osd_rm_start(self,
                      osd_id: List[str],
                      replace: bool = False,
                      force: bool = False,
                      zap: bool = False,
                      no_destroy: bool = False) -> HandleCommandResult:
        """Remove OSD daemons"""
        completion = self.remove_osds(osd_id, replace=replace, force=force,
                                      zap=zap, no_destroy=no_destroy)
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
        """Start, stop, restart, redeploy, reconfig, or rotate-key for a specific daemon"""
        if '.' not in name:
            raise OrchestratorError('%s is not a valid daemon name' % name)
        completion = self.daemon_action(action.value, name)
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command('orch daemon redeploy')
    def _daemon_action_redeploy(self,
                                name: str,
                                image: Optional[str] = None) -> HandleCommandResult:
        """Redeploy a daemon (with a specific image)"""
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
            yaml_objs: Iterator = yaml.safe_load_all(inbuf)
            specs: List[Union[ServiceSpec, HostSpec]] = []
            # YAML '---' document separator with no content generates
            # None entries in the output. Let's skip them silently.
            content = [o for o in yaml_objs if o is not None]
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
                   zonegroup: Optional[str] = None,
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
            rgw_zonegroup=zonegroup,
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

    @_cli_write_command('orch set-unmanaged')
    def _set_unmanaged(self, service_name: str) -> HandleCommandResult:
        """Set 'unmanaged: true' for the given service name"""
        completion = self.set_unmanaged(service_name, True)
        raise_if_exception(completion)
        out = completion.result_str()
        return HandleCommandResult(stdout=out)

    @_cli_write_command('orch set-managed')
    def _set_managed(self, service_name: str) -> HandleCommandResult:
        """Set 'unmanaged: false' for the given service name"""
        completion = self.set_unmanaged(service_name, False)
        raise_if_exception(completion)
        out = completion.result_str()
        return HandleCommandResult(stdout=out)

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

    @_cli_write_command('orch tuned-profile apply')
    def _apply_tuned_profiles(self,
                              profile_name: Optional[str] = None,
                              placement: Optional[str] = None,
                              settings: Optional[str] = None,
                              no_overwrite: bool = False,
                              inbuf: Optional[str] = None) -> HandleCommandResult:
        """Add or update a tuned profile"""
        usage = """Usage:
  ceph orch tuned-profile apply -i <yaml spec>
  ceph orch tuned-profile apply <profile_name> [--placement=<placement_string>] [--settings='option=value,option2=value2']
        """
        if inbuf:
            if profile_name or placement or settings:
                raise OrchestratorValidationError(usage)
            yaml_objs: Iterator = yaml.safe_load_all(inbuf)
            specs: List[TunedProfileSpec] = []
            # YAML '---' document separator with no content generates
            # None entries in the output. Let's skip them silently.
            content = [o for o in yaml_objs if o is not None]
            for spec in content:
                specs.append(TunedProfileSpec.from_json(spec))
        else:
            if not profile_name:
                raise OrchestratorValidationError(usage)
            placement_spec = PlacementSpec.from_string(
                placement) if placement else PlacementSpec(host_pattern='*')
            settings_dict = {}
            if settings:
                settings_list = settings.split(',')
                for setting in settings_list:
                    if '=' not in setting:
                        raise SpecValidationError('settings defined on cli for tuned profile must '
                                                  + 'be of format "setting_name=value,setting_name2=value2" etc.')
                    name, value = setting.split('=', 1)
                    settings_dict[name.strip()] = value.strip()
            tuned_profile_spec = TunedProfileSpec(
                profile_name=profile_name, placement=placement_spec, settings=settings_dict)
            specs = [tuned_profile_spec]
        completion = self.apply_tuned_profiles(specs, no_overwrite)
        res = raise_if_exception(completion)
        return HandleCommandResult(stdout=res)

    @_cli_write_command('orch tuned-profile rm')
    def _rm_tuned_profiles(self, profile_name: str) -> HandleCommandResult:
        completion = self.rm_tuned_profile(profile_name)
        res = raise_if_exception(completion)
        return HandleCommandResult(stdout=res)

    @_cli_read_command('orch tuned-profile ls')
    def _tuned_profile_ls(self, format: Format = Format.plain) -> HandleCommandResult:
        completion = self.tuned_profile_ls()
        profiles: List[TunedProfileSpec] = raise_if_exception(completion)
        if format != Format.plain:
            return HandleCommandResult(stdout=to_format(profiles, format, many=True, cls=TunedProfileSpec))
        else:
            out = ''
            for profile in profiles:
                out += f'profile_name: {profile.profile_name}\n'
                out += f'placement: {profile.placement.pretty_str()}\n'
                out += 'settings:\n'
                for k, v in profile.settings.items():
                    out += f'  {k}: {v}\n'
                out += '---\n'
            return HandleCommandResult(stdout=out)

    @_cli_write_command('orch tuned-profile add-setting')
    def _tuned_profile_add_setting(self, profile_name: str, setting: str, value: str) -> HandleCommandResult:
        completion = self.tuned_profile_add_setting(profile_name, setting, value)
        res = raise_if_exception(completion)
        return HandleCommandResult(stdout=res)

    @_cli_write_command('orch tuned-profile rm-setting')
    def _tuned_profile_rm_setting(self, profile_name: str, setting: str) -> HandleCommandResult:
        completion = self.tuned_profile_rm_setting(profile_name, setting)
        res = raise_if_exception(completion)
        return HandleCommandResult(stdout=res)

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
                    tags: bool = False,
                    show_all_versions: Optional[bool] = False
                    ) -> HandleCommandResult:
        """Check for available versions (or tags) we can upgrade to"""
        completion = self.upgrade_ls(image, tags, show_all_versions)
        r = raise_if_exception(completion)
        out = json.dumps(r, indent=4)
        return HandleCommandResult(stdout=out)

    @_cli_write_command('orch upgrade status')
    def _upgrade_status(self) -> HandleCommandResult:
        """Check the status of any potential ongoing upgrade operation"""
        completion = self.upgrade_status()
        status = raise_if_exception(completion)
        r = {
            'target_image': status.target_image,
            'in_progress': status.in_progress,
            'which': status.which,
            'services_complete': status.services_complete,
            'progress': status.progress,
            'message': status.message,
            'is_paused': status.is_paused,
        }
        out = json.dumps(r, indent=4)
        return HandleCommandResult(stdout=out)

    @_cli_write_command('orch upgrade start')
    def _upgrade_start(self,
                       image: Optional[str] = None,
                       _end_positional_: int = 0,
                       daemon_types: Optional[str] = None,
                       hosts: Optional[str] = None,
                       services: Optional[str] = None,
                       limit: Optional[int] = None,
                       ceph_version: Optional[str] = None) -> HandleCommandResult:
        """Initiate upgrade"""
        self._upgrade_check_image_name(image, ceph_version)
        dtypes = daemon_types.split(',') if daemon_types is not None else None
        service_names = services.split(',') if services is not None else None
        completion = self.upgrade_start(image, ceph_version, dtypes, hosts, service_names, limit)
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
