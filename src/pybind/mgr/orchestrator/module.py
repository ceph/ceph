import datetime
import errno
import json
from typing import List, Set, Optional, Iterator, cast, Dict, Any, Union
import re
import ast

import yaml
from prettytable import PrettyTable

from ceph.deployment.inventory import Device
from ceph.deployment.drive_group import DriveGroupSpec, DeviceSelection
from ceph.deployment.service_spec import PlacementSpec, ServiceSpec

from mgr_util import format_bytes, to_pretty_timedelta
from mgr_module import MgrModule, HandleCommandResult

from ._interface import OrchestratorClientMixin, DeviceLightLoc, _cli_read_command, \
    raise_if_exception, _cli_write_command, TrivialReadCompletion, OrchestratorError, \
    NoOrchestrator, OrchestratorValidationError, NFSServiceSpec, \
    RGWSpec, InventoryFilter, InventoryHost, HostSpec, CLICommandMeta, \
    ServiceDescription, DaemonDescription, IscsiServiceSpec, json_to_generic_spec, GenericSpec


def nice_delta(now, t, suffix=''):
    if t:
        return to_pretty_timedelta(now - t) + suffix
    else:
        return '-'


def to_format(what, format: str, many: bool, cls):
    def to_json_1(obj):
        if hasattr(obj, 'to_json'):
            return obj.to_json()
        return obj

    def to_json_n(objs):
        return [to_json_1(o) for o in objs]

    to_json = to_json_n if many else to_json_1

    if format == 'json':
        return json.dumps(to_json(what), sort_keys=True)
    elif format == 'json-pretty':
        return json.dumps(to_json(what), indent=2, sort_keys=True)
    elif format == 'yaml':
        # fun with subinterpreters again. pyyaml depends on object identity.
        # as what originates from a different subinterpreter we have to copy things here.
        if cls:
            flat = to_json(what)
            copy = [cls.from_json(o) for o in flat] if many else cls.from_json(flat)
        else:
            copy = what

        def to_yaml_1(obj):
            if hasattr(obj, 'yaml_representer'):
                return obj
            return to_json_1(obj)

        def to_yaml_n(objs):
            return [to_yaml_1(o) for o in objs]

        to_yaml = to_yaml_n if many else to_yaml_1

        if many:
            return yaml.dump_all(to_yaml(copy), default_flow_style=False)
        return yaml.dump(to_yaml(copy), default_flow_style=False)


def generate_preview_tables(data):
    error = [x.get('error') for x in data if x.get('error')]
    if error:
        return json.dumps(error)
    warning = [x.get('warning') for x in data if x.get('warning')]
    osd_table = preview_table_osd(data)
    service_table = preview_table_services(data)
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


def preview_table_osd(data):
    table = PrettyTable(header_style='upper', title='OSDSPEC PREVIEWS', border=True)
    table.field_names = "service name host data db wal".split()
    table.align = 'l'
    table.left_padding_width = 0
    table.right_padding_width = 2
    for osd_data in data:
        if osd_data.get('service_type') != 'osd':
            continue
        for host, specs in osd_data.get('data').items():
            for spec in specs:
                if spec.get('error'):
                    return spec.get('message')
                dg_name = spec.get('osdspec')
                for osd in spec.get('data', {}).get('osds', []):
                    db_path = '-'
                    wal_path = '-'
                    block_db = osd.get('block.db', {}).get('path')
                    block_wal = osd.get('block.wal', {}).get('path')
                    block_data = osd.get('data', {}).get('path', '')
                    if not block_data:
                        continue
                    if block_db:
                        db_path = spec.get('data', {}).get('vg', {}).get('devices', [])
                    if block_wal:
                        wal_path = spec.get('data', {}).get('wal_vg', {}).get('devices', [])
                    table.add_row(('osd', dg_name, host, block_data, db_path, wal_path))
    return table.get_string()


def preview_table_services(data):
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
        'name=format,type=CephChoices,strings=plain|json|json-pretty|yaml,req=false',
        'List hosts')
    def _get_hosts(self, format='plain'):
        completion = self.get_hosts()
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        if format != 'plain':
            output = to_format(completion.result, format, many=True, cls=HostSpec)
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
        "name=format,type=CephChoices,strings=plain|json|json-pretty|yaml,req=false "
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

        if format != 'plain':
            return HandleCommandResult(stdout=to_format(completion.result, format, many=True, cls=InventoryHost))
        else:
            out = []

            table = PrettyTable(
                ['HOST', 'PATH', 'TYPE', 'SIZE', 'DEVICE_ID', 'MODEL', 'VENDOR', 'ROTATIONAL', 'AVAIL',
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
                            d.sys_api.get('model') or 'n/a',
                            d.sys_api.get('vendor') or 'n/a',
                            d.sys_api.get('rotational') or 'n/a',
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
        "name=export,type=CephBool,req=false "
        "name=format,type=CephChoices,strings=plain|json|json-pretty|yaml,req=false "
        "name=refresh,type=CephBool,req=false",
        'List services known to orchestrator')
    def _list_services(self, host=None, service_type=None, service_name=None, export=False, format='plain', refresh=False):

        if export and format == 'plain':
            format = 'yaml'

        completion = self.describe_service(service_type,
                                           service_name,
                                           refresh=refresh)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        services: List[ServiceDescription] = completion.result

        def ukn(s):
            return '<unknown>' if s is None else s

        # Sort the list for display
        services.sort(key=lambda s: (ukn(s.spec.service_name())))

        if len(services) == 0:
            return HandleCommandResult(stdout="No services reported")
        elif format != 'plain':
            if export:
                data = [s.spec for s in services]
                return HandleCommandResult(stdout=to_format(data, format, many=True, cls=ServiceSpec))
            else:
                return HandleCommandResult(stdout=to_format(services, format, many=True, cls=ServiceDescription))
        else:
            now = datetime.datetime.utcnow()
            table = PrettyTable(
                ['NAME', 'RUNNING', 'REFRESHED', 'AGE',
                 'PLACEMENT',
                 'IMAGE NAME', 'IMAGE ID'
                ],
                border=False)
            table.align['NAME'] = 'l'
            table.align['RUNNING'] = 'r'
            table.align['REFRESHED'] = 'l'
            table.align['AGE'] = 'l'
            table.align['IMAGE NAME'] = 'l'
            table.align['IMAGE ID'] = 'l'
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
                table.add_row((
                    s.spec.service_name(),
                    '%d/%d' % (s.running, s.size),
                    nice_delta(now, s.last_refresh, ' ago'),
                    nice_delta(now, s.created),
                    pl,
                    ukn(s.container_image_name),
                    ukn(s.container_image_id)[0:12],
                ))

            return HandleCommandResult(stdout=table.get_string())

    @_cli_read_command(
        'orch ps',
        "name=hostname,type=CephString,req=false "
        "name=service_name,type=CephString,req=false "
        "name=daemon_type,type=CephString,req=false "
        "name=daemon_id,type=CephString,req=false "
        "name=format,type=CephChoices,strings=plain|json|json-pretty|yaml,req=false "
        "name=refresh,type=CephBool,req=false",
        'List daemons known to orchestrator')
    def _list_daemons(self, hostname=None, service_name=None, daemon_type=None, daemon_id=None, format='plain', refresh=False):
        completion = self.list_daemons(service_name,
                                       daemon_type,
                                       daemon_id=daemon_id,
                                       host=hostname,
                                       refresh=refresh)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        daemons: List[DaemonDescription] = completion.result

        def ukn(s):
            return '<unknown>' if s is None else s
        # Sort the list for display
        daemons.sort(key=lambda s: (ukn(s.daemon_type), ukn(s.hostname), ukn(s.daemon_id)))

        if format != 'plain':
            return HandleCommandResult(stdout=to_format(daemons, format, many=True, cls=DaemonDescription))
        else:
            if len(daemons) == 0:
                return HandleCommandResult(stdout="No daemons reported")

            now = datetime.datetime.utcnow()
            table = PrettyTable(
                ['NAME', 'HOST', 'STATUS', 'REFRESHED', 'AGE',
                 'VERSION', 'IMAGE NAME', 'IMAGE ID', 'CONTAINER ID'],
                border=False)
            table.align = 'l'
            table.left_padding_width = 0
            table.right_padding_width = 2
            for s in sorted(daemons, key=lambda s: s.name()):
                if s.status_desc:
                    status = s.status_desc
                else:
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
                    ukn(s.container_id)))

            return HandleCommandResult(stdout=table.get_string())

    def set_unmanaged_flag(self,
                           unmanaged_flag: bool,
                           service_type: str = 'osd',
                           service_name=None
                           ) -> HandleCommandResult:
        # setting unmanaged for $service_name
        completion = self.describe_service(service_name=service_name, service_type=service_type)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        services: List[ServiceDescription] = completion.result
        specs = list()
        for service in services:
            spec = service.spec
            spec.unmanaged = unmanaged_flag
            specs.append(spec)
        completion = self.apply(cast(List[GenericSpec], specs))
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        if specs:
            return HandleCommandResult(stdout=f"Changed <unmanaged> flag to <{unmanaged_flag}> for "
                                              f"{[spec.service_name() for spec in specs]}")
        else:
            return HandleCommandResult(stdout=f"No specs found with the <service_name> -> {service_name}")

    @_cli_write_command(
        'orch osd spec',
        'name=service_name,type=CephString,req=false '
        'name=preview,type=CephBool,req=false '
        'name=unmanaged,type=CephBool,req=false '
        "name=format,type=CephChoices,strings=plain|json|json-pretty|yaml,req=false",
        'Common operations on an OSDSpec. Allows previewing and changing the unmanaged flag.')
    def _misc_osd(self,
                  preview: bool = False,
                  service_name: Optional[str] = None,
                  unmanaged=None,
                  format: Optional[str] = 'plain',
                  ) -> HandleCommandResult:
        usage = """
usage:
  ceph orch osd spec --preview
  ceph orch osd spec --unmanaged=true|false 
  ceph orch osd spec --service-name <service_name> --preview
  ceph orch osd spec --service-name <service_name> --unmanaged=true|false (defaults to false)
  
Restrictions:

    Mutexes:
    * --preview ,--unmanaged 

    Although it it's possible to set these at the same time, we will lack a proper response to each
    action, possibly shadowing any failures.

Description:

    * --service-name
        If flag is omitted, assume to target all existing OSDSpecs.
        Needs either --unamanged or --preview.

    * --unmanaged
        Applies <unamanged> flag to targeted --service-name.
        If --service-name is omitted, target all OSDSpecs
        
Examples:

    # ceph orch osd spec --preview
    
    Queries all available OSDSpecs for previews
    
    # ceph orch osd spec --service-name my-osdspec-name --preview
    
    Queries only the specified <my-osdspec-name> for previews
    
    # ceph orch osd spec --unmanaged=true
    
    # Changes flags of all available OSDSpecs to true
    
    # ceph orch osd spec --service-name my-osdspec-name --unmanaged=true
    
    Changes the unmanaged flag of <my-osdspec-name> to true
"""

        def print_preview(previews, format_to):
            if format != 'plain':
                return to_format(previews, format_to, many=False, cls=None)
            else:
                table = PrettyTable(
                    ['NAME', 'HOST', 'DATA', 'DB', 'WAL'],
                    border=False)
                table.align = 'l'
                table.left_padding_width = 0
                table.right_padding_width = 1
                for host, data in previews.items():
                    for spec in data:
                        if spec.get('error'):
                            return spec.get('message')
                        dg_name = spec.get('osdspec')
                        for osd in spec.get('data', {}).get('osds', []):
                            db_path = '-'
                            wal_path = '-'
                            block_db = osd.get('block.db', {}).get('path')
                            block_wal = osd.get('block.wal', {}).get('path')
                            block_data = osd.get('data', {}).get('path', '')
                            if not block_data:
                                continue
                            if block_db:
                                db_path = spec.get('data', {}).get('vg', {}).get('devices', [])
                            if block_wal:
                                wal_path = spec.get('data', {}).get('wal_vg', {}).get('devices', [])
                            table.add_row((dg_name, host, block_data, db_path, wal_path))
                ret = table.get_string()
                if not ret:
                    ret = "No preview available"
                return ret

        if preview and (unmanaged is not None):
            return HandleCommandResult(-errno.EINVAL, stderr=usage)

        if service_name:
            if preview:
                completion = self.preview_osdspecs(osdspec_name=service_name)
                self._orchestrator_wait([completion])
                raise_if_exception(completion)
                out = completion.result_str()
                return HandleCommandResult(stdout=print_preview(ast.literal_eval(out), format))
            if unmanaged is not None:
                return self.set_unmanaged_flag(service_name=service_name, unmanaged_flag=unmanaged)

            return HandleCommandResult(-errno.EINVAL, stderr=usage)

        if preview:
            completion = self.preview_osdspecs()
            self._orchestrator_wait([completion])
            raise_if_exception(completion)
            out = completion.result_str()
            return HandleCommandResult(stdout=print_preview(ast.literal_eval(out), format))

        if unmanaged is not None:
            return self.set_unmanaged_flag(unmanaged_flag=unmanaged)

        return HandleCommandResult(-errno.EINVAL, stderr=usage)

    @_cli_write_command(
        'orch apply osd',
        'name=all_available_devices,type=CephBool,req=false '
        'name=dry_run,type=CephBool,req=false '
        'name=unmanaged,type=CephBool,req=false '
        "name=format,type=CephChoices,strings=plain|json|json-pretty|yaml,req=false",
        'Create OSD daemon(s) using a drive group spec')
    def _apply_osd(self,
                   all_available_devices: bool = False,
                   format: str = 'plain',
                   unmanaged=None,
                   dry_run=None,
                   inbuf: Optional[str] = None) -> HandleCommandResult:
        """Apply DriveGroupSpecs to create OSDs"""
        usage = """
usage:
  ceph orch apply osd -i <json_file/yaml_file> [--dry-run]
  ceph orch apply osd --all-available-devices [--dry-run] [--unmanaged]
  
Restrictions:
  
  Mutexes:
  * -i, --all-available-devices
  * -i, --unmanaged (this would overwrite the osdspec loaded from a file)
  
  Parameters:
  
  * --unmanaged
     Only works with --all-available-devices.
  
Description:
  
  * -i
    An inbuf object like a file or a json/yaml blob containing a valid OSDSpec
    
  * --all-available-devices
    The most simple OSDSpec there is. Takes all as 'available' marked devices
    and creates standalone OSDs on them.
    
  * --unmanaged
    Set a the unmanaged flag for all--available-devices (default is False)
    
Examples:

   # ceph orch apply osd -i <file.yml|json>
   
   Applies one or more OSDSpecs found in <file>
   
   # ceph orch osd apply --all-available-devices --unmanaged=true
   
   Creates and applies simple OSDSpec with the unmanaged flag set to <true>
"""

        if inbuf and all_available_devices:
            # mutually exclusive
            return HandleCommandResult(-errno.EINVAL, stderr=usage)

        if not inbuf and not all_available_devices:
            # one parameter must be present
            return HandleCommandResult(-errno.EINVAL, stderr=usage)

        if inbuf:
            if unmanaged is not None:
                return HandleCommandResult(-errno.EINVAL, stderr=usage)
            try:
                drivegroups = yaml.safe_load_all(inbuf)

                dg_specs = []
                for dg in drivegroups:
                    spec = DriveGroupSpec.from_json(dg)
                    if dry_run:
                        spec.preview_only = True
                    dg_specs.append(spec)

                completion = self.apply(dg_specs)
                self._orchestrator_wait([completion])
                raise_if_exception(completion)
                out = completion.result_str()
                if dry_run:
                    completion = self.plan(dg_specs)
                    self._orchestrator_wait([completion])
                    raise_if_exception(completion)
                    data = completion.result
                    if format == 'plain':
                        out = preview_table_osd(data)
                    else:
                        out = to_format(data, format, many=True, cls=None)
                return HandleCommandResult(stdout=out)

            except ValueError as e:
                msg = 'Failed to read JSON/YAML input: {}'.format(str(e)) + usage
                return HandleCommandResult(-errno.EINVAL, stderr=msg)
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
            # This acts weird when abstracted to a function
            completion = self.apply(dg_specs)
            self._orchestrator_wait([completion])
            raise_if_exception(completion)
            out = completion.result_str()
            if dry_run:
                completion = self.plan(dg_specs)
                self._orchestrator_wait([completion])
                data = completion.result
                if format == 'plain':
                    out = preview_table_osd(data)
                else:
                    out = to_format(data, format, many=True, cls=None)
            return HandleCommandResult(stdout=out)

        return HandleCommandResult(-errno.EINVAL, stderr=usage)

    @_cli_write_command(
        'orch daemon add osd',
        "name=svc_arg,type=CephString,req=false",
        'Create an OSD service. Either --svc_arg=host:drives')
    def _daemon_add_osd(self, svc_arg=None):
        # type: (Optional[str]) -> HandleCommandResult
        """Create one or more OSDs"""

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
            drive_group = DriveGroupSpec(placement=PlacementSpec(host_pattern=host_name), data_devices=devs)
        except (TypeError, KeyError, ValueError):
            msg = "Invalid host:device spec: '{}'".format(svc_arg) + usage
            return HandleCommandResult(-errno.EINVAL, stderr=msg)

        completion = self.create_osds(drive_group)
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
        'orch daemon add',
        'name=daemon_type,type=CephChoices,strings=mon|mgr|rbd-mirror|crash|alertmanager|grafana|node-exporter|prometheus,req=false '
        'name=placement,type=CephString,req=false',
        'Add daemon(s)')
    def _daemon_add_misc(self,
                         daemon_type: Optional[str] = None,
                         placement: Optional[str] = None,
                         inbuf: Optional[str] = None) -> HandleCommandResult:
        usage = f"""Usage:
    ceph orch daemon add -i <json_file>
    ceph orch daemon add {daemon_type or '<daemon_type>'} <placement>"""
        if inbuf:
            if daemon_type or placement:
                raise OrchestratorValidationError(usage)
            spec = ServiceSpec.from_json(yaml.safe_load(inbuf))
        else:
            spec = PlacementSpec.from_string(placement)
            assert daemon_type
            spec = ServiceSpec(daemon_type, placement=spec)

        daemon_type = spec.service_type

        if daemon_type == 'mon':
            completion = self.add_mon(spec)
        elif daemon_type == 'mgr':
            completion = self.add_mgr(spec)
        elif daemon_type == 'rbd-mirror':
            completion = self.add_rbd_mirror(spec)
        elif daemon_type == 'crash':
            completion = self.add_crash(spec)
        elif daemon_type == 'alertmanager':
            completion = self.add_alertmanager(spec)
        elif daemon_type == 'grafana':
            completion = self.add_grafana(spec)
        elif daemon_type == 'node-exporter':
            completion = self.add_node_exporter(spec)
        elif daemon_type == 'prometheus':
            completion = self.add_prometheus(spec)
        elif daemon_type == 'mds':
            completion = self.add_mds(spec)
        elif daemon_type == 'rgw':
            completion = self.add_rgw(spec)
        elif daemon_type == 'nfs':
            completion = self.add_nfs(spec)
        elif daemon_type == 'iscsi':
            completion = self.add_iscsi(spec)
        else:
            raise OrchestratorValidationError(f'unknown daemon type `{daemon_type}`')

        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch daemon add mds',
        'name=fs_name,type=CephString '
        'name=placement,type=CephString,req=false',
        'Start MDS daemon(s)')
    def _mds_add(self,
                 fs_name: str,
                 placement: Optional[str] = None,
                 inbuf: Optional[str] = None) -> HandleCommandResult:
        if inbuf:
            raise OrchestratorValidationError('unrecognized command -i; -h or --help for usage')

        spec = ServiceSpec(
            service_type='mds',
            service_id=fs_name,
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
        'name=subcluster,type=CephString,req=false '
        'name=port,type=CephInt,req=false '
        'name=ssl,type=CephBool,req=false '
        'name=placement,type=CephString,req=false',
        'Start RGW daemon(s)')
    def _rgw_add(self,
                 realm_name: str,
                 zone_name: str,
                 subcluster: Optional[str] = None,
                 port: Optional[int] = None,
                 ssl: bool = False,
                 placement: Optional[str] = None,
                 inbuf: Optional[str] = None) -> HandleCommandResult:
        if inbuf:
            raise OrchestratorValidationError('unrecognized command -i; -h or --help for usage')

        spec = RGWSpec(
            rgw_realm=realm_name,
            rgw_zone=zone_name,
            subcluster=subcluster,
            rgw_frontend_port=port,
            ssl=ssl,
            placement=PlacementSpec.from_string(placement),
        )

        completion = self.add_rgw(spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch daemon add nfs',
        "name=svc_id,type=CephString "
        "name=pool,type=CephString "
        "name=namespace,type=CephString,req=false "
        'name=placement,type=CephString,req=false',
        'Start NFS daemon(s)')
    def _nfs_add(self,
                 svc_id: str,
                 pool: str,
                 namespace: Optional[str] = None,
                 placement: Optional[str] = None,
                 inbuf: Optional[str] = None) -> HandleCommandResult:
        if inbuf:
            raise OrchestratorValidationError('unrecognized command -i; -h or --help for usage')

        spec = NFSServiceSpec(
            service_id=svc_id,
            pool=pool,
            namespace=namespace,
            placement=PlacementSpec.from_string(placement),
        )

        completion = self.add_nfs(spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch daemon add iscsi',
        'name=pool,type=CephString '
        'name=api_user,type=CephString '
        'name=api_password,type=CephString '
        'name=trusted_ip_list,type=CephString,req=false '
        'name=placement,type=CephString,req=false',
        'Start iscsi daemon(s)')
    def _iscsi_add(self,
                   pool: str,
                   api_user: str,
                   api_password: str,
                   trusted_ip_list: Optional[str] = None,
                   placement: Optional[str] = None,
                   inbuf: Optional[str] = None) -> HandleCommandResult:
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

        completion = self.add_iscsi(spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
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
            (daemon_type) = name.split('.')[0]
            if not force and daemon_type in ['osd', 'mon', 'prometheus']:
                raise OrchestratorError('must pass --force to REMOVE daemon with potentially PRECIOUS DATA for %s' % name)
        completion = self.remove_daemons(names)
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
        'orch apply',
        'name=service_type,type=CephChoices,strings=mon|mgr|rbd-mirror|crash|alertmanager|grafana|node-exporter|prometheus,req=false '
        'name=dry_run,type=CephBool,req=false '
        'name=placement,type=CephString,req=false '
        'name=format,type=CephChoices,strings=plain|json|json-pretty|yaml,req=false '
        'name=unmanaged,type=CephBool,req=false',
        'Update the size or placement for a service or apply a large yaml spec')
    def _apply_misc(self,
                    service_type: Optional[str] = None,
                    placement: Optional[str] = None,
                    unmanaged: bool = False,
                    dry_run: bool = False,
                    format: str = 'plain',
                    inbuf: Optional[str] = None) -> HandleCommandResult:
        usage = """Usage:
  ceph orch apply -i <yaml spec> [--dry-run]
  ceph orch apply <service_type> <placement> [--unmanaged]
        """
        if inbuf:
            if service_type or placement or unmanaged:
                raise OrchestratorValidationError(usage)
            content: Iterator = yaml.safe_load_all(inbuf)
            specs: List[Union[ServiceSpec, HostSpec]] = []
            for s in content:
                spec = json_to_generic_spec(s)
                if dry_run and not isinstance(spec, HostSpec):
                    spec.preview_only = dry_run
                specs.append(spec)
        else:
            placementspec = PlacementSpec.from_string(placement)
            assert service_type
            specs = [ServiceSpec(service_type, placement=placementspec, unmanaged=unmanaged, preview_only=dry_run)]

        completion = self.apply(specs)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        out = completion.result_str()
        if dry_run:
            completion = self.plan(specs)
            self._orchestrator_wait([completion])
            raise_if_exception(completion)
            data = completion.result
            if format == 'plain':
                out = generate_preview_tables(data)
            else:
                out = to_format(data, format, many=True, cls=None)
        return HandleCommandResult(stdout=out)

    @_cli_write_command(
        'orch apply mds',
        'name=fs_name,type=CephString '
        'name=dry_run,type=CephBool,req=false '
        'name=placement,type=CephString,req=false '
        'name=unmanaged,type=CephBool,req=false '
        'name=format,type=CephChoices,strings=plain|json|json-pretty|yaml,req=false',
        'Update the number of MDS instances for the given fs_name')
    def _apply_mds(self,
                   fs_name: str,
                   placement: Optional[str] = None,
                   dry_run: bool = False,
                   format: str = 'plain',
                   unmanaged: bool = False,
                   inbuf: Optional[str] = None) -> HandleCommandResult:
        if inbuf:
            raise OrchestratorValidationError('unrecognized command -i; -h or --help for usage')

        spec = ServiceSpec(
            service_type='mds',
            service_id=fs_name,
            placement=PlacementSpec.from_string(placement),
            unmanaged=unmanaged,
            preview_only=dry_run)

        completion = self.apply_mds(spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        out = completion.result_str()
        if dry_run:
            completion = self.plan([spec])
            self._orchestrator_wait([completion])
            raise_if_exception(completion)
            data = completion.result
            if format == 'plain':
                out = preview_table_services(data)
            else:
                out = to_format(data, format, many=True, cls=None)
        return HandleCommandResult(stdout=out)

    @_cli_write_command(
        'orch apply rgw',
        'name=realm_name,type=CephString '
        'name=zone_name,type=CephString '
        'name=subcluster,type=CephString,req=false '
        'name=port,type=CephInt,req=false '
        'name=ssl,type=CephBool,req=false '
        'name=placement,type=CephString,req=false '
        'name=dry_run,type=CephBool,req=false '
        'name=format,type=CephChoices,strings=plain|json|json-pretty|yaml,req=false ' 
        'name=unmanaged,type=CephBool,req=false',
        'Update the number of RGW instances for the given zone')
    def _apply_rgw(self,
                   realm_name: str,
                   zone_name: str,
                   subcluster: Optional[str] = None,
                   port: Optional[int] = None,
                   ssl: bool = False,
                   placement: Optional[str] = None,
                   dry_run: bool = False,
                   format: str = 'plain',
                   unmanaged: bool = False,
                   inbuf: Optional[str] = None) -> HandleCommandResult:
        if inbuf:
            raise OrchestratorValidationError('unrecognized command -i; -h or --help for usage')

        spec = RGWSpec(
            rgw_realm=realm_name,
            rgw_zone=zone_name,
            subcluster=subcluster,
            rgw_frontend_port=port,
            ssl=ssl,
            placement=PlacementSpec.from_string(placement),
            unmanaged=unmanaged,
            preview_only=dry_run
        )

        completion = self.apply_rgw(spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        out = completion.result_str()
        if dry_run:
            completion = self.plan([spec])
            self._orchestrator_wait([completion])
            raise_if_exception(completion)
            data = completion.result
            if format == 'plain':
                out = preview_table_services(data)
            else:
                out = to_format(data, format, many=True, cls=None)
        return HandleCommandResult(stdout=out)

    @_cli_write_command(
        'orch apply nfs',
        'name=svc_id,type=CephString '
        'name=pool,type=CephString '
        'name=namespace,type=CephString,req=false '
        'name=placement,type=CephString,req=false '
        'name=dry_run,type=CephBool,req=false '
        'name=format,type=CephChoices,strings=plain|json|json-pretty|yaml,req=false ' 
        'name=unmanaged,type=CephBool,req=false',
        'Scale an NFS service')
    def _apply_nfs(self,
                   svc_id: str,
                   pool: str,
                   namespace: Optional[str] = None,
                   placement: Optional[str] = None,
                   format: str = 'plain',
                   dry_run: bool = False,
                   unmanaged: bool = False,
                   inbuf: Optional[str] = None) -> HandleCommandResult:
        if inbuf:
            raise OrchestratorValidationError('unrecognized command -i; -h or --help for usage')

        spec = NFSServiceSpec(
            service_id=svc_id,
            pool=pool,
            namespace=namespace,
            placement=PlacementSpec.from_string(placement),
            unmanaged=unmanaged,
            preview_only=dry_run
        )

        completion = self.apply_nfs(spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        out = completion.result_str()
        if dry_run:
            completion = self.plan([spec])
            self._orchestrator_wait([completion])
            raise_if_exception(completion)
            data = completion.result
            if format == 'plain':
                out = preview_table_services(data)
            else:
                out = to_format(data, format, many=True, cls=None)
        return HandleCommandResult(stdout=out)

    @_cli_write_command(
        'orch apply iscsi',
        'name=pool,type=CephString '
        'name=api_user,type=CephString '
        'name=api_password,type=CephString '
        'name=trusted_ip_list,type=CephString,req=false '
        'name=placement,type=CephString,req=false '
        'name=dry_run,type=CephBool,req=false '
        'name=format,type=CephChoices,strings=plain|json|json-pretty|yaml,req=false ' 
        'name=unmanaged,type=CephBool,req=false',
        'Scale an iSCSI service')
    def _apply_iscsi(self,
                     pool: str,
                     api_user: str,
                     api_password: str,
                     trusted_ip_list: Optional[str] = None,
                     placement: Optional[str] = None,
                     unmanaged: bool = False,
                     dry_run: bool = False,
                     format: str = 'plain',
                     inbuf: Optional[str] = None) -> HandleCommandResult:
        if inbuf:
            raise OrchestratorValidationError('unrecognized command -i; -h or --help for usage')

        spec = IscsiServiceSpec(
            service_id='iscsi',
            pool=pool,
            api_user=api_user,
            api_password=api_password,
            trusted_ip_list=trusted_ip_list,
            placement=PlacementSpec.from_string(placement),
            unmanaged=unmanaged,
            preview_only=dry_run
        )

        completion = self.apply_iscsi(spec)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        out = completion.result_str()
        if dry_run:
            completion = self.plan([spec])
            self._orchestrator_wait([completion])
            raise_if_exception(completion)
            data = completion.result
            if format == 'plain':
                out = preview_table_services(data)
            else:
                out = to_format(data, format, many=True, cls=None)
        return HandleCommandResult(stdout=out)

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
        'orch pause',
        desc='Pause orchestrator background work')
    def _pause(self):
        self.pause()
        return HandleCommandResult()

    @_cli_write_command(
        'orch resume',
        desc='Resume orchestrator background work (if paused)')
    def _resume(self):
        self.resume()
        return HandleCommandResult()

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
        'name=format,type=CephChoices,strings=plain|json|json-pretty|yaml,req=false',
        desc='Report configured backend and its status')
    def _status(self, format='plain'):
        o = self._select_orchestrator()
        if o is None:
            raise NoOrchestrator()

        avail, why = self.available()
        result = {
            "backend": o
        }
        if avail is not None:
            result['available'] = avail
            if not avail:
                result['reason'] = why

        if format != 'plain':
            output = to_format(result, format, many=False, cls=None)
        else:
            output = "Backend: {0}".format(result['backend'])
            if 'available' in result:
                output += "\nAvailable: {0}".format(result['available'])
                if 'reason' in result:
                    output += ' ({0})'.format(result['reason'])
        return HandleCommandResult(stdout=output)

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
            assert e.args == ('hello, world',)

        e2 = self.remote('selftest', 'remote_from_orchestrator_cli_self_test', "OrchestratorError")
        try:
            raise_if_exception(e2)
            assert False
        except OrchestratorError as e:
            assert e.args == ('hello, world',)

        c = TrivialReadCompletion(result=True)
        assert c.has_result

    @staticmethod
    def _upgrade_check_image_name(image, ceph_version):
        """
        >>> OrchestratorCli._upgrade_check_image_name('v15.2.0', None)
        Traceback (most recent call last):
        orchestrator._interface.OrchestratorValidationError: Error: unable to pull image name `v15.2.0`.
          Maybe you meant `--ceph-version 15.2.0`?

        """
        if image and re.match(r'^v?\d+\.\d+\.\d+$', image) and ceph_version is None:
            ver = image[1:] if image.startswith('v') else image
            s =  f"Error: unable to pull image name `{image}`.\n" \
                 f"  Maybe you meant `--ceph-version {ver}`?"
            raise OrchestratorValidationError(s)

    @_cli_write_command(
        'orch upgrade check',
        'name=image,type=CephString,req=false '
        'name=ceph_version,type=CephString,req=false',
        desc='Check service versions vs available and target containers')
    def _upgrade_check(self, image=None, ceph_version=None):
        self._upgrade_check_image_name(image, ceph_version)
        completion = self.upgrade_check(image=image, version=ceph_version)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch upgrade status',
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
        'orch upgrade start',
        'name=image,type=CephString,req=false '
        'name=ceph_version,type=CephString,req=false',
        desc='Initiate upgrade')
    def _upgrade_start(self, image=None, ceph_version=None):
        self._upgrade_check_image_name(image, ceph_version)
        completion = self.upgrade_start(image, ceph_version)
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch upgrade pause',
        desc='Pause an in-progress upgrade')
    def _upgrade_pause(self):
        completion = self.upgrade_pause()
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch upgrade resume',
        desc='Resume paused upgrade')
    def _upgrade_resume(self):
        completion = self.upgrade_resume()
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_cli_write_command(
        'orch upgrade stop',
        desc='Stop an in-progress upgrade')
    def _upgrade_stop(self):
        completion = self.upgrade_stop()
        self._orchestrator_wait([completion])
        raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())
