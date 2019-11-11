import errno
import json
from functools import wraps

from prettytable import PrettyTable

try:
    from typing import List, Set, Optional
except ImportError:
    pass  # just for type checking.


from ceph.deployment.drive_group import DriveGroupSpec, DriveGroupValidationError, \
    DeviceSelection
from mgr_module import MgrModule, CLICommand, HandleCommandResult

import orchestrator


def handle_exception(prefix, cmd_args, desc, perm, func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (orchestrator.OrchestratorError, ImportError) as e:
            # Do not print Traceback for expected errors.
            return HandleCommandResult(-errno.ENOENT, stderr=str(e))
        except NotImplementedError:
            msg = 'This Orchestrator does not support `{}`'.format(prefix)
            return HandleCommandResult(-errno.ENOENT, stderr=msg)

    return CLICommand(prefix, cmd_args, desc, perm)(wrapper)


def _cli_command(perm):
    def inner_cli_command(prefix, cmd_args="", desc=""):
        return lambda func: handle_exception(prefix, cmd_args, desc, perm, func)
    return inner_cli_command


_read_cli = _cli_command('r')
_write_cli = _cli_command('rw')

class OrchestratorCli(orchestrator.OrchestratorClientMixin, MgrModule):
    MODULE_OPTIONS = [
        {'name': 'orchestrator'}
    ]

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
        # type: (str) -> List[orchestrator.DeviceLightLoc]
        locs = [d['location'] for d in self.get('devices')['devices'] if d['devid'] == dev_id]
        return [orchestrator.DeviceLightLoc(**l) for l in  sum(locs, [])]

    @_read_cli(prefix='device ls-lights',
               desc='List currently active device indicator lights')
    def _device_ls(self):
        return HandleCommandResult(
            stdout=json.dumps({
                'ident': list(self.ident),
                'fault': list(self.fault)
                }, indent=4))

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

    @_write_cli(prefix='device light',
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

    @_write_cli('orchestrator host add',
                "name=host,type=CephString,req=true",
                'Add a host')
    def _add_host(self, host):
        completion = self.add_host(host)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_write_cli('orchestrator host rm',
                "name=host,type=CephString,req=true",
                'Remove a host')
    def _remove_host(self, host):
        completion = self.remove_host(host)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_read_cli('orchestrator host ls',
               desc='List hosts')
    def _get_hosts(self):
        completion = self.get_hosts()
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        result = "\n".join(map(lambda node: node.name, completion.result))
        return HandleCommandResult(stdout=result)

    @_read_cli('orchestrator device ls',
               "name=host,type=CephString,n=N,req=false "
               "name=format,type=CephChoices,strings=json|plain,req=false "
               "name=refresh,type=CephBool,req=false",
               'List devices on a node')
    def _list_devices(self, host=None, format='plain', refresh=False):
        # type: (List[str], str, bool) -> HandleCommandResult
        """
        Provide information about storage devices present in cluster hosts

        Note: this does not have to be completely synchronous. Slightly out of
        date hardware inventory is fine as long as hardware ultimately appears
        in the output of this command.
        """
        nf = orchestrator.InventoryFilter(nodes=host) if host else None

        completion = self.get_inventory(node_filter=nf, refresh=refresh)

        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)

        if format == 'json':
            data = [n.to_json() for n in completion.result]
            return HandleCommandResult(stdout=json.dumps(data))
        else:
            # Return a human readable version
            result = ""

            for inventory_node in completion.result:
                result += "Host {0}:\n".format(inventory_node.name)

                if inventory_node.devices:
                    result += inventory_node.devices[0].pretty_print(only_header=True)
                else:
                    result += "No storage devices found"

                for d in inventory_node.devices:
                    result += d.pretty_print()
                result += "\n"

            return HandleCommandResult(stdout=result)

    @_read_cli('orchestrator service ls',
               "name=host,type=CephString,req=false "
               "name=svc_type,type=CephChoices,strings=mon|mgr|osd|mds|iscsi|nfs|rgw|rbd-mirror,req=false "
               "name=svc_id,type=CephString,req=false "
               "name=format,type=CephChoices,strings=json|plain,req=false "
               "name=refresh,type=CephBool,req=false",
               'List services known to orchestrator')
    def _list_services(self, host=None, svc_type=None, svc_id=None, format='plain', refresh=False):
        # XXX this is kind of confusing for people because in the orchestrator
        # context the service ID for MDS is the filesystem ID, not the daemon ID

        completion = self.describe_service(svc_type, svc_id, host, refresh=refresh)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        services = completion.result

        def ukn(s):
            return '<unknown>' if s is None else s
        # Sort the list for display
        services.sort(key=lambda s: (ukn(s.service_type), ukn(s.nodename), ukn(s.service_instance)))

        if len(services) == 0:
            return HandleCommandResult(stdout="No services reported")
        elif format == 'json':
            data = [s.to_json() for s in services]
            return HandleCommandResult(stdout=json.dumps(data))
        else:
            table = PrettyTable(
                ['TYPE', 'ID', 'HOST', 'CONTAINER', 'VERSION', 'STATUS',
                 'DESCRIPTION'],
                border=False)
            table.align = 'l'
            table.left_padding_width = 0
            table.right_padding_width = 1
            for s in services:
                if s.service is None:
                    service_id = s.service_instance
                else:
                    service_id = "{0}.{1}".format(s.service, s.service_instance)
                status = {
                    -1: 'error',
                    0: 'stopped',
                    1: 'running',
                    None: '<unknown>'
                }[s.status]

                table.add_row((
                    s.service_type,
                    service_id,
                    ukn(s.nodename),
                    ukn(s.container_id),
                    ukn(s.version),
                    status,
                    ukn(s.status_desc)))

            return HandleCommandResult(stdout=table.get_string())

    @_write_cli('orchestrator osd create',
                "name=svc_arg,type=CephString,req=false",
                'Create an OSD service. Either --svc_arg=host:drives or -i <drive_group>')
    def _create_osd(self, svc_arg=None, inbuf=None):
        # type: (str, str) -> HandleCommandResult
        """Create one or more OSDs"""

        usage = """
Usage:
  ceph orchestrator osd create -i <json_file>
  ceph orchestrator osd create host:device1,device2,...
"""

        if inbuf:
            try:
                drive_group = DriveGroupSpec.from_json(json.loads(inbuf))
            except ValueError as e:
                msg = 'Failed to read JSON input: {}'.format(str(e)) + usage
                return HandleCommandResult(-errno.EINVAL, stderr=msg)

        elif svc_arg:
            try:
                node_name, block_device = svc_arg.split(":")
                block_devices = block_device.split(',')
            except (TypeError, KeyError, ValueError):
                msg = "Invalid host:device spec: '{}'".format(svc_arg) + usage
                return HandleCommandResult(-errno.EINVAL, stderr=msg)

            devs = DeviceSelection(paths=block_devices)
            drive_group = DriveGroupSpec(node_name, data_devices=devs)
        else:
            return HandleCommandResult(-errno.EINVAL, stderr=usage)

        # TODO: Remove this and make the orchestrator composable
        #   Like a future or so.
        host_completion = self.get_hosts()
        self._orchestrator_wait([host_completion])
        orchestrator.raise_if_exception(host_completion)
        all_hosts = [h.name for h in host_completion.result]

        try:
            drive_group.validate(all_hosts)
        except DriveGroupValidationError as e:
            return HandleCommandResult(-errno.EINVAL, stderr=str(e))

        completion = self.create_osds(drive_group, all_hosts)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        self.log.warning(str(completion.result))
        return HandleCommandResult(stdout=completion.result_str())

    @_write_cli('orchestrator osd rm',
                "name=svc_id,type=CephString,n=N",
                'Remove OSD services')
    def _osd_rm(self, svc_id):
        # type: (List[str]) -> HandleCommandResult
        """
        Remove OSD's
        :cmd : Arguments for remove the osd
        """
        completion = self.remove_osds(svc_id)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_write_cli('orchestrator rbd-mirror add',
                "name=num,type=CephInt,req=false "
                "name=hosts,type=CephString,n=N,req=false",
                'Create an rbd-mirror service')
    def _rbd_mirror_add(self, num=None, hosts=None):
        spec = orchestrator.StatelessServiceSpec(
            None,
            placement=orchestrator.PlacementSpec(nodes=hosts),
            count=num or 1)
        completion = self.add_rbd_mirror(spec)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_write_cli('orchestrator rbd-mirror update',
                "name=num,type=CephInt,req=true "
                "name=hosts,type=CephString,n=N,req=false",
                'Update the number of rbd-mirror instances')
    def _rbd_mirror_update(self, num, hosts=None):
        spec = orchestrator.StatelessServiceSpec(
            None,
            placement=orchestrator.PlacementSpec(nodes=hosts),
            count=num or 1)
        completion = self.update_rbd_mirror(spec)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_write_cli('orchestrator rbd-mirror rm',
                "name=name,type=CephString,req=false",
                'Remove rbd-mirror service or rbd-mirror service instance')
    def _rbd_mirror_rm(self, name=None):
        completion = self.remove_rbd_mirror(name)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_write_cli('orchestrator mds add',
                "name=fs_name,type=CephString "
                "name=num,type=CephInt,req=false "
                "name=hosts,type=CephString,n=N,req=false",
                'Create an MDS service')
    def _mds_add(self, fs_name, num=None, hosts=None):
        spec = orchestrator.StatelessServiceSpec(
            fs_name,
            placement=orchestrator.PlacementSpec(nodes=hosts),
            count=num or 1)
        completion = self.add_mds(spec)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_write_cli('orchestrator mds update',
                "name=fs_name,type=CephString "
                "name=num,type=CephInt,req=true "
                "name=hosts,type=CephString,n=N,req=false",
                'Update the number of MDS instances for the given fs_name')
    def _mds_update(self, fs_name, num, hosts=None):
        spec = orchestrator.StatelessServiceSpec(
            fs_name,
            placement=orchestrator.PlacementSpec(nodes=hosts),
            count=num or 1)
        completion = self.update_mds(spec)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_write_cli('orchestrator mds rm',
                "name=name,type=CephString",
                'Remove an MDS service (mds id or fs_name)')
    def _mds_rm(self, name):
        completion = self.remove_mds(name)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_write_cli('orchestrator rgw add',
                'name=zone_name,type=CephString,req=false '
                'name=num,type=CephInt,req=false '
                "name=hosts,type=CephString,n=N,req=false",
                'Create an RGW service. A complete <rgw_spec> can be provided'\
                ' using <-i> to customize completelly the RGW service')
    def _rgw_add(self, zone_name, num, hosts, inbuf=None):
        usage = """
Usage:
  ceph orchestrator rgw add -i <json_file>
  ceph orchestrator rgw add <zone_name>
        """

        if inbuf:
            try:
                rgw_spec = orchestrator.RGWSpec.from_json(json.loads(inbuf))
            except ValueError as e:
                msg = 'Failed to read JSON input: {}'.format(str(e)) + usage
                return HandleCommandResult(-errno.EINVAL, stderr=msg)
        elif zone_name:
            rgw_spec = orchestrator.RGWSpec(
                rgw_zone=zone_name,
                placement=orchestrator.PlacementSpec(nodes=hosts),
                count=num or 1)
        else:
            return HandleCommandResult(-errno.EINVAL, stderr=usage)

        completion = self.add_rgw(rgw_spec)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_write_cli('orchestrator rgw update',
                "name=zone_name,type=CephString "
                "name=num,type=CephInt "
                "name=hosts,type=CephString,n=N,req=false",
                'Update the number of RGW instances for the given zone')
    def _rgw_update(self, zone_name, num, hosts=None):
        spec = orchestrator.RGWSpec(
            rgw_zone=zone_name,
            placement=orchestrator.PlacementSpec(nodes=hosts),
            count=num or 1)
        completion = self.update_rgw(spec)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_write_cli('orchestrator rgw rm',
                "name=name,type=CephString",
                'Remove an RGW service')
    def _rgw_rm(self, name):
        completion = self.remove_rgw(name)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_write_cli('orchestrator nfs add',
                "name=svc_arg,type=CephString "
                "name=pool,type=CephString "
                "name=namespace,type=CephString,req=false",
                'Create an NFS service')
    def _nfs_add(self, svc_arg, pool, namespace=None):
        spec = orchestrator.NFSServiceSpec(svc_arg, pool=pool, namespace=namespace)
        spec.validate_add()
        completion = self.add_nfs(spec)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_write_cli('orchestrator nfs update',
                "name=svc_id,type=CephString "
                "name=num,type=CephInt",
                'Scale an NFS service')
    def _nfs_update(self, svc_id, num):
        spec = orchestrator.NFSServiceSpec(svc_id, count=num)
        completion = self.update_nfs(spec)
        self._orchestrator_wait([completion])
        return HandleCommandResult(stdout=completion.result_str())

    @_write_cli('orchestrator nfs rm',
                "name=svc_id,type=CephString",
                'Remove an NFS service')
    def _nfs_rm(self, svc_id):
        completion = self.remove_nfs(svc_id)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_write_cli('orchestrator service',
                "name=action,type=CephChoices,strings=start|stop|reload|restart|redeploy "
                "name=svc_type,type=CephString "
                "name=svc_name,type=CephString",
                'Start, stop or reload an entire service (i.e. all daemons)')
    def _service_action(self, action, svc_type, svc_name):
        completion = self.service_action(action, svc_type, service_name=svc_name)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_write_cli('orchestrator service-instance',
                "name=action,type=CephChoices,strings=start|stop|reload|restart|redeploy "
                "name=svc_type,type=CephString "
                "name=svc_id,type=CephString",
                'Start, stop or reload a specific service instance')
    def _service_instance_action(self, action, svc_type, svc_id):
        completion = self.service_action(action, svc_type, service_id=svc_id)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_write_cli('orchestrator mgr update',
                "name=num,type=CephInt,req=true "
                "name=hosts,type=CephString,n=N,req=false",
                'Update the number of manager instances')
    def _update_mgrs(self, num, hosts=None):
        hosts = hosts if hosts is not None else []

        if num <= 0:
            return HandleCommandResult(-errno.EINVAL,
                    stderr="Invalid number of mgrs: require {} > 0".format(num))

        def split_host(host):
            """Split host into host and name parts"""
            # TODO: stricter validation
            a = host.split('=', 1)
            if len(a) == 1:
                return (a[0], None)
            else:
                assert len(a) == 2
                return tuple(a)

        if hosts:
            try:
                hosts = list(map(split_host, hosts))
            except Exception as e:
                msg = "Failed to parse host list: '{}': {}".format(hosts, e)
                return HandleCommandResult(-errno.EINVAL, stderr=msg)

        completion = self.update_mgrs(num, hosts)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_write_cli('orchestrator mon update',
                "name=num,type=CephInt,req=true "
                "name=hosts,type=CephString,n=N,req=false",
                'Update the number of monitor instances')
    def _update_mons(self, num, hosts=None):
        hosts = hosts if hosts is not None else []

        if num <= 0:
            return HandleCommandResult(-errno.EINVAL,
                    stderr="Invalid number of mons: require {} > 0".format(num))

        def split_host(host):
            """Split host into host and network parts"""
            # TODO: stricter validation
            parts = host.split(":", 1)
            if len(parts) == 1:
                return (parts[0], None)
            elif len(parts) == 2:
                return (parts[0], parts[1])
            else:
                raise RuntimeError("Invalid host specification: "
                        "'{}'".format(host))

        if hosts:
            try:
                hosts = list(map(split_host, hosts))
            except Exception as e:
                msg = "Failed to parse host list: '{}': {}".format(hosts, e)
                return HandleCommandResult(-errno.EINVAL, stderr=msg)

        completion = self.update_mons(num, hosts)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=completion.result_str())

    @_write_cli('orchestrator set backend',
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

    @_read_cli('orchestrator status',
               desc='Report configured backend and its status')
    def _status(self):
        o = self._select_orchestrator()
        if o is None:
            raise orchestrator.NoOrchestrator()

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

        e = self.remote('selftest', 'remote_from_orchestrator_cli_self_test', "ZeroDivisionError")
        try:
            orchestrator.raise_if_exception(e)
            assert False
        except ZeroDivisionError as e:
            assert e.args == ('hello', 'world')

        e = self.remote('selftest', 'remote_from_orchestrator_cli_self_test', "OrchestratorError")
        try:
            orchestrator.raise_if_exception(e)
            assert False
        except orchestrator.OrchestratorError as e:
            assert e.args == ('hello', 'world')
