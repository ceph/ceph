import errno
import json

try:
    from typing import Dict, List
except ImportError:
    pass  # just for type checking.

from functools import wraps

from mgr_module import MgrModule, HandleCommandResult, CLICommand

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

    def _select_orchestrator(self):
        return self.get_module_option("orchestrator")

    @_write_cli('orchestrator host add',
                "name=host,type=CephString,req=true",
                'Add a host')
    def _add_host(self, host):
        completion = self.add_host(host)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=str(completion.result))

    @_write_cli('orchestrator host rm',
                "name=host,type=CephString,req=true",
                'Remove a host')
    def _remove_host(self, host):
        completion = self.remove_host(host)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=str(completion.result))

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
               "name=svc_type,type=CephChoices,strings=mon|mgr|osd|mds|nfs|rgw|rbd-mirror,req=false "
               "name=svc_id,type=CephString,req=false "
               "name=format,type=CephChoices,strings=json|plain,req=false",
               'List services known to orchestrator')
    def _list_services(self, host=None, svc_type=None, svc_id=None, format='plain'):
        # XXX this is kind of confusing for people because in the orchestrator
        # context the service ID for MDS is the filesystem ID, not the daemon ID

        completion = self.describe_service(svc_type, svc_id, host)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        services = completion.result

        # Sort the list for display
        services.sort(key=lambda s: (s.service_type, s.nodename, s.service_instance))

        if len(services) == 0:
            return HandleCommandResult(stdout="No services reported")
        elif format == 'json':
            data = [s.to_json() for s in services]
            return HandleCommandResult(stdout=json.dumps(data))
        else:
            lines = []
            for s in services:
                if s.service == None:
                    service_id = s.service_instance
                else:
                    service_id = "{0}.{1}".format(s.service, s.service_instance)

                lines.append("{0} {1} {2} {3} {4} {5}".format(
                    s.service_type,
                    service_id,
                    s.nodename,
                    s.container_id,
                    s.version,
                    s.rados_config_location))

            return HandleCommandResult(stdout="\n".join(lines))

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
                drive_group = orchestrator.DriveGroupSpec.from_json(json.loads(inbuf))
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

            devs = orchestrator.DeviceSelection(paths=block_devices)
            drive_group = orchestrator.DriveGroupSpec(node_name, data_devices=devs)
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
        except orchestrator.DriveGroupValidationError as e:
            return HandleCommandResult(-errno.EINVAL, stderr=str(e))

        completion = self.create_osds(drive_group, all_hosts)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        self.log.warning(str(completion.result))
        return HandleCommandResult(stdout=str(completion.result))

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
        return HandleCommandResult(stdout=str(completion.result))

    def _add_stateless_svc(self, svc_type, spec):
        completion = self.add_stateless_service(svc_type, spec)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult()

    @_write_cli('orchestrator mds add',
                "name=svc_arg,type=CephString",
                'Create an MDS service')
    def _mds_add(self, svc_arg):
        spec = orchestrator.StatelessServiceSpec()
        spec.name = svc_arg
        return self._add_stateless_svc("mds", spec)

    @_write_cli('orchestrator rgw add',
                "name=svc_arg,type=CephString",
                'Create an RGW service')
    def _rgw_add(self, svc_arg):
        spec = orchestrator.StatelessServiceSpec()
        spec.name = svc_arg
        return self._add_stateless_svc("rgw", spec)

    @_write_cli('orchestrator nfs add',
                "name=svc_arg,type=CephString "
                "name=pool,type=CephString "
                "name=namespace,type=CephString,req=false",
                'Create an NFS service')
    def _nfs_add(self, svc_arg, pool, namespace=None):
        spec = orchestrator.StatelessServiceSpec()
        spec.name = svc_arg
        spec.extended = { "pool":pool }
        if namespace is not None:
            spec.extended["namespace"] = namespace
        return self._add_stateless_svc("nfs", spec)

    def _rm_stateless_svc(self, svc_type, svc_id):
        completion = self.remove_stateless_service(svc_type, svc_id)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult()

    @_write_cli('orchestrator mds rm',
                "name=svc_id,type=CephString",
                'Remove an MDS service')
    def _mds_rm(self, svc_id):
        return self._rm_stateless_svc("mds", svc_id)

    @_write_cli('orchestrator rgw rm',
                "name=svc_id,type=CephString",
                'Remove an RGW service')
    def _rgw_rm(self, svc_id):
        return self._rm_stateless_svc("rgw", svc_id)

    @_write_cli('orchestrator nfs rm',
                "name=svc_id,type=CephString",
                'Remove an NFS service')
    def _nfs_rm(self, svc_id):
        return self._rm_stateless_svc("nfs", svc_id)

    @_write_cli('orchestrator nfs update',
                "name=svc_id,type=CephString "
                "name=num,type=CephInt",
                'Scale an NFS service')
    def _nfs_update(self, svc_id, num):
        spec = orchestrator.StatelessServiceSpec()
        spec.name = svc_id
        spec.count = num
        completion = self.update_stateless_service("nfs", spec)
        self._orchestrator_wait([completion])
        return HandleCommandResult()

    @_write_cli('orchestrator service',
                "name=action,type=CephChoices,strings=start|stop|reload "
                "name=svc_type,type=CephString "
                "name=svc_name,type=CephString",
                'Start, stop or reload an entire service (i.e. all daemons)')
    def _service_action(self, action, svc_type, svc_name):
        completion = self.service_action(action, svc_type, service_name=svc_name)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult()

    @_write_cli('orchestrator service-instance',
                "name=action,type=CephChoices,strings=start|stop|reload "
                "name=svc_type,type=CephString "
                "name=svc_id,type=CephString",
                'Start, stop or reload a specific service instance')
    def _service_instance_action(self, action, svc_type, svc_id):
        completion = self.service_action(action, svc_type, service_id=svc_id)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult()

    @_write_cli('orchestrator mgr update',
                "name=num,type=CephInt,req=true "
                "name=hosts,type=CephString,n=N,req=false",
                'Update the number of manager instances')
    def _update_mgrs(self, num, hosts=None):
        hosts = hosts if hosts is not None else []

        if num <= 0:
            return HandleCommandResult(-errno.EINVAL,
                    stderr="Invalid number of mgrs: require {} > 0".format(num))

        completion = self.update_mgrs(num, hosts)
        self._orchestrator_wait([completion])
        orchestrator.raise_if_exception(completion)
        return HandleCommandResult(stdout=str(completion.result))

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
            parts = host.split(":")
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
        return HandleCommandResult(stdout=str(completion.result))

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

        if module_name == "":
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
