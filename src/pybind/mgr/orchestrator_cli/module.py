import errno
import json

try:
    from typing import Dict
except ImportError:
    pass  # just for type checking.

from functools import wraps
from typing import List

from mgr_module import MgrModule, HandleCommandResult, CLIWriteCommand, CLIReadCommand

import orchestrator


class NoOrchestrator(Exception):
    def __init__(self):
        super(NoOrchestrator, self).__init__("No orchestrator configured (try "
                                             "`ceph orchestrator set backend`)")


def handle_exceptions(func):

    @wraps(func)
    def inner(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (NoOrchestrator, ImportError) as e:
            return HandleCommandResult(-errno.ENOENT, stderr=str(e))
    return inner


class OrchestratorCli(orchestrator.OrchestratorClientMixin, MgrModule):
    MODULE_OPTIONS = [
        {'name': 'orchestrator'}
    ]

    def _select_orchestrator(self):
        o = self.get_module_option("orchestrator")
        if o is None:
            raise NoOrchestrator()

        return o

    @CLIReadCommand('orchestrator device ls',
                    "name=host,type=CephString,n=N,req=false "
                    "name=format,type=CephChoices,strings=json|plain,req=false",
                    'List devices on a node')
    @handle_exceptions
    def _list_devices(self, host=None, format='plain'):
        # type: (List[str], str) -> HandleCommandResult
        """
        Provide information about storage devices present in cluster hosts

        Note: this does not have to be completely synchronous. Slightly out of
        date hardware inventory is fine as long as hardware ultimately appears
        in the output of this command.
        """
        nf = orchestrator.InventoryFilter(nodes=host) if host else None

        completion = self.get_inventory(node_filter=nf)

        self._orchestrator_wait([completion])

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

    @CLIReadCommand('orchestrator service ls',
                    "name=host,type=CephString,req=false "
                    "name=svc_type,type=CephChoices,strings=mon|mgr|osd|mds|nfs|rgw|rbd-mirror,req=false "
                    "name=svc_id,type=CephString,req=false "
                    "name=format,type=CephChoices,strings=json|plain,req=false",
                    'List services known to orchestrator')
    @handle_exceptions
    def _list_services(self, host=None, svc_type=None, svc_id=None, format='plain'):
        # XXX this is kind of confusing for people because in the orchestrator
        # context the service ID for MDS is the filesystem ID, not the daemon ID

        completion = self.describe_service(svc_type, svc_id, host)
        self._orchestrator_wait([completion])
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

    @CLIWriteCommand('orchestrator osd create',
                     "name=svc_arg,type=CephString,req=false",
                     'Create an OSD service. Either --svc_arg=host:drives or -i <drive_group>')
    @handle_exceptions
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
        all_hosts = [h.name for h in host_completion.result]

        try:
            drive_group.validate(all_hosts)
        except orchestrator.DriveGroupValidationError as e:
                return HandleCommandResult(-errno.EINVAL, stderr=str(e))

        completion = self.create_osds(drive_group, all_hosts)
        self._orchestrator_wait([completion])
        self.log.warning(str(completion.result))
        return HandleCommandResult(stdout=str(completion.result))

    @CLIWriteCommand('orchestrator osd rm',
                     "name=svc_id,type=CephString,n=N",
                     'Remove OSD services')
    @handle_exceptions
    def _osd_rm(self, svc_id):
        # type: (List[str]) -> HandleCommandResult
        """
        Remove OSD's
        :cmd : Arguments for remove the osd
        """
        completion = self.remove_osds(svc_id)
        self._orchestrator_wait([completion])
        return HandleCommandResult(stdout=str(completion.result))

    def _add_stateless_svc(self, svc_type, spec):
        completion = self.add_stateless_service(svc_type, spec)
        self._orchestrator_wait([completion])
        return HandleCommandResult()

    @CLIWriteCommand('orchestrator mds add',
                     "name=svc_arg,type=CephString",
                     'Create an MDS service')
    @handle_exceptions
    def _mds_add(self, svc_arg):
        spec = orchestrator.StatelessServiceSpec()
        spec.name = svc_arg
        return self._add_stateless_svc("mds", spec)

    @CLIWriteCommand('orchestrator rgw add',
                     "name=svc_arg,type=CephString",
                     'Create an RGW service')
    @handle_exceptions
    def _rgw_add(self, svc_arg):
        spec = orchestrator.StatelessServiceSpec()
        spec.name = svc_arg
        return self._add_stateless_svc("rgw", spec)

    @CLIWriteCommand('orchestrator nfs add',
                     "name=svc_arg,type=CephString "
                     "name=pool,type=CephString "
                     "name=namespace,type=CephString,req=false",
                     'Create an NFS service')
    @handle_exceptions
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
        return HandleCommandResult()

    @CLIWriteCommand('orchestrator mds rm',
                     "name=svc_id,type=CephString",
                     'Remove an MDS service')
    def _mds_rm(self, svc_id):
        return self._rm_stateless_svc("mds", svc_id)

    @handle_exceptions
    @CLIWriteCommand('orchestrator rgw rm',
                     "name=svc_id,type=CephString",
                     'Remove an RGW service')
    def _rgw_rm(self, svc_id):
        return self._rm_stateless_svc("rgw", svc_id)

    @CLIWriteCommand('orchestrator nfs rm',
                     "name=svc_id,type=CephString",
                     'Remove an NFS service')
    @handle_exceptions
    def _nfs_rm(self, svc_id):
        return self._rm_stateless_svc("nfs", svc_id)

    @CLIWriteCommand('orchestrator service',
                     "name=action,type=CephChoices,strings=start|stop|reload "
                     "name=svc_type,type=CephString "
                     "name=svc_name,type=CephString",
                     'Start, stop or reload an entire service (i.e. all daemons)')
    @handle_exceptions
    def _service_action(self, action, svc_type, svc_name):
        completion = self.service_action(action, svc_type, service_name=svc_name)
        self._orchestrator_wait([completion])
        return HandleCommandResult()

    @CLIWriteCommand('orchestrator service-instance',
                     "name=action,type=CephChoices,strings=start|stop|reload "
                     "name=svc_type,type=CephString "
                     "name=svc_id,type=CephString",
                     'Start, stop or reload a specific service instance')
    @handle_exceptions
    def _service_instance_action(self, action, svc_type, svc_id):
        completion = self.service_action(action, svc_type, service_id=svc_id)
        self._orchestrator_wait([completion])
        return HandleCommandResult()

    @CLIWriteCommand('orchestrator set backend',
                     "name=module_name,type=CephString,req=true",
                     'Select orchestrator module backend')
    @handle_exceptions
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

    @CLIReadCommand('orchestrator status',
                    desc='Report configured backend and its status')
    @handle_exceptions
    def _status(self):
        avail, why = self.available()

        if avail is None:
            # The module does not report its availability
            return HandleCommandResult(stdout="Backend: {0}".format(self._select_orchestrator()))
        else:
            return HandleCommandResult(stdout="Backend: {0}\nAvailable: {1}{2}".format(
                                           self._select_orchestrator(),
                                           avail,
                                           " ({0})".format(why) if not avail else ""
                                       ))
