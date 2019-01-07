import errno
import json
import time
from mgr_module import MgrModule, HandleCommandResult

import orchestrator


class NoOrchestrator(Exception):
    pass


class OrchestratorCli(MgrModule):
    MODULE_OPTIONS = [
        {'name': 'orchestrator'}
    ]
    COMMANDS = [
        {
            'cmd': "orchestrator device ls "
                   "name=host,type=CephString,req=false"
                   "name=format,type=CephChoices,strings=json|plain,req=false ",
            "desc": "List devices on a node",
            "perm": "r"
        },
        {
            'cmd': "orchestrator service ls "
                   "name=host,type=CephString,req=false "
                   "name=svc_type,type=CephString,req=false "
                   "name=svc_id,type=CephString,req=false "
                   "name=format,type=CephChoices,strings=json|plain,req=false ",
            "desc": "List services known to orchestrator" ,
            "perm": "r"
        },
        {
            'cmd': "orchestrator service status "
                   "name=host,type=CephString,req=false "
                   "name=svc_type,type=CephString "
                   "name=svc_id,type=CephString "
                   "name=format,type=CephChoices,strings=json|plain,req=false ",
            "desc": "Get orchestrator state for Ceph service",
            "perm": "r"
        },
        {
            'cmd': "orchestrator service add "
                   "name=svc_type,type=CephString "
                   "name=svc_arg,type=CephString ",
            "desc": "Create a service of any type",
            "perm": "rw"
        },
        {
            'cmd': "orchestrator service rm "
                   "name=svc_type,type=CephString "
                   "name=svc_id,type=CephString ",
            "desc": "Remove a service",
            "perm": "rw"
        },
        {
            'cmd': "orchestrator set backend "
                   "name=module,type=CephString,req=true",
            "desc": "Select orchestrator module backend",
            "perm": "rw"
        },
        {
            "cmd": "orchestrator status",
            "desc": "Report configured backend and its status",
            "perm": "r"
        }
    ]

    def _select_orchestrator(self):
        o = self.get_module_option("orchestrator")
        if o is None:
            raise NoOrchestrator()

        return o

    def _oremote(self, *args, **kwargs):
        """
        Helper for invoking `remote` on whichever orchestrator is enabled
        """
        return self.remote(self._select_orchestrator(),
                           *args, **kwargs)

    def _wait(self, completions):
        """
        Helper to wait for completions to complete (reads) or
        become persistent (writes).

        Waits for writes to be *persistent* but not *effective*.
        """

        while not self._oremote("wait", completions):

            if any(c.should_wait for c in completions):
                time.sleep(5)
            else:
                break

        if all(hasattr(c, 'error') and getattr(c, 'error')for c in completions):
            raise Exception([getattr(c, 'error') for c in completions])

    def _list_devices(self, cmd):
        """
        This (all lines starting with ">") is how it is supposed to work. As of
        now, it's not yet implemented:
        > :returns: Either JSON:
        >     [
        >       {
        >         "name": "sda",
        >         "host": "foo",
        >         ... lots of stuff from ceph-volume ...
        >         "stamp": when this state was refreshed
        >       },
        >     ]
        >
        > or human readable:
        >
        >     HOST  DEV  SIZE  DEVID(vendor\\_model\\_serial)   IN-USE  TIMESTAMP
        >
        > Note: needs ceph-volume on the host.

        Note: this does not have to be completely synchronous. Slightly out of
        date hardware inventory is fine as long as hardware ultimately appears
        in the output of this command.
        """
        host = cmd.get('host', None)

        if host:
            nf = orchestrator.InventoryFilter()
            nf.nodes = [host]
        else:
            nf = None

        completion = self._oremote("get_inventory", node_filter=nf)

        self._wait([completion])

        if cmd.get('format', 'plain') == 'json':
            data = [n.to_json() for n in completion.result]
            return HandleCommandResult(stdout=json.dumps(data))
        else:
            # Return a human readable version
            result = ""
            for inventory_node in completion.result:
                result += "{0}:\n".format(inventory_node.name)
                for d in inventory_node.devices:
                    result += "  {0} ({1}, {2}b)\n".format(
                        d.id, d.type, d.size)
                result += "\n"

            return HandleCommandResult(stdout=result)

    def _list_services(self, cmd):
        hostname = cmd.get('host', None)
        svc_id = cmd.get('svc_id', None)
        svc_type = cmd.get('svc_type', None)
        # XXX this is kind of confusing for people because in the orchestrator
        # context the service ID for MDS is the filesystem ID, not the daemon ID

        completion = self._oremote("describe_service", svc_type, svc_id, hostname)

        self._wait([completion])

        services = completion.result

        # Sort the list for display
        services.sort(key=lambda s: (s.service_type, s.nodename, s.daemon_name))

        if len(services) == 0:
            return HandleCommandResult(stdout="No services reported")
        elif cmd.get('format', 'plain') == 'json':
            data = [s.to_json() for s in services]
            return HandleCommandResult(stdout=json.dumps(data))
        else:
            lines = []
            for s in services:
                lines.append("{0}.{1} {2} {3} {4} {5}".format(
                    s.service_type,
                    s.daemon_name,
                    s.nodename,
                    s.container_id,
                    s.version,
                    s.rados_config_location))

            return HandleCommandResult(stdout="\n".join(lines))

    def _service_add(self, cmd):
        svc_type = cmd['svc_type']
        if svc_type == "osd":
            device_spec = cmd['svc_arg']
            try:
                node_name, block_device = device_spec.split(":")
            except TypeError:
                return HandleCommandResult(-errno.EINVAL,
                                           stderr="Invalid device spec, should be <node>:<device>")

            spec = orchestrator.OsdCreationSpec()
            spec.node = node_name
            spec.format = "bluestore"
            spec.drive_group = orchestrator.DriveGroupSpec([block_device])

            completion = self._oremote("create_osds", spec)
            self._wait([completion])

            return HandleCommandResult()

        elif svc_type == "mds":
            fs_name = cmd['svc_arg']

            spec = orchestrator.StatelessServiceSpec()
            spec.name = fs_name

            completion = self._oremote(
                "add_stateless_service",
                svc_type,
                spec
            )
            self._wait([completion])

            return HandleCommandResult()
        elif svc_type == "rgw":
            store_name = cmd['svc_arg']

            spec = orchestrator.StatelessServiceSpec()
            spec.name = store_name

            completion = self._oremote(
                "add_stateless_service",
                svc_type,
                spec
            )
            self._wait([completion])

            return HandleCommandResult()
        else:
            raise NotImplementedError(svc_type)

    def _service_rm(self, cmd):
        svc_type = cmd['svc_type']
        svc_id = cmd['svc_id']

        completion = self._oremote("remove_stateless_service", svc_type, svc_id)
        self._wait([completion])
        return HandleCommandResult()

    def _set_backend(self, cmd):
        """
        We implement a setter command instead of just having the user
        modify the setting directly, so that we can validate they're setting
        it to a module that really exists and is enabled.

        There isn't a mechanism for ensuring they don't *disable* the module
        later, but this is better than nothing.
        """

        mgr_map = self.get("mgr_map")
        module_name = cmd['module']

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
                                           stdout="Module '{module_name}' is not enabled. \n Run "
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

    def _status(self):
        try:
            avail, why = self._oremote("available")
        except NoOrchestrator:
            return HandleCommandResult(stderr="No orchestrator configured (try "
                                       "`ceph orchestrator set backend`)")

        if avail is None:
            # The module does not report its availability
            return HandleCommandResult(stdout="Backend: {0}".format(self._select_orchestrator()))
        else:
            return HandleCommandResult(stdout="Backend: {0}\nAvailable: {1}{2}".format(
                                           self._select_orchestrator(),
                                           avail,
                                           " ({0})".format(why) if not avail else ""
                                       ))

    def handle_command(self, inbuf, cmd):
        try:
            return self._handle_command(inbuf, cmd)
        except NoOrchestrator:
            return HandleCommandResult(-errno.ENODEV, stderr="No orchestrator configured")
        except ImportError as e:
            return HandleCommandResult(-errno.ENOENT, stderr=str(e))
        except NotImplementedError:
            return HandleCommandResult(-errno.EINVAL, stderr="Command not found")

    def _handle_command(self, _, cmd):
        if cmd['prefix'] == "orchestrator device ls":
            return self._list_devices(cmd)
        elif cmd['prefix'] == "orchestrator service ls":
            return self._list_services(cmd)
        elif cmd['prefix'] == "orchestrator service status":
            return self._list_services(cmd)  # TODO: create more detailed output
        elif cmd['prefix'] == "orchestrator service add":
            return self._service_add(cmd)
        elif cmd['prefix'] == "orchestrator service rm":
            return self._service_rm(cmd)
        elif cmd['prefix'] == "orchestrator set backend":
            return self._set_backend(cmd)
        elif cmd['prefix'] == "orchestrator status":
            return self._status()
        else:
            raise NotImplementedError()

