import errno
import time
from mgr_module import MgrModule

import orchestrator


class NoOrchestrator(Exception):
    pass


class OrchestratorCli(MgrModule):
    OPTIONS = [
        {'name': 'orchestrator'}
    ]
    COMMANDS = [
        {
            'cmd': "orchestrator device ls "
                   "name=node,type=CephString,req=false",
            "desc": "List devices on a node",
            "perm": "r"
        },
        {
            'cmd': "orchestrator service status "
                   "name=svc_type,type=CephString "
                   "name=svc_id,type=CephString ",
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
            'cmd': "orchestrator set backend "
                   "name=module,type=CephString,req=true",
            "desc": "Select orchestrator module backend",
            "perm": "rw"
        },
    ]

    def _select_orchestrator(self):
        o = self.get_config("orchestrator")
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
        done = False

        while done is False:
            done = self._oremote("wait", completions)

            if not done:
                any_nonpersistent = False
                for c in completions:
                    if c.is_read:
                        if not c.is_complete:
                            any_nonpersistent = True
                            break
                    else:
                        if not c.is_persistent:
                            any_nonpersistent = True
                            break

                if any_nonpersistent:
                    time.sleep(5)
                else:
                    done = True

    def _list_devices(self, cmd):
        node = cmd.get('node', None)

        if node:
            nf = orchestrator.InventoryFilter()
            nf.nodes = [node]
        else:
            nf = None

        completion = self._oremote("get_inventory", node_filter=nf)

        self._wait([completion])

        # Spit out a human readable version
        result = ""

        for inventory_node in completion.result:
            result += "{0}:\n".format(inventory_node.name)
            for d in inventory_node.devices:
                result += "  {0} ({1}, {2}b)\n".format(
                    d.id, d.type, d.size)
            result += "\n"

        return 0, result, ""

    def _service_status(self, cmd):
        svc_type = cmd['svc_type']
        svc_id = cmd['svc_id']

        # XXX this is kind of confusing for people because in the orchestrator
        # context the service ID for MDS is the filesystem ID, not the daemon ID

        completion = self._oremote("describe_service", svc_type, svc_id)

        self._wait([completion])

        service_description = completion.result
        #assert isinstance(service_description, orchestrator.ServiceDescription)

        if len(service_description.locations) == 0:
            return 0, "", "No locations reported"
        else:
            lines = []
            for l in service_description.locations:
                lines.append("{0}.{1} {2} {3}".format(
                    svc_type,
                    l.daemon_name,
                    l.nodename,
                    l.container_id))

            return 0, "\n".join(lines), ""

    def _service_add(self, cmd):
        svc_type = cmd['svc_type']
        if svc_type == "osd":
            device_spec = cmd['svc_arg']
            try:
                node_name, block_device = device_spec.split(":")
            except TypeError:
                return -errno.EINVAL, "", "Invalid device spec, should be <node>:<device>"

            spec = orchestrator.OsdCreationSpec()
            spec.node = node_name
            spec.format = "bluestore"
            spec.drive_group = orchestrator.DriveGroupSpec([block_device])

            completion = self._oremote("create_osds", spec)
            self._wait([completion])

            return 0, "", "Success."

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

            return 0, "", "Success."
        else:
            raise NotImplementedError(svc_type)

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
            self.set_config("orchestrator", None)
            return 0, "", ""

        for module in mgr_map['available_modules']:
            if module['name'] != module_name:
                continue

            if not module['can_run']:
                continue

            enabled = module['name'] in mgr_map['modules']
            if not enabled:
                return -errno.EINVAL, "", "Module '{0}' is not enabled".format(
                    module_name
                )

            try:
                is_orchestrator = self.remote(module_name,
                                              "is_orchestrator_module")
            except NameError:
                is_orchestrator = False

            if not is_orchestrator:
                return -errno.EINVAL, "",\
                       "'{0}' is not an orchestrator module".format(
                           module_name)

            self.set_config("orchestrator", module_name)

            return 0, "", ""

        return -errno.ENOENT, "", "Module '{0}' not found".format(
            module_name
        )

    def handle_command(self, inbuf, cmd):
        try:
            return self._handle_command(inbuf, cmd)
        except NoOrchestrator:
            return -errno.ENODEV, "", "No orchestrator configured"
        except ImportError as e:
            return -errno.ENOENT, "", e.message
        except NotImplementedError:
            return -errno.EINVAL, "", "Command not found"

    def _handle_command(self, _, cmd):
        if cmd['prefix'] == "orchestrator device ls":
            return self._list_devices(cmd)
        elif cmd['prefix'] == "orchestrator service status":
            return self._service_status(cmd)
        elif cmd['prefix'] == "orchestrator service add":
            return self._service_add(cmd)
        if cmd['prefix'] == "orchestrator set backend":
            return self._set_backend(cmd)
        else:
            raise NotImplementedError()
