import errno
import json
from collections import namedtuple

try:
    from typing import Dict, List, Generator, Callable, Union, Tuple, Optional, Any
except ImportError:
    pass  # just for type checking.

from functools import wraps

from mgr_module import MgrModule, HandleCommandResult, CLICommand, CLIWriteCommand, CLIReadCommand

try:
    from more_itertools import unzip, partition
except ImportError:
    try:
        from itertools import tee, filterfalse
    except ImportError:
        from itertools import tee

        def filterfalse(p, l):
            return filter(lambda x: not p(x), l)

    def partition(pred, iterable):
        t1, t2 = tee(iterable)
        return filterfalse(pred, t1), filter(pred, t2)

    def unzip(iterable):
        return zip(*iterable)

import orchestrator


def handle_exceptions(prefix):
    def _handle_exceptions(func):
        @wraps(func)
        def inner(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except (orchestrator.OrchestratorError, ImportError) as e:
                return HandleCommandResult(-errno.ENOENT, stderr=str(e))
            except NotImplementedError:
                msg = 'This Orchestrator does not support `{}`'.format(prefix)
                return HandleCommandResult(-errno.ENOENT, stderr=msg)
        return inner
    return _handle_exceptions


# Reasons for storing generators in stead of completions are:
# 1. some CLI commands do some (very valid) massaging with the result
# 2. some CLI commands may use more than one completion
# 3. Using an alternative appoach would split up the synchronous code into asynchronous completion
#    handlers. And I don't like asynchronous completion handlers
#
# The  alternative would be to use something like _Completion.next() that executes the
# massaging.
GenComp = namedtuple('GenComp', ['name', 'gen', 'comp'])


def _handle_generator(self, should_wait, name, it):
    # type: (OrchestratorCli, bool, str, Generator[Union[orchestrator._Completion, HandleCommandResult], orchestrator._Completion, None]) -> HandleCommandResult
    c = next(it)
    if should_wait:
        while not isinstance(c, HandleCommandResult):
            self._orchestrator_wait([c])
            e = orchestrator.get_exception_from_completion(c)
            if e is not None:
                it.throw(e)
            else:
                c = it.send(c)

        it.close()
        return c
    else:
        self._completions.append(GenComp(name, it, c))
        return HandleCommandResult(stdout='queued. Continue execution with `ceph orchestrator wait`')


def handle_generator(func):
    @wraps(func)
    def inner(self, *args, **kwargs):
        wait = kwargs.pop('wait', True)
        return _handle_generator(self, wait, func.__name__, func(self, *args, **kwargs))
    return inner


def _cli_command(perm):
    def chain_decorators(prefix, cmd_args="", desc=""):
        """
        shortcut for
        >>> @CLICommand("prefix")
        ... @handle_exceptions("prefix")
        ... @handle_generator
        ... def foo(self):...
        """
        def mk_wrapper(func):
            func = handle_generator(func)
            func = handle_exceptions(prefix)(func)
            func = CLICommand(prefix, cmd_args, desc, perm)(func)
            return func
        return mk_wrapper
    return chain_decorators


_read_cli = _cli_command('r')
_write_cli = _cli_command('rw')


class OrchestratorCli(orchestrator.OrchestratorClientMixin, MgrModule):
    MODULE_OPTIONS = [
        {'name': 'orchestrator'}
    ]

    def __init__(self, *args, **kwargs):
        super(OrchestratorCli, self).__init__(*args, **kwargs)
        self._completions = []  # type: List[GenComp]

    def _select_orchestrator(self):
        return self.get_module_option("orchestrator")

    @CLIWriteCommand('orchestrator wait',
                     "name=refs,type=CephString,req=false,n=N",
                     'Waits for all running completions')
    @handle_exceptions('orchestrator wait')
    def _wait(self, refs=None):
        gen_results = []
        while len(self._completions):
            _, __, completions = unzip(self._completions)  # type: Tuple[Any, Any, List[orchestrator._Completion]]
            self._orchestrator_wait([c for c in completions if c.should_wait])

            def advance(e):
                try:
                    return GenComp(getattr(e.comp, 'message', e.name), e.gen, e.gen.send(e.comp))
                except StopIteration:
                    return GenComp(e.name, e.gen, HandleCommandResult(errno.EINVAL, stderr="Already finished."))

            new_completions = [advance(e) for e in self._completions]
            incomplete, complete = partition(lambda g_c: isinstance(g_c.comp, HandleCommandResult),
                                             new_completions)
            self._completions = list(incomplete)
            gen_results.extend(list(complete))
            self.log.error('gen_results={} self._completions={}, new_completions={}'.format(gen_results, self._completions, new_completions))

        if gen_results:
            _, needs_close, results = unzip(gen_results)
            for g in needs_close:
                g.close()

            def indent(s, i='    '):
                return '\n'.join(i + l for l in s.splitlines())

            return HandleCommandResult(stdout='\n'.join('{}\n{}'.format(r.name, indent(r.comp.stdout)) for r in gen_results))
        return HandleCommandResult()

    @_write_cli('orchestrator host add',
                "name=host,type=CephString,req=true "
                "name=wait,type=CephBool,req=false",
                'Add a host')
    def _add_host(self, host):
        completion = yield self.add_host(host)
        yield HandleCommandResult(stdout=str(completion.result))

    @_write_cli('orchestrator host rm',
                "name=host,type=CephString,req=true "
                "name=wait,type=CephBool,req=false",
                'Remove a host')
    def _remove_host(self, host):
        completion = yield self.remove_host(host)
        yield HandleCommandResult(stdout=str(completion.result))

    @_read_cli('orchestrator host ls',
               "name=wait,type=CephBool,req=false",
               'List hosts')
    def _get_hosts(self):
        completion = yield self.get_hosts()
        result = "\n".join(map(lambda node: node.name, completion.result))
        yield HandleCommandResult(stdout=result)

    @_read_cli('orchestrator device ls',
               "name=host,type=CephString,n=N,req=false "
               "name=format,type=CephChoices,strings=json|plain,req=false "
               "name=refresh,type=CephBool,req=false "
               "name=wait,type=CephBool,req=false",
               'List devices on a node')
    def _list_devices(self, host=None, format='plain', refresh=False):
        # type: (Optional[List[str]], str, bool) -> Generator[Union[orchestrator._Completion, HandleCommandResult], orchestrator._Completion, None]
        """
        Provide information about storage devices present in cluster hosts

        Note: this does not have to be completely synchronous. Slightly out of
        date hardware inventory is fine as long as hardware ultimately appears
        in the output of this command.
        """
        nf = orchestrator.InventoryFilter(nodes=host) if host else None

        completion = yield self.get_inventory(node_filter=nf, refresh=refresh)

        if format == 'json':
            data = [n.to_json() for n in completion.result]
            yield HandleCommandResult(stdout=json.dumps(data))
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

            yield HandleCommandResult(stdout=result)

    @_read_cli('orchestrator service ls',
               "name=host,type=CephString,req=false "
               "name=svc_type,type=CephChoices,strings=mon|mgr|osd|mds|nfs|rgw|rbd-mirror,req=false "
               "name=svc_id,type=CephString,req=false "
               "name=format,type=CephChoices,strings=json|plain,req=false "
               "name=wait,type=CephBool,req=false",
               'List services known to orchestrator')
    def _list_services(self, host=None, svc_type=None, svc_id=None, format='plain'):
        # XXX this is kind of confusing for people because in the orchestrator
        # context the service ID for MDS is the filesystem ID, not the daemon ID

        completion = yield self.describe_service(svc_type, svc_id, host)
        services = completion.result

        # Sort the list for display
        services.sort(key=lambda s: (s.service_type, s.nodename, s.service_instance))

        if len(services) == 0:
            yield HandleCommandResult(stdout="No services reported")
        elif format == 'json':
            data = [s.to_json() for s in services]
            yield HandleCommandResult(stdout=json.dumps(data))
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

            yield HandleCommandResult(stdout="\n".join(lines))

    @_write_cli('orchestrator osd create',
                "name=svc_arg,type=CephString,req=false "
                "name=wait,type=CephBool,req=false",
                'Create an OSD service. Either --svc_arg=host:drives or -i <drive_group>')
    def _create_osd(self, svc_arg=None, inbuf=None):
        # type: (Optional[str], Optional[str]) -> Generator[Union[orchestrator._Completion, HandleCommandResult], orchestrator._Completion, None]
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
                yield HandleCommandResult(-errno.EINVAL, stderr=msg)
                return

        elif svc_arg:
            try:
                node_name, block_device = svc_arg.split(":")
                block_devices = block_device.split(',')
            except (TypeError, KeyError, ValueError):
                msg = "Invalid host:device spec: '{}'".format(svc_arg) + usage
                yield HandleCommandResult(-errno.EINVAL, stderr=msg)
                return

            devs = orchestrator.DeviceSelection(paths=block_devices)
            drive_group = orchestrator.DriveGroupSpec(node_name, data_devices=devs)
        else:
            yield HandleCommandResult(-errno.EINVAL, stderr=usage)
            return

        # TODO: Remove this and make the orchestrator composable
        #   Like a future or so.
        host_completion = yield self.get_hosts()
        all_hosts = [h.name for h in host_completion.result]

        try:
            drive_group.validate(all_hosts)
        except orchestrator.DriveGroupValidationError as e:
            yield HandleCommandResult(-errno.EINVAL, stderr=str(e))
            return

        completion = yield self.create_osds(drive_group, all_hosts)
        self.log.warning(str(completion.result))
        yield HandleCommandResult(stdout=str(completion.result))

    @_write_cli('orchestrator osd rm',
                "name=svc_id,type=CephString,n=N "
                "name=wait,type=CephBool,req=false",
                'Remove OSD services')
    def _osd_rm(self, svc_id):
        # type: (List[str]) -> Generator[Union[orchestrator._Completion, HandleCommandResult], orchestrator._Completion, None]
        """
        Remove OSD's
        :cmd : Arguments for remove the osd
        """
        completion = yield self.remove_osds(svc_id)
        yield HandleCommandResult(stdout=str(completion.result))

    def _add_stateless_svc(self, svc_type, spec):
        completion = yield self.add_stateless_service(svc_type, spec)
        yield HandleCommandResult(stdout=str(completion.result))

    @_write_cli('orchestrator mds add',
                "name=svc_arg,type=CephString "
                "name=wait,type=CephBool,req=false",
                'Create an MDS service')
    def _mds_add(self, svc_arg):
        spec = orchestrator.StatelessServiceSpec()
        spec.name = svc_arg
        return self._add_stateless_svc("mds", spec)

    @_write_cli('orchestrator rgw add',
                "name=svc_arg,type=CephString "
                "name=wait,type=CephBool,req=false",
                'Create an RGW service')
    def _rgw_add(self, svc_arg):
        spec = orchestrator.StatelessServiceSpec()
        spec.name = svc_arg
        return self._add_stateless_svc("rgw", spec)

    @_write_cli('orchestrator nfs add',
                "name=svc_arg,type=CephString "
                "name=pool,type=CephString "
                "name=namespace,type=CephString,req=false "
                "name=wait,type=CephBool,req=false",
                'Create an NFS service')
    def _nfs_add(self, svc_arg, pool, namespace=None):
        spec = orchestrator.StatelessServiceSpec()
        spec.name = svc_arg
        spec.extended = { "pool":pool }
        if namespace is not None:
            spec.extended["namespace"] = namespace
        return self._add_stateless_svc("nfs", spec)

    def _rm_stateless_svc(self, svc_type, svc_id):
        completion = yield self.remove_stateless_service(svc_type, svc_id)
        yield HandleCommandResult(stdout=str(completion.result))

    @_write_cli('orchestrator mds rm',
                "name=svc_id,type=CephString",
                'Remove an MDS service')
    def _mds_rm(self, svc_id):
        return self._rm_stateless_svc("mds", svc_id)

    @_write_cli('orchestrator rgw rm',
                "name=svc_id,type=CephString "
                "name=wait,type=CephBool,req=false",
                'Remove an RGW service')
    def _rgw_rm(self, svc_id):
        return self._rm_stateless_svc("rgw", svc_id)

    @_write_cli('orchestrator nfs rm',
                "name=svc_id,type=CephString "
                "name=wait,type=CephBool,req=false",
                'Remove an NFS service')
    def _nfs_rm(self, svc_id):
        return self._rm_stateless_svc("nfs", svc_id)

    @_write_cli('orchestrator nfs update',
                "name=svc_id,type=CephString "
                "name=num,type=CephInt "
                "name=wait,type=CephBool,req=false",
                'Scale an NFS service')
    def _nfs_update(self, svc_id, num):
        spec = orchestrator.StatelessServiceSpec()
        spec.name = svc_id
        spec.count = num
        completion = yield self.update_stateless_service("nfs", spec)
        yield HandleCommandResult(stdout=str(completion.result))

    @_write_cli('orchestrator service',
                "name=action,type=CephChoices,strings=start|stop|reload "
                "name=svc_type,type=CephString "
                "name=svc_name,type=CephString "
                "name=wait,type=CephBool,req=false",
                'Start, stop or reload an entire service (i.e. all daemons)')
    def _service_action(self, action, svc_type, svc_name):
        completion = yield self.service_action(action, svc_type, service_name=svc_name)
        yield HandleCommandResult(stdout=str(completion.result))

    @_write_cli('orchestrator service-instance',
                "name=action,type=CephChoices,strings=start|stop|reload "
                "name=svc_type,type=CephString "
                "name=svc_id,type=CephString "
                "name=wait,type=CephBool,req=false",
                'Start, stop or reload a specific service instance')
    def _service_instance_action(self, action, svc_type, svc_id):
        completion = yield self.service_action(action, svc_type, service_id=svc_id)
        yield HandleCommandResult(stdout=str(completion.result))

    @_write_cli('orchestrator mgr update',
                "name=num,type=CephInt,req=true "
                "name=hosts,type=CephString,n=N,req=false "
                "name=wait,type=CephBool,req=false",
                'Update the number of manager instances')
    def _update_mgrs(self, num, hosts=None):
        hosts = hosts if hosts is not None else []

        if num <= 0:
            yield HandleCommandResult(-errno.EINVAL,
                    stderr="Invalid number of mgrs: require {} > 0".format(num))
            return

        completion = yield self.update_mgrs(num, hosts)
        yield HandleCommandResult(stdout=str(completion.result))

    @_write_cli('orchestrator mon update',
                "name=num,type=CephInt,req=true "
                "name=hosts,type=CephString,n=N,req=false "
                "name=wait,type=CephBool,req=false",
                'Update the number of monitor instances')
    def _update_mons(self, num, hosts=None):
        hosts = hosts if hosts is not None else []

        if num <= 0:
            yield HandleCommandResult(-errno.EINVAL,
                    stderr="Invalid number of mons: require {} > 0".format(num))
            return

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
                yield HandleCommandResult(-errno.EINVAL, stderr=msg)
                return

        completion = yield self.update_mons(num, hosts)
        yield HandleCommandResult(stdout=str(completion.result))

    @CLIWriteCommand('orchestrator set backend',
                     "name=module_name,type=CephString,req=true",
                     'Select orchestrator module backend')
    @handle_exceptions('orchestrator set backend')
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
    @handle_exceptions('orchestrator status')
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
