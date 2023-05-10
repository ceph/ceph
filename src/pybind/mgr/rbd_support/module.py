"""
RBD support module
"""

import errno
import rados
import rbd
import traceback

from mgr_module import MgrModule
from threading import Thread, Event

from .common import NotAuthorizedError
from .mirror_snapshot_schedule import MirrorSnapshotScheduleHandler
from .perf import PerfHandler
from .task import TaskHandler
from .trash_purge_schedule import TrashPurgeScheduleHandler


class Module(MgrModule):
    COMMANDS = [
        {
            "cmd": "rbd mirror snapshot schedule add "
                   "name=level_spec,type=CephString "
                   "name=interval,type=CephString "
                   "name=start_time,type=CephString,req=false ",
            "desc": "Add rbd mirror snapshot schedule",
            "perm": "w"
        },
        {
            "cmd": "rbd mirror snapshot schedule remove "
                   "name=level_spec,type=CephString "
                   "name=interval,type=CephString,req=false "
                   "name=start_time,type=CephString,req=false ",
            "desc": "Remove rbd mirror snapshot schedule",
            "perm": "w"
        },
        {
            "cmd": "rbd mirror snapshot schedule list "
                   "name=level_spec,type=CephString,req=false ",
            "desc": "List rbd mirror snapshot schedule",
            "perm": "r"
        },
        {
            "cmd": "rbd mirror snapshot schedule status "
                   "name=level_spec,type=CephString,req=false ",
            "desc": "Show rbd mirror snapshot schedule status",
            "perm": "r"
        },
        {
            "cmd": "rbd perf image stats "
                   "name=pool_spec,type=CephString,req=false "
                   "name=sort_by,type=CephChoices,strings="
                   "write_ops|write_bytes|write_latency|"
                   "read_ops|read_bytes|read_latency,"
                   "req=false ",
            "desc": "Retrieve current RBD IO performance stats",
            "perm": "r"
        },
        {
            "cmd": "rbd perf image counters "
                   "name=pool_spec,type=CephString,req=false "
                   "name=sort_by,type=CephChoices,strings="
                   "write_ops|write_bytes|write_latency|"
                   "read_ops|read_bytes|read_latency,"
                   "req=false ",
            "desc": "Retrieve current RBD IO performance counters",
            "perm": "r"
        },
        {
            "cmd": "rbd task add flatten "
                   "name=image_spec,type=CephString",
            "desc": "Flatten a cloned image asynchronously in the background",
            "perm": "w"
        },
        {
            "cmd": "rbd task add remove "
                   "name=image_spec,type=CephString",
            "desc": "Remove an image asynchronously in the background",
            "perm": "w"
        },
        {
            "cmd": "rbd task add trash remove "
                   "name=image_id_spec,type=CephString",
            "desc": "Remove an image from the trash asynchronously in the background",
            "perm": "w"
        },
        {
            "cmd": "rbd task add migration execute "
                   "name=image_spec,type=CephString",
            "desc": "Execute an image migration asynchronously in the background",
            "perm": "w"
        },
        {
            "cmd": "rbd task add migration commit "
                   "name=image_spec,type=CephString",
            "desc": "Commit an executed migration asynchronously in the background",
            "perm": "w"
        },
        {
            "cmd": "rbd task add migration abort "
                   "name=image_spec,type=CephString",
            "desc": "Abort a prepared migration asynchronously in the background",
            "perm": "w"
        },
        {
            "cmd": "rbd task cancel "
                   "name=task_id,type=CephString ",
            "desc": "Cancel a pending or running asynchronous task",
            "perm": "r"
        },
        {
            "cmd": "rbd task list "
                   "name=task_id,type=CephString,req=false ",
            "desc": "List pending or running asynchronous tasks",
            "perm": "r"
        },
        {
            "cmd": "rbd trash purge schedule add "
                   "name=level_spec,type=CephString "
                   "name=interval,type=CephString "
                   "name=start_time,type=CephString,req=false ",
            "desc": "Add rbd trash purge schedule",
            "perm": "w"
        },
        {
            "cmd": "rbd trash purge schedule remove "
                   "name=level_spec,type=CephString "
                   "name=interval,type=CephString,req=false "
                   "name=start_time,type=CephString,req=false ",
            "desc": "Remove rbd trash purge schedule",
            "perm": "w"
        },
        {
            "cmd": "rbd trash purge schedule list "
                   "name=level_spec,type=CephString,req=false ",
            "desc": "List rbd trash purge schedule",
            "perm": "r"
        },
        {
            "cmd": "rbd trash purge schedule status "
                   "name=level_spec,type=CephString,req=false ",
            "desc": "Show rbd trash purge schedule status",
            "perm": "r"
        }
    ]
    MODULE_OPTIONS = [
        {'name': MirrorSnapshotScheduleHandler.MODULE_OPTION_NAME},
        {'name': MirrorSnapshotScheduleHandler.MODULE_OPTION_NAME_MAX_CONCURRENT_SNAP_CREATE, 'type': 'int', 'default': 10},
        {'name': TrashPurgeScheduleHandler.MODULE_OPTION_NAME},
    ]

    mirror_snapshot_schedule = None
    perf = None
    task = None
    trash_purge_schedule = None

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.client_blocklisted = Event()
        self.module_ready = False
        self.init_handlers()
        self.recovery_thread = Thread(target=self.run)
        self.recovery_thread.start()

    def init_handlers(self):
        self.mirror_snapshot_schedule = MirrorSnapshotScheduleHandler(self)
        self.perf = PerfHandler(self)
        self.task = TaskHandler(self)
        self.trash_purge_schedule = TrashPurgeScheduleHandler(self)

    def setup_handlers(self):
        self.log.info("starting setup")
        # new RADOS client is created and registered in the MgrMap
        # implicitly here as 'rados' is a property attribute.
        self.rados.wait_for_latest_osdmap()
        self.mirror_snapshot_schedule.setup()
        self.perf.setup()
        self.task.setup()
        self.trash_purge_schedule.setup()
        self.log.info("setup complete")
        self.module_ready = True

    def run(self):
        self.log.info("recovery thread starting")
        try:
            while True:
                try:
                    self.setup_handlers()
                except (rados.ConnectionShutdown, rbd.ConnectionShutdown):
                    self.log.exception("setup_handlers: client blocklisted")
                    self.log.info("recovering from double blocklisting")
                else:
                    # block until RADOS client is blocklisted
                    self.client_blocklisted.wait()
                    self.log.info("recovering from blocklisting")
                self.shutdown()
                self.client_blocklisted.clear()
                self.init_handlers()
        except Exception as ex:
            self.log.fatal("Fatal runtime error: {}\n{}".format(
                ex, traceback.format_exc()))

    def shutdown(self):
        self.module_ready = False
        self.mirror_snapshot_schedule.shutdown()
        self.trash_purge_schedule.shutdown()
        self.task.shutdown()
        self.perf.shutdown()
        # shut down client and deregister it from MgrMap
        super().shutdown()

    def handle_command(self, inbuf, cmd):
        if not self.module_ready:
            return -errno.EAGAIN, "", ""
        # ensure we have latest pools available
        self.rados.wait_for_latest_osdmap()

        prefix = cmd['prefix']
        try:
            try:
                if prefix.startswith('rbd mirror snapshot schedule '):
                    return self.mirror_snapshot_schedule.handle_command(
                        inbuf, prefix[29:], cmd)
                elif prefix.startswith('rbd perf '):
                    return self.perf.handle_command(inbuf, prefix[9:], cmd)
                elif prefix.startswith('rbd task '):
                    return self.task.handle_command(inbuf, prefix[9:], cmd)
                elif prefix.startswith('rbd trash purge schedule '):
                    return self.trash_purge_schedule.handle_command(
                        inbuf, prefix[25:], cmd)

            except NotAuthorizedError:
                raise
            except Exception as ex:
                # log the full traceback but don't send it to the CLI user
                self.log.fatal("Fatal runtime error: {}\n{}".format(
                    ex, traceback.format_exc()))
                raise

        except (rados.ConnectionShutdown, rbd.ConnectionShutdown) as ex:
            self.log.debug("handle_command: client blocklisted")
            self.client_blocklisted.set()
            return -errno.EAGAIN, "", str(ex)
        except rados.Error as ex:
            return -ex.errno, "", str(ex)
        except rbd.OSError as ex:
            return -ex.errno, "", str(ex)
        except rbd.Error as ex:
            return -errno.EINVAL, "", str(ex)
        except KeyError as ex:
            return -errno.ENOENT, "", str(ex)
        except ValueError as ex:
            return -errno.EINVAL, "", str(ex)
        except NotAuthorizedError as ex:
            return -errno.EACCES, "", str(ex)

        raise NotImplementedError(cmd['prefix'])
