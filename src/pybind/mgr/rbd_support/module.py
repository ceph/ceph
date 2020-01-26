"""
RBD support module
"""

import errno
import rados
import rbd
import traceback

from mgr_module import MgrModule

from .common import NotAuthorizedError
from .mirror_snapshot_schedule import MirrorSnapshotScheduleHandler
from .perf import PerfHandler
from .task import TaskHandler


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
        }
    ]
    MODULE_OPTIONS = [
        {'name': 'mirror_snapshot_schedule'},
    ]

    mirror_snapshot_schedule = None
    perf = None
    task = None

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.mirror_snapshot_schedule = MirrorSnapshotScheduleHandler(self)
        self.perf = PerfHandler(self)
        self.task = TaskHandler(self)

    def handle_command(self, inbuf, cmd):
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

            except NotAuthorizedError:
                raise
            except Exception as ex:
                # log the full traceback but don't send it to the CLI user
                self.log.fatal("Fatal runtime error: {}\n{}".format(
                    ex, traceback.format_exc()))
                raise

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
