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
from .trash_purge_schedule import TrashPurgeScheduleHandler


class Module(MgrModule):
    COMMANDS = MirrorSnapshotScheduleHandler.MODULE_COMMANDS + \
        PerfHandler.MODULE_COMMANDS + \
        TaskHandler.MODULE_COMMANDS + \
        TrashPurgeScheduleHandler.MODULE_COMMANDS
    MODULE_OPTIONS = [
        {'name': MirrorSnapshotScheduleHandler.MODULE_OPTION_NAME},
        {'name': TrashPurgeScheduleHandler.MODULE_OPTION_NAME},
    ]

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.rados.wait_for_latest_osdmap()
        self.handlers = [
            MirrorSnapshotScheduleHandler(self),
            PerfHandler(self),
            TaskHandler(self),
            TrashPurgeScheduleHandler(self),
        ]

    def handle_command(self, inbuf, cmd):
        # ensure we have latest pools available
        self.rados.wait_for_latest_osdmap()

        prefix = cmd['prefix']
        try:
            try:
                for handler in self.handlers:
                    if prefix.startswith(handler.COMMAND_PREFIX):
                        return handler.handle_command(
                            inbuf, prefix[len(handler.COMMAND_PREFIX):], cmd)

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
