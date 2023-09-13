"""
RBD support module
"""

import enum
import errno
import functools
import inspect
import rados
import rbd
import traceback
from typing import cast, Any, Callable, Optional, Tuple, TypeVar

from mgr_module import CLIReadCommand, CLIWriteCommand, MgrModule, Option
from threading import Thread, Event

from .common import NotAuthorizedError
from .mirror_snapshot_schedule import image_validator, namespace_validator, \
    LevelSpec, MirrorSnapshotScheduleHandler
from .perf import PerfHandler, OSD_PERF_QUERY_COUNTERS
from .task import TaskHandler
from .trash_purge_schedule import TrashPurgeScheduleHandler


class ImageSortBy(enum.Enum):
    write_ops = 'write_ops'
    write_bytes = 'write_bytes'
    write_latency = 'write_latency'
    read_ops = 'read_ops'
    read_bytes = 'read_bytes'
    read_latency = 'read_latency'


FuncT = TypeVar('FuncT', bound=Callable)


def with_latest_osdmap(func: FuncT) -> FuncT:
    @functools.wraps(func)
    def wrapper(self: 'Module', *args: Any, **kwargs: Any) -> Tuple[int, str, str]:
        if not self.module_ready:
            return (-errno.EAGAIN, "",
                    "rbd_support module is not ready, try again")
        # ensure we have latest pools available
        self.rados.wait_for_latest_osdmap()
        try:
            try:
                return func(self, *args, **kwargs)
            except NotAuthorizedError:
                raise
            except Exception:
                # log the full traceback but don't send it to the CLI user
                self.log.exception("Fatal runtime error: ")
                raise
        except (rados.ConnectionShutdown, rbd.ConnectionShutdown) as ex:
            self.log.debug("with_latest_osdmap: client blocklisted")
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

    wrapper.__signature__ = inspect.signature(func)  # type: ignore[attr-defined]
    return cast(FuncT, wrapper)


class Module(MgrModule):
    MODULE_OPTIONS = [
        Option(name=MirrorSnapshotScheduleHandler.MODULE_OPTION_NAME),
        Option(name=MirrorSnapshotScheduleHandler.MODULE_OPTION_NAME_MAX_CONCURRENT_SNAP_CREATE,
               type='int',
               default=10),
        Option(name=TrashPurgeScheduleHandler.MODULE_OPTION_NAME),
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(Module, self).__init__(*args, **kwargs)
        self.client_blocklisted = Event()
        self.module_ready = False
        self.init_handlers()
        self.recovery_thread = Thread(target=self.run)
        self.recovery_thread.start()

    def init_handlers(self) -> None:
        self.mirror_snapshot_schedule = MirrorSnapshotScheduleHandler(self)
        self.perf = PerfHandler(self)
        self.task = TaskHandler(self)
        self.trash_purge_schedule = TrashPurgeScheduleHandler(self)

    def setup_handlers(self) -> None:
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

    def run(self) -> None:
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

    def shutdown(self) -> None:
        self.module_ready = False
        self.mirror_snapshot_schedule.shutdown()
        self.trash_purge_schedule.shutdown()
        self.task.shutdown()
        self.perf.shutdown()
        # shut down client and deregister it from MgrMap
        super().shutdown()

    @CLIWriteCommand('rbd mirror snapshot schedule add')
    @with_latest_osdmap
    def mirror_snapshot_schedule_add(self,
                                     level_spec: str,
                                     interval: str,
                                     start_time: Optional[str] = None) -> Tuple[int, str, str]:
        """
        Add rbd mirror snapshot schedule
        """
        spec = LevelSpec.from_name(self, level_spec, namespace_validator, image_validator)
        return self.mirror_snapshot_schedule.add_schedule(spec, interval, start_time)

    @CLIWriteCommand('rbd mirror snapshot schedule remove')
    @with_latest_osdmap
    def mirror_snapshot_schedule_remove(self,
                                        level_spec: str,
                                        interval: Optional[str] = None,
                                        start_time: Optional[str] = None) -> Tuple[int, str, str]:
        """
        Remove rbd mirror snapshot schedule
        """
        spec = LevelSpec.from_name(self, level_spec, namespace_validator, image_validator)
        return self.mirror_snapshot_schedule.remove_schedule(spec, interval, start_time)

    @CLIReadCommand('rbd mirror snapshot schedule list')
    @with_latest_osdmap
    def mirror_snapshot_schedule_list(self,
                                      level_spec: str = '') -> Tuple[int, str, str]:
        """
        List rbd mirror snapshot schedule
        """
        spec = LevelSpec.from_name(self, level_spec, namespace_validator, image_validator)
        return self.mirror_snapshot_schedule.list(spec)

    @CLIReadCommand('rbd mirror snapshot schedule status')
    @with_latest_osdmap
    def mirror_snapshot_schedule_status(self,
                                        level_spec: str = '') -> Tuple[int, str, str]:
        """
        Show rbd mirror snapshot schedule status
        """
        spec = LevelSpec.from_name(self, level_spec, namespace_validator, image_validator)
        return self.mirror_snapshot_schedule.status(spec)

    @CLIReadCommand('rbd perf image stats')
    @with_latest_osdmap
    def perf_image_stats(self,
                         pool_spec: Optional[str] = None,
                         sort_by: Optional[ImageSortBy] = None) -> Tuple[int, str, str]:
        """
        Retrieve current RBD IO performance stats
        """
        with self.perf.lock:
            sort_by_name = sort_by.name if sort_by else OSD_PERF_QUERY_COUNTERS[0]
            return self.perf.get_perf_stats(pool_spec, sort_by_name)

    @CLIReadCommand('rbd perf image counters')
    @with_latest_osdmap
    def perf_image_counters(self,
                            pool_spec: Optional[str] = None,
                            sort_by: Optional[ImageSortBy] = None) -> Tuple[int, str, str]:
        """
        Retrieve current RBD IO performance counters
        """
        with self.perf.lock:
            sort_by_name = sort_by.name if sort_by else OSD_PERF_QUERY_COUNTERS[0]
            return self.perf.get_perf_counters(pool_spec, sort_by_name)

    @CLIWriteCommand('rbd task add flatten')
    @with_latest_osdmap
    def task_add_flatten(self, image_spec: str) -> Tuple[int, str, str]:
        """
        Flatten a cloned image asynchronously in the background
        """
        with self.task.lock:
            return self.task.queue_flatten(image_spec)

    @CLIWriteCommand('rbd task add remove')
    @with_latest_osdmap
    def task_add_remove(self, image_spec: str) -> Tuple[int, str, str]:
        """
        Remove an image asynchronously in the background
        """
        with self.task.lock:
            return self.task.queue_remove(image_spec)

    @CLIWriteCommand('rbd task add trash remove')
    @with_latest_osdmap
    def task_add_trash_remove(self, image_id_spec: str) -> Tuple[int, str, str]:
        """
        Remove an image from the trash asynchronously in the background
        """
        with self.task.lock:
            return self.task.queue_trash_remove(image_id_spec)

    @CLIWriteCommand('rbd task add migration execute')
    @with_latest_osdmap
    def task_add_migration_execute(self, image_spec: str) -> Tuple[int, str, str]:
        """
        Execute an image migration asynchronously in the background
        """
        with self.task.lock:
            return self.task.queue_migration_execute(image_spec)

    @CLIWriteCommand('rbd task add migration commit')
    @with_latest_osdmap
    def task_add_migration_commit(self, image_spec: str) -> Tuple[int, str, str]:
        """
        Commit an executed migration asynchronously in the background
        """
        with self.task.lock:
            return self.task.queue_migration_commit(image_spec)

    @CLIWriteCommand('rbd task add migration abort')
    @with_latest_osdmap
    def task_add_migration_abort(self, image_spec: str) -> Tuple[int, str, str]:
        """
        Abort a prepared migration asynchronously in the background
        """
        with self.task.lock:
            return self.task.queue_migration_abort(image_spec)

    @CLIWriteCommand('rbd task cancel')
    @with_latest_osdmap
    def task_cancel(self, task_id: str) -> Tuple[int, str, str]:
        """
        Cancel a pending or running asynchronous task
        """
        with self.task.lock:
            return self.task.task_cancel(task_id)

    @CLIReadCommand('rbd task list')
    @with_latest_osdmap
    def task_list(self, task_id: Optional[str] = None) -> Tuple[int, str, str]:
        """
        List pending or running asynchronous tasks
        """
        with self.task.lock:
            return self.task.task_list(task_id)

    @CLIWriteCommand('rbd trash purge schedule add')
    @with_latest_osdmap
    def trash_purge_schedule_add(self,
                                 level_spec: str,
                                 interval: str,
                                 start_time: Optional[str] = None) -> Tuple[int, str, str]:
        """
        Add rbd trash purge schedule
        """
        spec = LevelSpec.from_name(self, level_spec, allow_image_level=False)
        return self.trash_purge_schedule.add_schedule(spec, interval, start_time)

    @CLIWriteCommand('rbd trash purge schedule remove')
    @with_latest_osdmap
    def trash_purge_schedule_remove(self,
                                    level_spec: str,
                                    interval: Optional[str] = None,
                                    start_time: Optional[str] = None) -> Tuple[int, str, str]:
        """
        Remove rbd trash purge schedule
        """
        spec = LevelSpec.from_name(self, level_spec, allow_image_level=False)
        return self.trash_purge_schedule.remove_schedule(spec, interval, start_time)

    @CLIReadCommand('rbd trash purge schedule list')
    @with_latest_osdmap
    def trash_purge_schedule_list(self,
                                  level_spec: str = '') -> Tuple[int, str, str]:
        """
        List rbd trash purge schedule
        """
        spec = LevelSpec.from_name(self, level_spec, allow_image_level=False)
        return self.trash_purge_schedule.list(spec)

    @CLIReadCommand('rbd trash purge schedule status')
    @with_latest_osdmap
    def trash_purge_schedule_status(self,
                                    level_spec: str = '') -> Tuple[int, str, str]:
        """
        Show rbd trash purge schedule status
        """
        spec = LevelSpec.from_name(self, level_spec, allow_image_level=False)
        return self.trash_purge_schedule.status(spec)
