import errno
import json
import rados
import rbd
import traceback

from datetime import datetime
from threading import Condition, Lock, Thread
from typing import Any, Dict, List, NamedTuple, Optional, Set, Tuple, Union

from .common import get_rbd_pools
from .schedule import LevelSpec, Schedules


def namespace_validator(ioctx: rados.Ioctx) -> None:
    mode = rbd.RBD().mirror_mode_get(ioctx)
    if mode != rbd.RBD_MIRROR_MODE_IMAGE:
        raise ValueError("namespace {} is not in mirror image mode".format(
            ioctx.get_namespace()))

def group_validator(group: rbd.Group) -> None:
    try:
        info = group.mirror_group_get_info()
    except rbd.ObjectNotFound:
        raise rbd.InvalidArgument("Error getting mirror group info")
    if info['image_mode'] != rbd.RBD_MIRROR_IMAGE_MODE_SNAPSHOT:
        raise rbd.InvalidArgument("Invalid mirror group mode")

class GroupSpec(NamedTuple):
    pool_id: str
    namespace: str
    group_id: str


class MirrorGroupSnapshotScheduleHandler:
    MODULE_OPTION_NAME = "mirror_group_snapshot_schedule"
    SCHEDULE_OID = "rbd_mirror_group_snapshot_schedule"
    REFRESH_DELAY_SECONDS = 60.0

    def __init__(self, module: Any) -> None:
        self.lock = Lock()
        self.condition = Condition(self.lock)
        self.module = module
        self.log = module.log
        self.last_refresh_groups = datetime(1970, 1, 1)
        # self.create_snapshot_requests = CreateSnapshotRequests(self)

        self.stop_thread = False
        self.thread = Thread(target=self.run)

    def setup(self) -> None:
        self.init_schedule_queue()
        self.thread.start()

    def shutdown(self) -> None:
        self.log.info("MirrorGroupSnapshotScheduleHandler: shutting down")
        self.stop_thread = True
        if self.thread.is_alive():
            self.log.debug("MirrorGroupSnapshotScheduleHandler: joining thread")
            self.thread.join()
        # self.create_snapshot_requests.wait_for_pending()
        self.log.info("MirrorGroupSnapshotScheduleHandler: shut down")

    def run(self) -> None:
        try:
            self.log.info("MirrorGroupSnapshotScheduleHandler: starting")
            while not self.stop_thread:
                refresh_delay = self.refresh_groups()
                with self.lock:
                    (group_spec, wait_time) = self.dequeue()
                    if not group_spec:
                        self.condition.wait(min(wait_time, refresh_delay))
                        continue
                pool_id, namespace, group_id = group_spec
                # self.create_snapshot_requests.add(pool_id, namespace, group_id)
                self.create_group_snapshot(pool_id, namespace, group_id)
                with self.lock:
                    self.enqueue(datetime.now(), pool_id, namespace, group_id)

        except (rados.ConnectionShutdown, rbd.ConnectionShutdown):
            self.log.exception("MirrorGroupSnapshotScheduleHandler: client blocklisted")
            self.module.client_blocklisted.set()
        except Exception as ex:
            self.log.fatal("Fatal runtime error: {}\n{}".format(
                ex, traceback.format_exc()))

    def create_group_snapshot(self, pool_id, namespace, group_id):
        try:
            with self.module.rados.open_ioctx2(int(pool_id)) as ioctx:
                ioctx.set_namespace(namespace)
                group_name = rbd.RBD().group_get_name(ioctx, group_id)
                if group_name is None:
                    return
                group = rbd.Group(ioctx, group_name)
                mirror_info = group.mirror_group_get_info()
                if mirror_info['image_mode'] != rbd.RBD_MIRROR_IMAGE_MODE_SNAPSHOT:
                    return
                if mirror_info['state'] != rbd.RBD_MIRROR_GROUP_ENABLED or \
                   not mirror_info['primary']:
                    return
                snap_id = group.mirror_group_create_snapshot()
                self.log.debug(
                    "create_group_snapshot: {}/{}/{}: snap_id={}".format(
                        ioctx.get_pool_name(), namespace, group_name,
                        snap_id))
        except Exception as e:
            self.log.error(
                "exception when creating group snapshot for {}/{}/{}: {}".format(
                    pool_id, namespace, group_id, e))

    def init_schedule_queue(self) -> None:
        # schedule_time => group_spec
        self.queue: Dict[str, List[GroupSpec]] = {}
        # pool_id => {namespace => {group_id => group_name}}
        self.groups: Dict[str, Dict[str, Dict[str, str]]] = {}
        self.schedules = Schedules(self)
        self.refresh_groups()
        self.log.debug("MirrorGroupSnapshotScheduleHandler: queue is initialized")

    def load_schedules(self) -> None:
        self.log.info("MirrorGroupSnapshotScheduleHandler: load_schedules")
        self.schedules.load(namespace_validator, group_validator=group_validator)

    def refresh_groups(self) -> float:
        elapsed = (datetime.now() - self.last_refresh_groups).total_seconds()
        if elapsed < self.REFRESH_DELAY_SECONDS:
            return self.REFRESH_DELAY_SECONDS - elapsed

        self.log.debug("MirrorGroupSnapshotScheduleHandler: refresh_groups")

        with self.lock:
            self.load_schedules()
            if not self.schedules:
                self.log.debug("MirrorGroupSnapshotScheduleHandler: no schedules")
                self.groups = {}
                self.queue = {}
                self.last_refresh_groups = datetime.now()
                return self.REFRESH_DELAY_SECONDS

        groups: Dict[str, Dict[str, Dict[str, str]]] = {}

        for pool_id, pool_name in get_rbd_pools(self.module).items():
            if not self.schedules.intersects(
                    LevelSpec.from_pool_spec(pool_id, pool_name)):
                continue
            with self.module.rados.open_ioctx2(int(pool_id)) as ioctx:
                self.load_pool_groups(ioctx, groups)

        with self.lock:
            self.refresh_queue(groups)
            self.groups = groups

        self.last_refresh_groups = datetime.now()
        return self.REFRESH_DELAY_SECONDS

    def load_pool_groups(self,
                         ioctx: rados.Ioctx,
                         groups: Dict[str, Dict[str, Dict[str, str]]]) -> None:
        pool_id = str(ioctx.get_pool_id())
        pool_name = ioctx.get_pool_name()
        groups[pool_id] = {}

        self.log.debug("load_pool_groups: pool={}".format(pool_name))

        try:
            namespaces = [''] + rbd.RBD().namespace_list(ioctx)
            for namespace in namespaces:
                if not self.schedules.intersects(
                        LevelSpec.from_pool_spec(int(pool_id), pool_name, namespace)):
                    continue
                self.log.debug("load_pool_groups: pool={}, namespace={}".format(
                    pool_name, namespace))
                groups[pool_id][namespace] = {}
                ioctx.set_namespace(namespace)
                mirror_groups = dict(rbd.RBD().mirror_group_info_list(
                    ioctx, rbd.RBD_MIRROR_IMAGE_MODE_SNAPSHOT))
                if not mirror_groups:
                    continue
                group_names = dict(
                    [(x['id'], x['name']) for x in filter(
                        lambda x: x['id'] in mirror_groups,
                        rbd.RBD().group_list2(ioctx))])
                for group_id, info in mirror_groups.items():
                    if not info['primary']:
                        continue
                    group_name = group_names.get(group_id)
                    if not group_name:
                        continue
                    if namespace:
                        name = "{}/{}/{}".format(pool_name, namespace,
                                                 group_name)
                    else:
                        name = "{}/{}".format(pool_name, group_name)
                    self.log.debug(
                        "load_pool_groups: adding group {}".format(name))
                    groups[pool_id][namespace][group_id] = name
        except rbd.ConnectionShutdown:
            raise
        except Exception as e:
            self.log.error(
                "load_pool_groups: exception when scanning pool {}: {}".format(
                    pool_name, e))

    def rebuild_queue(self) -> None:
        now = datetime.now()

        # don't remove from queue "due" groups
        now_string = datetime.strftime(now, "%Y-%m-%d %H:%M:00")

        for schedule_time in list(self.queue):
            if schedule_time > now_string:
                del self.queue[schedule_time]

        if not self.schedules:
            return

        for pool_id in self.groups:
            for namespace in self.groups[pool_id]:
                for group_id in self.groups[pool_id][namespace]:
                    self.enqueue(now, pool_id, namespace, group_id)

        self.condition.notify()

    def refresh_queue(self,
                      current_groups: Dict[str, Dict[str, Dict[str, str]]]) -> None:
        now = datetime.now()

        for pool_id in self.groups:
            for namespace in self.groups[pool_id]:
                for group_id in self.groups[pool_id][namespace]:
                    if pool_id not in current_groups or \
                       namespace not in current_groups[pool_id] or \
                       group_id not in current_groups[pool_id][namespace]:
                        self.remove_from_queue(pool_id, namespace, group_id)

        for pool_id in current_groups:
            for namespace in current_groups[pool_id]:
                for group_id in current_groups[pool_id][namespace]:
                    if pool_id not in self.groups or \
                       namespace not in self.groups[pool_id] or \
                       group_id not in self.groups[pool_id][namespace]:
                        self.enqueue(now, pool_id, namespace, group_id)

        self.condition.notify()

    def enqueue(self, now: datetime, pool_id: str, namespace: str, group_id: str) -> None:
        schedule = self.schedules.find(pool_id, namespace, group_id)
        if not schedule:
            self.log.debug(
                "MirrorGroupSnapshotScheduleHandler: no schedule for {}/{}/{}".format(
                    pool_id, namespace, group_id))
            return

        schedule_time = schedule.next_run(now)
        if schedule_time not in self.queue:
            self.queue[schedule_time] = []
        self.log.debug(
            "MirrorGroupSnapshotScheduleHandler: scheduling {}/{}/{} at {}".format(
                pool_id, namespace, group_id, schedule_time))
        group_spec = GroupSpec(pool_id, namespace, group_id)
        if group_spec not in self.queue[schedule_time]:
            self.queue[schedule_time].append(group_spec)

    def dequeue(self) -> Tuple[Optional[GroupSpec], float]:
        if not self.queue:
            return None, 1000.0

        now = datetime.now()
        schedule_time = sorted(self.queue)[0]

        if datetime.strftime(now, "%Y-%m-%d %H:%M:%S") < schedule_time:
            wait_time = (datetime.strptime(schedule_time,
                                           "%Y-%m-%d %H:%M:%S") - now)
            return None, wait_time.total_seconds()

        groups = self.queue[schedule_time]
        group = groups.pop(0)
        if not groups:
            del self.queue[schedule_time]
        return group, 0.0

    def remove_from_queue(self, pool_id: str, namespace: str, group_id: str) -> None:
        self.log.debug(
            "MirrorGroupSnapshotScheduleHandler: descheduling {}/{}/{}".format(
                pool_id, namespace, group_id))

        empty_slots = []
        group_spec = GroupSpec(pool_id, namespace, group_id)
        for schedule_time, groups in self.queue.items():
            if group_spec in groups:
                groups.remove(group_spec)
                if not groups:
                    empty_slots.append(schedule_time)
        for schedule_time in empty_slots:
            del self.queue[schedule_time]

    def add_schedule(self,
                     level_spec: LevelSpec,
                     interval: str,
                     start_time: Optional[str]) -> Tuple[int, str, str]:
        self.log.debug(
            "MirrorGroupSnapshotScheduleHandler: add_schedule: level_spec={}, interval={}, start_time={}".format(
                level_spec.name, interval, start_time))

        # TODO: optimize to rebuild only affected part of the queue
        with self.lock:
            self.schedules.add(level_spec, interval, start_time)
            self.rebuild_queue()
        return 0, "", ""

    def remove_schedule(self,
                        level_spec: LevelSpec,
                        interval: Optional[str],
                        start_time: Optional[str]) -> Tuple[int, str, str]:
        self.log.debug(
            "MirrorGroupSnapshotScheduleHandler: remove_schedule: level_spec={}, interval={}, start_time={}".format(
                level_spec.name, interval, start_time))

        # TODO: optimize to rebuild only affected part of the queue
        with self.lock:
            self.schedules.remove(level_spec, interval, start_time)
            self.rebuild_queue()
        return 0, "", ""

    def list(self, level_spec: LevelSpec) -> Tuple[int, str, str]:
        self.log.debug(
            "MirrorGroupSnapshotScheduleHandler: list: level_spec={}".format(
                level_spec.name))

        with self.lock:
            result = self.schedules.to_list(level_spec)

        return 0, json.dumps(result, indent=4, sort_keys=True), ""

    def status(self, level_spec: LevelSpec) -> Tuple[int, str, str]:
        self.log.debug(
            "MirrorGroupSnapshotScheduleHandler: status: level_spec={}".format(
                level_spec.name))

        scheduled_groups = []
        with self.lock:
            for schedule_time in sorted(self.queue):
                for pool_id, namespace, group_id in self.queue[schedule_time]:
                    if not level_spec.matches(pool_id, namespace, group_id=group_id):
                        continue
                    group_name = self.groups[pool_id][namespace][group_id]
                    scheduled_groups.append({
                        'schedule_time': schedule_time,
                        'group': group_name
                    })
        return 0, json.dumps({'scheduled_groups': scheduled_groups},
                             indent=4, sort_keys=True), ""
