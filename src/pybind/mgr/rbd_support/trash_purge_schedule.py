import errno
import json
import rados
import rbd
import re
import traceback

from datetime import datetime
from threading import Condition, Lock, Thread
from typing import Any, Dict, List, Optional, Tuple

from .common import get_rbd_pools
from .schedule import LevelSpec, Interval, StartTime, Schedule, Schedules


class TrashPurgeScheduleHandler:
    MODULE_OPTION_NAME = "trash_purge_schedule"
    SCHEDULE_OID = "rbd_trash_purge_schedule"

    lock = Lock()
    condition = Condition(lock)
    thread = None

    def __init__(self, module: Any) -> None:
        self.module = module
        self.log = module.log
        self.last_refresh_pools = datetime(1970, 1, 1)

        self.init_schedule_queue()

        self.thread = Thread(target=self.run)
        self.thread.start()

    def run(self) -> None:
        try:
            self.log.info("TrashPurgeScheduleHandler: starting")
            while True:
                self.refresh_pools()
                with self.lock:
                    (ns_spec, wait_time) = self.dequeue()
                    if not ns_spec:
                        self.condition.wait(min(wait_time, 60))
                        continue
                pool_id, namespace = ns_spec
                self.trash_purge(pool_id, namespace)
                with self.lock:
                    self.enqueue(datetime.now(), pool_id, namespace)

        except Exception as ex:
            self.log.fatal("Fatal runtime error: {}\n{}".format(
                ex, traceback.format_exc()))

    def trash_purge(self, pool_id: str, namespace: str) -> None:
        try:
            with self.module.rados.open_ioctx2(int(pool_id)) as ioctx:
                ioctx.set_namespace(namespace)
                rbd.RBD().trash_purge(ioctx, datetime.now())
        except Exception as e:
            self.log.error("exception when purging {}/{}: {}".format(
                pool_id, namespace, e))

    def init_schedule_queue(self) -> None:
        self.queue: Dict[str, List[Tuple[str, str]]] = {}
        # pool_id => {namespace => pool_name}
        self.pools: Dict[str, Dict[str, str]] = {}
        self.refresh_pools()
        self.log.debug("scheduler queue is initialized")

    def load_schedules(self) -> None:
        self.log.info("TrashPurgeScheduleHandler: load_schedules")

        schedules = Schedules(self)
        schedules.load()
        with self.lock:
            self.schedules = schedules

    def refresh_pools(self) -> None:
        if (datetime.now() - self.last_refresh_pools).seconds < 60:
            return

        self.log.debug("TrashPurgeScheduleHandler: refresh_pools")

        self.load_schedules()

        pools: Dict[str, Dict[str, str]] = {}

        for pool_id, pool_name in get_rbd_pools(self.module).items():
            if not self.schedules.intersects(
                    LevelSpec.from_pool_spec(pool_id, pool_name)):
                continue
            with self.module.rados.open_ioctx2(int(pool_id)) as ioctx:
                self.load_pool(ioctx, pools)

        with self.lock:
            self.refresh_queue(pools)
            self.pools = pools

        self.last_refresh_pools = datetime.now()

    def load_pool(self, ioctx: rados.Ioctx, pools: Dict[str, Dict[str, str]]) -> None:
        pool_id = str(ioctx.get_pool_id())
        pool_name = ioctx.get_pool_name()
        pools[pool_id] = {}
        pool_namespaces = ['']

        self.log.debug("load_pool: {}".format(pool_name))

        try:
            pool_namespaces += rbd.RBD().namespace_list(ioctx)
        except rbd.OperationNotSupported:
            self.log.debug("namespaces not supported")
        except Exception as e:
            self.log.error("exception when scanning pool {}: {}".format(
                pool_name, e))

        for namespace in pool_namespaces:
            pools[pool_id][namespace] = pool_name

    def rebuild_queue(self) -> None:
        with self.lock:
            now = datetime.now()

            # don't remove from queue "due" images
            now_string = datetime.strftime(now, "%Y-%m-%d %H:%M:00")

            for schedule_time in list(self.queue):
                if schedule_time > now_string:
                    del self.queue[schedule_time]

            if not self.schedules:
                return

            for pool_id, namespaces in self.pools.items():
                for namespace in namespaces:
                    self.enqueue(now, pool_id, namespace)

            self.condition.notify()

    def refresh_queue(self, current_pools: Dict[str, Dict[str, str]]) -> None:
        now = datetime.now()

        for pool_id, namespaces in self.pools.items():
            for namespace in namespaces:
                if pool_id not in current_pools or \
                   namespace not in current_pools[pool_id]:
                    self.remove_from_queue(pool_id, namespace)

        for pool_id, namespaces in current_pools.items():
            for namespace in namespaces:
                if pool_id not in self.pools or \
                   namespace not in self.pools[pool_id]:
                    self.enqueue(now, pool_id, namespace)

        self.condition.notify()

    def enqueue(self, now: datetime, pool_id: str, namespace: str) -> None:

        schedule = self.schedules.find(pool_id, namespace)
        if not schedule:
            return

        schedule_time = schedule.next_run(now)
        if schedule_time not in self.queue:
            self.queue[schedule_time] = []
        self.log.debug("schedule {}/{} at {}".format(
            pool_id, namespace, schedule_time))
        ns_spec = (pool_id, namespace)
        if ns_spec not in self.queue[schedule_time]:
            self.queue[schedule_time].append((pool_id, namespace))

    def dequeue(self) -> Tuple[Optional[Tuple[str, str]], float]:
        if not self.queue:
            return None, 1000.0

        now = datetime.now()
        schedule_time = sorted(self.queue)[0]

        if datetime.strftime(now, "%Y-%m-%d %H:%M:%S") < schedule_time:
            wait_time = (datetime.strptime(schedule_time,
                                           "%Y-%m-%d %H:%M:%S") - now)
            return None, wait_time.total_seconds()

        namespaces = self.queue[schedule_time]
        namespace = namespaces.pop(0)
        if not namespaces:
            del self.queue[schedule_time]
        return namespace, 0.0

    def remove_from_queue(self, pool_id: str, namespace: str) -> None:
        empty_slots = []
        for schedule_time, namespaces in self.queue.items():
            if (pool_id, namespace) in namespaces:
                namespaces.remove((pool_id, namespace))
                if not namespaces:
                    empty_slots.append(schedule_time)
        for schedule_time in empty_slots:
            del self.queue[schedule_time]

    def add_schedule(self,
                     level_spec: LevelSpec,
                     interval: str,
                     start_time: Optional[str]) -> Tuple[int, str, str]:
        self.log.debug(
            "add_schedule: level_spec={}, interval={}, start_time={}".format(
                level_spec.name, interval, start_time))

        with self.lock:
            self.schedules.add(level_spec, interval, start_time)

        # TODO: optimize to rebuild only affected part of the queue
        self.rebuild_queue()
        return 0, "", ""

    def remove_schedule(self,
                        level_spec: LevelSpec,
                        interval: Optional[str],
                        start_time: Optional[str]) -> Tuple[int, str, str]:
        self.log.debug(
            "remove_schedule: level_spec={}, interval={}, start_time={}".format(
                level_spec.name, interval, start_time))

        with self.lock:
            self.schedules.remove(level_spec, interval, start_time)

        # TODO: optimize to rebuild only affected part of the queue
        self.rebuild_queue()
        return 0, "", ""

    def list(self, level_spec: LevelSpec) -> Tuple[int, str, str]:
        self.log.debug("list: level_spec={}".format(level_spec.name))

        with self.lock:
            result = self.schedules.to_list(level_spec)

        return 0, json.dumps(result, indent=4, sort_keys=True), ""

    def status(self, level_spec: LevelSpec) -> Tuple[int, str, str]:
        self.log.debug("status: level_spec={}".format(level_spec.name))

        scheduled = []
        with self.lock:
            for schedule_time in sorted(self.queue):
                for pool_id, namespace in self.queue[schedule_time]:
                    if not level_spec.matches(pool_id, namespace):
                        continue
                    pool_name = self.pools[pool_id][namespace]
                    scheduled.append({
                        'schedule_time' : schedule_time,
                        'pool_id' : pool_id,
                        'pool_name' : pool_name,
                        'namespace' : namespace
                    })
        return 0, json.dumps({'scheduled' : scheduled}, indent=4,
                             sort_keys=True), ""
