import errno
import json
import rados
import rbd
import re
import traceback

from datetime import datetime
from threading import Condition, Lock, Thread

from .common import get_rbd_pools
from .schedule import (LevelSpec, Interval, StartTime, Schedule, Schedules,
                       RetentionPolicy, RetentionPolicies)


class SnapshotScheduleHandler:
    MODULE_COMMANDS = [
        {
            "cmd": "rbd snapshot schedule period add "
                   "name=level_spec,type=CephString "
                   "name=interval,type=CephString "
                   "name=start_time,type=CephString,req=false ",
            "desc": "Add rbd snapshot schedule period",
            "perm": "w"
        },
        {
            "cmd": "rbd snapshot schedule period remove "
                   "name=level_spec,type=CephString "
                   "name=interval,type=CephString,req=false "
                   "name=start_time,type=CephString,req=false ",
            "desc": "Remove rbd snapshot schedule period(s)",
            "perm": "w"
        },
        {
            "cmd": "rbd snapshot schedule period list "
                   "name=level_spec,type=CephString,req=false ",
            "desc": "List rbd snapshot schedule periods",
            "perm": "r"
        },
        {
            "cmd": "rbd snapshot schedule period status "
                   "name=level_spec,type=CephString,req=false ",
            "desc": "Show rbd snapshot schedule status",
            "perm": "r"
        },
        {
            "cmd": "rbd snapshot schedule retention add "
                   "name=level_spec,type=CephString "
                   "name=interval,type=CephString "
                   "name=count,type=CephInt,req=false ",
            "desc": "Add rbd snapshot schedule retention policy",
            "perm": "w"
        },
        {
            "cmd": "rbd snapshot schedule retention remove "
                   "name=level_spec,type=CephString "
                   "name=interval,type=CephString,req=false ",
            "desc": "Remove rbd snapshot schedule retention policy",
            "perm": "w"
        },
        {
            "cmd": "rbd snapshot schedule retention list "
                   "name=level_spec,type=CephString,req=false ",
            "desc": "List rbd snapshot schedule retention policy",
            "perm": "r"
        },
        {
            "cmd": "rbd snapshot schedule retention status "
                   "name=level_spec,type=CephString,req=false ",
            "desc": "Show rbd snapshot schedule retention status",
            "perm": "r"
        }
    ]
    MODULE_OPTION_NAME = "snapshot_schedule"
    COMMAND_PREFIX = "rbd snapshot schedule "
    SCHEDULE_OID = "rbd_snapshot_schedule"
    SNAP_PREFIX = "scheduled-"

    lock = Lock()
    condition = Condition(lock)
    thread = None

    def __init__(self, module):
        self.module = module
        self.log = module.log
        self.last_refresh_images = datetime(1970, 1, 1)

        self.init_queues()

        self.thread = Thread(target=self.run)
        self.thread.start()

    def run(self):
        try:
            self.log.info("SnapshotScheduleHandler: starting")
            while True:
                self.refresh_images()

                with self.lock:
                    (image_spec, schedule_time, schedule_wait_time) = \
                        self.schedule_dequeue()
                if image_spec:
                    pool_id, namespace, image_id = image_spec
                    self.create_snapshot(pool_id, namespace, image_id,
                                         self.make_snap_name(schedule_time))
                    with self.lock:
                        self.schedule_enqueue(datetime.now(), pool_id,
                                              namespace, image_id)
                    continue

                with self.lock:
                    (image_spec, retention_wait_time) = self.retention_dequeue()
                if image_spec:
                    pool_id, namespace, image_id = image_spec
                    self.remove_expired_snapshots(pool_id, namespace, image_id)
                    with self.lock:
                        self.retention_enqueue(datetime.now(), pool_id,
                                               namespace, image_id)
                    continue

                with self.lock:
                    wait_time = min(schedule_wait_time, retention_wait_time, 60)
                    self.condition.wait(wait_time)

        except Exception as ex:
            self.log.fatal("Fatal runtime error: {}\n{}".format(
                ex, traceback.format_exc()))

    def make_snap_name(self, schedule_time):
        t = datetime.strptime(schedule_time, "%Y-%m-%d %H:%M:%S")
        return '{}{:04}-{:02}-{:02}-{:02}_{:02}'.format(
            self.SNAP_PREFIX, t.year, t.month, t.day, t.hour, t.minute)

    def create_snapshot(self, pool_id, namespace, image_id, snap_name):
        try:
            with self.module.rados.open_ioctx2(int(pool_id)) as ioctx:
                ioctx.set_namespace(namespace)
                with rbd.Image(ioctx, image_id=image_id) as image:
                    image.create_snap(snap_name)
                    self.log.debug(
                        "create_snapshot: {}/{}/{}@{}".format(
                            ioctx.get_pool_name(), namespace, image.get_name(),
                            snap_name))

        except Exception as e:
            self.log.error(
                "exception when creating snapshot {}/{}/{}@{}: {}".format(
                    pool_id, namespace, image_id, snap_name, e))

    def remove_expired_snapshots(self, pool_id, namespace, image_id):
        policy = self.retention_policies.find(pool_id, namespace, image_id)
        if not policy:
            return

        try:
            with self.module.rados.open_ioctx2(int(pool_id)) as ioctx:
                ioctx.set_namespace(namespace)
                with rbd.Image(ioctx, image_id=image_id) as image:
                    fmt = '{}%Y-%m-%d-%H_%M'.format(self.SNAP_PREFIX)
                    rgx = '{}\d\d\d\d-\d\d-\d\d-\d\d_\d\d'.format(self.SNAP_PREFIX)
                    scheduled = {s['name'] : datetime.strptime(s['name'], fmt)
                                 for s in image.list_snaps()
                                 if re.match(rgx, s['name'])}
                    expired_snaps = policy.apply(datetime.now(), scheduled)

                    if not expired_snaps:
                        return

                    for snap_name in expired_snaps:
                        self.log.debug("remove expired snapshot {}/{}/{}@{}".format(
                            pool_id, namespace, image_id, snap_name))
                        try:
                            image.remove_snap(snap_name)
                        except Exception as e:
                            self.log.error(
                                "exception when removing snapshot {}/{}/{}@{}: {}".format(
                                pool_id, namespace, image_id, snap_name, e))
        except Exception as e:
            self.log.error(
                "exception when removing image {}/{}/{} snapshots: {}".format(
                    pool_id, namespace, image_id, e))

    def init_queues(self):
        self.schedule_queue = {}
        self.schedule_images = {}
        self.retention_queue = {}
        self.retention_images = {}
        self.refresh_images()
        self.log.debug("scheduler queues are initialized")

    def load_schedules(self):
        self.log.info("SnapshotScheduleHandler: load_schedules")

        schedules = Schedules(self, format=2)
        schedules.load()
        with self.lock:
            self.schedules = schedules

    def load_retention_policies(self):
        self.log.info("SnapshotScheduleHandler: load_retention_policies")

        retention_policies = RetentionPolicies(self)
        retention_policies.load()
        with self.lock:
            self.retention_policies = retention_policies

    def refresh_images(self):
        if (datetime.now() - self.last_refresh_images).seconds < 60:
            return

        self.log.debug("SnapshotScheduleHandler: refresh_images")

        self.load_schedules()
        self.load_retention_policies()

        with self.lock:
            if not self.schedules and not self.retention_policies:
                self.schedule_images = {}
                self.schedule_queue = {}
                self.retention_images = {}
                self.retention_queue = {}
                self.last_refresh_images = datetime.now()
                return

        images = {}

        for pool_id, pool_name in get_rbd_pools(self.module).items():
            if not self.schedules.intersects(
                    LevelSpec.from_pool_spec(pool_id, pool_name)):
                continue
            with self.module.rados.open_ioctx2(int(pool_id)) as ioctx:
                self.load_pool_images(ioctx, self.schedules.intersects, images)

        with self.lock:
            self.refresh_schedule_queue(images)
            self.schedule_images = images

        images = {}

        for pool_id, pool_name in get_rbd_pools(self.module).items():
            if not self.retention_policies.intersects(
                    LevelSpec.from_pool_spec(pool_id, pool_name)):
                continue
            with self.module.rados.open_ioctx2(int(pool_id)) as ioctx:
                self.load_pool_images(ioctx, self.retention_policies.intersects,
                                      images)
        with self.lock:
            self.refresh_retention_queue(images)
            self.retention_images = images

        self.last_refresh_images = datetime.now()

    def load_pool_images(self, ioctx, intersects_func, images):
        pool_id = str(ioctx.get_pool_id())
        pool_name = ioctx.get_pool_name()
        images[pool_id] = {}
        pool_namespaces = ['']

        self.log.debug("load_images: {}".format(pool_name))

        try:
            try:
                pool_namespaces += rbd.RBD().namespace_list(ioctx)
            except rbd.OperationNotSupported:
                self.log.debug("namespaces not supported")

            for namespace in pool_namespaces:
                if not intersects_func(
                        LevelSpec.from_pool_spec(pool_id, pool_name, namespace)):
                    continue
                self.log.debug("load_pool_images: pool={}, namespace={}".format(
                    pool_name, namespace))
                images[pool_id][namespace] = {}
                ioctx.set_namespace(namespace)
                for image in rbd.RBD().list2(ioctx):
                    if not image['id']:
                        continue
                    if namespace:
                        name = "{}/{}/{}".format(pool_name, namespace,
                                                 image['name'])
                    else:
                        name = "{}/{}".format(pool_name, image['name'])
                    images[pool_id][namespace][image['id']] = name
        except Exception as e:
            self.log.error(
                "load_pool_images: exception when scanning pool {}: {}".format(
                    pool_name, e))

    def rebuild_schedule_queue(self):
        with self.lock:
            now = datetime.now()

            # don't remove from queue "due" images
            now_string = datetime.strftime(now, "%Y-%m-%d %H:%M:00")

            for schedule_time in list(self.schedule_queue):
                if schedule_time > now_string:
                    del self.schedule_queue[schedule_time]

            if not self.schedules:
                return

            for pool_id in self.schedule_images:
                for namespace in self.schedule_images[pool_id]:
                    for image_id in self.schedule_images[pool_id][namespace]:
                        self.schedule_enqueue(now, pool_id, namespace, image_id)

            self.condition.notify()

    def refresh_schedule_queue(self, current_images):
        now = datetime.now()

        for pool_id in self.schedule_images:
            for namespace in self.schedule_images[pool_id]:
                for image_id in self.schedule_images[pool_id][namespace]:
                    if pool_id not in current_images or \
                       namespace not in current_images[pool_id] or \
                       image_id not in current_images[pool_id][namespace]:
                        self.remove_from_schedule_queue(pool_id, namespace,
                                                        image_id)
        for pool_id in current_images:
            for namespace in current_images[pool_id]:
                for image_id in current_images[pool_id][namespace]:
                    if pool_id not in self.schedule_images or \
                       namespace not in self.schedule_images[pool_id] or \
                       image_id not in self.schedule_images[pool_id][namespace]:
                        self.schedule_enqueue(now, pool_id, namespace, image_id)

        self.condition.notify()

    def schedule_enqueue(self, now, pool_id, namespace, image_id):

        schedule = self.schedules.find(pool_id, namespace, image_id)
        if not schedule:
            return

        schedule_time = schedule.next_run(now)
        if schedule_time not in self.schedule_queue:
            self.schedule_queue[schedule_time] = []
        self.log.debug("schedule image {}/{}/{} at {}".format(
            pool_id, namespace, image_id, schedule_time))
        image_spec = (pool_id, namespace, image_id)
        if image_spec not in self.schedule_queue[schedule_time]:
            self.schedule_queue[schedule_time].append((pool_id, namespace,
                                                       image_id))

    def schedule_dequeue(self):
        if not self.schedule_queue:
            return None, None, 1000

        now = datetime.now()
        schedule_time = sorted(self.schedule_queue)[0]

        if datetime.strftime(now, "%Y-%m-%d %H:%M:%S") < schedule_time:
            wait_time = (datetime.strptime(schedule_time,
                                           "%Y-%m-%d %H:%M:%S") - now)
            return None, None, wait_time.total_seconds()

        images = self.schedule_queue[schedule_time]
        image = images.pop(0)
        if not images:
            del self.schedule_queue[schedule_time]
        return image, schedule_time, 0

    def remove_from_schedule_queue(self, pool_id, namespace, image_id):
        empty_slots = []
        for schedule_time, images in self.schedule_queue.items():
            if (pool_id, namespace, image_id) in images:
                images.remove((pool_id, namespace, image_id))
                if not images:
                    empty_slots.append(schedule_time)
        for schedule_time in empty_slots:
            del self.schedule_queue[schedule_time]

    def rebuild_retention_queue(self):
        with self.lock:
            now = datetime.now()

            # don't remove from queue "due" images
            now_string = datetime.strftime(now, "%Y-%m-%d %H:%M:00")

            for schedule_time in list(self.retention_queue):
                if schedule_time > now_string:
                    del self.retention_queue[schedule_time]

            if not self.retention_policies:
                return

            for pool_id in self.retention_images:
                for namespace in self.retention_images[pool_id]:
                    for image_id in self.retention_images[pool_id][namespace]:
                        self.retention_enqueue(now, pool_id, namespace,
                                               image_id)
            self.condition.notify()

    def refresh_retention_queue(self, current_images):
        now = datetime.now()

        for pool_id in self.retention_images:
            for namespace in self.retention_images[pool_id]:
                for image_id in self.retention_images[pool_id][namespace]:
                    if pool_id not in current_images or \
                       namespace not in current_images[pool_id] or \
                       image_id not in current_images[pool_id][namespace]:
                        self.remove_from_retention_queue(pool_id, namespace,
                                                         image_id)
        for pool_id in current_images:
            for namespace in current_images[pool_id]:
                for image_id in current_images[pool_id][namespace]:
                    if pool_id not in self.retention_images or \
                       namespace not in self.retention_images[pool_id] or \
                       image_id not in self.retention_images[pool_id][namespace]:
                        self.retention_enqueue(now, pool_id, namespace, image_id)

        self.condition.notify()

    def retention_enqueue(self, now, pool_id, namespace, image_id):

        policy = self.retention_policies.find(pool_id, namespace, image_id)
        if not policy:
            return

        schedule_time = policy.next_run(now)
        if schedule_time not in self.retention_queue:
            self.retention_queue[schedule_time] = []
        self.log.debug("schedule retention for image {}/{}/{} at {}".format(
            pool_id, namespace, image_id, schedule_time))
        image_spec = (pool_id, namespace, image_id)
        if image_spec not in self.retention_queue[schedule_time]:
            self.retention_queue[schedule_time].append(image_spec)


    def retention_dequeue(self):
        if not self.retention_queue:
            return None, 1000

        now = datetime.now()
        schedule_time = sorted(self.retention_queue)[0]

        if datetime.strftime(now, "%Y-%m-%d %H:%M:%S") < schedule_time:
            wait_time = (datetime.strptime(schedule_time,
                                           "%Y-%m-%d %H:%M:%S") - now)
            return None, wait_time.total_seconds()

        images = self.retention_queue[schedule_time]
        image = images.pop(0)
        if not images:
            del self.retention_queue[schedule_time]
        return image, 0

    def remove_from_retention_queue(self, pool_id, namespace, image_id):
        empty_slots = []
        for schedule_time, images in self.retention_queue.items():
            if (pool_id, namespace, image_id) in images:
                images.remove((pool_id, namespace, image_id))
                if not images:
                    empty_slots.append(schedule_time)
        for schedule_time in empty_slots:
            del self.retention_queue[schedule_time]

    def add_schedule(self, level_spec, interval, start_time):
        self.log.debug(
            "add_schedule: level_spec={}, interval={}, start_time={}".format(
                level_spec.name, interval, start_time))

        with self.lock:
            self.schedules.add(level_spec, interval, start_time)

        # TODO: optimize to rebuild only affected part of the queue
        self.rebuild_schedule_queue()
        return 0, "", ""

    def remove_schedule(self, level_spec, interval, start_time):
        self.log.debug(
            "remove_schedule: level_spec={}, interval={}, start_time={}".format(
                level_spec.name, interval, start_time))

        with self.lock:
            self.schedules.remove(level_spec, interval, start_time)

        # TODO: optimize to rebuild only affected part of the queue
        self.rebuild_schedule_queue()
        return 0, "", ""

    def list_schedule(self, level_spec):
        self.log.debug("list_schedule: level_spec={}".format(level_spec.name))

        with self.lock:
            result = self.schedules.to_list(level_spec)

        return 0, json.dumps(result, indent=4, sort_keys=True), ""

    def schedule_status(self, level_spec):
        self.log.debug("schedule_status: level_spec={}".format(level_spec.name))

        scheduled_images = []
        with self.lock:
            for schedule_time in sorted(self.schedule_queue):
                for pool_id, namespace, image_id in \
                      self.schedule_queue[schedule_time]:
                    if not level_spec.matches(pool_id, namespace, image_id):
                        continue
                    image_name = self.schedule_images[pool_id][namespace][image_id]
                    scheduled_images.append({
                        'schedule_time' : schedule_time,
                        'image' : image_name
                    })
        return 0, json.dumps({'scheduled_images' : scheduled_images},
                             indent=4, sort_keys=True), ""

    def add_retention_policy(self, level_spec, interval, count):
        self.log.debug(
            "add_retention_policy: level_spec={}, interval={}, count={}".format(
                level_spec.name, interval, count))

        with self.lock:
            self.retention_policies.add(level_spec, interval,
                                        count and int(count) or None)

        # TODO: optimize to rebuild only affected part of the queue
        self.rebuild_retention_queue()
        return 0, "", ""

    def remove_retention_policy(self, level_spec, interval):
        self.log.debug(
            "remove_retention_policy: level_spec={}, interval={}".format(
                level_spec.name, interval))

        with self.lock:
            self.retention_policies.remove(level_spec, interval)

        # TODO: optimize to rebuild only affected part of the queue
        self.rebuild_retention_queue()
        return 0, "", ""

    def list_retention_policy(self, level_spec):
        self.log.debug("list_schedule: level_spec={}".format(level_spec.name))

        with self.lock:
            result = self.retention_policies.to_list(level_spec)

        return 0, json.dumps(result, indent=4, sort_keys=True), ""

    def retention_status(self, level_spec):
        self.log.debug("retention_status: level_spec={}".format(
            level_spec.name))

        retained_images = []
        with self.lock:
            for schedule_time in sorted(self.retention_queue):
                for pool_id, namespace, image_id in \
                      self.retention_queue[schedule_time]:
                    if not level_spec.matches(pool_id, namespace, image_id):
                        continue
                    image_name = self.retention_images[pool_id][namespace][image_id]
                    retained_images.append({
                        'schedule_time' : schedule_time,
                        'image' : image_name
                    })
        return 0, json.dumps({'retained_images' : retained_images},
                                 indent=4, sort_keys=True), ""

    def handle_command(self, inbuf, prefix, cmd):
        level_spec_name = cmd.get('level_spec', "")

        try:
            level_spec = LevelSpec.from_name(self, level_spec_name)
        except ValueError as e:
            return -errno.EINVAL, '', "Invalid level spec {}: {}".format(
                level_spec_name, e)

        if prefix == 'period add':
            return self.add_schedule(level_spec, cmd['interval'],
                                     cmd.get('start_time'))
        elif prefix == 'period remove':
            return self.remove_schedule(level_spec, cmd.get('interval'),
                                        cmd.get('start_time'))
        elif prefix == 'period list':
            return self.list_schedule(level_spec)
        elif prefix == 'period status':
            return self.schedule_status(level_spec)
        elif prefix == 'retention add':
            return self.add_retention_policy(level_spec, cmd['interval'],
                                             cmd.get('count'))
        elif prefix == 'retention remove':
            return self.remove_retention_policy(level_spec, cmd.get('interval'))
        elif prefix == 'retention list':
            return self.list_retention_policy(level_spec)
        elif prefix == 'retention status':
            return self.retention_status(level_spec)

        raise NotImplementedError(cmd['prefix'])
