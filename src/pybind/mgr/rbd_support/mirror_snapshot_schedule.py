import errno
import json
import rados
import rbd
import re
import traceback

from datetime import datetime
from threading import Condition, Lock, Thread

from .common import get_rbd_pools
from .schedule import LevelSpec, Interval, StartTime, Schedule, Schedules

MIRRORING_OID = "rbd_mirroring"

def namespace_validator(ioctx):
    mode = rbd.RBD().mirror_mode_get(ioctx)
    if mode != rbd.RBD_MIRROR_MODE_IMAGE:
        raise ValueError("namespace {} is not in mirror image mode".format(
            ioctx.get_namespace()))

def image_validator(image):
    mode = image.mirror_image_get_mode()
    if mode != rbd.RBD_MIRROR_IMAGE_MODE_SNAPSHOT:
        raise rbd.InvalidArgument("Invalid mirror image mode")

class Watchers:

    lock = Lock()

    def __init__(self, handler):
        self.rados = handler.module.rados
        self.log = handler.log
        self.watchers = {}
        self.updated = {}
        self.error = {}
        self.epoch = {}

    def __del__(self):
        self.unregister_all()

    def _clean_watcher(self, pool_id, namespace, watch_id):
        assert self.lock.locked()

        del self.watchers[pool_id, namespace]
        self.updated.pop(watch_id, None)
        self.error.pop(watch_id, None)
        self.epoch.pop(watch_id, None)

    def check(self, pool_id, namespace, epoch):
        error = None
        with self.lock:
            watch = self.watchers.get((pool_id, namespace))
            if watch is not None:
                error = self.error.get(watch.get_id())
                if not error:
                    updated = self.updated[watch.get_id()]
                    self.updated[watch.get_id()] = False
                    self.epoch[watch.get_id()] = epoch
                    return updated
        if error:
            self.unregister(pool_id, namespace)

        if self.register(pool_id, namespace):
            return self.check(pool_id, namespace, epoch)
        else:
            return True

    def register(self, pool_id, namespace):

        def callback(notify_id, notifier_id, watch_id, data):
            self.log.debug("watcher {}: got notify {} from {}".format(
                watch_id, notify_id, notifier_id))

            with self.lock:
                self.updated[watch_id] = True

        def error_callback(watch_id, error):
            self.log.debug("watcher {}: got errror {}".format(
                watch_id, error))

            with self.lock:
                self.error[watch_id] = error

        try:
            ioctx = self.rados.open_ioctx2(int(pool_id))
            ioctx.set_namespace(namespace)
            watch = ioctx.watch(MIRRORING_OID, callback, error_callback)
        except rados.ObjectNotFound:
            self.log.debug(
                "{}/{}/{} watcher not registered: object not found".format(
                    pool_id, namespace, MIRRORING_OID))
            return False

        self.log.debug("{}/{}/{} watcher {} registered".format(
            pool_id, namespace, MIRRORING_OID, watch.get_id()))

        with self.lock:
            self.watchers[pool_id, namespace] = watch
            self.updated[watch.get_id()] = True
        return True

    def unregister(self, pool_id, namespace):

        with self.lock:
            watch = self.watchers[pool_id, namespace]

        watch_id = watch.get_id()

        try:
            watch.close()

            self.log.debug("{}/{}/{} watcher {} unregistered".format(
                pool_id, namespace, MIRRORING_OID, watch_id))

        except rados.Error as e:
            self.log.debug(
                "exception when unregistering {}/{} watcher: {}".format(
                    pool_id, namespace, e))

        with self.lock:
            self._clean_watcher(pool_id, namespace, watch_id)

    def unregister_all(self):
        with self.lock:
            watchers = list(self.watchers)

        for pool_id, namespace in watchers:
            self.unregister(pool_id, namespace)

    def unregister_stale(self, current_epoch):
        with self.lock:
            watchers = list(self.watchers)

        for pool_id, namespace in watchers:
            with self.lock:
                watch = self.watchers[pool_id, namespace]
                if self.epoch.get(watch.get_id()) == current_epoch:
                    continue

            self.log.debug("{}/{}/{} watcher {} stale".format(
                pool_id, namespace, MIRRORING_OID, watch_id))

            self.unregister(pool_id, namespace)


class MirrorSnapshotScheduleHandler:
    MODULE_OPTION_NAME = "mirror_snapshot_schedule"
    SCHEDULE_OID = "rbd_mirror_snapshot_schedule"

    lock = Lock()
    condition = Condition(lock)
    thread = None

    def __init__(self, module):
        self.module = module
        self.log = module.log
        self.last_refresh_images = datetime(1970, 1, 1)

        self.init_schedule_queue()

        self.thread = Thread(target=self.run)
        self.thread.start()

    def _cleanup(self):
        self.watchers.unregister_all()

    def run(self):
        try:
            self.log.info("MirrorSnapshotScheduleHandler: starting")
            while True:
                self.refresh_images()
                with self.lock:
                    (image_spec, wait_time) = self.dequeue()
                    if not image_spec:
                        self.condition.wait(min(wait_time, 60))
                        continue
                pool_id, namespace, image_id = image_spec
                self.create_snapshot(pool_id, namespace, image_id)
                with self.lock:
                    self.enqueue(datetime.now(), pool_id, namespace, image_id)

        except Exception as ex:
            self.log.fatal("Fatal runtime error: {}\n{}".format(
                ex, traceback.format_exc()))

    def create_snapshot(self, pool_id, namespace, image_id):
        try:
            with self.module.rados.open_ioctx2(int(pool_id)) as ioctx:
                ioctx.set_namespace(namespace)
                with rbd.Image(ioctx, image_id=image_id) as image:
                    mode = image.mirror_image_get_mode()
                    if mode != rbd.RBD_MIRROR_IMAGE_MODE_SNAPSHOT:
                        return
                    info = image.mirror_image_get_info()
                    if info['state'] != rbd.RBD_MIRROR_IMAGE_ENABLED or \
                       not info['primary']:
                        return
                    snap_id = image.mirror_image_create_snapshot()
                    self.log.debug(
                        "create_snapshot: {}/{}/{}: snap_id={}".format(
                            ioctx.get_pool_name(), namespace, image.get_name(),
                            snap_id))
        except Exception as e:
            self.log.error(
                "exception when creating snapshot for {}/{}/{}: {}".format(
                    pool_id, namespace, image_id, e))


    def init_schedule_queue(self):
        self.queue = {}
        self.images = {}
        self.watchers = Watchers(self)
        self.refresh_images()
        self.log.debug("scheduler queue is initialized")

    def load_schedules(self):
        self.log.info("MirrorSnapshotScheduleHandler: load_schedules")

        schedules = Schedules(self)
        schedules.load(namespace_validator, image_validator)
        with self.lock:
            self.schedules = schedules

    def refresh_images(self):
        if (datetime.now() - self.last_refresh_images).seconds < 60:
            return

        self.log.debug("MirrorSnapshotScheduleHandler: refresh_images")

        self.load_schedules()

        with self.lock:
            if not self.schedules:
                self.watchers.unregister_all()
                self.images = {}
                self.queue = {}
                self.last_refresh_images = datetime.now()
                return

        epoch = int(datetime.now().strftime('%s'))
        images = {}

        for pool_id, pool_name in get_rbd_pools(self.module).items():
            if not self.schedules.intersects(
                    LevelSpec.from_pool_spec(pool_id, pool_name)):
                continue
            with self.module.rados.open_ioctx2(int(pool_id)) as ioctx:
                self.load_pool_images(ioctx, epoch, images)

        with self.lock:
            self.refresh_queue(images)
            self.images = images

        self.watchers.unregister_stale(epoch)
        self.last_refresh_images = datetime.now()

    def load_pool_images(self, ioctx, epoch, images):
        pool_id = str(ioctx.get_pool_id())
        pool_name = ioctx.get_pool_name()
        images[pool_id] = {}

        self.log.debug("load_pool_images: pool={}".format(pool_name))

        try:
            namespaces = [''] + rbd.RBD().namespace_list(ioctx)
            for namespace in namespaces:
                if not self.schedules.intersects(
                        LevelSpec.from_pool_spec(pool_id, pool_name, namespace)):
                    continue
                self.log.debug("load_pool_images: pool={}, namespace={}".format(
                    pool_name, namespace))
                images[pool_id][namespace] = {}
                ioctx.set_namespace(namespace)
                updated = self.watchers.check(pool_id, namespace, epoch)
                if not updated:
                    self.log.debug("load_pool_images: {}/{} not updated".format(
                        pool_name, namespace))
                    with self.lock:
                        images[pool_id][namespace] = \
                            self.images[pool_id][namespace]
                    continue
                mirror_images = dict(rbd.RBD().mirror_image_info_list(
                    ioctx, rbd.RBD_MIRROR_IMAGE_MODE_SNAPSHOT))
                if not mirror_images:
                    continue
                image_names = dict(
                    [(x['id'], x['name']) for x in filter(
                        lambda x: x['id'] in mirror_images,
                        rbd.RBD().list2(ioctx))])
                for image_id in mirror_images:
                    image_name = image_names.get(image_id)
                    if not image_name:
                        continue
                    if namespace:
                        name = "{}/{}/{}".format(pool_name, namespace,
                                                 image_name)
                    else:
                        name = "{}/{}".format(pool_name, image_name)
                    self.log.debug(
                        "load_pool_images: adding image {}".format(name))
                    images[pool_id][namespace][image_id] = name
        except Exception as e:
            self.log.error(
                "load_pool_images: exception when scanning pool {}: {}".format(
                    pool_name, e))

    def rebuild_queue(self):
        with self.lock:
            now = datetime.now()

            # don't remove from queue "due" images
            now_string = datetime.strftime(now, "%Y-%m-%d %H:%M:00")

            for schedule_time in list(self.queue):
                if schedule_time > now_string:
                    del self.queue[schedule_time]

            if not self.schedules:
                return

            for pool_id in self.images:
                for namespace in self.images[pool_id]:
                    for image_id in self.images[pool_id][namespace]:
                        self.enqueue(now, pool_id, namespace, image_id)

            self.condition.notify()

    def refresh_queue(self, current_images):
        now = datetime.now()

        for pool_id in self.images:
            for namespace in self.images[pool_id]:
                for image_id in self.images[pool_id][namespace]:
                    if pool_id not in current_images or \
                       namespace not in current_images[pool_id] or \
                       image_id not in current_images[pool_id][namespace]:
                        self.remove_from_queue(pool_id, namespace, image_id)

        for pool_id in current_images:
            for namespace in current_images[pool_id]:
                for image_id in current_images[pool_id][namespace]:
                    if pool_id not in self.images or \
                       namespace not in self.images[pool_id] or \
                       image_id not in self.images[pool_id][namespace]:
                        self.enqueue(now, pool_id, namespace, image_id)

        self.condition.notify()

    def enqueue(self, now, pool_id, namespace, image_id):

        schedule = self.schedules.find(pool_id, namespace, image_id)
        if not schedule:
            return

        schedule_time = schedule.next_run(now)
        if schedule_time not in self.queue:
            self.queue[schedule_time] = []
        self.log.debug("schedule image {}/{}/{} at {}".format(
            pool_id, namespace, image_id, schedule_time))
        image_spec = (pool_id, namespace, image_id)
        if image_spec not in self.queue[schedule_time]:
            self.queue[schedule_time].append((pool_id, namespace, image_id))

    def dequeue(self):
        if not self.queue:
            return None, 1000

        now = datetime.now()
        schedule_time = sorted(self.queue)[0]

        if datetime.strftime(now, "%Y-%m-%d %H:%M:%S") < schedule_time:
            wait_time = (datetime.strptime(schedule_time,
                                           "%Y-%m-%d %H:%M:%S") - now)
            return None, wait_time.total_seconds()

        images = self.queue[schedule_time]
        image = images.pop(0)
        if not images:
            del self.queue[schedule_time]
        return image, 0

    def remove_from_queue(self, pool_id, namespace, image_id):
        empty_slots = []
        for schedule_time, images in self.queue.items():
            if (pool_id, namespace, image_id) in images:
                images.remove((pool_id, namespace, image_id))
                if not images:
                    empty_slots.append(schedule_time)
        for schedule_time in empty_slots:
            del self.queue[schedule_time]

    def add_schedule(self, level_spec, interval, start_time):
        self.log.debug(
            "add_schedule: level_spec={}, interval={}, start_time={}".format(
                level_spec.name, interval, start_time))

        with self.lock:
            self.schedules.add(level_spec, interval, start_time)

        # TODO: optimize to rebuild only affected part of the queue
        self.rebuild_queue()
        return 0, "", ""

    def remove_schedule(self, level_spec, interval, start_time):
        self.log.debug(
            "remove_schedule: level_spec={}, interval={}, start_time={}".format(
                level_spec.name, interval, start_time))

        with self.lock:
            self.schedules.remove(level_spec, interval, start_time)

        # TODO: optimize to rebuild only affected part of the queue
        self.rebuild_queue()
        return 0, "", ""

    def list(self, level_spec):
        self.log.debug("list: level_spec={}".format(level_spec.name))

        with self.lock:
            result = self.schedules.to_list(level_spec)

        return 0, json.dumps(result, indent=4, sort_keys=True), ""

    def status(self, level_spec):
        self.log.debug("status: level_spec={}".format(level_spec.name))

        scheduled_images = []
        with self.lock:
            for schedule_time in sorted(self.queue):
                for pool_id, namespace, image_id in self.queue[schedule_time]:
                    if not level_spec.matches(pool_id, namespace, image_id):
                        continue
                    image_name = self.images[pool_id][namespace][image_id]
                    scheduled_images.append({
                        'schedule_time' : schedule_time,
                        'image' : image_name
                    })
        return 0, json.dumps({'scheduled_images' : scheduled_images},
                             indent=4, sort_keys=True), ""

    def handle_command(self, inbuf, prefix, cmd):
        level_spec_name = cmd.get('level_spec', "")

        try:
            level_spec = LevelSpec.from_name(self, level_spec_name,
                                             namespace_validator,
                                             image_validator)
        except ValueError as e:
            return -errno.EINVAL, '', "Invalid level spec {}: {}".format(
                level_spec_name, e)

        if prefix == 'add':
            return self.add_schedule(level_spec, cmd['interval'],
                                     cmd.get('start_time'))
        elif prefix == 'remove':
            return self.remove_schedule(level_spec, cmd.get('interval'),
                                        cmd.get('start_time'))
        elif prefix == 'list':
            return self.list(level_spec)
        elif prefix == 'status':
            return self.status(level_spec)

        raise NotImplementedError(cmd['prefix'])
