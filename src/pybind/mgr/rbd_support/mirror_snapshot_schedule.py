import errno
import json
import rados
import rbd
import re
import traceback

from datetime import datetime, timedelta, time
from threading import Condition, Lock, Thread

from .common import get_rbd_pools

SCHEDULE_OID = "rbd_mirror_snapshot_schedule"

SCHEDULE_INTERVAL = "interval"
SCHEDULE_START_TIME = "start_time"


class Interval:

    def __init__(self, minutes):
        self.minutes = minutes

    def __eq__(self, interval):
        return self.minutes == interval.minutes

    def __hash__(self):
        return hash(self.minutes)

    def to_string(self):
        if self.minutes % (60 * 24) == 0:
            interval = int(self.minutes / (60 * 24))
            units = 'd'
        elif self.minutes % 60 == 0:
            interval = int(self.minutes / 60)
            units = 'h'
        else:
            interval = int(self.minutes)
            units = 'm'

        return "{}{}".format(interval, units)

    @classmethod
    def from_string(cls, interval):
        match = re.match(r'^(\d+)(d|h|m)?$', interval)
        if not match:
            raise ValueError("Invalid interval ({})".format(interval))

        minutes = int(match.group(1))
        if match.group(2) == 'd':
            minutes *= 60 * 24
        elif match.group(2) == 'h':
            minutes *= 60

        return Interval(minutes)


class StartTime:

    def __init__(self, hour, minute, tzinfo):
        self.time = time(hour, minute, tzinfo=tzinfo)
        self.minutes = self.time.hour * 60 + self.time.minute
        if self.time.tzinfo:
            self.minutes += int(self.time.utcoffset().seconds / 60)

    def __eq__(self, start_time):
        return self.minutes == start_time.minutes

    def __hash__(self):
        return hash(self.minutes)

    def to_string(self):
        return self.time.isoformat()

    @classmethod
    def from_string(cls, start_time):
        if not start_time:
            return None

        try:
            t = time.fromisoformat(start_time)
        except ValueError as e:
            raise ValueError("Invalid start time {}: {}".format(start_time, e))

        return StartTime(t.hour, t.minute, tzinfo=t.tzinfo)

class LevelSpec:

    def __init__(self, name, id, pool_id, namespace, image_id):
        self.name = name
        self.id = id
        self.pool_id = pool_id
        self.namespace = namespace
        self.image_id = image_id

    def is_global(self):
        return self.pool_id is None

    def get_pool_id(self):
        return self.pool_id

    def matches(self, pool_id, namespace, image_id):
        if self.pool_id and self.pool_id != pool_id:
            return False
        if self.namespace and self.namespace != namespace:
            return False
        if self.image_id and self.image_id != image_id:
            return False
        return True

    @classmethod
    def from_name(cls, handler, name):
        # parse names like:
        # '', 'rbd/', 'rbd/ns/', 'rbd//image', 'rbd/image', 'rbd/ns/image'
        match = re.match(r'^(?:([^/]+)/(?:(?:([^/]*)/|)(?:([^/@]+))?)?)?$',
                         name)
        if not match:
            raise ValueError("failed to parse {}".format(name))

        id = ""
        pool_id = None
        namespace = None
        image_name = None
        image_id = None
        if match.group(1):
            pool_name = match.group(1)
            try:
                pool_id = handler.module.rados.pool_lookup(pool_name)
                if pool_id is None:
                    raise ValueError("pool {} does not exist".format(pool_name))
                pool_id = str(pool_id)
                id += pool_id
                if match.group(2) is not None or match.group(3):
                    id += "/"
                    with handler.module.rados.open_ioctx(pool_name) as ioctx:
                        namespace = match.group(2) or ""
                        if namespace:
                            namespaces = rbd.RBD().namespace_list(ioctx)
                            if namespace not in namespaces:
                                raise ValueError(
                                    "namespace {} does not exist".format(
                                        namespace))
                            id += namespace
                            ioctx.set_namespace(namespace)
                            pool_mode = rbd.RBD().mirror_mode_get(ioctx)
                            if pool_mode != rbd.RBD_MIRROR_MODE_IMAGE:
                                raise ValueError(
                                    "namespace {} is not in mirror image mode".format(
                                        namespace))
                        if match.group(3):
                            image_name = match.group(3)
                            try:
                                with rbd.Image(ioctx, image_name,
                                               read_only=True) as image:
                                    image_id = image.id()
                                    id += "/" + image_id
                                    mode = image.mirror_image_get_mode()
                                    if mode != rbd.RBD_MIRROR_IMAGE_MODE_SNAPSHOT:
                                        raise rbd.InvalidArgument(
                                            "Invalid mirror mode")
                            except rbd.ImageNotFound:
                                raise ValueError("image {} does not exist".format(
                                    image_name))
                            except rbd.InvalidArgument:
                                raise ValueError(
                                    "image {} is not in snapshot mirror mode".format(
                                        image_name))

            except rados.ObjectNotFound:
                raise ValueError("pool {} does not exist".format(pool_name))

        # normalize possible input name like 'rbd//image'
        if not namespace and image_name:
            name = "{}/{}".format(pool_name, image_name)

        return LevelSpec(name, id, pool_id, namespace, image_id)

    @classmethod
    def from_id(cls, handler, id):
        # parse ids like:
        # '', '123', '123/', '123/ns', '123//image_id', '123/ns/image_id'
        match = re.match(r'^(?:(\d+)(?:/([^/]*)(?:/([^/@]+))?)?)?$', id)
        if not match:
            raise ValueError("failed to parse: {}".format(id))

        name = ""
        pool_id = None
        namespace = None
        image_id = None
        if match.group(1):
            pool_id = match.group(1)
            try:
                pool_name = handler.module.rados.pool_reverse_lookup(
                    int(pool_id))
                if pool_name is None:
                    raise ValueError("pool {} does not exist".format(pool_name))
                name += pool_name + "/"
                if match.group(2) is not None or match.group(3):
                    with handler.module.rados.open_ioctx(pool_name) as ioctx:
                        namespace = match.group(2) or ""
                        if namespace:
                            namespaces = rbd.RBD().namespace_list(ioctx)
                            if namespace not in namespaces:
                                raise ValueError(
                                    "namespace {} does not exist".format(
                                        namespace))
                            name += namespace + "/"
                            ioctx.set_namespace(namespace)
                            pool_mode = rbd.RBD().mirror_mode_get(ioctx)
                            if pool_mode != rbd.RBD_MIRROR_MODE_IMAGE:
                                raise ValueError(
                                    "namespace {} is not in mirror image mode".format(
                                        namespace))
                        elif not match.group(3):
                            name += "/"
                        if match.group(3):
                            image_id = match.group(3)
                            try:
                                with rbd.Image(ioctx, image_id=image_id,
                                               read_only=True) as image:
                                    image_name = image.get_name()
                                    name += image_name
                                    mode = image.mirror_image_get_mode()
                                    if mode != rbd.RBD_MIRROR_IMAGE_MODE_SNAPSHOT:
                                        raise rbd.InvalidArgument(
                                            "Invalid mirror mode")
                            except rbd.ImageNotFound:
                                raise ValueError("image {} does not exist".format(
                                    image_id))
                            except rbd.InvalidArgument:
                                raise ValueError(
                                    "image {} is not in snapshot mirror mode".format(
                                        image_id))

            except rados.ObjectNotFound:
                raise ValueError("pool {} does not exist".format(pool_id))

        return LevelSpec(name, id, pool_id, namespace, image_id)


class Schedule:

    def __init__(self, name):
        self.name = name
        self.items = set()

    def __len__(self):
        return len(self.items)

    def add(self, interval, start_time=None):
        self.items.add((interval, start_time))

    def remove(self, interval, start_time=None):
        self.items.discard((interval, start_time))

    def to_list(self):
        return [{SCHEDULE_INTERVAL: i[0].to_string(),
                 SCHEDULE_START_TIME: i[1] and i[1].to_string() or None}
                for i in self.items]

    def to_json(self):
        return json.dumps(self.to_list(), indent=4, sort_keys=True)

    @classmethod
    def from_json(cls, name, val):
        try:
            items = json.loads(val)
            schedule = Schedule(name)
            for item in items:
                interval = Interval.from_string(item[SCHEDULE_INTERVAL])
                start_time = item[SCHEDULE_START_TIME] and \
                    StartTime.from_string(item[SCHEDULE_START_TIME]) or None
                schedule.add(interval, start_time)
            return schedule
        except json.JSONDecodeError as e:
            raise ValueError("Invalid JSON ({})".format(str(e)))
        except KeyError as e:
            raise ValueError(
                "Invalid schedule format (missing key {})".format(str(e)))
        except TypeError as e:
            raise ValueError("Invalid schedule format ({})".format(str(e)))


class MirrorSnapshotScheduleHandler:
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


    @classmethod
    def format_image_spec(cls, image_spec):
        image = image_spec[2]
        if image_spec[1]:
            image = "{}/{}".format(image_spec[1], image)
        if image_spec[0]:
            image = "{}/{}".format(image_spec[0], image)
        return image

    def init_schedule_queue(self):
        self.queue = {}
        self.images = {}
        self.refresh_images()
        self.log.debug("scheduler queue is initialized")

    def load_schedules(self):
        self.log.info("MirrorSnapshotScheduleHandler: load_schedules")

        schedules = {}
        schedule_cfg = self.module.get_localized_module_option(
            'mirror_snapshot_schedule', '')
        if schedule_cfg:
            try:
                schedule = Schedule.from_json('', schedule_cfg)
                schedules[''] = schedule
            except ValueError:
                self.log.error("Failed to decode configured schedule {}".format(
                    schedule_cfg))

        for pool_id, pool_name in get_rbd_pools(self.module).items():
            with self.module.rados.open_ioctx2(int(pool_id)) as ioctx:
                self.load_pool_schedules(ioctx, schedules)

        with self.lock:
            self.schedules = schedules

    def load_pool_schedules(self, ioctx, schedules):
        pool_id = ioctx.get_pool_id()
        pool_name = ioctx.get_pool_name()
        stale_keys = ()
        start_after = ''
        try:
            while True:
                with rados.ReadOpCtx() as read_op:
                    self.log.info("load_schedules: {}, start_after={}".format(
                        pool_name, start_after))
                    it, ret = ioctx.get_omap_vals(read_op, start_after, "", 128)
                    ioctx.operate_read_op(read_op, SCHEDULE_OID)

                    it = list(it)
                    for k, v in it:
                        start_after = k
                        v = v.decode()
                        self.log.info("load_schedule: {} {}".format(k, v))

                        try:
                            try:
                                level_spec = LevelSpec.from_id(self, k)
                            except ValueError:
                                self.log.debug(
                                    "Stail schedule key {} in pool".format(
                                        k, pool_name))
                                stale_keys += (k,)
                                continue

                            schedule = Schedule.from_json(level_spec.name, v)
                            schedules[k] = schedule
                        except ValueError:
                            self.log.error(
                                "Failed to decode schedule: pool={}, {} {}".format(
                                    pool_name, k, v))

                    if not it:
                        break

        except StopIteration:
            pass
        except rados.ObjectNotFound:
            # rbd_mirror_snapshot_schedule DNE
            pass

        if stale_keys:
            with rados.WriteOpCtx() as write_op:
                ioctx.remove_omap_keys(write_op, stale_keys)
                ioctx.operate_write_op(write_op, SCHEDULE_OID)

    def refresh_images(self):
        if (datetime.now() - self.last_refresh_images).seconds < 60:
            return

        self.log.debug("MirrorSnapshotScheduleHandler: refresh_images")

        self.load_schedules()

        with self.lock:
            if not self.schedules:
                self.images = {}
                self.queue = {}
                self.last_refresh_images = datetime.now()
                return

        images = {}

        for pool_id, pool_name in get_rbd_pools(self.module).items():
            with self.module.rados.open_ioctx2(int(pool_id)) as ioctx:
                self.load_pool_images(ioctx, images)

        with self.lock:
            self.refresh_queue(images)
            self.images = images

        self.last_refresh_images = datetime.now()

    def load_pool_images(self, ioctx, images):
        pool_id = str(ioctx.get_pool_id())
        pool_name = ioctx.get_pool_name()
        images[pool_id] = {}

        try:
            namespaces = [''] + rbd.RBD().namespace_list(ioctx)
            for namespace in namespaces:
                images[pool_id][namespace] = {}
                ioctx.set_namespace(namespace)
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
                    self.log.debug("Adding image {}".format(name))
                    images[pool_id][namespace][image_id] = name
        except Exception as e:
            self.log.error("exception when scanning pool {}: {}".format(
                pool_name, e))
            pass

    def find_schedule(self, pool_id, namespace, image_id):
        levels = [None, pool_id, namespace, image_id]
        while levels:
            level_spec_id = "/".join(levels[1:])
            if level_spec_id in self.schedules:
                return self.schedules[level_spec_id]
            del levels[-1]
        return None

    def calc_schedule_time(self, schedule, now):
        schedule_time = None
        for item in schedule.items:
            period = timedelta(minutes=item[0].minutes)
            start_time = datetime(1970, 1, 1)
            if item[1]:
                start_time += timedelta(minutes=item[1].minutes)
            time = start_time + \
                (int((now - start_time) / period) + 1) * period
            if schedule_time is None or time < schedule_time:
                schedule_time = time
        return datetime.strftime(schedule_time, "%Y-%m-%d %H:%M:00")

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

        schedule = self.find_schedule(pool_id, namespace, image_id)
        if not schedule:
            return

        schedule_time = self.calc_schedule_time(schedule, now)
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
        image = images[0]
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

    def save_schedule(self, level_spec, schedule):
        if level_spec.is_global():
            schedule_cfg = schedule and schedule.to_json() or ''
            self.module.set_localized_module_option('mirror_snapshot_schedule',
                                                    schedule_cfg)
            return

        pool_id = level_spec.get_pool_id()
        with self.module.rados.open_ioctx2(int(pool_id)) as ioctx:
            with rados.WriteOpCtx() as write_op:
                if schedule:
                    ioctx.set_omap(write_op, (level_spec.id, ),
                                   (schedule.to_json(), ))
                else:
                    ioctx.remove_omap_keys(write_op, (level_spec.id, ))
                ioctx.operate_write_op(write_op, SCHEDULE_OID)


    def add_schedule(self, level_spec_name, interval, start_time):
        self.log.debug(
            "add_schedule: level_spec={}, interval={}, start_time={}".format(
                level_spec_name, interval, start_time))
        try:
            level_spec = LevelSpec.from_name(self, level_spec_name)
        except ValueError as e:
            return -errno.EINVAL, '', "Invalid level spec {}: {}".format(
                level_spec_name, e)

        with self.lock:
            schedule = self.schedules.get(level_spec.id, Schedule(level_spec_name))
            schedule.add(Interval.from_string(interval),
                         StartTime.from_string(start_time))
            self.save_schedule(level_spec, schedule)
            self.schedules[level_spec.id] = schedule

        # TODO: optimize to rebuild only affected part of the queue
        self.rebuild_queue()
        return 0, "", ""

    def remove_schedule(self, level_spec_name, interval, start_time):
        try:
            level_spec = LevelSpec.from_name(self, level_spec_name)
        except ValueError as e:
            return -errno.EINVAL, '', "Invalid level spec {}: {}".format(
                level_spec_name, e)

        with self.lock:
            schedule = self.schedules.pop(level_spec.id, None)
            if not schedule:
                return -errno.ENOENT, '', "No schedule for {}".format(level_spec_name)

            if interval is None:
                schedule = None
            else:
                schedule.remove(Interval.from_string(interval),
                                StartTime.from_string(start_time))
            if schedule:
                self.schedules[level_spec.id] = schedule
            self.save_schedule(level_spec, schedule)

        # TODO: optimize to rebuild only affected part of the queue
        self.rebuild_queue()
        return 0, "", ""

    def list(self, level_spec_name):
        self.log.debug("list: level_spec={}".format(level_spec_name))
        try:
            level_spec = LevelSpec.from_name(self, level_spec_name)
        except ValueError as e:
            return -errno.EINVAL, '', "Invalid level spec {}: {}".format(
                level_spec_name, e)
        with self.lock:
            schedule = self.schedules.get(level_spec.id)
            if schedule is None:
                return -errno.ENOENT, '', "No schedule for {}".format(
                    level_spec_name)
            return 0, schedule.to_json(), ""

    def dump(self):
        self.log.debug("dump")
        result = {}
        with self.lock:
            for level_spec_id, schedule in self.schedules.items():
                result[level_spec_id] = {
                    'name' : schedule.name,
                    'schedule' : schedule.to_list(),
                }
        return 0, json.dumps(result, indent=4, sort_keys=True), ""

    def status(self, level_spec_name):
        self.log.debug("status: level_spec={}".format(level_spec_name))

        if not level_spec_name:
            level_spec_name = ""

        try:
            level_spec = LevelSpec.from_name(self, level_spec_name)
        except ValueError as e:
            return -errno.EINVAL, '', "Invalid level spec {}: {}".format(
                level_spec_name, e)

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
        if prefix == 'add':
            return self.add_schedule(cmd['level_spec'], cmd['interval'],
                                     cmd.get('start_time'))
        elif prefix == 'remove':
            return self.remove_schedule(cmd['level_spec'], cmd.get('interval'),
                                        cmd.get('start_time'))
        elif prefix == 'list':
            return self.list(cmd['level_spec'])
        elif prefix == 'dump':
            return self.dump()
        elif prefix == 'status':
            return self.status(cmd.get('level_spec', None))

        raise NotImplementedError(cmd['prefix'])
