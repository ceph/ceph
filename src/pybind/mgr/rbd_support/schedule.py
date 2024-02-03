import datetime
import json
import rados
import rbd
import re

from dateutil.parser import parse
from typing import cast, Any, Callable, Dict, List, Optional, Set, Tuple, TYPE_CHECKING

from .common import get_rbd_pools
if TYPE_CHECKING:
    from .module import Module

SCHEDULE_INTERVAL = "interval"
SCHEDULE_START_TIME = "start_time"


class LevelSpec:

    def __init__(self,
                 name: str,
                 id: str,
                 pool_id: Optional[str],
                 namespace: Optional[str],
                 image_id: Optional[str] = None) -> None:
        self.name = name
        self.id = id
        self.pool_id = pool_id
        self.namespace = namespace
        self.image_id = image_id

    def __eq__(self, level_spec: Any) -> bool:
        return self.id == level_spec.id

    def is_child_of(self, level_spec: 'LevelSpec') -> bool:
        if level_spec.is_global():
            return not self.is_global()
        if level_spec.pool_id != self.pool_id:
            return False
        if level_spec.namespace is None:
            return self.namespace is not None
        if level_spec.namespace != self.namespace:
            return False
        if level_spec.image_id is None:
            return self.image_id is not None
        return False

    def is_global(self) -> bool:
        return self.pool_id is None

    def get_pool_id(self) -> Optional[str]:
        return self.pool_id

    def matches(self,
                pool_id: str,
                namespace: str,
                image_id: Optional[str] = None) -> bool:
        if self.pool_id and self.pool_id != pool_id:
            return False
        if self.namespace and self.namespace != namespace:
            return False
        if self.image_id and self.image_id != image_id:
            return False
        return True

    def intersects(self, level_spec: 'LevelSpec') -> bool:
        if self.pool_id is None or level_spec.pool_id is None:
            return True
        if self.pool_id != level_spec.pool_id:
            return False
        if self.namespace is None or level_spec.namespace is None:
            return True
        if self.namespace != level_spec.namespace:
            return False
        if self.image_id is None or level_spec.image_id is None:
            return True
        if self.image_id != level_spec.image_id:
            return False
        return True

    @classmethod
    def make_global(cls) -> 'LevelSpec':
        return LevelSpec("", "", None, None, None)

    @classmethod
    def from_pool_spec(cls,
                       pool_id: int,
                       pool_name: str,
                       namespace: Optional[str] = None) -> 'LevelSpec':
        if namespace is None:
            id = "{}".format(pool_id)
            name = "{}/".format(pool_name)
        else:
            id = "{}/{}".format(pool_id, namespace)
            name = "{}/{}/".format(pool_name, namespace)
        return LevelSpec(name, id, str(pool_id), namespace, None)

    @classmethod
    def from_name(cls,
                  module: 'Module',
                  name: str,
                  namespace_validator: Optional[Callable] = None,
                  image_validator: Optional[Callable] = None,
                  allow_image_level: bool = True) -> 'LevelSpec':
        # parse names like:
        # '', 'rbd/', 'rbd/ns/', 'rbd//image', 'rbd/image', 'rbd/ns/image'
        match = re.match(r'^(?:([^/]+)/(?:(?:([^/]*)/|)(?:([^/@]+))?)?)?$',
                         name)
        if not match:
            raise ValueError("failed to parse {}".format(name))
        if match.group(3) and not allow_image_level:
            raise ValueError(
                "invalid name {}: image level is not allowed".format(name))

        id = ""
        pool_id = None
        namespace = None
        image_name = None
        image_id = None
        if match.group(1):
            pool_name = match.group(1)
            try:
                pool_id = module.rados.pool_lookup(pool_name)
                if pool_id is None:
                    raise ValueError("pool {} does not exist".format(pool_name))
                if pool_id not in get_rbd_pools(module):
                    raise ValueError("{} is not an RBD pool".format(pool_name))
                pool_id = str(pool_id)
                id += pool_id
                if match.group(2) is not None or match.group(3):
                    id += "/"
                    with module.rados.open_ioctx(pool_name) as ioctx:
                        namespace = match.group(2) or ""
                        if namespace:
                            namespaces = rbd.RBD().namespace_list(ioctx)
                            if namespace not in namespaces:
                                raise ValueError(
                                    "namespace {} does not exist".format(
                                        namespace))
                            id += namespace
                            ioctx.set_namespace(namespace)
                            if namespace_validator:
                                namespace_validator(ioctx)
                        if match.group(3):
                            image_name = match.group(3)
                            try:
                                with rbd.Image(ioctx, image_name,
                                               read_only=True) as image:
                                    image_id = image.id()
                                    id += "/" + image_id
                                    if image_validator:
                                        image_validator(image)
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
    def from_id(cls,
                handler: Any,
                id: str,
                namespace_validator: Optional[Callable] = None,
                image_validator: Optional[Callable] = None) -> 'LevelSpec':
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
                            if namespace_validator:
                                ioctx.set_namespace(namespace)
                        elif not match.group(3):
                            name += "/"
                        if match.group(3):
                            image_id = match.group(3)
                            try:
                                with rbd.Image(ioctx, image_id=image_id,
                                               read_only=True) as image:
                                    image_name = image.get_name()
                                    name += image_name
                                    if image_validator:
                                        image_validator(image)
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


class Interval:

    def __init__(self, minutes: int) -> None:
        self.minutes = minutes

    def __eq__(self, interval: Any) -> bool:
        return self.minutes == interval.minutes

    def __hash__(self) -> int:
        return hash(self.minutes)

    def to_string(self) -> str:
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
    def from_string(cls, interval: str) -> 'Interval':
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

    def __init__(self,
                 hour: int,
                 minute: int,
                 tzinfo: Optional[datetime.tzinfo]) -> None:
        self.time = datetime.time(hour, minute, tzinfo=tzinfo)
        self.minutes = self.time.hour * 60 + self.time.minute
        if self.time.tzinfo:
            utcoffset = cast(datetime.timedelta, self.time.utcoffset())
            self.minutes += int(utcoffset.seconds / 60)

    def __eq__(self, start_time: Any) -> bool:
        return self.minutes == start_time.minutes

    def __hash__(self) -> int:
        return hash(self.minutes)

    def to_string(self) -> str:
        return self.time.isoformat()

    @classmethod
    def from_string(cls, start_time: Optional[str]) -> Optional['StartTime']:
        if not start_time:
            return None

        try:
            t = parse(start_time).timetz()
        except ValueError as e:
            raise ValueError("Invalid start time {}: {}".format(start_time, e))

        return StartTime(t.hour, t.minute, tzinfo=t.tzinfo)


class Schedule:

    def __init__(self, name: str) -> None:
        self.name = name
        self.items: Set[Tuple[Interval, Optional[StartTime]]] = set()

    def __len__(self) -> int:
        return len(self.items)

    def add(self,
            interval: Interval,
            start_time: Optional[StartTime] = None) -> None:
        self.items.add((interval, start_time))

    def remove(self,
               interval: Interval,
               start_time: Optional[StartTime] = None) -> None:
        self.items.discard((interval, start_time))

    def next_run(self, now: datetime.datetime) -> str:
        schedule_time = None
        for interval, opt_start in self.items:
            period = datetime.timedelta(minutes=interval.minutes)
            start_time = datetime.datetime(1970, 1, 1)
            if opt_start:
                start = cast(StartTime, opt_start)
                start_time += datetime.timedelta(minutes=start.minutes)
            time = start_time + \
                (int((now - start_time) / period) + 1) * period
            if schedule_time is None or time < schedule_time:
                schedule_time = time
        if schedule_time is None:
            raise ValueError('no items is added')
        return datetime.datetime.strftime(schedule_time, "%Y-%m-%d %H:%M:00")

    def to_list(self) -> List[Dict[str, Optional[str]]]:
        def item_to_dict(interval: Interval,
                         start_time: Optional[StartTime]) -> Dict[str, Optional[str]]:
            if start_time:
                schedule_start_time: Optional[str] = start_time.to_string()
            else:
                schedule_start_time = None
            return {SCHEDULE_INTERVAL: interval.to_string(),
                    SCHEDULE_START_TIME: schedule_start_time}
        return [item_to_dict(interval, start_time)
                for interval, start_time in self.items]

    def to_json(self) -> str:
        return json.dumps(self.to_list(), indent=4, sort_keys=True)

    @classmethod
    def from_json(cls, name: str, val: str) -> 'Schedule':
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


class Schedules:

    def __init__(self, handler: Any) -> None:
        self.handler = handler
        self.level_specs: Dict[str, LevelSpec] = {}
        self.schedules: Dict[str, Schedule] = {}

        # Previous versions incorrectly stored the global config in
        # the localized module option. Check the config is here and fix it.
        schedule_cfg = self.handler.module.get_module_option(
            self.handler.MODULE_OPTION_NAME, '')
        if not schedule_cfg:
            schedule_cfg = self.handler.module.get_localized_module_option(
                self.handler.MODULE_OPTION_NAME, '')
            if schedule_cfg:
                self.handler.module.set_module_option(
                    self.handler.MODULE_OPTION_NAME, schedule_cfg)
        self.handler.module.set_localized_module_option(
            self.handler.MODULE_OPTION_NAME, None)

    def __len__(self) -> int:
        return len(self.schedules)

    def load(self,
             namespace_validator: Optional[Callable] = None,
             image_validator: Optional[Callable] = None) -> None:
        self.level_specs = {}
        self.schedules = {}

        schedule_cfg = self.handler.module.get_module_option(
            self.handler.MODULE_OPTION_NAME, '')
        if schedule_cfg:
            try:
                level_spec = LevelSpec.make_global()
                self.level_specs[level_spec.id] = level_spec
                schedule = Schedule.from_json(level_spec.name, schedule_cfg)
                self.schedules[level_spec.id] = schedule
            except ValueError:
                self.handler.log.error(
                    "Failed to decode configured schedule {}".format(
                        schedule_cfg))

        for pool_id, pool_name in get_rbd_pools(self.handler.module).items():
            try:
                with self.handler.module.rados.open_ioctx2(int(pool_id)) as ioctx:
                    self.load_from_pool(ioctx, namespace_validator,
                                        image_validator)
            except rados.ConnectionShutdown:
                raise
            except rados.Error as e:
                self.handler.log.error(
                    "Failed to load schedules for pool {}: {}".format(
                        pool_name, e))

    def load_from_pool(self,
                       ioctx: rados.Ioctx,
                       namespace_validator: Optional[Callable],
                       image_validator: Optional[Callable]) -> None:
        pool_name = ioctx.get_pool_name()
        stale_keys = []
        start_after = ''
        try:
            while True:
                with rados.ReadOpCtx() as read_op:
                    self.handler.log.info(
                        "load_schedules: {}, start_after={}".format(
                            pool_name, start_after))
                    it, ret = ioctx.get_omap_vals(read_op, start_after, "", 128)
                    ioctx.operate_read_op(read_op, self.handler.SCHEDULE_OID)

                    it = list(it)
                    for k, v in it:
                        start_after = k
                        v = v.decode()
                        self.handler.log.info(
                            "load_schedule: {} {}".format(k, v))
                        try:
                            try:
                                level_spec = LevelSpec.from_id(
                                    self.handler, k, namespace_validator,
                                    image_validator)
                            except ValueError:
                                self.handler.log.debug(
                                    "Stale schedule key %s in pool %s",
                                    k, pool_name)
                                stale_keys.append(k)
                                continue

                            self.level_specs[level_spec.id] = level_spec
                            schedule = Schedule.from_json(level_spec.name, v)
                            self.schedules[level_spec.id] = schedule
                        except ValueError:
                            self.handler.log.error(
                                "Failed to decode schedule: pool={}, {} {}".format(
                                    pool_name, k, v))
                    if not it:
                        break

        except StopIteration:
            pass
        except rados.ObjectNotFound:
            pass

        if stale_keys:
            with rados.WriteOpCtx() as write_op:
                ioctx.remove_omap_keys(write_op, stale_keys)
                ioctx.operate_write_op(write_op, self.handler.SCHEDULE_OID)

    def save(self, level_spec: LevelSpec, schedule: Optional[Schedule]) -> None:
        if level_spec.is_global():
            schedule_cfg = schedule and schedule.to_json() or None
            self.handler.module.set_module_option(
                self.handler.MODULE_OPTION_NAME, schedule_cfg)
            return

        pool_id = level_spec.get_pool_id()
        assert pool_id
        with self.handler.module.rados.open_ioctx2(int(pool_id)) as ioctx:
            with rados.WriteOpCtx() as write_op:
                if schedule:
                    ioctx.set_omap(write_op, (level_spec.id, ),
                                   (schedule.to_json(), ))
                else:
                    ioctx.remove_omap_keys(write_op, (level_spec.id, ))
                ioctx.operate_write_op(write_op, self.handler.SCHEDULE_OID)

    def add(self,
            level_spec: LevelSpec,
            interval: str,
            start_time: Optional[str]) -> None:
        schedule = self.schedules.get(level_spec.id, Schedule(level_spec.name))
        schedule.add(Interval.from_string(interval),
                     StartTime.from_string(start_time))
        self.schedules[level_spec.id] = schedule
        self.level_specs[level_spec.id] = level_spec
        self.save(level_spec, schedule)

    def remove(self,
               level_spec: LevelSpec,
               interval: Optional[str],
               start_time: Optional[str]) -> None:
        schedule = self.schedules.pop(level_spec.id, None)
        if schedule:
            if interval is None:
                schedule = None
            else:
                try:
                    schedule.remove(Interval.from_string(interval),
                                    StartTime.from_string(start_time))
                finally:
                    if schedule:
                        self.schedules[level_spec.id] = schedule
            if not schedule:
                del self.level_specs[level_spec.id]
        self.save(level_spec, schedule)

    def find(self,
             pool_id: str,
             namespace: str,
             image_id: Optional[str] = None) -> Optional['Schedule']:
        levels = [pool_id, namespace]
        if image_id:
            levels.append(image_id)
        nr_levels = len(levels)
        while nr_levels >= 0:
            # an empty spec id implies global schedule
            level_spec_id = "/".join(levels[:nr_levels])
            found = self.schedules.get(level_spec_id)
            if found is not None:
                return found
            nr_levels -= 1
        return None

    def intersects(self, level_spec: LevelSpec) -> bool:
        for ls in self.level_specs.values():
            if ls.intersects(level_spec):
                return True
        return False

    def to_list(self, level_spec: LevelSpec) -> Dict[str, dict]:
        if level_spec.id in self.schedules:
            parent: Optional[LevelSpec] = level_spec
        else:
            # try to find existing parent
            parent = None
            for level_spec_id in self.schedules:
                ls = self.level_specs[level_spec_id]
                if ls == level_spec:
                    parent = ls
                    break
                if level_spec.is_child_of(ls) and \
                   (not parent or ls.is_child_of(parent)):
                    parent = ls
            if not parent:
                # set to non-existing parent so we still could list its children
                parent = level_spec

        result = {}
        for level_spec_id, schedule in self.schedules.items():
            ls = self.level_specs[level_spec_id]
            if ls == parent or ls == level_spec or ls.is_child_of(level_spec):
                result[level_spec_id] = {
                    'name': schedule.name,
                    'schedule': schedule.to_list(),
                }
        return result
