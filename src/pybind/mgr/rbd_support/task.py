import errno
import json
import rados
import rbd
import re
import traceback
import uuid

from contextlib import contextmanager
from datetime import datetime, timedelta
from functools import partial, wraps
from threading import Condition, Lock, Thread
from typing import cast, Any, Callable, Dict, Iterator, List, Optional, Tuple, TypeVar

from .common import (authorize_request, extract_pool_key, get_rbd_pools,
                     is_authorized, GLOBAL_POOL_KEY)


RBD_TASK_OID = "rbd_task"

TASK_SEQUENCE = "sequence"
TASK_ID = "id"
TASK_REFS = "refs"
TASK_MESSAGE = "message"
TASK_RETRY_ATTEMPTS = "retry_attempts"
TASK_RETRY_TIME = "retry_time"
TASK_RETRY_MESSAGE = "retry_message"
TASK_IN_PROGRESS = "in_progress"
TASK_PROGRESS = "progress"
TASK_CANCELED = "canceled"

TASK_REF_POOL_NAME = "pool_name"
TASK_REF_POOL_NAMESPACE = "pool_namespace"
TASK_REF_IMAGE_NAME = "image_name"
TASK_REF_IMAGE_ID = "image_id"
TASK_REF_ACTION = "action"

TASK_REF_ACTION_FLATTEN = "flatten"
TASK_REF_ACTION_REMOVE = "remove"
TASK_REF_ACTION_TRASH_REMOVE = "trash remove"
TASK_REF_ACTION_MIGRATION_EXECUTE = "migrate execute"
TASK_REF_ACTION_MIGRATION_COMMIT = "migrate commit"
TASK_REF_ACTION_MIGRATION_ABORT = "migrate abort"

VALID_TASK_ACTIONS = [TASK_REF_ACTION_FLATTEN,
                      TASK_REF_ACTION_REMOVE,
                      TASK_REF_ACTION_TRASH_REMOVE,
                      TASK_REF_ACTION_MIGRATION_EXECUTE,
                      TASK_REF_ACTION_MIGRATION_COMMIT,
                      TASK_REF_ACTION_MIGRATION_ABORT]

TASK_RETRY_INTERVAL = timedelta(seconds=30)
TASK_MAX_RETRY_INTERVAL = timedelta(seconds=300)
MAX_COMPLETED_TASKS = 50


T = TypeVar('T')
FuncT = TypeVar('FuncT', bound=Callable[..., Any])


class Throttle:
    def __init__(self: Any, throttle_period: timedelta) -> None:
        self.throttle_period = throttle_period
        self.time_of_last_call = datetime.min

    def __call__(self: 'Throttle', fn: FuncT) -> FuncT:
        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            now = datetime.now()
            if self.time_of_last_call + self.throttle_period <= now:
                self.time_of_last_call = now
                return fn(*args, **kwargs)
        return cast(FuncT, wrapper)


TaskRefsT = Dict[str, str]


class Task:
    def __init__(self, sequence: int, task_id: str, message: str, refs: TaskRefsT):
        self.sequence = sequence
        self.task_id = task_id
        self.message = message
        self.refs = refs
        self.retry_message: Optional[str] = None
        self.retry_attempts = 0
        self.retry_time: Optional[datetime] = None
        self.in_progress = False
        self.progress = 0.0
        self.canceled = False
        self.failed = False
        self.progress_posted = False

    def __str__(self) -> str:
        return self.to_json()

    @property
    def sequence_key(self) -> bytes:
        return "{0:016X}".format(self.sequence).encode()

    def cancel(self) -> None:
        self.canceled = True
        self.fail("Operation canceled")

    def fail(self, message: str) -> None:
        self.failed = True
        self.failure_message = message

    def to_dict(self) -> Dict[str, Any]:
        d = {TASK_SEQUENCE: self.sequence,
             TASK_ID: self.task_id,
             TASK_MESSAGE: self.message,
             TASK_REFS: self.refs
             }
        if self.retry_message:
            d[TASK_RETRY_MESSAGE] = self.retry_message
        if self.retry_attempts:
            d[TASK_RETRY_ATTEMPTS] = self.retry_attempts
        if self.retry_time:
            d[TASK_RETRY_TIME] = self.retry_time.isoformat()
        if self.in_progress:
            d[TASK_IN_PROGRESS] = True
            d[TASK_PROGRESS] = self.progress
        if self.canceled:
            d[TASK_CANCELED] = True
        return d

    def to_json(self) -> str:
        return str(json.dumps(self.to_dict()))

    @classmethod
    def from_json(cls, val: str) -> 'Task':
        try:
            d = json.loads(val)
            action = d.get(TASK_REFS, {}).get(TASK_REF_ACTION)
            if action not in VALID_TASK_ACTIONS:
                raise ValueError("Invalid task action: {}".format(action))

            return Task(d[TASK_SEQUENCE], d[TASK_ID], d[TASK_MESSAGE], d[TASK_REFS])
        except json.JSONDecodeError as e:
            raise ValueError("Invalid JSON ({})".format(str(e)))
        except KeyError as e:
            raise ValueError("Invalid task format (missing key {})".format(str(e)))


# pool_name, namespace, image_name
ImageSpecT = Tuple[str, str, str]
# pool_name, namespace
PoolSpecT = Tuple[str, str]
MigrationStatusT = Dict[str, str]


class TaskHandler:
    lock = Lock()
    condition = Condition(lock)

    in_progress_task = None
    tasks_by_sequence: Dict[int, Task] = dict()
    tasks_by_id: Dict[str, Task] = dict()

    completed_tasks: List[Task] = []

    sequence = 0

    def __init__(self, module: Any) -> None:
        self.module = module
        self.log = module.log

        self.stop_thread = False
        self.thread = Thread(target=self.run)

    def setup(self) -> None:
        with self.lock:
            self.init_task_queue()
        self.thread.start()

    @property
    def default_pool_name(self) -> str:
        return self.module.get_ceph_option("rbd_default_pool")

    def extract_pool_spec(self, pool_spec: str) -> PoolSpecT:
        pool_spec = extract_pool_key(pool_spec)
        if pool_spec == GLOBAL_POOL_KEY:
            pool_spec = (self.default_pool_name, '')
        return cast(PoolSpecT, pool_spec)

    def extract_image_spec(self, image_spec: str) -> ImageSpecT:
        match = re.match(r'^(?:([^/]+)/(?:([^/]+)/)?)?([^/@]+)$',
                         image_spec or '')
        if not match:
            raise ValueError("Invalid image spec: {}".format(image_spec))
        return (match.group(1) or self.default_pool_name, match.group(2) or '',
                match.group(3))

    def shutdown(self) -> None:
        self.log.info("TaskHandler: shutting down")
        self.stop_thread = True
        if self.thread.is_alive():
            self.log.debug("TaskHandler: joining thread")
            self.thread.join()
        self.log.info("TaskHandler: shut down")

    def run(self) -> None:
        try:
            self.log.info("TaskHandler: starting")
            while not self.stop_thread:
                with self.lock:
                    now = datetime.now()
                    for sequence in sorted([sequence for sequence, task
                                            in self.tasks_by_sequence.items()
                                            if not task.retry_time or task.retry_time <= now]):
                        self.execute_task(sequence)

                    self.condition.wait(5)
                    self.log.debug("TaskHandler: tick")

        except (rados.ConnectionShutdown, rbd.ConnectionShutdown):
            self.log.exception("TaskHandler: client blocklisted")
            self.module.client_blocklisted.set()
        except Exception as ex:
            self.log.fatal("Fatal runtime error: {}\n{}".format(
                ex, traceback.format_exc()))

    @contextmanager
    def open_ioctx(self, spec: PoolSpecT) -> Iterator[rados.Ioctx]:
        try:
            with self.module.rados.open_ioctx(spec[0]) as ioctx:
                ioctx.set_namespace(spec[1])
                yield ioctx
        except rados.ObjectNotFound:
            self.log.error("Failed to locate pool {}".format(spec[0]))
            raise

    @classmethod
    def format_image_spec(cls, image_spec: ImageSpecT) -> str:
        image = image_spec[2]
        if image_spec[1]:
            image = "{}/{}".format(image_spec[1], image)
        if image_spec[0]:
            image = "{}/{}".format(image_spec[0], image)
        return image

    def init_task_queue(self) -> None:
        for pool_id, pool_name in get_rbd_pools(self.module).items():
            try:
                with self.module.rados.open_ioctx2(int(pool_id)) as ioctx:
                    self.load_task_queue(ioctx, pool_name)

                    try:
                        namespaces = rbd.RBD().namespace_list(ioctx)
                    except rbd.OperationNotSupported:
                        self.log.debug("Namespaces not supported")
                        continue

                    for namespace in namespaces:
                        ioctx.set_namespace(namespace)
                        self.load_task_queue(ioctx, pool_name)

            except rados.ObjectNotFound:
                # pool DNE
                pass

        if self.tasks_by_sequence:
            self.sequence = list(sorted(self.tasks_by_sequence.keys()))[-1]

        self.log.debug("sequence={}, tasks_by_sequence={}, tasks_by_id={}".format(
            self.sequence, str(self.tasks_by_sequence), str(self.tasks_by_id)))

    def load_task_queue(self, ioctx: rados.Ioctx, pool_name: str) -> None:
        pool_spec = pool_name
        if ioctx.nspace:
            pool_spec += "/{}".format(ioctx.nspace)

        start_after = ''
        try:
            while True:
                with rados.ReadOpCtx() as read_op:
                    self.log.info("load_task_task: {}, start_after={}".format(
                        pool_spec, start_after))
                    it, ret = ioctx.get_omap_vals(read_op, start_after, "", 128)
                    ioctx.operate_read_op(read_op, RBD_TASK_OID)

                    it = list(it)
                    for k, v in it:
                        start_after = k
                        v = v.decode()
                        self.log.info("load_task_task: task={}".format(v))

                        try:
                            task = Task.from_json(v)
                            self.append_task(task)
                        except ValueError:
                            self.log.error("Failed to decode task: pool_spec={}, task={}".format(pool_spec, v))

                    if not it:
                        break

        except StopIteration:
            pass
        except rados.ObjectNotFound:
            # rbd_task DNE
            pass

    def append_task(self, task: Task) -> None:
        self.tasks_by_sequence[task.sequence] = task
        self.tasks_by_id[task.task_id] = task

    def task_refs_match(self, task_refs: TaskRefsT, refs: TaskRefsT) -> bool:
        if TASK_REF_IMAGE_ID not in refs and TASK_REF_IMAGE_ID in task_refs:
            task_refs = task_refs.copy()
            del task_refs[TASK_REF_IMAGE_ID]

        self.log.debug("task_refs_match: ref1={}, ref2={}".format(task_refs, refs))
        return task_refs == refs

    def find_task(self, refs: TaskRefsT) -> Optional[Task]:
        self.log.debug("find_task: refs={}".format(refs))

        # search for dups and return the original
        for task_id in reversed(sorted(self.tasks_by_id.keys())):
            task = self.tasks_by_id[task_id]
            if self.task_refs_match(task.refs, refs):
                return task

        # search for a completed task (message replay)
        for task in reversed(self.completed_tasks):
            if self.task_refs_match(task.refs, refs):
                return task
        else:
            return None

    def add_task(self,
                 ioctx: rados.Ioctx,
                 message: str,
                 refs: TaskRefsT) -> str:
        self.log.debug("add_task: message={}, refs={}".format(message, refs))

        # ensure unique uuid across all pools
        while True:
            task_id = str(uuid.uuid4())
            if task_id not in self.tasks_by_id:
                break

        self.sequence += 1
        task = Task(self.sequence, task_id, message, refs)

        # add the task to the rbd_task omap
        task_json = task.to_json()
        omap_keys = (task.sequence_key, )
        omap_vals = (str.encode(task_json), )
        self.log.info("adding task: %s %s",
                      omap_keys[0].decode(),
                      omap_vals[0].decode())

        with rados.WriteOpCtx() as write_op:
            ioctx.set_omap(write_op, omap_keys, omap_vals)
            ioctx.operate_write_op(write_op, RBD_TASK_OID)
        self.append_task(task)

        self.condition.notify()
        return task_json

    def remove_task(self,
                    ioctx: Optional[rados.Ioctx],
                    task: Task,
                    remove_in_memory: bool = True) -> None:
        self.log.info("remove_task: task={}".format(str(task)))
        if ioctx:
            try:
                with rados.WriteOpCtx() as write_op:
                    omap_keys = (task.sequence_key, )
                    ioctx.remove_omap_keys(write_op, omap_keys)
                    ioctx.operate_write_op(write_op, RBD_TASK_OID)
            except rados.ObjectNotFound:
                pass

        if remove_in_memory:
            try:
                del self.tasks_by_id[task.task_id]
                del self.tasks_by_sequence[task.sequence]

                # keep a record of the last N tasks to help avoid command replay
                # races
                if not task.failed and not task.canceled:
                    self.log.debug("remove_task: moving to completed tasks")
                    self.completed_tasks.append(task)
                    self.completed_tasks = self.completed_tasks[-MAX_COMPLETED_TASKS:]

            except KeyError:
                pass

    def execute_task(self, sequence: int) -> None:
        task = self.tasks_by_sequence[sequence]
        self.log.info("execute_task: task={}".format(str(task)))

        pool_valid = False
        try:
            with self.open_ioctx((task.refs[TASK_REF_POOL_NAME],
                                  task.refs[TASK_REF_POOL_NAMESPACE])) as ioctx:
                pool_valid = True

                action = task.refs[TASK_REF_ACTION]
                execute_fn = {TASK_REF_ACTION_FLATTEN: self.execute_flatten,
                              TASK_REF_ACTION_REMOVE: self.execute_remove,
                              TASK_REF_ACTION_TRASH_REMOVE: self.execute_trash_remove,
                              TASK_REF_ACTION_MIGRATION_EXECUTE: self.execute_migration_execute,
                              TASK_REF_ACTION_MIGRATION_COMMIT: self.execute_migration_commit,
                              TASK_REF_ACTION_MIGRATION_ABORT: self.execute_migration_abort
                              }.get(action)
                if not execute_fn:
                    self.log.error("Invalid task action: {}".format(action))
                else:
                    task.in_progress = True
                    self.in_progress_task = task

                    self.lock.release()
                    try:
                        execute_fn(ioctx, task)

                    except rbd.OperationCanceled:
                        self.log.info("Operation canceled: task={}".format(
                            str(task)))

                    finally:
                        self.lock.acquire()

                        task.in_progress = False
                        self.in_progress_task = None

                    self.complete_progress(task)
                    self.remove_task(ioctx, task)

        except rados.ObjectNotFound as e:
            self.log.error("execute_task: {}".format(e))
            if pool_valid:
                task.retry_message = "{}".format(e)
                self.update_progress(task, 0)
            else:
                # pool DNE -- remove in-memory task
                self.complete_progress(task)
                self.remove_task(None, task)

        except (rados.ConnectionShutdown, rbd.ConnectionShutdown):
            raise

        except (rados.Error, rbd.Error) as e:
            self.log.error("execute_task: {}".format(e))
            task.retry_message = "{}".format(e)
            self.update_progress(task, 0)

        finally:
            task.in_progress = False
            task.retry_attempts += 1
            task.retry_time = datetime.now() + min(
                TASK_RETRY_INTERVAL * task.retry_attempts,
                TASK_MAX_RETRY_INTERVAL)

    def progress_callback(self, task: Task, current: int, total: int) -> int:
        progress = float(current) / float(total)
        self.log.debug("progress_callback: task={}, progress={}".format(
            str(task), progress))

        # avoid deadlocking when a new command comes in during a progress callback
        if not self.lock.acquire(False):
            return 0

        try:
            if not self.in_progress_task or self.in_progress_task.canceled:
                return -rbd.ECANCELED
            self.in_progress_task.progress = progress
        finally:
            self.lock.release()

        if not task.progress_posted:
            # delayed creation of progress event until first callback
            self.post_progress(task, progress)
        else:
            self.throttled_update_progress(task, progress)

        return 0

    def execute_flatten(self, ioctx: rados.Ioctx, task: Task) -> None:
        self.log.info("execute_flatten: task={}".format(str(task)))

        try:
            with rbd.Image(ioctx, task.refs[TASK_REF_IMAGE_NAME]) as image:
                image.flatten(on_progress=partial(self.progress_callback, task))
        except rbd.InvalidArgument:
            task.fail("Image does not have parent")
            self.log.info("{}: task={}".format(task.failure_message, str(task)))
        except rbd.ImageNotFound:
            task.fail("Image does not exist")
            self.log.info("{}: task={}".format(task.failure_message, str(task)))

    def execute_remove(self, ioctx: rados.Ioctx, task: Task) -> None:
        self.log.info("execute_remove: task={}".format(str(task)))

        try:
            rbd.RBD().remove(ioctx, task.refs[TASK_REF_IMAGE_NAME],
                             on_progress=partial(self.progress_callback, task))
        except rbd.ImageNotFound:
            task.fail("Image does not exist")
            self.log.info("{}: task={}".format(task.failure_message, str(task)))

    def execute_trash_remove(self, ioctx: rados.Ioctx, task: Task) -> None:
        self.log.info("execute_trash_remove: task={}".format(str(task)))

        try:
            rbd.RBD().trash_remove(ioctx, task.refs[TASK_REF_IMAGE_ID],
                                   on_progress=partial(self.progress_callback, task))
        except rbd.ImageNotFound:
            task.fail("Image does not exist")
            self.log.info("{}: task={}".format(task.failure_message, str(task)))

    def execute_migration_execute(self, ioctx: rados.Ioctx, task: Task) -> None:
        self.log.info("execute_migration_execute: task={}".format(str(task)))

        try:
            rbd.RBD().migration_execute(ioctx, task.refs[TASK_REF_IMAGE_NAME],
                                        on_progress=partial(self.progress_callback, task))
        except rbd.ImageNotFound:
            task.fail("Image does not exist")
            self.log.info("{}: task={}".format(task.failure_message, str(task)))
        except rbd.InvalidArgument:
            task.fail("Image is not migrating")
            self.log.info("{}: task={}".format(task.failure_message, str(task)))

    def execute_migration_commit(self, ioctx: rados.Ioctx, task: Task) -> None:
        self.log.info("execute_migration_commit: task={}".format(str(task)))

        try:
            rbd.RBD().migration_commit(ioctx, task.refs[TASK_REF_IMAGE_NAME],
                                       on_progress=partial(self.progress_callback, task))
        except rbd.ImageNotFound:
            task.fail("Image does not exist")
            self.log.info("{}: task={}".format(task.failure_message, str(task)))
        except rbd.InvalidArgument:
            task.fail("Image is not migrating or migration not executed")
            self.log.info("{}: task={}".format(task.failure_message, str(task)))

    def execute_migration_abort(self, ioctx: rados.Ioctx, task: Task) -> None:
        self.log.info("execute_migration_abort: task={}".format(str(task)))

        try:
            rbd.RBD().migration_abort(ioctx, task.refs[TASK_REF_IMAGE_NAME],
                                      on_progress=partial(self.progress_callback, task))
        except rbd.ImageNotFound:
            task.fail("Image does not exist")
            self.log.info("{}: task={}".format(task.failure_message, str(task)))
        except rbd.InvalidArgument:
            task.fail("Image is not migrating")
            self.log.info("{}: task={}".format(task.failure_message, str(task)))

    def complete_progress(self, task: Task) -> None:
        if not task.progress_posted:
            # ensure progress event exists before we complete/fail it
            self.post_progress(task, 0)

        self.log.debug("complete_progress: task={}".format(str(task)))
        try:
            if task.failed:
                self.module.remote("progress", "fail", task.task_id,
                                   task.failure_message)
            else:
                self.module.remote("progress", "complete", task.task_id)
        except ImportError:
            # progress module is disabled
            pass

    def _update_progress(self, task: Task, progress: float) -> None:
        self.log.debug("update_progress: task={}, progress={}".format(str(task), progress))
        try:
            refs = {"origin": "rbd_support"}
            refs.update(task.refs)

            self.module.remote("progress", "update", task.task_id,
                               task.message, progress, refs)
        except ImportError:
            # progress module is disabled
            pass

    def post_progress(self, task: Task, progress: float) -> None:
        self._update_progress(task, progress)
        task.progress_posted = True

    def update_progress(self, task: Task, progress: float) -> None:
        if task.progress_posted:
            self._update_progress(task, progress)

    @Throttle(timedelta(seconds=1))
    def throttled_update_progress(self, task: Task, progress: float) -> None:
        self.update_progress(task, progress)

    def queue_flatten(self, image_spec: str) -> Tuple[int, str, str]:
        image_spec = self.extract_image_spec(image_spec)

        authorize_request(self.module, image_spec[0], image_spec[1])
        self.log.info("queue_flatten: {}".format(image_spec))

        refs = {TASK_REF_ACTION: TASK_REF_ACTION_FLATTEN,
                TASK_REF_POOL_NAME: image_spec[0],
                TASK_REF_POOL_NAMESPACE: image_spec[1],
                TASK_REF_IMAGE_NAME: image_spec[2]}

        with self.open_ioctx(image_spec[:2]) as ioctx:
            try:
                with rbd.Image(ioctx, image_spec[2]) as image:
                    refs[TASK_REF_IMAGE_ID] = image.id()

                    try:
                        parent_image_id = image.parent_id()
                    except rbd.ImageNotFound:
                        parent_image_id = None

            except rbd.ImageNotFound:
                pass

            task = self.find_task(refs)
            if task:
                return 0, task.to_json(), ''

            if TASK_REF_IMAGE_ID not in refs:
                raise rbd.ImageNotFound("Image {} does not exist".format(
                    self.format_image_spec(image_spec)), errno=errno.ENOENT)
            if not parent_image_id:
                raise rbd.ImageNotFound("Image {} does not have a parent".format(
                    self.format_image_spec(image_spec)), errno=errno.ENOENT)

            return 0, self.add_task(ioctx,
                                    "Flattening image {}".format(
                                        self.format_image_spec(image_spec)),
                                    refs), ""

    def queue_remove(self, image_spec: str) -> Tuple[int, str, str]:
        image_spec = self.extract_image_spec(image_spec)

        authorize_request(self.module, image_spec[0], image_spec[1])
        self.log.info("queue_remove: {}".format(image_spec))

        refs = {TASK_REF_ACTION: TASK_REF_ACTION_REMOVE,
                TASK_REF_POOL_NAME: image_spec[0],
                TASK_REF_POOL_NAMESPACE: image_spec[1],
                TASK_REF_IMAGE_NAME: image_spec[2]}

        with self.open_ioctx(image_spec[:2]) as ioctx:
            try:
                with rbd.Image(ioctx, image_spec[2]) as image:
                    refs[TASK_REF_IMAGE_ID] = image.id()
                    snaps = list(image.list_snaps())

            except rbd.ImageNotFound:
                pass

            task = self.find_task(refs)
            if task:
                return 0, task.to_json(), ''

            if TASK_REF_IMAGE_ID not in refs:
                raise rbd.ImageNotFound("Image {} does not exist".format(
                    self.format_image_spec(image_spec)), errno=errno.ENOENT)
            if snaps:
                raise rbd.ImageBusy("Image {} has snapshots".format(
                    self.format_image_spec(image_spec)), errno=errno.EBUSY)

            return 0, self.add_task(ioctx,
                                    "Removing image {}".format(
                                        self.format_image_spec(image_spec)),
                                    refs), ''

    def queue_trash_remove(self, image_id_spec: str) -> Tuple[int, str, str]:
        image_id_spec = self.extract_image_spec(image_id_spec)

        authorize_request(self.module, image_id_spec[0], image_id_spec[1])
        self.log.info("queue_trash_remove: {}".format(image_id_spec))

        refs = {TASK_REF_ACTION: TASK_REF_ACTION_TRASH_REMOVE,
                TASK_REF_POOL_NAME: image_id_spec[0],
                TASK_REF_POOL_NAMESPACE: image_id_spec[1],
                TASK_REF_IMAGE_ID: image_id_spec[2]}
        task = self.find_task(refs)
        if task:
            return 0, task.to_json(), ''

        # verify that image exists in trash
        with self.open_ioctx(image_id_spec[:2]) as ioctx:
            rbd.RBD().trash_get(ioctx, image_id_spec[2])

            return 0, self.add_task(ioctx,
                                    "Removing image {} from trash".format(
                                        self.format_image_spec(image_id_spec)),
                                    refs), ''

    def get_migration_status(self,
                             ioctx: rados.Ioctx,
                             image_spec: ImageSpecT) -> Optional[MigrationStatusT]:
        try:
            return rbd.RBD().migration_status(ioctx, image_spec[2])
        except (rbd.InvalidArgument, rbd.ImageNotFound):
            return None

    def validate_image_migrating(self,
                                 image_spec: ImageSpecT,
                                 migration_status: Optional[MigrationStatusT]) -> None:
        if not migration_status:
            raise rbd.InvalidArgument("Image {} is not migrating".format(
                self.format_image_spec(image_spec)), errno=errno.EINVAL)

    def resolve_pool_name(self, pool_id: str) -> str:
        osd_map = self.module.get('osd_map')
        for pool in osd_map['pools']:
            if pool['pool'] == pool_id:
                return pool['pool_name']
        return '<unknown>'

    def queue_migration_execute(self, image_spec: str) -> Tuple[int, str, str]:
        image_spec = self.extract_image_spec(image_spec)

        authorize_request(self.module, image_spec[0], image_spec[1])
        self.log.info("queue_migration_execute: {}".format(image_spec))

        refs = {TASK_REF_ACTION: TASK_REF_ACTION_MIGRATION_EXECUTE,
                TASK_REF_POOL_NAME: image_spec[0],
                TASK_REF_POOL_NAMESPACE: image_spec[1],
                TASK_REF_IMAGE_NAME: image_spec[2]}

        with self.open_ioctx(image_spec[:2]) as ioctx:
            status = self.get_migration_status(ioctx, image_spec)
            if status:
                refs[TASK_REF_IMAGE_ID] = status['dest_image_id']

            task = self.find_task(refs)
            if task:
                return 0, task.to_json(), ''

            self.validate_image_migrating(image_spec, status)
            assert status
            if status['state'] not in [rbd.RBD_IMAGE_MIGRATION_STATE_PREPARED,
                                       rbd.RBD_IMAGE_MIGRATION_STATE_EXECUTING]:
                raise rbd.InvalidArgument("Image {} is not in ready state".format(
                    self.format_image_spec(image_spec)), errno=errno.EINVAL)

            source_pool = self.resolve_pool_name(status['source_pool_id'])
            dest_pool = self.resolve_pool_name(status['dest_pool_id'])
            return 0, self.add_task(ioctx,
                                    "Migrating image {} to {}".format(
                                        self.format_image_spec((source_pool,
                                                                status['source_pool_namespace'],
                                                                status['source_image_name'])),
                                        self.format_image_spec((dest_pool,
                                                                status['dest_pool_namespace'],
                                                                status['dest_image_name']))),
                                    refs), ''

    def queue_migration_commit(self, image_spec: str) -> Tuple[int, str, str]:
        image_spec = self.extract_image_spec(image_spec)

        authorize_request(self.module, image_spec[0], image_spec[1])
        self.log.info("queue_migration_commit: {}".format(image_spec))

        refs = {TASK_REF_ACTION: TASK_REF_ACTION_MIGRATION_COMMIT,
                TASK_REF_POOL_NAME: image_spec[0],
                TASK_REF_POOL_NAMESPACE: image_spec[1],
                TASK_REF_IMAGE_NAME: image_spec[2]}

        with self.open_ioctx(image_spec[:2]) as ioctx:
            status = self.get_migration_status(ioctx, image_spec)
            if status:
                refs[TASK_REF_IMAGE_ID] = status['dest_image_id']

            task = self.find_task(refs)
            if task:
                return 0, task.to_json(), ''

            self.validate_image_migrating(image_spec, status)
            assert status
            if status['state'] != rbd.RBD_IMAGE_MIGRATION_STATE_EXECUTED:
                raise rbd.InvalidArgument("Image {} has not completed migration".format(
                    self.format_image_spec(image_spec)), errno=errno.EINVAL)

            return 0, self.add_task(ioctx,
                                    "Committing image migration for {}".format(
                                        self.format_image_spec(image_spec)),
                                    refs), ''

    def queue_migration_abort(self, image_spec: str) -> Tuple[int, str, str]:
        image_spec = self.extract_image_spec(image_spec)

        authorize_request(self.module, image_spec[0], image_spec[1])
        self.log.info("queue_migration_abort: {}".format(image_spec))

        refs = {TASK_REF_ACTION: TASK_REF_ACTION_MIGRATION_ABORT,
                TASK_REF_POOL_NAME: image_spec[0],
                TASK_REF_POOL_NAMESPACE: image_spec[1],
                TASK_REF_IMAGE_NAME: image_spec[2]}

        with self.open_ioctx(image_spec[:2]) as ioctx:
            status = self.get_migration_status(ioctx, image_spec)
            if status:
                refs[TASK_REF_IMAGE_ID] = status['dest_image_id']

            task = self.find_task(refs)
            if task:
                return 0, task.to_json(), ''

            self.validate_image_migrating(image_spec, status)
            return 0, self.add_task(ioctx,
                                    "Aborting image migration for {}".format(
                                        self.format_image_spec(image_spec)),
                                    refs), ''

    def task_cancel(self, task_id: str) -> Tuple[int, str, str]:
        self.log.info("task_cancel: {}".format(task_id))

        task = self.tasks_by_id.get(task_id)
        if not task or not is_authorized(self.module,
                                         task.refs[TASK_REF_POOL_NAME],
                                         task.refs[TASK_REF_POOL_NAMESPACE]):
            return -errno.ENOENT, '', "No such task {}".format(task_id)

        task.cancel()

        remove_in_memory = True
        if self.in_progress_task and self.in_progress_task.task_id == task_id:
            self.log.info("Attempting to cancel in-progress task: {}".format(str(self.in_progress_task)))
            remove_in_memory = False

        # complete any associated event in the progress module
        self.complete_progress(task)

        # remove from rbd_task omap
        with self.open_ioctx((task.refs[TASK_REF_POOL_NAME],
                              task.refs[TASK_REF_POOL_NAMESPACE])) as ioctx:
            self.remove_task(ioctx, task, remove_in_memory)

        return 0, "", ""

    def task_list(self, task_id: Optional[str]) -> Tuple[int, str, str]:
        self.log.info("task_list: {}".format(task_id))

        if task_id:
            task = self.tasks_by_id.get(task_id)
            if not task or not is_authorized(self.module,
                                             task.refs[TASK_REF_POOL_NAME],
                                             task.refs[TASK_REF_POOL_NAMESPACE]):
                return -errno.ENOENT, '', "No such task {}".format(task_id)

            return 0, json.dumps(task.to_dict(), indent=4, sort_keys=True), ""
        else:
            tasks = []
            for sequence in sorted(self.tasks_by_sequence.keys()):
                task = self.tasks_by_sequence[sequence]
                if is_authorized(self.module,
                                 task.refs[TASK_REF_POOL_NAME],
                                 task.refs[TASK_REF_POOL_NAMESPACE]):
                    tasks.append(task.to_dict())

            return 0, json.dumps(tasks, indent=4, sort_keys=True), ""
