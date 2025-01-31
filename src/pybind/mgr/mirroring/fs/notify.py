import errno
import json
import logging
import threading
import time

import rados

from .utils import MIRROR_OBJECT_PREFIX, AsyncOpTracker

log = logging.getLogger(__name__)

class Notifier:
    def __init__(self, ioctx):
        self.ioctx = ioctx

    @staticmethod
    def instance_object(instance_id):
        return f'{MIRROR_OBJECT_PREFIX}.{instance_id}'

    def notify_cbk(self, dir_path, callback):
        def cbk(_, r, acks, timeouts):
            log.debug(f'Notifier.notify_cbk: ret {r} acks: {acks} timeouts: {timeouts}')
            callback(dir_path, r)
        return cbk

    def notify(self, dir_path, message, callback):
        try:
            instance_id = message[0]
            message = message[1]
            log.debug(f'Notifier.notify: {instance_id} {message} for {dir_path}')
            self.ioctx.aio_notify(
                Notifier.instance_object(
                    instance_id), self.notify_cbk(dir_path, callback), msg=message)
        except rados.Error as e:
            log.error(f'Notifier exception: {e}')
            raise e

class InstanceWatcher:
    INSTANCE_TIMEOUT = 30
    NOTIFY_INTERVAL = 1

    class Listener:
        def handle_instances(self, added, removed):
            raise NotImplementedError()

    def __init__(self, ioctx, instances, listener):
        self.ioctx = ioctx
        self.listener = listener
        self.instances = {}
        for instance_id, data in instances.items():
            self.instances[instance_id] = {'addr': data['addr'],
                                           'seen': time.time()}
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)
        self.done = threading.Event()
        self.waiting = threading.Event()
        self.notify_task = None
        self.schedule_notify_task()

    def schedule_notify_task(self):
        assert self.notify_task == None
        self.notify_task = threading.Timer(InstanceWatcher.NOTIFY_INTERVAL, self.notify)
        self.notify_task.start()

    def wait_and_stop(self):
        with self.lock:
            log.info('InstanceWatcher.wait_and_stop')
            self.waiting.set()
            self.cond.wait_for(lambda: self.done.is_set())
            log.info('waiting done')
            assert self.notify_task == None

    def handle_notify(self, _, r, acks, timeouts):
        log.debug(f'InstanceWatcher.handle_notify r={r} acks={acks} timeouts={timeouts}')
        with self.lock:
            try:
                added = {}
                removed = {}
                if acks is None:
                    acks = []
                ackd_instances = []
                for ack in acks:
                    instance_id = str(ack[0])
                    ackd_instances.append(instance_id)
                    # sender data is quoted
                    notifier_data = json.loads(ack[2].decode('utf-8'))
                    log.debug(f'InstanceWatcher.handle_notify: {instance_id}: {notifier_data}')
                    if not instance_id in self.instances:
                        self.instances[instance_id] = {}
                        added[instance_id] = notifier_data['addr']
                    self.instances[instance_id]['addr'] = notifier_data['addr']
                    self.instances[instance_id]['seen'] = time.time()
                # gather non responders
                now = time.time()
                for instance_id in list(self.instances.keys()):
                    data = self.instances[instance_id]
                    if (now - data['seen'] > InstanceWatcher.INSTANCE_TIMEOUT) or \
                       (self.waiting.is_set() and instance_id not in ackd_instances):
                        removed[instance_id] = data['addr']
                        self.instances.pop(instance_id)
                if added or removed:
                    self.listener.handle_instances(added, removed)
            except Exception as e:
                log.warn(f'InstanceWatcher.handle_notify exception: {e}')
            finally:
                if not self.instances and self.waiting.is_set():
                    self.done.set()
                    self.cond.notifyAll()
                else:
                    self.schedule_notify_task()

    def notify(self):
        with self.lock:
            self.notify_task = None
            try:
                log.debug('InstanceWatcher.notify')
                self.ioctx.aio_notify(MIRROR_OBJECT_PREFIX, self.handle_notify)
            except rados.Error as e:
                log.warn(f'InstanceWatcher exception: {e}')
                self.schedule_notify_task()
