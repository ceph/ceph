import errno
import json
import logging
import threading
import time

import rados

from .utils import MIRROR_OBJECT_PREFIX, AsyncOpTracker

log = logging.getLogger(__name__)

class Notifier():
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

class InstanceWatcher():
    INSTANCE_TIMEOUT = 30
    NOTIFY_INTERVAL = 1

    class Listener(object):
        def handle_instances(self, added, removed):
            raise NotImplementedError()

    def __init__(self, ioctx, instances, listener):
        self.ioctx = ioctx
        self.listener = listener
        self.instances = {}
        for instance_id, data in instances.items():
            self.instances[instance_id] = {'addr': data['addr'], 'seen': time.time()}
        self.lock = threading.Lock()
        self.stopping = threading.Event()
        self.op_tracker = AsyncOpTracker()
        self.notify_task = None
        self.schedule_notify_task()

    def schedule_notify_task(self):
        assert self.notify_task == None
        self.notify_task = threading.Timer(InstanceWatcher.NOTIFY_INTERVAL, self.notify)
        self.notify_task.start()

    def stop(self):
        with self.lock:
            log.debug('InstanceWatcher.stop')
            self.stopping.set()
            if self.notify_task:
                self.notify_task.cancel()
        self.op_tracker.wait_for_ops()
        log.debug('InstanceWatcher.stop done')

    def handle_notify(self, _, r, acks, timeouts):
        log.debug(f'InstanceWatcher.handle_notify r={r} acks={acks} timeouts={timeouts}')
        with self.lock:
            try:
                added = {}
                removed = {}
                if acks is None:
                    acks = []
                for ack in acks:
                    instance_id = str(ack[0])
                    notifier_data = ack[2]
                    log.debug(f'InstanceWatcher.handle_notify: {instance_id}: {notifier_data}')
                    if not instance_id in self.instances:
                        added[instance_id] = notifier_data
                        self.instances[instance_id] = {'addr': notifier_data}
                    self.instances[instance_id]['seen'] = time.time()
                # gather non responders
                now = time.time()
                for instance_id in list(self.instances.keys()):
                    data = self.instances[instance_id]
                    if now - data['seen'] > InstanceWatcher.INSTANCE_TIMEOUT:
                        removed[instance_id] = data['addr']
                        self.instances.pop(instance_id)
                if added or removed:
                    self.listener.handle_instances(added, removed)
            except Exception as e:
                log.warn(f'InstanceWatcher.handle_notify exception: {e}')
            finally:
                self.op_tracker.finish_async_op()
                if not self.stopping.is_set():
                    self.schedule_notify_task()

    def notify(self):
        with self.lock:
            self.notify_task = None
            if self.stopping.is_set():
                log.debug('InstanceWatcher: exiting')
                return
            try:
                self.op_tracker.start_async_op()
                log.debug('InstanceWatcher.notify')
                self.ioctx.aio_notify(MIRROR_OBJECT_PREFIX, self.handle_notify)
            except rados.Error as e:
                log.warn(f'InstanceWatcher exception: {e}')
                self.schedule_notify_task()
