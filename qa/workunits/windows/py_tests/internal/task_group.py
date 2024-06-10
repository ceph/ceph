from concurrent import futures
import logging
import threading


LOG = logging.getLogger()


class TaskGroup(object):
    def __init__(self, max_workers=1, stop_on_error=True):
        self._executor = futures.ThreadPoolExecutor(max_workers=max_workers)
        self._lock = threading.Lock()

        self.errors = 0
        self.completed = 0
        self.pending = 0

        self.stopped = False
        self.stop_on_error = stop_on_error

        self._submitted_tasks = []

    def _wrap_task(self, task):
        def wrapper():
            with self._lock:
                if self.stopped:
                    self.pending -= 1
                    return

            try:
                task()
            except Exception as ex:
                with self._lock:
                    if self.stop_on_error:
                        self.stopped = True

                    self.errors += 1
                    LOG.exception(
                        "Task exception: %s. Total exceptions: %d",
                        ex, self.errors)
            finally:
                with self._lock:
                    self.completed += 1
                    self.pending -= 1
                    LOG.info("Completed tasks: %d. Pending: %d",
                             self.completed, self.pending)

        return wrapper

    def submit(self, task):
        task_wrapper = self._wrap_task(task)

        with self._lock:
            self.pending += 1

        submitted_task = self._executor.submit(task_wrapper)
        self._submitted_tasks.append(submitted_task)

    def join(self):
        LOG.info("Waiting for %d tasks to complete.",
                 len(self._submitted_tasks))
        futures.wait(self._submitted_tasks)
        LOG.info("Tasks completed.")
