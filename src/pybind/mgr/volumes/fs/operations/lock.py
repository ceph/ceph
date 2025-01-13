from contextlib import contextmanager
import logging
from threading import Lock
from typing import Dict

log = logging.getLogger(__name__)

# singleton design pattern taken from http://www.aleax.it/5ep.html

class GlobalLock(object):
    """
    Global lock to serialize operations in mgr/volumes. This lock
    is currently held when accessing (opening) a volume to perform
    group/subvolume operations. Since this is a big lock, it's rather
    inefficient -- but right now it's ok since mgr/volumes does not
    expect concurrent operations via its APIs.

    As and when features get added (such as clone, where mgr/volumes
    would maintain subvolume states in the filesystem), there might
    be a need to allow concurrent operations. In that case it would
    be nice to implement an efficient path based locking mechanism.

    See: https://people.eecs.berkeley.edu/~kubitron/courses/cs262a-F14/projects/reports/project6_report.pdf
    """
    _shared_state: Dict = {
        'lock' : Lock(),
        'init' : False
    }

    def __init__(self):
        with self._shared_state['lock']:
            if not self._shared_state['init']:
                self._shared_state['init'] = True
        # share this state among all instances
        self.__dict__ = self._shared_state

    @contextmanager
    def lock_op(self):
        log.debug("entering global lock")
        with self._shared_state['lock']:
            log.debug("acquired global lock")
            yield
        log.debug("exited global lock")
