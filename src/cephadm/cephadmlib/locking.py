# locking.py - cephadm mutual exclusion via file locking

import fcntl
import logging
import os
import time

from typing import Any, Optional

from .context import CephadmContext
from .constants import LOCK_DIR, QUIET_LOG_LEVEL

logger = logging.getLogger()


# this is an abbreviated version of
# https://github.com/benediktschmitt/py-filelock/blob/master/filelock.py
# that drops all of the compatibility (this is Unix/Linux only).


class Timeout(TimeoutError):
    """
    Raised when the lock could not be acquired in *timeout*
    seconds.
    """

    def __init__(self, lock_file: str) -> None:
        #: The path of the file lock.
        self.lock_file = lock_file
        return None

    def __str__(self) -> str:
        temp = "The file lock '{}' could not be acquired.".format(
            self.lock_file
        )
        return temp


class _Acquire_ReturnProxy(object):
    def __init__(self, lock: 'FileLock') -> None:
        self.lock = lock
        return None

    def __enter__(self) -> 'FileLock':
        return self.lock

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.lock.release()
        return None


class FileLock(object):
    def __init__(
        self, ctx: CephadmContext, name: str, timeout: int = -1
    ) -> None:
        if not os.path.exists(LOCK_DIR):
            os.mkdir(LOCK_DIR, 0o700)
        self._lock_file = os.path.join(LOCK_DIR, name + '.lock')
        self.ctx = ctx

        # The file descriptor for the *_lock_file* as it is returned by the
        # os.open() function.
        # This file lock is only NOT None, if the object currently holds the
        # lock.
        self._lock_file_fd: Optional[int] = None
        self.timeout = timeout
        # The lock counter is used for implementing the nested locking
        # mechanism. Whenever the lock is acquired, the counter is increased and
        # the lock is only released, when this value is 0 again.
        self._lock_counter = 0
        return None

    @property
    def is_locked(self) -> bool:
        return self._lock_file_fd is not None

    def acquire(
        self, timeout: Optional[int] = None, poll_intervall: float = 0.05
    ) -> _Acquire_ReturnProxy:
        """
        Acquires the file lock or fails with a :exc:`Timeout` error.
        .. code-block:: python
            # You can use this method in the context manager (recommended)
            with lock.acquire():
                pass
            # Or use an equivalent try-finally construct:
            lock.acquire()
            try:
                pass
            finally:
                lock.release()
        :arg float timeout:
            The maximum time waited for the file lock.
            If ``timeout < 0``, there is no timeout and this method will
            block until the lock could be acquired.
            If ``timeout`` is None, the default :attr:`~timeout` is used.
        :arg float poll_intervall:
            We check once in *poll_intervall* seconds if we can acquire the
            file lock.
        :raises Timeout:
            if the lock could not be acquired in *timeout* seconds.
        .. versionchanged:: 2.0.0
            This method returns now a *proxy* object instead of *self*,
            so that it can be used in a with statement without side effects.
        """

        # Use the default timeout, if no timeout is provided.
        if timeout is None:
            timeout = self.timeout

        # Increment the number right at the beginning.
        # We can still undo it, if something fails.
        self._lock_counter += 1

        lock_id = id(self)
        lock_filename = self._lock_file
        start_time = time.time()
        try:
            while True:
                if not self.is_locked:
                    logger.log(
                        QUIET_LOG_LEVEL,
                        'Acquiring lock %s on %s',
                        lock_id,
                        lock_filename,
                    )
                    self._acquire()

                if self.is_locked:
                    logger.log(
                        QUIET_LOG_LEVEL,
                        'Lock %s acquired on %s',
                        lock_id,
                        lock_filename,
                    )
                    break
                elif timeout >= 0 and time.time() - start_time > timeout:
                    logger.warning(
                        'Timeout acquiring lock %s on %s',
                        lock_id,
                        lock_filename,
                    )
                    raise Timeout(self._lock_file)
                else:
                    logger.log(
                        QUIET_LOG_LEVEL,
                        'Lock %s not acquired on %s, waiting %s seconds ...',
                        lock_id,
                        lock_filename,
                        poll_intervall,
                    )
                    time.sleep(poll_intervall)
        except Exception:
            # Something did go wrong, so decrement the counter.
            self._lock_counter = max(0, self._lock_counter - 1)

            raise
        return _Acquire_ReturnProxy(lock=self)

    def release(self, force: bool = False) -> None:
        """
        Releases the file lock.
        Please note, that the lock is only completely released, if the lock
        counter is 0.
        Also note, that the lock file itself is not automatically deleted.
        :arg bool force:
            If true, the lock counter is ignored and the lock is released in
            every case.
        """
        if self.is_locked:
            self._lock_counter -= 1

            if self._lock_counter == 0 or force:
                # lock_id = id(self)
                # lock_filename = self._lock_file

                # Can't log in shutdown:
                #  File "/usr/lib64/python3.9/logging/__init__.py", line 1175, in _open
                #    NameError: name 'open' is not defined
                # logger.debug('Releasing lock %s on %s', lock_id, lock_filename)
                self._release()
                self._lock_counter = 0
                # logger.debug('Lock %s released on %s', lock_id, lock_filename)

        return None

    def __enter__(self) -> 'FileLock':
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.release()
        return None

    def __del__(self) -> None:
        self.release(force=True)
        return None

    def _acquire(self) -> None:
        open_mode = os.O_RDWR | os.O_CREAT | os.O_TRUNC
        fd = os.open(self._lock_file, open_mode)

        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (IOError, OSError):
            os.close(fd)
        else:
            self._lock_file_fd = fd
        return None

    def _release(self) -> None:
        # Do not remove the lockfile:
        #
        #   https://github.com/benediktschmitt/py-filelock/issues/31
        #   https://stackoverflow.com/questions/17708885/flock-removing-locked-file-without-race-condition
        fd = self._lock_file_fd
        self._lock_file_fd = None
        fcntl.flock(fd, fcntl.LOCK_UN)  # type: ignore
        os.close(fd)  # type: ignore
        return None
