#!/usr/bin/python3 -s

import time
import os
import fcntl
import subprocess
import uuid
import sys

from typing import Optional, Any

LOCK_DIR = '/run/cephadm'
DATA_DIR = '/var/lib/ceph'

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
    def __init__(self, name: str, timeout: int = -1) -> None:
        if not os.path.exists(LOCK_DIR):
            os.mkdir(LOCK_DIR, 0o700)
        self._lock_file = os.path.join(LOCK_DIR, name + '.lock')

        self._lock_file_fd: Optional[int] = None
        self.timeout = timeout
        self._lock_counter = 0
        return None

    @property
    def is_locked(self) -> bool:
        return self._lock_file_fd is not None

    def acquire(self, timeout: Optional[int] = None, poll_intervall: float = 0.05) -> _Acquire_ReturnProxy:
        # Use the default timeout, if no timeout is provided.
        if timeout is None:
            timeout = self.timeout

        # Increment the number right at the beginning.
        # We can still undo it, if something fails.
        self._lock_counter += 1

        start_time = time.time()
        try:
            while True:
                if not self.is_locked:
                    self._acquire()

                if self.is_locked:
                    break
                elif timeout >= 0 and time.time() - start_time > timeout:
                    raise Exception(self._lock_file)
                else:
                    time.sleep(poll_intervall)
        except Exception:
            # Something did go wrong, so decrement the counter.
            self._lock_counter = max(0, self._lock_counter - 1)

            raise
        return _Acquire_ReturnProxy(lock=self)

    def release(self, force: bool = False) -> None:
        if self.is_locked:
            self._lock_counter -= 1

            if self._lock_counter == 0 or force:
                self._release()
                self._lock_counter = 0

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
        fd = self._lock_file_fd
        self._lock_file_fd = None
        fcntl.flock(fd, fcntl.LOCK_UN)  # type: ignore
        os.close(fd)  # type: ignore
        return None

def _is_fsid(s):
    try:
        uuid.UUID(s)
    except ValueError:
        return False
    return True

def find_fsid():
    if not os.path.exists(DATA_DIR):
        raise Exception(f'{DATA_DIR} does not exist. Aborting...')

    for d in os.listdir(DATA_DIR):
        # assume the first thing we find that is an fsid
        # is what we want. Not expecting multiple clusters
        # to have been installed here.
        if _is_fsid(d):
            return d
    raise Exception(f'No fsid dir found in {DATA_DIR} does not exist. Aborting...')

def main():
    print('Looking for cluster fsid...')
    fsid = find_fsid()
    print(f'Found fsid {fsid}')

    print('Setting cephadm command timeout to 120...')
    subprocess.run(['cephadm', 'shell', '--', 'ceph', 'config', 'set',
                    'mgr', 'mgr/cephadm/default_cephadm_command_timeout', '120'],
                    check=True)

    print('Taking hold of cephadm lock for 300 seconds...')
    lock = FileLock(fsid, 300)
    lock.acquire()

    print('Triggering cephadm device refresh...')
    subprocess.run(['cephadm', 'shell', '--', 'ceph', 'orch', 'device', 'ls', '--refresh'],
                    check=True)

    print('Sleeping 150 seconds to allow for timeout to occur...')
    time.sleep(150)

    print('Checking ceph health detail...')
    # directing stdout to res.stdout via "capture_stdout" option
    # (and same for stderr) seems to have been added in python 3.7.
    # Using files so this works with 3.6 as well
    with open('/tmp/ceph-health-detail-stdout', 'w') as f_stdout:
        with open('/tmp/ceph-health-detail-stderr', 'w') as f_stderr:
            subprocess.run(['cephadm', 'shell', '--', 'ceph', 'health', 'detail'],
                           check=True, stdout=f_stdout, stderr=f_stderr)

    res_stdout = open('/tmp/ceph-health-detail-stdout', 'r').read()
    res_stderr = open('/tmp/ceph-health-detail-stderr', 'r').read()
    print(f'"cephadm shell -- ceph health detail" stdout:\n{res_stdout}')
    print(f'"cephadm shell -- ceph health detail" stderr:\n{res_stderr}')

    print('Checking for correct health warning in health detail...')
    if 'CEPHADM_REFRESH_FAILED' not in res_stdout:
        raise Exception('No health warning caused by timeout was raised')
    if 'Command "cephadm ceph-volume -- inventory" timed out' not in res_stdout:
        raise Exception('Health warnings did not contain message about time out')

    print('Health warnings found succesfully. Exiting.')
    return 0

    
if __name__ == '__main__':
    if os.getuid() != 0:
        print('Trying to run myself with sudo...')
        os.execvp('sudo', [sys.executable] + list(sys.argv))
    main()
