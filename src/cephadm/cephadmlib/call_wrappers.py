# call_wrappers.py - functions to wrap calling external commands


import asyncio
import logging
import os
import subprocess
import sys

from enum import Enum
from typing import Callable, List, Dict, Optional, Any, Tuple, NoReturn

from .constants import QUIET_LOG_LEVEL, DEFAULT_TIMEOUT
from .context import CephadmContext
from .exceptions import TimeoutExpired

logger = logging.getLogger()


async def run_func(func: Callable, cmd: str) -> subprocess.CompletedProcess:
    logger.debug(f'running function {func.__name__}, with parms: {cmd}')
    response = func(cmd)
    return response


async def concurrent_tasks(func: Callable, cmd_list: List[str]) -> List[Any]:
    tasks = []
    for cmd in cmd_list:
        tasks.append(run_func(func, cmd))

    data = await asyncio.gather(*tasks)

    return data


class CallVerbosity(Enum):
    #####
    # Format:
    # Normal Operation: <log-level-when-no-errors>, Errors: <log-level-when-error>
    #
    # NOTE: QUIET log level is custom level only used when --verbose is passed
    #####

    # Normal Operation: None, Errors: None
    SILENT = 0
    # Normal Operation: QUIET, Error: QUIET
    QUIET = 1
    # Normal Operation: DEBUG, Error: DEBUG
    DEBUG = 2
    # Normal Operation: QUIET, Error: INFO
    QUIET_UNLESS_ERROR = 3
    # Normal Operation: DEBUG, Error: INFO
    VERBOSE_ON_FAILURE = 4
    # Normal Operation: INFO, Error: INFO
    VERBOSE = 5

    def success_log_level(self) -> int:
        _verbosity_level_to_log_level = {
            self.SILENT: 0,
            self.QUIET: QUIET_LOG_LEVEL,
            self.DEBUG: logging.DEBUG,
            self.QUIET_UNLESS_ERROR: QUIET_LOG_LEVEL,
            self.VERBOSE_ON_FAILURE: logging.DEBUG,
            self.VERBOSE: logging.INFO,
        }
        return _verbosity_level_to_log_level[self]  # type: ignore

    def error_log_level(self) -> int:
        _verbosity_level_to_log_level = {
            self.SILENT: 0,
            self.QUIET: QUIET_LOG_LEVEL,
            self.DEBUG: logging.DEBUG,
            self.QUIET_UNLESS_ERROR: logging.INFO,
            self.VERBOSE_ON_FAILURE: logging.INFO,
            self.VERBOSE: logging.INFO,
        }
        return _verbosity_level_to_log_level[self]  # type: ignore


# disable coverage for the next block. this is copy-n-paste
# from other code for compatibilty on older python versions
if sys.version_info < (3, 8):  # pragma: no cover
    import itertools
    import threading
    import warnings
    from asyncio import events

    class ThreadedChildWatcher(asyncio.AbstractChildWatcher):
        """Threaded child watcher implementation.
        The watcher uses a thread per process
        for waiting for the process finish.
        It doesn't require subscription on POSIX signal
        but a thread creation is not free.
        The watcher has O(1) complexity, its performance doesn't depend
        on amount of spawn processes.
        """

        def __init__(self) -> None:
            self._pid_counter = itertools.count(0)
            self._threads: Dict[Any, Any] = {}

        def is_active(self) -> bool:
            return True

        def close(self) -> None:
            self._join_threads()

        def _join_threads(self) -> None:
            """Internal: Join all non-daemon threads"""
            threads = [
                thread
                for thread in list(self._threads.values())
                if thread.is_alive() and not thread.daemon
            ]
            for thread in threads:
                thread.join()

        def __enter__(self) -> Any:
            return self

        def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            pass

        def __del__(self, _warn: Any = warnings.warn) -> None:
            threads = [
                thread
                for thread in list(self._threads.values())
                if thread.is_alive()
            ]
            if threads:
                _warn(
                    f'{self.__class__} has registered but not finished child processes',
                    ResourceWarning,
                    source=self,
                )

        def add_child_handler(
            self, pid: Any, callback: Any, *args: Any
        ) -> None:
            loop = events.get_event_loop()
            thread = threading.Thread(
                target=self._do_waitpid,
                name=f'waitpid-{next(self._pid_counter)}',
                args=(loop, pid, callback, args),
                daemon=True,
            )
            self._threads[pid] = thread
            thread.start()

        def remove_child_handler(self, pid: Any) -> bool:
            # asyncio never calls remove_child_handler() !!!
            # The method is no-op but is implemented because
            # abstract base classe requires it
            return True

        def attach_loop(self, loop: Any) -> None:
            pass

        def _do_waitpid(
            self, loop: Any, expected_pid: Any, callback: Any, args: Any
        ) -> None:
            assert expected_pid > 0

            try:
                pid, status = os.waitpid(expected_pid, 0)
            except ChildProcessError:
                # The child process is already reaped
                # (may happen if waitpid() is called elsewhere).
                pid = expected_pid
                returncode = 255
                logger.warning(
                    'Unknown child process pid %d, will report returncode 255',
                    pid,
                )
            else:
                if os.WIFEXITED(status):
                    returncode = os.WEXITSTATUS(status)
                elif os.WIFSIGNALED(status):
                    returncode = -os.WTERMSIG(status)
                else:
                    raise ValueError(f'unknown wait status {status}')
                if loop.get_debug():
                    logger.debug(
                        'process %s exited with returncode %s',
                        expected_pid,
                        returncode,
                    )

            if loop.is_closed():
                logger.warning(
                    'Loop %r that handles pid %r is closed', loop, pid
                )
            else:
                loop.call_soon_threadsafe(callback, pid, returncode, *args)

            self._threads.pop(expected_pid)

    # unlike SafeChildWatcher which handles SIGCHLD in the main thread,
    # ThreadedChildWatcher runs in a separated thread, hence allows us to
    # run create_subprocess_exec() in non-main thread, see
    # https://bugs.python.org/issue35621
    asyncio.set_child_watcher(ThreadedChildWatcher())


try:
    from asyncio import run as async_run  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover
    # disable coverage for this block. it should be a copy-n-paste from
    # from newer libs for compatibilty on older python versions
    def async_run(coro):  # type: ignore
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(coro)
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            finally:
                asyncio.set_event_loop(None)
                loop.close()


def call(
    ctx: CephadmContext,
    command: List[str],
    desc: Optional[str] = None,
    verbosity: CallVerbosity = CallVerbosity.VERBOSE_ON_FAILURE,
    timeout: Optional[int] = DEFAULT_TIMEOUT,
    **kwargs: Any,
) -> Tuple[str, str, int]:
    """
    Wrap subprocess.Popen to

    - log stdout/stderr to a logger,
    - decode utf-8
    - cleanly return out, err, returncode

    :param timeout: timeout in seconds
    """

    prefix = command[0] if desc is None else desc
    if prefix:
        prefix += ': '
    timeout = timeout or ctx.timeout

    async def run_with_timeout() -> Tuple[str, str, int]:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=os.environ.copy(),
        )
        assert process.stdout
        assert process.stderr
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout,
            )
        except asyncio.TimeoutError:
            # try to terminate the process assuming it is still running.  It's
            # possible that even after killing the process it will not
            # complete, particularly if it is D-state.  If that happens the
            # process.wait call will block, but we're no worse off than before
            # when the timeout did not work.  Additionally, there are other
            # corner-cases we could try and handle here but we decided to start
            # simple.
            process.kill()
            await process.wait()
            logger.info(prefix + f'timeout after {timeout} seconds')
            return '', '', 124
        else:
            assert process.returncode is not None
            return (
                stdout.decode('utf-8'),
                stderr.decode('utf-8'),
                process.returncode,
            )

    stdout, stderr, returncode = async_run(run_with_timeout())
    log_level = verbosity.success_log_level()
    if returncode != 0:
        log_level = verbosity.error_log_level()
        logger.log(
            log_level,
            f'Non-zero exit code {returncode} from {" ".join(command)}',
        )
    for line in stdout.splitlines():
        logger.log(log_level, prefix + 'stdout ' + line)
    for line in stderr.splitlines():
        logger.log(log_level, prefix + 'stderr ' + line)
    return stdout, stderr, returncode


def call_throws(
    ctx: CephadmContext,
    command: List[str],
    desc: Optional[str] = None,
    verbosity: CallVerbosity = CallVerbosity.VERBOSE_ON_FAILURE,
    timeout: Optional[int] = DEFAULT_TIMEOUT,
    **kwargs: Any,
) -> Tuple[str, str, int]:
    out, err, ret = call(ctx, command, desc, verbosity, timeout, **kwargs)
    if ret:
        for s in (out, err):
            if s.strip() and len(s.splitlines()) <= 2:  # readable message?
                raise RuntimeError(
                    f'Failed command: {" ".join(command)}: {s}'
                )
        raise RuntimeError('Failed command: %s' % ' '.join(command))
    return out, err, ret


def call_timeout(ctx, command, timeout):
    # type: (CephadmContext, List[str], int) -> int
    logger.debug(
        'Running command (timeout=%s): %s' % (timeout, ' '.join(command))
    )

    def raise_timeout(command, timeout):
        # type: (List[str], int) -> NoReturn
        msg = 'Command `%s` timed out after %s seconds' % (command, timeout)
        logger.debug(msg)
        raise TimeoutExpired(msg)

    try:
        return subprocess.call(
            command, timeout=timeout, env=os.environ.copy()
        )
    except subprocess.TimeoutExpired:
        raise_timeout(command, timeout)
