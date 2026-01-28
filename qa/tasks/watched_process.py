"""
WatchedProcess process base class.

This can be applied to an object that we want the DaemonWatchdog to monitor
for failure, and bark when it sees an error
"""
import logging

from abc import ABCMeta, abstractmethod
from typing import Optional, Any, Dict

from gevent.greenlet import Greenlet

class WatchedProcess(Greenlet, metaclass=ABCMeta):
    """
    WatchedProcess:

    The abstract base class for a process that the DaemonWatchdog can monitor
    for errors. It is based on the ThrasherGreenlet class in qa/tasks/thrasher.py
    """
    def __init__(self, ctx: Dict[Any, Any], log, process: str, cluster: str, sub_processes: Dict[str, Any]) -> None:
        self.log = log
        self._exception: Optional[BaseException] = None
        self._sub_processes = sub_processes
        self._name: str = f"{process}-{cluster}"
        ctx.ceph[cluster].watched_processes.append(self)

    @property
    def exception(self) -> Optional[BaseException]:
        return self._exception

    @property
    def id(self) -> str:
        """
        Return a string identifier for this process
        """
        return self._name

    def set_exception(self, e: Exception) -> None:
        """
        Set the exception for this process
        """
        self._exception = e

    def try_set_exception(self, e: Exception) -> None:
        """
        Set the exception for this process unless there is a previous exception
        """
        if (self._exception == None):
            self._exception = e

    def stop(self) -> None:
        """
        Stop the remote process running
        """
        debug: str = f"Stopping {self._name}"
        if self._exception:
            debug += f" due to exception {self._exception}"
        self.log.debug(debug)
        for test_id, proc in self._sub_processes.items():
            self.log.info("Stopping instance %s", test_id)
            proc.stdin.close()
