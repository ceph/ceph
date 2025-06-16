from dataclasses import dataclass, field, asdict
from .capacity import Capacity
from .base import MCMAgentBase
import threading
from .registry import registry

@dataclass
@registry.register()
class ClusterInfo(MCMAgentBase):
    _fsid: str = None
    _capacity: Capacity = None
    _version: str = None
    _health: str = None
    __lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def __init__(self, fsid: str = None, used_bytes: float = 0, total_bytes: float = 0, health: str = None, version: str = None):
        self.__lock = threading.Lock()
        self._fsid = fsid
        self._capacity = Capacity(used_bytes, total_bytes)
        self._version = version
        self._health = health

    @property
    def fsid(self):
        with self.__lock:
            return self._fsid

    @fsid.setter
    def fsid(self, value: str):
        with self.__lock:
            self._fsid = value

    @property
    def capacity(self):
        with self.__lock:
            return self._capacity

    @capacity.setter
    def capacity(self, value: Capacity):
        if not isinstance(value, Capacity):
            raise TypeError("capacity must be a Capacity instance")
        with self.__lock:
            self._capacity = value

    @property
    def version(self):
        with self.__lock:
            return self._version

    @version.setter
    def version(self, value: str):
        with self.__lock:
            self._version = value

    @property
    def health(self):
        with self.__lock:
            return self._health

    @health.setter
    def health(self, value: str):
        with self.__lock:
            self._health = value

    def to_json(self) -> dict:
        with self.__lock:
            return {
                "fsid": self._fsid,
                "capacity": {
                    "used": self._capacity.used.value,
                    "total": self._capacity.total.value,
                    "usage": self._capacity.usage,
                },
                "version": self._version,
                "health": self._health,
            }
