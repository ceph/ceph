from dataclasses import dataclass, field, asdict
from .capacity import Capacity
from .base import MCMAgentBase
import threading
from .registry import registry

@dataclass
@registry.register()
class ClusterInfo(MCMAgentBase):
    _fsid: str
    _capacity: Capacity
    _version: str
    _health: str
    __lock: threading.Lock = field(repr=False)

    def __init__(self, fsid: str=None, used_bytes: float=0, total_bytes: float=0, health: str=None, version: str=None):
        self._fsid = fsid
        self.__lock = threading.Lock()
        self._capacity = Capacity(used_bytes, total_bytes)
        self._version = version
        self._health = health

    def to_json(self):
        data = asdict(self)
        data.pop('__lock')
        return data

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
        if value:
            if not isinstance(value, Capacity):
                raise TypeError("capacity must be an Capacity instance")
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