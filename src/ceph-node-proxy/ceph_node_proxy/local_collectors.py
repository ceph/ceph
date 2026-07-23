from abc import ABC, abstractmethod
from typing import Any, Dict

from ceph_node_proxy.fcm_stats import collect_fcm_stats


class LocalCollector(ABC):
    """Collects node-local hardware metrics outside of Redfish."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Category name exposed in node-proxy reports."""

    @abstractmethod
    def update(self) -> None:
        """Refresh collector data from the local OS."""

    @abstractmethod
    def get_data(self) -> Dict[str, Any]:
        """Return the latest collected data."""

    def flush(self) -> None:
        """Clear cached collector data."""


class LocalCollectorRunner:
    def __init__(self, collectors: list[LocalCollector]) -> None:
        self._collectors = collectors

    def update(self) -> None:
        for collector in self._collectors:
            collector.update()

    def flush(self) -> None:
        for collector in self._collectors:
            collector.flush()

    def categories(self) -> list[str]:
        return [collector.name for collector in self._collectors]

    def get_category(self, name: str) -> Dict[str, Any]:
        for collector in self._collectors:
            if collector.name == name:
                return collector.get_data()
        return {}


class FCMCollector(LocalCollector):
    """Collect FCM stats via NVMe ioctls."""

    def __init__(self) -> None:
        self._data: Dict[str, Any] = {}

    @property
    def name(self) -> str:
        return "fcm"

    def update(self) -> None:
        self._data = {"local": collect_fcm_stats()}

    def get_data(self) -> Dict[str, Any]:
        return dict(self._data)

    def flush(self) -> None:
        self._data = {}
