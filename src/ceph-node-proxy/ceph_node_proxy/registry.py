from typing import Dict, Type

# Built-in implementations
from ceph_node_proxy.atollon import AtollonSystem
from ceph_node_proxy.baseredfishsystem import BaseRedfishSystem
from ceph_node_proxy.redfishdellsystem import RedfishDellSystem
from ceph_node_proxy.util import get_logger

REDFISH_SYSTEM_CLASSES: Dict[str, Type[BaseRedfishSystem]] = {
    "generic": BaseRedfishSystem,
    "dell": RedfishDellSystem,
    "atollon": AtollonSystem,
}

logger = get_logger(__name__)


def _load_entry_point_systems() -> None:
    try:
        import pkg_resources  # type: ignore[import-not-found]

    except ImportError:
        logger.debug(
            "pkg_resources not available; only built-in Redfish systems will be used."
        )
        return

    for ep in pkg_resources.iter_entry_points("ceph_node_proxy.systems"):
        try:
            REDFISH_SYSTEM_CLASSES[ep.name] = ep.load()
        except (ImportError, AttributeError, ModuleNotFoundError) as e:
            logger.warning(
                "Failed to load Redfish system entry point %s: %s", ep.name, e
            )


def get_system_class(vendor: str) -> Type[BaseRedfishSystem]:
    """Return the Redfish system class for the given vendor.
    Falls back to generic."""
    return REDFISH_SYSTEM_CLASSES.get(vendor, BaseRedfishSystem)


_load_entry_point_systems()
