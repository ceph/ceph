from typing import Dict, List, Type

from ceph_node_proxy.atollon import AtollonRedfishProvider
from ceph_node_proxy.baseredfishsystem import BaseRedfishSystem
from ceph_node_proxy.local_collectors import FCMCollector, LocalCollector, LocalCollectorRunner
from ceph_node_proxy.node_backend import NodeBackend
from ceph_node_proxy.redfishdellsystem import RedfishDellSystem
from ceph_node_proxy.util import Config, get_logger

REDFISH_PROVIDER_CLASSES: Dict[str, Type[BaseRedfishSystem]] = {
    "generic": BaseRedfishSystem,
    "dell": RedfishDellSystem,
    "atollon": AtollonRedfishProvider,
}

LOCAL_COLLECTOR_CLASSES_BY_VENDOR: Dict[str, List[Type[LocalCollector]]] = {
    "atollon": [FCMCollector],
}

logger = get_logger(__name__)


def _load_entry_point_redfish_providers() -> None:
    try:
        import pkg_resources  # type: ignore[import-not-found]

    except ImportError:
        logger.debug(
            "pkg_resources not available; only built-in Redfish providers will be used."
        )
        return

    for ep in pkg_resources.iter_entry_points("ceph_node_proxy.systems"):
        try:
            REDFISH_PROVIDER_CLASSES[ep.name] = ep.load()
        except (ImportError, AttributeError, ModuleNotFoundError) as exc:
            logger.warning(
                "Failed to load Redfish provider entry point %s: %s", ep.name, exc
            )


def get_redfish_provider_class(vendor: str) -> Type[BaseRedfishSystem]:
    """Return the Redfish provider class for the given vendor."""
    return REDFISH_PROVIDER_CLASSES.get(vendor, BaseRedfishSystem)


def get_system_class(vendor: str) -> Type[BaseRedfishSystem]:
    """Deprecated alias for get_redfish_provider_class."""
    return get_redfish_provider_class(vendor)


def create_local_collector_runner(vendor: str) -> LocalCollectorRunner:
    """Build the local collector runner configured for a vendor."""
    collector_classes = LOCAL_COLLECTOR_CLASSES_BY_VENDOR.get(vendor, [])
    collectors = [collector_cls() for collector_cls in collector_classes]
    return LocalCollectorRunner(collectors)


def create_node_backend(
    vendor: str,
    *,
    host: str,
    port: str,
    username: str,
    password: str,
    config: Config,
) -> NodeBackend:
    """Create the node backend facade for a vendor."""
    provider_cls = get_redfish_provider_class(vendor)
    redfish = provider_cls(
        host=host,
        port=port,
        username=username,
        password=password,
        config=config,
    )
    local_collectors = create_local_collector_runner(vendor)
    return NodeBackend(redfish, local_collectors)


_load_entry_point_redfish_providers()

# Backward-compatible alias used by older imports and entry point docs.
REDFISH_SYSTEM_CLASSES = REDFISH_PROVIDER_CLASSES
