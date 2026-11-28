"""Validate discovery and registration of cephadm service handlers."""

import importlib
import inspect
import pkgutil

from cephadm import services
from cephadm.services.cephadmservice import CephadmService, CephService
from cephadm.services.service_registry import service_registry


def test_all_service_handlers_registered():
    service_registry._discover_services()
    # Snapshot before walking: walk_packages imports every module, which
    # would register handlers that discovery alone does not reach.
    registered = dict(service_registry._registered_services)

    discovered = {}

    for module_info in pkgutil.walk_packages(
        services.__path__,
        prefix=services.__name__ + '.',
    ):
        module = importlib.import_module(module_info.name)

        for name, cls in inspect.getmembers(module, inspect.isclass):
            if cls.__module__ != module.__name__:
                continue

            if not issubclass(cls, CephadmService):
                continue

            # Exclude shared service abstractions.
            if cls in (CephadmService, CephService):
                continue

            # Concrete handlers must explicitly define a string TYPE.
            service_type = cls.__dict__.get('TYPE')
            if not isinstance(service_type, str):
                continue

            assert service_type not in discovered, (
                f'Duplicate service type {service_type!r}: '
                f'{discovered.get(service_type)} and {cls}'
            )

            discovered[service_type] = cls

            # Discovery imports only top-level modules and packages, so a
            # handler in a subpackage is reachable only via its __init__.
            relative = module.__name__[len(services.__name__) + 1:]
            if '.' in relative:
                package = importlib.import_module(
                    f'{services.__name__}.{relative.split(".")[0]}')
                assert getattr(package, name, None) is cls, (
                    f'{cls.__module__}.{name} is not exported '
                    f'by {package.__name__}/__init__.py'
                )

            assert registered.get(service_type) is cls, (
                f'{cls.__module__}.{name} '
                f'is not correctly registered for {service_type!r}'
            )

    assert discovered
    assert registered == discovered


def test_service_class_lookup_without_init():
    """get_service_class() resolves registered types before init_services()."""
    from cephadm.services.rgw import RgwService
    assert service_registry.get_service_class('rgw') is RgwService
    assert service_registry.get_service_class('no-such-service') is None


def test_default_placement_returns_a_copy():
    """Each call returns a separate PlacementSpec equal to the class default."""
    svc_cls = service_registry.get_service_class('crash')
    assert svc_cls is not None
    first = svc_cls.default_placement()
    second = svc_cls.default_placement()
    assert first is not None and first == svc_cls.DEFAULT_PLACEMENT
    assert first is not second
    assert first is not svc_cls.DEFAULT_PLACEMENT
    assert service_registry.get_service_class('agent').default_placement() is None


def test_config_section_and_auth_entity_come_from_service_class():
    """The entity helpers follow the attributes set on the service classes."""
    from cephadm.services.cephadmservice import get_auth_entity
    from cephadm.utils import name_to_config_section
    assert name_to_config_section('rgw.foo.a') == 'client.rgw.foo.a'
    assert name_to_config_section('crash.host1') == 'client.crash.host1'
    assert name_to_config_section('osd.1') == 'osd.1'
    assert name_to_config_section('grafana.host1') == 'mon'
    assert get_auth_entity('ceph-exporter', 'host1') == 'client.ceph-exporter.host1'
    assert get_auth_entity('node-proxy', 'x', host='host1') == 'client.node-proxy.host1'
    assert get_auth_entity('osd', '3') == 'osd.3'


# Daemon types the upgrade loop does not walk today. A new service either
# gets a place in utils.CEPH_UPGRADE_ORDER or is added here on purpose.
NOT_IN_UPGRADE_ORDER = {
    'agent',
    'container',
    'elasticsearch',
    'jaeger-agent',
    'jaeger-collector',
    'jaeger-query',
    'node-proxy',
    'snmp-gateway',
}


def test_every_daemon_type_has_an_upgrade_decision():
    """Each registered service's daemon types are either ordered for upgrade or listed as not upgraded."""
    from cephadm.utils import CEPH_UPGRADE_ORDER
    from orchestrator._interface import service_to_daemon_types
    service_registry._discover_services()
    for service_type in service_registry._registered_services:
        for daemon_type in service_to_daemon_types(service_type):
            assert (daemon_type in CEPH_UPGRADE_ORDER) != (daemon_type in NOT_IN_UPGRADE_ORDER), (
                f'{daemon_type}: add it to CEPH_UPGRADE_ORDER in cephadm/utils.py '
                f'or to NOT_IN_UPGRADE_ORDER in this test')
