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
