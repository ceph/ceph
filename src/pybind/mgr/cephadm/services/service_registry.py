"""
Cephadm Service Registry

This module provides a centralized service registry for managing and initializing Cephadm services.
It dynamically discovers, imports, and registers service classes in the services directory, ensuring
modularity and scalability. The `@register_cephadm_service` decorator relies on an automatic discovery
mechanism based on `pkgutil` and `importlib` which dynamically import modules, triggering the decorator
for service registration and eliminating the need for manual imports.

Key Features:
- Automatically discovers and imports all service modules during initialization.
- Registers service classes using the `@register_cephadm_service`.
- Manages the lifecycle of service instances, initialized via `init_services`.
- Provides a singleton `service_registry` for global access.

Usage:
1. Define a service class by deriving from the CephadmService base class.
2. Place @register_cephadm_service decorator above your class to register it automatically.
3. Call `service_registry.init_services(mgr)` to initialize all registered services.
4. Access services using `service_registry.get_service(service_type)`.
"""

import os
import logging
from typing import Type, Dict, TYPE_CHECKING
import importlib
import pkgutil

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator
    from .cephadmservice import CephadmService

logger = logging.getLogger(__name__)


class CephadmServiceRegistry:
    """Centralized registry for Cephadm services."""

    def __init__(self) -> None:
        self._services: Dict[str, "CephadmService"] = {}  # Initialized service instances
        self._registered_services: Dict[str, Type["CephadmService"]] = {}  # Mapping of service types to classes

    def register_service(self, service_type: str, service_cls: Type["CephadmService"]) -> None:
        """Registers a service class to the registry."""
        if service_type in self._registered_services:
            logger.warning(f"Service type '{service_type}' is already registered.")
        logger.debug(f"Registering service: {service_type}")
        self._registered_services[service_type] = service_cls

    def init_services(self, mgr: "CephadmOrchestrator") -> None:
        """Initializes and stores service instances using the provided orchestrator manager."""
        self._discover_services()
        for service_type, service_cls in self._registered_services.items():
            logger.debug(f"Initializing service: {service_type}")
            self._services[service_type] = service_cls(mgr)

    def _discover_services(self) -> None:
        """Dynamically discovers and imports all modules in the current directory."""
        current_package = __name__.rsplit(".", 1)[0]  # Get the package name
        package_path = os.path.dirname(__file__)  # Directory of this file
        for _, module_name, _ in pkgutil.iter_modules([package_path]):
            logger.debug(f"Importing service module: {current_package}.{module_name}")
            importlib.import_module(f"{current_package}.{module_name}")

    def get_service(self, service_type: str) -> "CephadmService":
        """Retrieves an initialized service instance by type."""
        return self._services[service_type]


def register_cephadm_service(cls: Type["CephadmService"]) -> Type["CephadmService"]:
    """
    Decorator to register a service class with the global service registry.
    """
    service_registry.register_service(str(cls.TYPE), cls)
    return cls


# Singleton instance of the registry
service_registry = CephadmServiceRegistry()
