from ..models.base import MCMAgentBase
from .interface import Collector
import importlib
import pkgutil
import mcm.agent.collectors

registered_collectors = {}

def register_collector(data_type: MCMAgentBase):
    def decorator(cls: Collector):
        cls._collector_data_type = data_type
        registered_collectors[cls] = data_type
        return cls
    return decorator

def load_all_collectors():
    base_package = mcm.agent.collectors
    prefix = base_package.__name__ + "."
    for finder, subpackage_name, ispkg in pkgutil.walk_packages(base_package.__path__, prefix):
        #try:
        imported = importlib.import_module(subpackage_name)
        if hasattr(imported, "__path__"):  # If it's a package
            for _, name, _ in pkgutil.walk_packages(imported.__path__, subpackage_name + "."):
                importlib.import_module(name)
        #except Exception as e:
        #    print(f"Failed to import {full_module_name}: {e}")

def clear_registry():
    registered_collectors.clear()
