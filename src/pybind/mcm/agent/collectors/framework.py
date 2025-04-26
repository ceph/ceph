from ..models.base import MCMAgentBase
from .interface import Collector
import importlib
import pkgutil
import mcm.agent.collectors
import sys

registered_collectors = {}

def register_collector(data_type: MCMAgentBase):
    def decorator(cls: Collector):
        cls._collector_data_type = data_type
        registered_collectors[cls] = data_type
        return cls
    return decorator

def load_all_collectors():
    """
    Dynamically discover, import or reload only actual collector plugins under subpackages of mcm.agent.collectors
    """
    clear_registry()
    base_package = mcm.agent.collectors
    prefix = base_package.__name__ + "."
    for finder, module_name, ispkg in pkgutil.walk_packages(base_package.__path__, prefix):
        if not module_name.startswith(prefix):
            continue  # Extra safety: must start with expected prefix
        remaining = module_name[len(prefix):]  # what comes after 'mcm.agent.collectors.'
        if "." not in remaining:
            # If no dot, it is direct module under collectors/ (like framework.py) → skip
            continue
        # Else, it's inside a subpackage (dashboard.cluster_details, cephadm.custom_container, etc)
        print("importing or reloading: ", module_name, "\n")
        if module_name in sys.modules:
            importlib.reload(sys.modules[module_name])
        else:
            importlib.import_module(module_name)

def clear_registry():
    registered_collectors.clear()
