from ..models.base import MCMAgentBase
from .interface import Despatcher
import importlib
import pkgutil

registered_despatchers = {}

def register(data_type: MCMAgentBase):
    def decorator(cls: Despatcher):
        cls._data_type = data_type
        registered_despatchers[data_type] = cls
        return cls
    return decorator

def load_all_despatchers():
    try:
        import mcm.agent.despatchers.plugins
        print("despatcher imported")
        for _, name, _ in pkgutil.walk_packages(mcm.agent.despatchers.plugins.__path__, prefix="mcm.agent.despatchers.plugins."):
            module = importlib.import_module(name)
            importlib.reload(module)
    except Exception as e:
        print(f"Failed to load dispatcher plugins: {e}")

def clear_registry():
    registered_despatchers.clear()