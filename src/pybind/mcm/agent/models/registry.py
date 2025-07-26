class ObjectRegistry:
    def __init__(self):
        self._registry = {}

    def register(self, name=None):
        def decorator(cls):
            key = name or cls.__name__
            self._registry[key] = cls()  # or store cls if you want the class
            return cls
        return decorator

    def get(self, name):
        return self._registry.get(name)

    def all(self):
        for name, obj in self._registry.items():
            print(f"Registered object: {name} → {obj}")
        return self._registry
    
registry = ObjectRegistry()