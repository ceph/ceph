import json

class MCMAgentBase():
    def remove_unserializable_attributes(self):
        for attr in list(vars(self).keys()):  # list of attribute names
            value = getattr(self, attr)
            try:
                json.dumps(value)  # Try to serialize
            except (TypeError, OverflowError):
                delattr(self, attr)  # If fails, remove attribute
