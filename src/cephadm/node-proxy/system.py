

class System:
    def __init__(self):
        self._system = {}

    def get_system(self):
        return self._system

    def get_status(self):
        return self._system['status']

    def get_metadata(self):
        return self._system['metadata']

    def get_processor(self):
        return self._system['processor']

    def get_memory(self):
        return self._system['memory']

    def get_power(self):
        return self._system['power']

    def get_network(self):
        return self._system['network']

    def get_storage(self):
        return self._system['storage']
