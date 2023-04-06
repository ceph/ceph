from util import Config

class System:
    def __init__(self, **kw):
        self._system = {}
        self.config: Config = kw['config']

    def get_system(self):
        raise NotImplementedError()

    def get_status(self):
        raise NotImplementedError()

    def get_metadata(self):
        raise NotImplementedError()

    def get_processors(self):
        raise NotImplementedError()

    def get_memory(self):
        raise NotImplementedError()

    def get_power(self):
        raise NotImplementedError()

    def get_network(self):
        raise NotImplementedError()

    def get_storage(self):
        raise NotImplementedError()
