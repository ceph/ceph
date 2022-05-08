class Plugin:
    def __init__(self, mgr):
        self.mgr = mgr

    def serve(self):
        pass

    def shutdown(self):
        pass


__all__ = []

plugin_names = ['debug_server']
__all__.extend(plugin_names)
