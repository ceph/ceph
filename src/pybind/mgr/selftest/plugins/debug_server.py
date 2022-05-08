import sys
import threading
from . import Plugin

try:
    import epdb
except:
    pass
    

class DebugServer(Plugin):
    def __init__(self, mgr):
        super(DebugServer, self).__init__(mgr)
        self.port = mgr.get_module_option('epdb_port', default=7777)
        self.epdb_thread = None

    def serve(self):
        if 'epdb' not in sys.modules:
            raise RuntimeError('epdb module couldn\'t be imported, please install epdb.')
        self.epdb_thread = threading.Thread(target=epdb.serve, args=(self.port,))
        self.epdb_thread.start()

    def shutdown(self):
        del(self.epdb_thread)
