"""
Thrasher base class
"""
class Thrasher(object):

    def __init__(self):
        super(Thrasher, self).__init__()
        self._exception = None

    @property
    def exception(self):
        return self._exception

    def set_thrasher_exception(self, e):
        self._exception = e
