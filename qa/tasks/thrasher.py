"""
Thrasher base class
"""
class Thrasher(object):

    def __init__(self, n):
        super(Thrasher, self).__init__()
        self._exception = None
        self._name = n

    @property
    def exception(self):
        return self._exception

    @exception.setter
    def exception(self, e):
        self._exception = e

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, n):
        self._name = n
