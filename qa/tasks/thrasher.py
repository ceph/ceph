"""
Thrasher base class
"""
from abc import ABCMeta, abstractmethod
class Thrasher(object):
    __metaclass__ = ABCMeta
    def __init__(self):
        super(Thrasher, self).__init__()
        self.exception = None

    @property
    def exception(self):
        return self._exception

    @exception.setter
    def exception(self, e):
        self._exception = e

    @abstractmethod
    def _do_thrash(self):
        pass
