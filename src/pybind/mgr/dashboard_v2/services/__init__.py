# -*- coding: utf-8 -*-
from __future__ import absolute_import

from six import add_metaclass


class ServiceMeta(type):
    @property
    def mgr(cls):
        """
        :return: Returns the MgrModule instance of this Ceph dashboard module.
        """
        return cls._mgr_module

    @mgr.setter
    def mgr(cls, value):
        """
        :param value: The MgrModule instance of the Ceph dashboard module.
        """
        cls._mgr_module = value


@add_metaclass(ServiceMeta)
class Service(object):
    """
    Base class for all services.
    """
    _mgr_module = None

    @property
    def mgr(self):
        """
        :return: Returns the MgrModule instance of this Ceph module.
        """
        return self._mgr_module
