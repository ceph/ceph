# -*- coding: utf-8 -*-

from mock import Mock

class BasePyOSDMap(object):
    pass

class BasePyOSDMapIncremental(object):
    pass

class BasePyCRUSH(object):
    pass

class BaseMgrStandbyModule(object):
    pass

class BaseMgrModule(object):
    def __init__(self, py_modules_ptr, this_ptr):
        pass

    def _ceph_get_version(self):
        return "ceph-13.0.0"

    def _ceph_log(self, *args):
        pass


