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
        self.config_key_map = {}

    def _ceph_get_version(self):
        return "ceph-13.0.0"

    def _ceph_get_mgr_id(self):
        return "x"

    def _ceph_set_config(self, key, value):
        self.config_key_map[key] = value

    def _ceph_get_config(self, key):
        return self.config_key_map.get(key, None)

    def _ceph_log(self, *args):
        pass


