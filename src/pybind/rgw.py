"""librgw Python ctypes wrapper
Copyright 2011, New Dream Network
"""
from ctypes import CDLL, c_char_p, c_size_t, c_void_p,\
    create_string_buffer, byref, Structure, c_uint64, c_ubyte, c_byte,\
    pointer, c_int
import ctypes
import datetime
import errno
import time

class Rgw(object):
    """librgw python wrapper"""
    def __init__(self):
        self.lib = CDLL('librgw.so.1')
        self.rgw = c_void_p(0)
        ret = self.lib.librgw_create(byref(self.rgw), 0)
        if (ret != 0):
            raise Exception("librgw_create failed with error %d" % ret)
    def __del__(self):
        self.lib.librgw_shutdown(self.rgw)
    def acl_bin2xml(self, blob):
        blob_buf = ctypes.create_string_buffer(blob[:])
        xml = c_char_p(0)
        ret = self.lib.librgw_acl_bin2xml(self.rgw, byref(blob_buf), len(blob), byref(xml))
        if (ret != 0):
            raise Exception("acl_bin2xml failed with error %d" % ret)
        rets = ctypes.string_at(xml)
        self.lib.librgw_free_xml(self.rgw, xml)
        return rets
    def acl_xml2bin(self, xml):
        blen = c_int(0)
        blob = c_void_p(0)
        ret = self.lib.librgw_acl_xml2bin(self.rgw, c_char_p(xml),
                                          byref(blob), byref(blen))
        if (ret != 0):
            raise Exception("acl_bin2xml failed with error %d" % ret)
        rets = ctypes.string_at(blob, blen)
        self.lib.librgw_free_bin(self.rgw, blob)
        return rets
