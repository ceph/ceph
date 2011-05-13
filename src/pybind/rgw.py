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
        self.lib = CDLL('librgw.so')
    def acl_bin2xml(self, blob):
        blob_buf = ctypes.create_string_buffer(blob[:])
        xml = c_char_p(0)
        ret = self.lib.librgw_acl_bin2xml(byref(blob_buf), len(blob), byref(xml))
        if (ret != 0):
            raise Exception(ret, "acl_bin2xml failed with error %d" % ret)
        retstr = str(xml)
        self.lib.librgw_free_xml(xml)
        return retstr
    def acl_xml2bin(self, xml):
        blen = c_int(0)
        blob = c_void_p(0)
        print "WATERMELON 1"
        ret = self.lib.librgw_acl_xml2bin(c_char_p(xml), byref(blob), byref(blen))
        if (ret != 0):
            raise Exception(ret, "acl_bin2xml failed with error %d" % ret)
        print "WATERMELON 2"
        retblob = ctypes.cast(blob, ctypes.POINTER(ctypes.c_ubyte * blen.value))
        rets = str(retblob)
        self.lib.librgw_free_bin(blob)
        return rets
