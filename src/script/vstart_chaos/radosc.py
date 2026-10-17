"""Minimal ctypes wrapper over the librados C API, so reads can carry
LIBRADOS_OPERATION_BALANCE_READS / LOCALIZE_READS (the python bindings have no
data read op that accepts flags)."""

import ctypes
import os

LIB = os.environ.get("RADOSC_LIB", os.environ.get("CEPH_BUILD", ".") + "/lib/librados.so.2")

NOFLAG = 0
BALANCE_READS = 1
LOCALIZE_READS = 2
POLICY_FLAGS = {"none": NOFLAG, "balance": BALANCE_READS,
                "localize": LOCALIZE_READS}

_l = ctypes.CDLL(LIB)
_vp = ctypes.c_void_p
_l.rados_create.argtypes = [ctypes.POINTER(_vp), ctypes.c_char_p]
_l.rados_conf_read_file.argtypes = [_vp, ctypes.c_char_p]
_l.rados_conf_set.argtypes = [_vp, ctypes.c_char_p, ctypes.c_char_p]
_l.rados_connect.argtypes = [_vp]
_l.rados_shutdown.argtypes = [_vp]
_l.rados_ioctx_create.argtypes = [_vp, ctypes.c_char_p, ctypes.POINTER(_vp)]
_l.rados_ioctx_destroy.argtypes = [_vp]
_l.rados_write_full.argtypes = [_vp, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_size_t]
_l.rados_write.argtypes = [_vp, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_size_t,
                           ctypes.c_uint64]
_l.rados_remove.argtypes = [_vp, ctypes.c_char_p]
_l.rados_create_read_op.restype = _vp
_l.rados_read_op_read.argtypes = [_vp, ctypes.c_uint64, ctypes.c_size_t,
                                  ctypes.c_char_p,
                                  ctypes.POINTER(ctypes.c_size_t),
                                  ctypes.POINTER(ctypes.c_int)]
_l.rados_read_op_read.restype = None
_l.rados_read_op_operate.argtypes = [_vp, _vp, ctypes.c_char_p, ctypes.c_int]
_l.rados_release_read_op.argtypes = [_vp]
_l.rados_release_read_op.restype = None


class RadosError(Exception):
    def __init__(self, what, rc):
        super().__init__(f"{what}: rc={rc} ({os.strerror(-rc) if rc < 0 else ''})")
        self.rc = rc


class Client:
    def __init__(self, pool, conf=None, crush_location=None, extra=None):
        self.cluster = _vp()
        rc = _l.rados_create(ctypes.byref(self.cluster), b"admin")
        if rc:
            raise RadosError("rados_create", rc)
        conffile = conf or os.environ.get("CEPH_CONF", os.environ.get("CEPH_BUILD", ".") + "/ceph.conf")
        _l.rados_conf_read_file(self.cluster, conffile.encode())
        keyring = os.environ.get("CEPH_KEYRING", os.environ.get("CEPH_BUILD", ".") + "/keyring")
        _l.rados_conf_set(self.cluster, b"keyring", keyring.encode())
        if crush_location:
            _l.rados_conf_set(self.cluster, b"crush_location",
                              crush_location.encode())
        for k, v in (extra or {}).items():
            _l.rados_conf_set(self.cluster, k.encode(), str(v).encode())
        rc = _l.rados_connect(self.cluster)
        if rc:
            raise RadosError("rados_connect", rc)
        self.io = _vp()
        rc = _l.rados_ioctx_create(self.cluster, pool.encode(), ctypes.byref(self.io))
        if rc:
            raise RadosError("rados_ioctx_create", rc)

    def write_full(self, oid, data):
        rc = _l.rados_write_full(self.io, oid.encode(), data, len(data))
        if rc < 0:
            raise RadosError(f"write_full {oid}", rc)

    def write(self, oid, data, off):
        rc = _l.rados_write(self.io, oid.encode(), data, len(data), off)
        if rc < 0:
            raise RadosError(f"write {oid}@{off}", rc)

    def remove(self, oid):
        return _l.rados_remove(self.io, oid.encode())

    def read(self, oid, off, length, flags=NOFLAG):
        op = _l.rados_create_read_op()
        buf = ctypes.create_string_buffer(length)
        got = ctypes.c_size_t(0)
        prval = ctypes.c_int(0)
        try:
            _l.rados_read_op_read(op, off, length, buf, ctypes.byref(got),
                                  ctypes.byref(prval))
            rc = _l.rados_read_op_operate(op, self.io, oid.encode(), flags)
        finally:
            _l.rados_release_read_op(op)
        if rc < 0:
            raise RadosError(f"read {oid}@{off}+{length}", rc)
        if prval.value < 0:
            raise RadosError(f"read {oid} prval", prval.value)
        return buf.raw[:got.value]

    def close(self):
        _l.rados_ioctx_destroy(self.io)
        _l.rados_shutdown(self.cluster)
