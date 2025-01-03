# cython: embedsignature=True
#
# Shared object for librbdpy
#
# Copyright 2016 Mehdi Abaakouk <sileht@redhat.com>

IF BUILD_DOC:
    cdef:
        ctypedef void* rados_t
        ctypedef void* rados_config_t
        ctypedef void* rados_ioctx_t
ELSE:
    cdef extern from "rados/librados.h" nogil:
        ctypedef void* rados_t
        ctypedef void* rados_config_t
        ctypedef void* rados_ioctx_t


cdef class Rados(object):
    cdef:
        rados_t cluster
        public object state
        public object monitor_callback
        public object monitor_callback2
        public object parsed_args
        public object conf_defaults
        public object conffile
        public object rados_id


cdef class Ioctx(object):
    cdef:
        object rados
        rados_ioctx_t io
        public object name
        public object state
        public object locator_key
        public object nspace

        # TODO(sileht): we need to track leaving completion objects
        # I guess we can do that in a lighter ways, but keep code simple
        # as before for now
        public object safe_completions
        public object complete_completions
        public object lock
