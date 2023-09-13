# cython: embedsignature=True

cdef extern from "time.h":
    ctypedef long int time_t
    ctypedef long int suseconds_t


cdef extern from "sys/time.h":
    cdef struct timeval:
        time_t tv_sec
        suseconds_t tv_usec
