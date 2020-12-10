# cython: embedsignature=True

cdef extern from "time.h":
    ctypedef long int time_t
    cdef struct timespec:
        time_t tv_sec
        long tv_nsec
