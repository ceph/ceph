cdef extern from "time.h":
    ctypedef long int time_t
    cdef struct timespec:
        time_t      tv_sec
        long int    tv_nsec

cdef extern from "<utime.h>":
    cdef struct utimbuf:
        time_t actime
        time_t modtime

cdef extern from "sys/types.h":
    ctypedef unsigned long mode_t
    ctypedef unsigned long dev_t

cdef extern from "sys/time.h":
    cdef struct timeval:
        long tv_sec
        long tv_usec

cdef extern from "sys/statvfs.h":
    cdef struct statvfs:
        unsigned long int f_bsize
        unsigned long int f_frsize
        unsigned long int f_blocks
        unsigned long int f_bfree
        unsigned long int f_bavail
        unsigned long int f_files
        unsigned long int f_ffree
        unsigned long int f_favail
        unsigned long int f_fsid
        unsigned long int f_flag
        unsigned long int f_namemax
        unsigned long int f_padding[32]

cdef extern from "<sys/uio.h>":
    cdef struct iovec:
        void *iov_base
        size_t iov_len

IF UNAME_SYSNAME == "FreeBSD" or UNAME_SYSNAME == "Darwin":
    cdef extern from "dirent.h":
        cdef struct dirent:
            long int d_ino
            unsigned short int d_reclen
            unsigned char d_type
            char d_name[256]
ELSE:
    cdef extern from "dirent.h":
        cdef struct dirent:
            long int d_ino
            unsigned long int d_off
            unsigned short int d_reclen
            unsigned char d_type
            char d_name[256]
