cdef extern from "time.h":
    ctypedef long int time_t


cdef extern from "sys/stat.h":
    cdef struct stat:
        unsigned long st_dev
        unsigned long st_ino
        unsigned long st_nlink
        unsigned int st_mode
        unsigned int st_uid
        unsigned int st_gid
        int __pad0
        unsigned long st_rdev
        long int st_size
        long int st_blksize
        long int st_blocks
        time_t st_atime
        time_t st_mtime
        time_t st_ctime
