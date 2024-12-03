# cython: embedsignature=True

# Make the bool type available as libcpp.bool, for both C and C++.
cimport libcpp
cdef extern from "<stdbool.h>":
    pass

cdef nogil:
    ctypedef void* librgw_t

    int librgw_create(librgw_t *rgw, int argc, char **argv):
        pass
    void librgw_shutdown(librgw_t rgw):
        pass


cdef nogil:
    enum:
        RGW_FS_TYPE_FILE
        RGW_FS_TYPE_DIRECTORY

        RGW_LOOKUP_FLAG_CREATE

        RGW_SETATTR_MODE
        RGW_SETATTR_UID
        RGW_SETATTR_GID
        RGW_SETATTR_MTIME
        RGW_SETATTR_ATIME
        RGW_SETATTR_SIZE
        RGW_SETATTR_CTIME

        RGW_READDIR_FLAG_NONE
        RGW_READDIR_FLAG_DOTDOT

        RGW_OPEN_FLAG_CREATE
        RGW_OPEN_FLAG_V3         # ops have v3 semantics
        RGW_OPEN_FLAG_STATELESS  # alias it

        RGW_CLOSE_FLAG_RELE

    ctypedef void *rgw_fh_hk
    cdef struct rgw_file_handle:
        rgw_fh_hk fh_hk
        void *fs_private
        int fh_type

    cdef struct rgw_fs:
        librgw_t rgw
        void *fs_private
        void *root_fh

    # mount info hypothetical--emulate Unix, support at least UUID-length fsid
    cdef struct rgw_statvfs:
        uint64_t  f_bsize    # file system block size
        uint64_t  f_frsize   # fragment size
        uint64_t  f_blocks   # size of fs in f_frsize units
        uint64_t  f_bfree    # free blocks
        uint64_t  f_bavail   # free blocks for unprivileged users
        uint64_t  f_files    # inodes
        uint64_t  f_ffree    # free inodes
        uint64_t  f_favail   # free inodes for unprivileged users
        uint64_t  f_fsid[2]  # /* file system ID
        uint64_t  f_flag     # mount flags
        uint64_t  f_namemax  # maximum filename length

    void rgwfile_version(int *major, int *minor, int *extra):
        pass

    int rgw_lookup(rgw_fs *fs,
                   rgw_file_handle *parent_fh, const char *path,
                   rgw_file_handle **fh, stat* st, uint32_t st_mask,
		   uint32_t flags):
        pass

    int rgw_lookup_handle(rgw_fs *fs, rgw_fh_hk *fh_hk,
                          rgw_file_handle **fh, uint32_t flags):
        pass

    int rgw_fh_rele(rgw_fs *fs, rgw_file_handle *fh,
                    uint32_t flags):
        pass

    int rgw_mount(librgw_t rgw, const char *uid, const char *key,
                  const char *secret, rgw_fs **fs, uint32_t flags):
        pass

    int rgw_umount(rgw_fs *fs, uint32_t flags):
        pass

    int rgw_statfs(rgw_fs *fs, rgw_file_handle *parent_fh,
                   rgw_statvfs *vfs_st, uint32_t flags):
        pass

    int rgw_create(rgw_fs *fs, rgw_file_handle *parent_fh,
                   const char *name, stat *st, uint32_t mask,
                   rgw_file_handle **fh, uint32_t posix_flags,
                   uint32_t flags):
        pass

    int rgw_mkdir(rgw_fs *fs,
                  rgw_file_handle *parent_fh,
                  const char *name, stat *st, uint32_t mask,
                  rgw_file_handle **fh, uint32_t flags):
        pass

    int rgw_rename(rgw_fs *fs,
                   rgw_file_handle *olddir, const char* old_name,
                   rgw_file_handle *newdir, const char* new_name,
                   uint32_t flags):
        pass

    int rgw_unlink(rgw_fs *fs,
                   rgw_file_handle *parent_fh, const char* path,
                   uint32_t flags):
        pass

    int rgw_readdir(rgw_fs *fs,
                    rgw_file_handle *parent_fh, uint64_t *offset,
                    libcpp.bool (*cb)(const char *name, void *arg, uint64_t offset, stat *st, uint32_t st_mask, uint32_t flags) nogil except? -9000,
                    void *cb_arg, libcpp.bool *eof, uint32_t flags) except? -9000:
        pass

    int rgw_getattr(rgw_fs *fs,
                    rgw_file_handle *fh, stat *st,
                    uint32_t flags):
        pass

    int rgw_setattr(rgw_fs *fs, rgw_file_handle *fh, stat *st,
                    uint32_t mask, uint32_t flags):
        pass

    int rgw_truncate(rgw_fs *fs, rgw_file_handle *fh, uint64_t size, uint32_t flags):
        pass

    int rgw_open(rgw_fs *fs, rgw_file_handle *parent_fh,
                 uint32_t posix_flags, uint32_t flags):
        pass

    int rgw_close(rgw_fs *fs, rgw_file_handle *fh,
                  uint32_t flags):
        pass

    int rgw_read(rgw_fs *fs,
                 rgw_file_handle *fh, uint64_t offset,
                 size_t length, size_t *bytes_read, void *buffer,
                 uint32_t flags):
        pass

    int rgw_write(rgw_fs *fs,
                  rgw_file_handle *fh, uint64_t offset,
                  size_t length, size_t *bytes_written, void *buffer,
                  uint32_t flags):
        pass

    int rgw_fsync(rgw_fs *fs, rgw_file_handle *fh,
                  uint32_t flags):
        pass

    int rgw_commit(rgw_fs *fs, rgw_file_handle *fh,
                   uint64_t offset, uint64_t length, uint32_t flags):
        pass
