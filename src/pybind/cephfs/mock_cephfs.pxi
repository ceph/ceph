# cython: embedsignature=True

from libc.stdint cimport *
from types cimport timespec


cdef:
    cdef struct statx "ceph_statx":
        uint32_t    stx_mask
        uint32_t    stx_blksize
        uint32_t    stx_nlink
        uint32_t    stx_uid
        uint32_t    stx_gid
        uint16_t    stx_mode
        uint64_t    stx_ino
        uint64_t    stx_size
        uint64_t    stx_blocks
        uint64_t    stx_dev
        uint64_t    stx_rdev
        timespec    stx_atime
        timespec    stx_ctime
        timespec    stx_mtime
        timespec    stx_btime
        uint64_t    stx_version

cdef nogil:
    cdef struct ceph_mount_info:
        int dummy

    cdef struct ceph_dir_result:
        int dummy

    cdef struct snap_metadata:
        const char *key
        const char *value

    cdef struct snap_info:
        uint64_t id
        size_t nr_snap_metadata
        snap_metadata *snap_metadata

    cdef struct ceph_snapdiff_info:
        int dummy

    cdef struct ceph_snapdiff_entry_t:
        int dummy

    ctypedef void* rados_t

    const char *ceph_version(int *major, int *minor, int *patch):
        pass

    int ceph_create(ceph_mount_info **cmount, const char * const id):
        pass
    int ceph_create_from_rados(ceph_mount_info **cmount, rados_t cluster):
        pass
    int ceph_init(ceph_mount_info *cmount):
        pass
    void ceph_shutdown(ceph_mount_info *cmount):
        pass

    int ceph_getaddrs(ceph_mount_info* cmount, char** addrs):
        pass
    int64_t ceph_get_fs_cid(ceph_mount_info *cmount):
        pass
    int ceph_conf_read_file(ceph_mount_info *cmount, const char *path_list):
        pass
    int ceph_conf_parse_argv(ceph_mount_info *cmount, int argc, const char **argv):
        pass
    int ceph_conf_get(ceph_mount_info *cmount, const char *option, char *buf, size_t len):
        pass
    int ceph_conf_set(ceph_mount_info *cmount, const char *option, const char *value):
        pass
    int ceph_set_mount_timeout(ceph_mount_info *cmount, uint32_t timeout):
        pass

    int ceph_mount(ceph_mount_info *cmount, const char *root):
        pass
    int ceph_select_filesystem(ceph_mount_info *cmount, const char *fs_name):
        pass
    int ceph_unmount(ceph_mount_info *cmount):
        pass
    int ceph_abort_conn(ceph_mount_info *cmount):
        pass
    uint64_t ceph_get_instance_id(ceph_mount_info *cmount):
        pass
    int ceph_fstatx(ceph_mount_info *cmount, int fd, statx *stx, unsigned want, unsigned flags):
        pass
    int ceph_statx(ceph_mount_info *cmount, const char *path, statx *stx, unsigned want, unsigned flags):
        pass
    int ceph_statfs(ceph_mount_info *cmount, const char *path, statvfs *stbuf):
        pass

    int ceph_setattrx(ceph_mount_info *cmount, const char *relpath, statx *stx, int mask, int flags):
        pass
    int ceph_fsetattrx(ceph_mount_info *cmount, int fd, statx *stx, int mask):
        pass
    int ceph_mds_command(ceph_mount_info *cmount, const char *mds_spec, const char **cmd, size_t cmdlen,
                         const char *inbuf, size_t inbuflen, char **outbuf, size_t *outbuflen,
                         char **outs, size_t *outslen):
        pass
    int ceph_rename(ceph_mount_info *cmount, const char *from_, const char *to):
        pass
    int ceph_link(ceph_mount_info *cmount, const char *existing, const char *newname):
        pass
    int ceph_unlink(ceph_mount_info *cmount, const char *path):
        pass
    int ceph_symlink(ceph_mount_info *cmount, const char *existing, const char *newname):
        pass
    int ceph_readlink(ceph_mount_info *cmount, const char *path, char *buf, int64_t size):
        pass
    int ceph_setxattr(ceph_mount_info *cmount, const char *path, const char *name,
                      const void *value, size_t size, int flags):
        pass
    int ceph_fsetxattr(ceph_mount_info *cmount, int fd, const char *name,
                       const void *value, size_t size, int flags):
        pass
    int ceph_lsetxattr(ceph_mount_info *cmount, const char *path, const char *name,
                       const void *value, size_t size, int flags):
        pass
    int ceph_getxattr(ceph_mount_info *cmount, const char *path, const char *name,
                      void *value, size_t size):
        pass
    int ceph_fgetxattr(ceph_mount_info *cmount, int fd, const char *name,
                       void *value, size_t size):
        pass
    int ceph_lgetxattr(ceph_mount_info *cmount, const char *path, const char *name,
                       void *value, size_t size):
        pass
    int ceph_removexattr(ceph_mount_info *cmount, const char *path, const char *name):
        pass
    int ceph_fremovexattr(ceph_mount_info *cmount, int fd, const char *name):
        pass
    int ceph_lremovexattr(ceph_mount_info *cmount, const char *path, const char *name):
        pass
    int ceph_listxattr(ceph_mount_info *cmount, const char *path, char *list, size_t size):
        pass
    int ceph_flistxattr(ceph_mount_info *cmount, int fd, char *list, size_t size):
        pass
    int ceph_llistxattr(ceph_mount_info *cmount, const char *path, char *list, size_t size):
        pass
    int ceph_write(ceph_mount_info *cmount, int fd, const char *buf, int64_t size, int64_t offset):
        pass
    int ceph_pwritev(ceph_mount_info *cmount, int fd, iovec *iov, int iovcnt, int64_t offset):
        pass
    int ceph_read(ceph_mount_info *cmount, int fd, char *buf, int64_t size, int64_t offset):
        pass
    int ceph_preadv(ceph_mount_info *cmount, int fd, iovec *iov, int iovcnt, int64_t offset):
        pass
    int ceph_flock(ceph_mount_info *cmount, int fd, int operation, uint64_t owner):
        pass
    int ceph_mknod(ceph_mount_info *cmount, const char *path, mode_t mode, dev_t rdev):
        pass
    int ceph_close(ceph_mount_info *cmount, int fd):
        pass
    int ceph_open(ceph_mount_info *cmount, const char *path, int flags, mode_t mode):
        pass
    int ceph_mkdir(ceph_mount_info *cmount, const char *path, mode_t mode):
        pass
    int ceph_mksnap(ceph_mount_info *cmount, const char *path, const char *name, mode_t mode, snap_metadata *snap_metadata, size_t nr_snap_metadata):
        pass
    int ceph_rmsnap(ceph_mount_info *cmount, const char *path, const char *name):
        pass
    int ceph_get_snap_info(ceph_mount_info *cmount, const char *path, snap_info *snap_info):
        pass
    void ceph_free_snap_info_buffer(snap_info *snap_info):
        pass
    int ceph_mkdirs(ceph_mount_info *cmount, const char *path, mode_t mode):
        pass
    int ceph_closedir(ceph_mount_info *cmount, ceph_dir_result *dirp):
        pass
    int ceph_opendir(ceph_mount_info *cmount, const char *name, ceph_dir_result **dirpp):
        pass
    void ceph_rewinddir(ceph_mount_info *cmount, ceph_dir_result *dirp):
        pass
    int64_t ceph_telldir(ceph_mount_info *cmount, ceph_dir_result *dirp):
        pass
    void ceph_seekdir(ceph_mount_info *cmount, ceph_dir_result *dirp, int64_t offset):
        pass
    int ceph_chdir(ceph_mount_info *cmount, const char *path):
        pass
    dirent * ceph_readdir(ceph_mount_info *cmount, ceph_dir_result *dirp):
        pass
    int ceph_open_snapdiff(ceph_mount_info *cmount, const char *root_path, const char *rel_path, const char *snap1path, const char *snap2root, ceph_snapdiff_info *out):
        pass
    int ceph_readdir_snapdiff(ceph_snapdiff_info *snapdiff, ceph_snapdiff_entry_t *out):
        pass
    int ceph_close_snapdiff(ceph_snapdiff_info *snapdiff):
        pass
    int ceph_rmdir(ceph_mount_info *cmount, const char *path):
        pass
    const char* ceph_getcwd(ceph_mount_info *cmount):
        pass
    int ceph_sync_fs(ceph_mount_info *cmount):
        pass
    int ceph_fsync(ceph_mount_info *cmount, int fd, int syncdataonly):
        pass
    int ceph_lazyio(ceph_mount_info *cmount, int fd, int enable):
        pass
    int ceph_lazyio_propagate(ceph_mount_info *cmount, int fd, int64_t offset, size_t count):
        pass
    int ceph_lazyio_synchronize(ceph_mount_info *cmount, int fd, int64_t offset, size_t count):
        pass
    int ceph_fallocate(ceph_mount_info *cmount, int fd, int mode, int64_t offset, int64_t length):
        pass
    int ceph_chmod(ceph_mount_info *cmount, const char *path, mode_t mode):
        pass
    int ceph_lchmod(ceph_mount_info *cmount, const char *path, mode_t mode):
        pass
    int ceph_fchmod(ceph_mount_info *cmount, int fd, mode_t mode):
        pass
    int ceph_chown(ceph_mount_info *cmount, const char *path, int uid, int gid):
        pass
    int ceph_lchown(ceph_mount_info *cmount, const char *path, int uid, int gid):
        pass
    int ceph_fchown(ceph_mount_info *cmount, int fd, int uid, int gid):
        pass
    int64_t ceph_lseek(ceph_mount_info *cmount, int fd, int64_t offset, int whence):
        pass
    void ceph_buffer_free(char *buf):
        pass
    mode_t ceph_umask(ceph_mount_info *cmount, mode_t mode):
        pass
    int ceph_utime(ceph_mount_info *cmount, const char *path, utimbuf *buf):
        pass
    int ceph_futime(ceph_mount_info *cmount, int fd, utimbuf *buf):
        pass
    int ceph_utimes(ceph_mount_info *cmount, const char *path, timeval times[2]):
        pass
    int ceph_lutimes(ceph_mount_info *cmount, const char *path, timeval times[2]):
        pass
    int ceph_futimes(ceph_mount_info *cmount, int fd, timeval times[2]):
        pass
    int ceph_futimens(ceph_mount_info *cmount, int fd, timespec times[2]):
        pass
    int ceph_get_file_replication(ceph_mount_info *cmount, int fh):
        pass
    int ceph_get_path_replication(ceph_mount_info *cmount, const char *path):
        pass
    int ceph_get_pool_id(ceph_mount_info *cmount, const char *pool_name):
        pass
    int ceph_get_pool_replication(ceph_mount_info *cmount, int pool_id):
        pass
    int ceph_debug_get_fd_caps(ceph_mount_info *cmount, int fd):
        pass
    int ceph_debug_get_file_caps(ceph_mount_info *cmount, const char *path):
        pass
    uint32_t ceph_get_cap_return_timeout(ceph_mount_info *cmount):
        pass
    void ceph_set_uuid(ceph_mount_info *cmount, const char *uuid):
        pass
    void ceph_set_session_timeout(ceph_mount_info *cmount, unsigned timeout):
        pass
    int ceph_get_file_layout(ceph_mount_info *cmount, int fh, int *stripe_unit, int *stripe_count, int *object_size, int *pg_pool):
        pass
    int ceph_get_file_pool_name(ceph_mount_info *cmount, int fh, char *buf, size_t buflen):
        pass
    int ceph_get_default_data_pool_name(ceph_mount_info *cmount, char *buf, size_t buflen):
        pass
