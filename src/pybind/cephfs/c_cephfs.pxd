from libc.stdint cimport *
from types cimport *

cdef extern from "cephfs/ceph_ll_client.h":
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

cdef extern from "cephfs/libcephfs.h" nogil:
    cdef struct ceph_mount_info:
        pass

    cdef struct ceph_dir_result:
        pass

    cdef struct snap_metadata:
        const char *key
        const char *value

    cdef struct snap_info:
        uint64_t id
        size_t nr_snap_metadata
        snap_metadata *snap_metadata

    cdef struct ceph_snapdiff_info:
        pass

    cdef struct ceph_snapdiff_entry_t:
        dirent dir_entry
        uint64_t snapid

    ctypedef void* rados_t

    const char *ceph_version(int *major, int *minor, int *patch)

    int ceph_create(ceph_mount_info **cmount, const char * const id)
    int ceph_create_from_rados(ceph_mount_info **cmount, rados_t cluster)
    int ceph_init(ceph_mount_info *cmount)
    void ceph_shutdown(ceph_mount_info *cmount)

    int ceph_getaddrs(ceph_mount_info* cmount, char** addrs)
    int64_t ceph_get_fs_cid(ceph_mount_info *cmount)
    int ceph_conf_read_file(ceph_mount_info *cmount, const char *path_list)
    int ceph_conf_parse_argv(ceph_mount_info *cmount, int argc, const char **argv)
    int ceph_conf_get(ceph_mount_info *cmount, const char *option, char *buf, size_t len)
    int ceph_conf_set(ceph_mount_info *cmount, const char *option, const char *value)
    int ceph_set_mount_timeout(ceph_mount_info *cmount, uint32_t timeout)

    int ceph_mount(ceph_mount_info *cmount, const char *root)
    int ceph_select_filesystem(ceph_mount_info *cmount, const char *fs_name)
    int ceph_unmount(ceph_mount_info *cmount)
    int ceph_abort_conn(ceph_mount_info *cmount)
    uint64_t ceph_get_instance_id(ceph_mount_info *cmount)
    int ceph_fstatx(ceph_mount_info *cmount, int fd, statx *stx, unsigned want, unsigned flags)
    int ceph_statx(ceph_mount_info *cmount, const char *path, statx *stx, unsigned want, unsigned flags)
    int ceph_statfs(ceph_mount_info *cmount, const char *path, statvfs *stbuf)

    int ceph_setattrx(ceph_mount_info *cmount, const char *relpath, statx *stx, int mask, int flags)
    int ceph_fsetattrx(ceph_mount_info *cmount, int fd, statx *stx, int mask)
    int ceph_mds_command(ceph_mount_info *cmount, const char *mds_spec, const char **cmd, size_t cmdlen,
                         const char *inbuf, size_t inbuflen, char **outbuf, size_t *outbuflen,
                         char **outs, size_t *outslen)
    int ceph_rename(ceph_mount_info *cmount, const char *from_, const char *to)
    int ceph_link(ceph_mount_info *cmount, const char *existing, const char *newname)
    int ceph_unlink(ceph_mount_info *cmount, const char *path)
    int ceph_symlink(ceph_mount_info *cmount, const char *existing, const char *newname)
    int ceph_readlink(ceph_mount_info *cmount, const char *path, char *buf, int64_t size)
    int ceph_setxattr(ceph_mount_info *cmount, const char *path, const char *name,
                      const void *value, size_t size, int flags)
    int ceph_fsetxattr(ceph_mount_info *cmount, int fd, const char *name,
                       const void *value, size_t size, int flags)
    int ceph_lsetxattr(ceph_mount_info *cmount, const char *path, const char *name,
                       const void *value, size_t size, int flags)
    int ceph_getxattr(ceph_mount_info *cmount, const char *path, const char *name,
                      void *value, size_t size)
    int ceph_fgetxattr(ceph_mount_info *cmount, int fd, const char *name,
                       void *value, size_t size)
    int ceph_lgetxattr(ceph_mount_info *cmount, const char *path, const char *name,
                       void *value, size_t size)
    int ceph_removexattr(ceph_mount_info *cmount, const char *path, const char *name)
    int ceph_fremovexattr(ceph_mount_info *cmount, int fd, const char *name)
    int ceph_lremovexattr(ceph_mount_info *cmount, const char *path, const char *name)
    int ceph_listxattr(ceph_mount_info *cmount, const char *path, char *list, size_t size)
    int ceph_flistxattr(ceph_mount_info *cmount, int fd, char *list, size_t size)
    int ceph_llistxattr(ceph_mount_info *cmount, const char *path, char *list, size_t size)
    int ceph_write(ceph_mount_info *cmount, int fd, const char *buf, int64_t size, int64_t offset)
    int ceph_pwritev(ceph_mount_info *cmount, int fd, iovec *iov, int iovcnt, int64_t offset)
    int ceph_read(ceph_mount_info *cmount, int fd, char *buf, int64_t size, int64_t offset)
    int ceph_preadv(ceph_mount_info *cmount, int fd, iovec *iov, int iovcnt, int64_t offset)
    int ceph_flock(ceph_mount_info *cmount, int fd, int operation, uint64_t owner)
    int ceph_mknod(ceph_mount_info *cmount, const char *path, mode_t mode, dev_t rdev)
    int ceph_close(ceph_mount_info *cmount, int fd)
    int ceph_open(ceph_mount_info *cmount, const char *path, int flags, mode_t mode)
    int ceph_mkdir(ceph_mount_info *cmount, const char *path, mode_t mode)
    int ceph_mksnap(ceph_mount_info *cmount, const char *path, const char *name, mode_t mode, snap_metadata *snap_metadata, size_t nr_snap_metadata)
    int ceph_rmsnap(ceph_mount_info *cmount, const char *path, const char *name)
    int ceph_get_snap_info(ceph_mount_info *cmount, const char *path, snap_info *snap_info)
    void ceph_free_snap_info_buffer(snap_info *snap_info)
    int ceph_mkdirs(ceph_mount_info *cmount, const char *path, mode_t mode)
    int ceph_closedir(ceph_mount_info *cmount, ceph_dir_result *dirp)
    int ceph_opendir(ceph_mount_info *cmount, const char *name, ceph_dir_result **dirpp)
    void ceph_rewinddir(ceph_mount_info *cmount, ceph_dir_result *dirp)
    int64_t ceph_telldir(ceph_mount_info *cmount, ceph_dir_result *dirp)
    void ceph_seekdir(ceph_mount_info *cmount, ceph_dir_result *dirp, int64_t offset)
    int ceph_chdir(ceph_mount_info *cmount, const char *path)
    dirent * ceph_readdir(ceph_mount_info *cmount, ceph_dir_result *dirp)
    int ceph_open_snapdiff(ceph_mount_info *cmount,
                           const char *root_path,
                           const char *rel_path,
                           const char *snap1,
                           const char *snap2,
                           ceph_snapdiff_info *out)
    int ceph_readdir_snapdiff(ceph_snapdiff_info *snapdiff, ceph_snapdiff_entry_t *out);
    int ceph_close_snapdiff(ceph_snapdiff_info *snapdiff)
    int ceph_rmdir(ceph_mount_info *cmount, const char *path)
    const char* ceph_getcwd(ceph_mount_info *cmount)
    int ceph_sync_fs(ceph_mount_info *cmount)
    int ceph_fsync(ceph_mount_info *cmount, int fd, int syncdataonly)
    int ceph_lazyio(ceph_mount_info *cmount, int fd, int enable)
    int ceph_lazyio_propagate(ceph_mount_info *cmount, int fd, int64_t offset, size_t count)
    int ceph_lazyio_synchronize(ceph_mount_info *cmount, int fd, int64_t offset, size_t count)
    int ceph_fallocate(ceph_mount_info *cmount, int fd, int mode, int64_t offset, int64_t length)
    int ceph_chmod(ceph_mount_info *cmount, const char *path, mode_t mode)
    int ceph_lchmod(ceph_mount_info *cmount, const char *path, mode_t mode)
    int ceph_fchmod(ceph_mount_info *cmount, int fd, mode_t mode)
    int ceph_chown(ceph_mount_info *cmount, const char *path, int uid, int gid)
    int ceph_lchown(ceph_mount_info *cmount, const char *path, int uid, int gid)
    int ceph_fchown(ceph_mount_info *cmount, int fd, int uid, int gid)
    int64_t ceph_lseek(ceph_mount_info *cmount, int fd, int64_t offset, int whence)
    void ceph_buffer_free(char *buf)
    mode_t ceph_umask(ceph_mount_info *cmount, mode_t mode)
    int ceph_utime(ceph_mount_info *cmount, const char *path, utimbuf *buf)
    int ceph_futime(ceph_mount_info *cmount, int fd, utimbuf *buf)
    int ceph_utimes(ceph_mount_info *cmount, const char *path, timeval times[2])
    int ceph_lutimes(ceph_mount_info *cmount, const char *path, timeval times[2])
    int ceph_futimes(ceph_mount_info *cmount, int fd, timeval times[2])
    int ceph_futimens(ceph_mount_info *cmount, int fd, timespec times[2])
    int ceph_get_file_replication(ceph_mount_info *cmount, int fh)
    int ceph_get_path_replication(ceph_mount_info *cmount, const char *path)
    int ceph_get_pool_id(ceph_mount_info *cmount, const char *pool_name)
    int ceph_get_pool_replication(ceph_mount_info *cmount, int pool_id)
    int ceph_debug_get_fd_caps(ceph_mount_info *cmount, int fd)
    int ceph_debug_get_file_caps(ceph_mount_info *cmount, const char *path)
    uint32_t ceph_get_cap_return_timeout(ceph_mount_info *cmount)
    void ceph_set_uuid(ceph_mount_info *cmount, const char *uuid)
    void ceph_set_session_timeout(ceph_mount_info *cmount, unsigned timeout)
    int ceph_get_file_layout(ceph_mount_info *cmount, int fh, int *stripe_unit, int *stripe_count, int *object_size, int *pg_pool)
    int ceph_get_file_pool_name(ceph_mount_info *cmount, int fh, char *buf, size_t buflen)
    int ceph_get_default_data_pool_name(ceph_mount_info *cmount, char *buf, size_t buflen)
