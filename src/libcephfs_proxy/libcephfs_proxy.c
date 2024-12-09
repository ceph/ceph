
#include <stdlib.h>

#include "include/cephfs/libcephfs.h"

#include "proxy_log.h"
#include "proxy_helpers.h"
#include "proxy_requests.h"

/* We override the definition of the ceph_mount_info structure to contain
 * internal proxy information. This is already a black box for libcephfs users,
 * so this won't be noticed. */
struct ceph_mount_info {
	proxy_link_t link;
	uint64_t cmount;
};

/* The global_cmount is used to stablish an initial connection to serve requests
 * not related to a real cmount, like ceph_version or ceph_userperm_new. */
static struct ceph_mount_info global_cmount = { PROXY_LINK_DISCONNECTED, 0 };

static bool client_stop(proxy_link_t *link)
{
	return false;
}

static int32_t proxy_connect(proxy_link_t *link)
{
	CEPH_REQ(hello, req, 0, ans, 0);
	char *path, *env;
	int32_t sd, err;

	path = PROXY_SOCKET;
	env = getenv(PROXY_SOCKET_ENV);
	if (env != NULL) {
		path = env;
	}

	sd = proxy_link_client(link, path, client_stop);
	if (sd < 0) {
		return sd;
	}

	req.id = LIBCEPHFS_LIB_CLIENT;
	err = proxy_link_send(sd, req_iov, 1);
	if (err < 0) {
		goto failed;
	}
	err = proxy_link_recv(sd, ans_iov, 1);
	if (err < 0) {
		goto failed;
	}

	proxy_log(LOG_INFO, 0, "Connected to libcephfsd version %d.%d",
		  ans.major, ans.minor);

	if ((ans.major != LIBCEPHFSD_MAJOR) ||
	    (ans.minor != LIBCEPHFSD_MINOR)) {
		err = proxy_log(LOG_ERR, ENOTSUP, "Version not supported");
		goto failed;
	}

	return sd;

failed:
	proxy_link_close(link);

	return err;
}

static void proxy_disconnect(proxy_link_t *link)
{
	proxy_link_close(link);
}

static int32_t proxy_global_connect(void)
{
	int32_t err;

	err = 0;

	if (!proxy_link_is_connected(&global_cmount.link)) {
		err = proxy_connect(&global_cmount.link);
	}

	return err;
}

static int32_t proxy_check(struct ceph_mount_info *cmount, int32_t err,
			   int32_t result)
{
	if (err < 0) {
		proxy_disconnect(&cmount->link);
		proxy_log(LOG_ERR, err, "Disconnected from libcephfsd");

		return err;
	}

	return result;
}

/* Macros to simplify communication with the server. */
#define CEPH_RUN(_cmount, _op, _req, _ans)                                 \
	({                                                                 \
		int32_t __err =                                            \
			CEPH_CALL((_cmount)->link.sd, _op, _req, _ans);    \
		__err = proxy_check(_cmount, __err, (_ans).header.result); \
		__err;                                                     \
	})

#define CEPH_PROCESS(_cmount, _op, _req, _ans)                      \
	({                                                          \
		int32_t __err = -ENOTCONN;                          \
		if (proxy_link_is_connected(&(_cmount)->link)) {    \
			(_req).cmount = (_cmount)->cmount;          \
			__err = CEPH_RUN(_cmount, _op, _req, _ans); \
		}                                                   \
		__err;                                              \
	})

__public int ceph_chdir(struct ceph_mount_info *cmount, const char *path)
{
	CEPH_REQ(ceph_chdir, req, 1, ans, 0);

	CEPH_STR_ADD(req, path, path);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_CHDIR, req, ans);
}

__public int ceph_conf_get(struct ceph_mount_info *cmount, const char *option,
			   char *buf, size_t len)
{
	CEPH_REQ(ceph_conf_get, req, 1, ans, 1);

	req.size = len;

	CEPH_STR_ADD(req, option, option);
	CEPH_BUFF_ADD(ans, buf, len);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_CONF_GET, req, ans);
}

__public int ceph_conf_read_file(struct ceph_mount_info *cmount,
				 const char *path_list)
{
	CEPH_REQ(ceph_conf_read_file, req, 1, ans, 0);

	CEPH_STR_ADD(req, path, path_list);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_CONF_READ_FILE, req, ans);
}

__public int ceph_conf_set(struct ceph_mount_info *cmount, const char *option,
			   const char *value)
{
	CEPH_REQ(ceph_conf_set, req, 2, ans, 0);

	CEPH_STR_ADD(req, option, option);
	CEPH_STR_ADD(req, value, value);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_CONF_SET, req, ans);
}

__public int ceph_create(struct ceph_mount_info **cmount, const char *const id)
{
	CEPH_REQ(ceph_create, req, 1, ans, 0);
	struct ceph_mount_info *ceph_mount;
	int32_t sd, err;

	ceph_mount = proxy_malloc(sizeof(struct ceph_mount_info));
	if (ceph_mount == NULL) {
		return -ENOMEM;
	}

	err = proxy_connect(&ceph_mount->link);
	if (err < 0) {
		goto failed;
	}
	sd = err;

	CEPH_STR_ADD(req, id, id);

	err = CEPH_CALL(sd, LIBCEPHFSD_OP_CREATE, req, ans);
	if ((err < 0) || ((err = ans.header.result) < 0)) {
		goto failed_link;
	}

	ceph_mount->cmount = ans.cmount;

	*cmount = ceph_mount;

	return 0;

failed_link:
	proxy_disconnect(&ceph_mount->link);

failed:
	proxy_free(ceph_mount);

	return err;
}

__public const char *ceph_getcwd(struct ceph_mount_info *cmount)
{
	static char cwd[PATH_MAX];
	int32_t err;

	CEPH_REQ(ceph_getcwd, req, 0, ans, 1);

	CEPH_BUFF_ADD(ans, cwd, sizeof(cwd));

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_GETCWD, req, ans);
	if (err >= 0) {
		return cwd;
	}

	errno = -err;

	return NULL;
}

__public int ceph_init(struct ceph_mount_info *cmount)
{
	CEPH_REQ(ceph_init, req, 0, ans, 0);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_INIT, req, ans);
}

__public int ceph_ll_close(struct ceph_mount_info *cmount,
			   struct Fh *filehandle)
{
	CEPH_REQ(ceph_ll_close, req, 0, ans, 0);

	req.fh = ptr_value(filehandle);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_CLOSE, req, ans);
}

__public int ceph_ll_create(struct ceph_mount_info *cmount, Inode *parent,
			    const char *name, mode_t mode, int oflags,
			    Inode **outp, Fh **fhp, struct ceph_statx *stx,
			    unsigned want, unsigned lflags,
			    const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_create, req, 1, ans, 1);
	int32_t err;

	req.userperm = ptr_value(perms);
	req.parent = ptr_value(parent);
	req.mode = mode;
	req.oflags = oflags;
	req.want = want;
	req.flags = lflags;

	CEPH_STR_ADD(req, name, name);
	CEPH_BUFF_ADD(ans, stx, sizeof(*stx));

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_CREATE, req, ans);
	if (err >= 0) {
		*outp = value_ptr(ans.inode);
		*fhp = value_ptr(ans.fh);
	}

	return err;
}

__public int ceph_ll_fallocate(struct ceph_mount_info *cmount, struct Fh *fh,
			       int mode, int64_t offset, int64_t length)
{
	CEPH_REQ(ceph_ll_fallocate, req, 0, ans, 0);

	req.fh = ptr_value(fh);
	req.mode = mode;
	req.offset = offset;
	req.length = length;

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_FALLOCATE, req, ans);
}

__public int ceph_ll_fsync(struct ceph_mount_info *cmount, struct Fh *fh,
			   int syncdataonly)
{
	CEPH_REQ(ceph_ll_fsync, req, 0, ans, 0);

	req.fh = ptr_value(fh);
	req.dataonly = syncdataonly;

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_FSYNC, req, ans);
}

__public int ceph_ll_getattr(struct ceph_mount_info *cmount, struct Inode *in,
			     struct ceph_statx *stx, unsigned int want,
			     unsigned int flags, const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_getattr, req, 0, ans, 1);

	req.userperm = ptr_value(perms);
	req.inode = ptr_value(in);
	req.want = want;
	req.flags = flags;

	CEPH_BUFF_ADD(ans, stx, sizeof(*stx));

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_GETATTR, req, ans);
}

__public int ceph_ll_getxattr(struct ceph_mount_info *cmount, struct Inode *in,
			      const char *name, void *value, size_t size,
			      const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_getxattr, req, 1, ans, 1);

	req.userperm = ptr_value(perms);
	req.inode = ptr_value(in);
	req.size = size;
	CEPH_STR_ADD(req, name, name);

	CEPH_BUFF_ADD(ans, value, size);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_GETXATTR, req, ans);
}

__public int ceph_ll_link(struct ceph_mount_info *cmount, struct Inode *in,
			  struct Inode *newparent, const char *name,
			  const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_link, req, 1, ans, 0);

	req.userperm = ptr_value(perms);
	req.inode = ptr_value(in);
	req.parent = ptr_value(newparent);
	CEPH_STR_ADD(req, name, name);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_LINK, req, ans);
}

__public int ceph_ll_listxattr(struct ceph_mount_info *cmount, struct Inode *in,
			       char *list, size_t buf_size, size_t *list_size,
			       const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_listxattr, req, 0, ans, 1);
	int32_t err;

	req.userperm = ptr_value(perms);
	req.inode = ptr_value(in);
	req.size = buf_size;

	CEPH_BUFF_ADD(ans, list, buf_size);

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_LISTXATTR, req, ans);
	if (err >= 0) {
		*list_size = ans.size;
	}

	return err;
}

__public int ceph_ll_lookup(struct ceph_mount_info *cmount, Inode *parent,
			    const char *name, Inode **out,
			    struct ceph_statx *stx, unsigned want,
			    unsigned flags, const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_lookup, req, 1, ans, 1);
	int32_t err;

	req.userperm = ptr_value(perms);
	req.parent = ptr_value(parent);
	req.want = want;
	req.flags = flags;
	CEPH_STR_ADD(req, name, name);

	CEPH_BUFF_ADD(ans, stx, sizeof(*stx));

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_LOOKUP, req, ans);
	if (err >= 0) {
		*out = value_ptr(ans.inode);
	}

	return err;
}

__public int ceph_ll_lookup_inode(struct ceph_mount_info *cmount,
				  struct inodeno_t ino, Inode **inode)
{
	CEPH_REQ(ceph_ll_lookup_inode, req, 0, ans, 0);
	int32_t err;

	req.ino = ino;

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_LOOKUP_INODE, req, ans);
	if (err >= 0) {
		*inode = value_ptr(ans.inode);
	}

	return err;
}

__public int ceph_ll_lookup_root(struct ceph_mount_info *cmount, Inode **parent)
{
	CEPH_REQ(ceph_ll_lookup_root, req, 0, ans, 0);
	int32_t err;

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_LOOKUP_ROOT, req, ans);
	if (err >= 0) {
		*parent = value_ptr(ans.inode);
	}

	return err;
}

__public off_t ceph_ll_lseek(struct ceph_mount_info *cmount,
			     struct Fh *filehandle, off_t offset, int whence)
{
	CEPH_REQ(ceph_ll_lseek, req, 0, ans, 0);
	int32_t err;

	req.fh = ptr_value(filehandle);
	req.offset = offset;
	req.whence = whence;

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_LSEEK, req, ans);
	if (err >= 0) {
		return ans.offset;
	}

	return err;
}

__public int ceph_ll_mkdir(struct ceph_mount_info *cmount, Inode *parent,
			   const char *name, mode_t mode, Inode **out,
			   struct ceph_statx *stx, unsigned want,
			   unsigned flags, const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_mkdir, req, 1, ans, 1);
	int32_t err;

	req.userperm = ptr_value(perms);
	req.parent = ptr_value(parent);
	req.mode = mode;
	req.want = want;
	req.flags = flags;
	CEPH_STR_ADD(req, name, name);

	CEPH_BUFF_ADD(ans, stx, sizeof(*stx));

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_MKDIR, req, ans);
	if (err >= 0) {
		*out = value_ptr(ans.inode);
	}

	return err;
}

__public int ceph_ll_mknod(struct ceph_mount_info *cmount, Inode *parent,
			   const char *name, mode_t mode, dev_t rdev,
			   Inode **out, struct ceph_statx *stx, unsigned want,
			   unsigned flags, const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_mknod, req, 1, ans, 1);
	int32_t err;

	req.userperm = ptr_value(perms);
	req.parent = ptr_value(parent);
	req.mode = mode;
	req.rdev = rdev;
	req.want = want;
	req.flags = flags;
	CEPH_STR_ADD(req, name, name);

	CEPH_BUFF_ADD(ans, stx, sizeof(*stx));

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_MKNOD, req, ans);
	if (err >= 0) {
		*out = value_ptr(ans.inode);
	}

	return err;
}

__public int ceph_ll_open(struct ceph_mount_info *cmount, struct Inode *in,
			  int flags, struct Fh **fh, const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_open, req, 0, ans, 0);
	int32_t err;

	req.userperm = ptr_value(perms);
	req.inode = ptr_value(in);
	req.flags = flags;

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_OPEN, req, ans);
	if (err >= 0) {
		*fh = value_ptr(ans.fh);
	}

	return err;
}

__public int ceph_ll_opendir(struct ceph_mount_info *cmount, struct Inode *in,
			     struct ceph_dir_result **dirpp,
			     const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_opendir, req, 0, ans, 0);
	int32_t err;

	req.userperm = ptr_value(perms);
	req.inode = ptr_value(in);

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_OPENDIR, req, ans);
	if (err >= 0) {
		*dirpp = value_ptr(ans.dir);
	}

	return err;
}

__public int ceph_ll_put(struct ceph_mount_info *cmount, struct Inode *in)
{
	CEPH_REQ(ceph_ll_put, req, 0, ans, 0);

	req.inode = ptr_value(in);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_PUT, req, ans);
}

__public int ceph_ll_read(struct ceph_mount_info *cmount, struct Fh *filehandle,
			  int64_t off, uint64_t len, char *buf)
{
	CEPH_REQ(ceph_ll_read, req, 0, ans, 1);

	req.fh = ptr_value(filehandle);
	req.offset = off;
	req.len = len;

	CEPH_BUFF_ADD(ans, buf, len);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_READ, req, ans);
}

__public int ceph_ll_readlink(struct ceph_mount_info *cmount, struct Inode *in,
			      char *buf, size_t bufsize, const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_readlink, req, 0, ans, 1);

	req.userperm = ptr_value(perms);
	req.inode = ptr_value(in);
	req.size = bufsize;

	CEPH_BUFF_ADD(ans, buf, bufsize);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_READLINK, req, ans);
}

__public int ceph_ll_releasedir(struct ceph_mount_info *cmount,
				struct ceph_dir_result *dir)
{
	CEPH_REQ(ceph_ll_releasedir, req, 0, ans, 0);

	req.dir = ptr_value(dir);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_RELEASEDIR, req, ans);
}

__public int ceph_ll_removexattr(struct ceph_mount_info *cmount,
				 struct Inode *in, const char *name,
				 const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_removexattr, req, 1, ans, 0);

	req.userperm = ptr_value(perms);
	req.inode = ptr_value(in);
	CEPH_STR_ADD(req, name, name);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_REMOVEXATTR, req, ans);
}

__public int ceph_ll_rename(struct ceph_mount_info *cmount,
			    struct Inode *parent, const char *name,
			    struct Inode *newparent, const char *newname,
			    const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_rename, req, 2, ans, 0);

	req.userperm = ptr_value(perms);
	req.old_parent = ptr_value(parent);
	req.new_parent = ptr_value(newparent);
	CEPH_STR_ADD(req, old_name, name);
	CEPH_STR_ADD(req, new_name, newname);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_RENAME, req, ans);
}

__public void ceph_rewinddir(struct ceph_mount_info *cmount,
			     struct ceph_dir_result *dirp)
{
	CEPH_REQ(ceph_rewinddir, req, 0, ans, 0);

	req.dir = ptr_value(dirp);

	CEPH_PROCESS(cmount, LIBCEPHFSD_OP_REWINDDIR, req, ans);
}

__public int ceph_ll_rmdir(struct ceph_mount_info *cmount, struct Inode *in,
			   const char *name, const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_rmdir, req, 1, ans, 0);

	req.userperm = ptr_value(perms);
	req.parent = ptr_value(in);
	CEPH_STR_ADD(req, name, name);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_RMDIR, req, ans);
}

__public int ceph_ll_setattr(struct ceph_mount_info *cmount, struct Inode *in,
			     struct ceph_statx *stx, int mask,
			     const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_setattr, req, 1, ans, 0);

	req.userperm = ptr_value(perms);
	req.inode = ptr_value(in);
	req.mask = mask;
	CEPH_BUFF_ADD(req, stx, sizeof(*stx));

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_SETATTR, req, ans);
}

__public int ceph_ll_setxattr(struct ceph_mount_info *cmount, struct Inode *in,
			      const char *name, const void *value, size_t size,
			      int flags, const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_setxattr, req, 2, ans, 0);

	req.userperm = ptr_value(perms);
	req.inode = ptr_value(in);
	req.size = size;
	req.flags = flags;
	CEPH_STR_ADD(req, name, name);
	CEPH_BUFF_ADD(req, value, size);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_SETXATTR, req, ans);
}

__public int ceph_ll_statfs(struct ceph_mount_info *cmount, struct Inode *in,
			    struct statvfs *stbuf)
{
	CEPH_REQ(ceph_ll_statfs, req, 0, ans, 1);

	req.inode = ptr_value(in);

	CEPH_BUFF_ADD(ans, stbuf, sizeof(*stbuf));

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_STATFS, req, ans);
}

__public int ceph_ll_symlink(struct ceph_mount_info *cmount, Inode *in,
			     const char *name, const char *value, Inode **out,
			     struct ceph_statx *stx, unsigned want,
			     unsigned flags, const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_symlink, req, 2, ans, 1);
	int32_t err;

	req.userperm = ptr_value(perms);
	req.parent = ptr_value(in);
	req.want = want;
	req.flags = flags;
	CEPH_STR_ADD(req, name, name);
	CEPH_STR_ADD(req, target, value);

	CEPH_BUFF_ADD(req, stx, sizeof(*stx));

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_SYMLINK, req, ans);
	if (err >= 0) {
		*out = value_ptr(ans.inode);
	}

	return err;
}

__public int ceph_ll_unlink(struct ceph_mount_info *cmount, struct Inode *in,
			    const char *name, const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_unlink, req, 1, ans, 0);

	req.userperm = ptr_value(perms);
	req.parent = ptr_value(in);
	CEPH_STR_ADD(req, name, name);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_UNLINK, req, ans);
}

__public int ceph_ll_walk(struct ceph_mount_info *cmount, const char *name,
			  Inode **i, struct ceph_statx *stx, unsigned int want,
			  unsigned int flags, const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_walk, req, 1, ans, 1);
	int32_t err;

	req.userperm = ptr_value(perms);
	req.want = want;
	req.flags = flags;
	CEPH_STR_ADD(req, path, name);

	CEPH_BUFF_ADD(ans, stx, sizeof(*stx));

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_WALK, req, ans);
	if (err >= 0) {
		*i = value_ptr(ans.inode);
	}

	return err;
}

__public int ceph_ll_write(struct ceph_mount_info *cmount,
			   struct Fh *filehandle, int64_t off, uint64_t len,
			   const char *data)
{
	CEPH_REQ(ceph_ll_write, req, 1, ans, 0);

	req.fh = ptr_value(filehandle);
	req.offset = off;
	req.len = len;
	CEPH_BUFF_ADD(req, data, len);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_WRITE, req, ans);
}

__public int ceph_mount(struct ceph_mount_info *cmount, const char *root)
{
	CEPH_REQ(ceph_mount, req, 1, ans, 0);

	CEPH_STR_ADD(req, root, root);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_MOUNT, req, ans);
}

__public struct dirent *ceph_readdir(struct ceph_mount_info *cmount,
				     struct ceph_dir_result *dirp)
{
	static struct dirent de;
	int32_t err;

	CEPH_REQ(ceph_readdir, req, 0, ans, 1);

	req.dir = ptr_value(dirp);

	CEPH_BUFF_ADD(ans, &de, sizeof(de));

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_READDIR, req, ans);
	if (err < 0) {
		errno = -err;
		return NULL;
	}
	if (ans.eod) {
		return NULL;
	}

	return &de;
}

__public int ceph_release(struct ceph_mount_info *cmount)
{
	CEPH_REQ(ceph_release, req, 0, ans, 0);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_RELEASE, req, ans);
}

__public int ceph_select_filesystem(struct ceph_mount_info *cmount,
				    const char *fs_name)
{
	CEPH_REQ(ceph_select_filesystem, req, 1, ans, 0);

	CEPH_STR_ADD(req, fs, fs_name);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_SELECT_FILESYSTEM, req, ans);
}

__public int ceph_unmount(struct ceph_mount_info *cmount)
{
	CEPH_REQ(ceph_unmount, req, 0, ans, 0);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_UNMOUNT, req, ans);
}

__public void ceph_userperm_destroy(UserPerm *perms)
{
	CEPH_REQ(ceph_userperm_destroy, req, 0, ans, 0);

	req.userperm = ptr_value(perms);

	CEPH_RUN(&global_cmount, LIBCEPHFSD_OP_USERPERM_DESTROY, req, ans);
}

__public UserPerm *ceph_userperm_new(uid_t uid, gid_t gid, int ngids,
				     gid_t *gidlist)
{
	CEPH_REQ(ceph_userperm_new, req, 1, ans, 0);
	int32_t err;

	req.uid = uid;
	req.gid = gid;
	req.groups = ngids;
	CEPH_BUFF_ADD(req, gidlist, sizeof(gid_t) * ngids);

	err = proxy_global_connect();
	if (err >= 0) {
		err = CEPH_RUN(&global_cmount, LIBCEPHFSD_OP_USERPERM_NEW, req,
			       ans);
	}
	if (err >= 0) {
		return value_ptr(ans.userperm);
	}

	errno = -err;

	return NULL;
}

__public const char *ceph_version(int *major, int *minor, int *patch)
{
	static char cached_version[128];
	static int32_t cached_major = -1, cached_minor, cached_patch;

	if (cached_major < 0) {
		CEPH_REQ(ceph_version, req, 0, ans, 1);
		int32_t err;

		CEPH_BUFF_ADD(ans, cached_version, sizeof(cached_version));

		err = proxy_global_connect();
		if (err >= 0) {
			err = CEPH_RUN(&global_cmount, LIBCEPHFSD_OP_VERSION,
				       req, ans);
		}

		if (err < 0) {
			*major = 0;
			*minor = 0;
			*patch = 0;

			return "Unknown";
		}

		cached_major = ans.major;
		cached_minor = ans.minor;
		cached_patch = ans.patch;
	}

	*major = cached_major;
	*minor = cached_minor;
	*patch = cached_patch;

	return cached_version;
}

__public UserPerm *ceph_mount_perms(struct ceph_mount_info *cmount)
{
	CEPH_REQ(ceph_mount_perms, req, 0, ans, 0);
	int32_t err;

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_MOUNT_PERMS, req, ans);
	if (err < 0) {
		errno = -err;
		return NULL;
	}

	return value_ptr(ans.userperm);
}
