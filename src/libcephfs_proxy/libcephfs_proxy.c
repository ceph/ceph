
#include <stdlib.h>

#include <linux/fscrypt.h>

#include "include/cephfs/libcephfs.h"

#include "proxy_log.h"
#include "proxy_helpers.h"
#include "proxy_requests.h"
#include "proxy_async.h"

/* We override the definition of UserPerm structure to contain internal user
 * credentials. This is already a black box for libcephfs users, so this won't
 * be noticed. */
struct UserPerm {
	uid_t uid;
	gid_t gid;
	uint32_t count;
	gid_t groups[];
};

/* We override the definition of the ceph_mount_info structure to contain
 * internal proxy information. This is already a black box for libcephfs users,
 * so this won't be noticed. */
struct ceph_mount_info {
	proxy_link_t link;
	proxy_link_negotiate_t neg;
	proxy_async_t async;
	char *cwd_path;
	uint64_t cmount;
};

/* The global_cmount is used to stablish an initial connection to serve requests
 * not related to a real cmount, like ceph_version or ceph_userperm_new. */
static struct ceph_mount_info global_cmount = {
	.link = PROXY_LINK_DISCONNECTED,
	.neg = {},
	.cmount = 0
};

static bool client_stop(proxy_link_t *link)
{
	return false;
}

static int32_t proxy_negotiation_check(proxy_link_negotiate_t *neg)
{
	proxy_log(LOG_INFO, 0, "Features enabled: %08x, protocol: %u",
		  neg->v1.enabled, neg->v2.protocol);

	return 0;
}

static int32_t proxy_connect(proxy_link_t *link)
{
	char *path, *env;
	int32_t sd;

	path = PROXY_SOCKET;
	env = getenv(PROXY_SOCKET_ENV);
	if (env != NULL) {
		path = env;
	}

	sd = proxy_link_client(link, path, client_stop);
	if (sd < 0) {
		return sd;
	}

	return sd;
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
		if (err < 0) {
			return err;
		}

		proxy_link_negotiate_init(&global_cmount.neg, 0, PROXY_FEAT_ALL,
					  0,
					  PROXY_FEAT_EMBEDDED_PERMS,
					  PROXY_LINK_PROTOCOL_VERSION);

		err = proxy_link_handshake_client(&global_cmount.link, err,
						  &global_cmount.neg,
						  proxy_negotiation_check);
		if (err < 0) {
			proxy_disconnect(&global_cmount.link);
		}
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
			(_req).v0.cmount = (_cmount)->cmount;       \
			__err = CEPH_RUN(_cmount, _op, _req, _ans); \
		}                                                   \
		__err;                                              \
	})

#define PROXY_EMBED_PERMS(_client, _req, _perms) \
	do { \
		if (proxy_embed_perms(_client, &_req.v0.userperm, _perms)) { \
			_req.v1.ngroups = _perms->count; \
			CEPH_BUFF_ADD(_req, _perms->groups, \
				      sizeof(gid_t) * _perms->count); \
		} else { \
			_req.v1.ngroups = 0; \
		} \
	} while (false)

static bool proxy_embed_perms(struct ceph_mount_info *cmount,
			      embedded_perms_t *embed, const UserPerm *perms)
{
	if ((cmount->neg.v1.enabled & PROXY_FEAT_EMBEDDED_PERMS) != 0) {
		embed->uid = perms->uid;
		embed->gid = perms->gid;
		return true;
	}

	embed->ptr = ptr_value(perms);

	return false;
}

__public int ceph_chdir(struct ceph_mount_info *cmount, const char *path)
{
	CEPH_REQ(ceph_chdir, req, 1, ans, 0);
	char *new_path;
	int32_t err;

	if (strcmp(path, cmount->cwd_path) == 0) {
		return 0;
	}

	new_path = proxy_strdup(path);
	if (new_path == NULL) {
		return -ENOMEM;
	}

	CEPH_STR_ADD(req, v0.path, path);

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_CHDIR, req, ans);
	if (err >= 0) {
		new_path = __atomic_exchange_n(&cmount->cwd_path, new_path,
					       __ATOMIC_SEQ_CST);
	}
	proxy_free(new_path);

	return err;
}

__public int ceph_conf_get(struct ceph_mount_info *cmount, const char *option,
			   char *buf, size_t len)
{
	CEPH_REQ(ceph_conf_get, req, 1, ans, 1);

	req.v0.size = len;

	CEPH_STR_ADD(req, v0.option, option);
	CEPH_BUFF_ADD(ans, buf, len);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_CONF_GET, req, ans);
}

__public int ceph_conf_read_file(struct ceph_mount_info *cmount,
				 const char *path_list)
{
	CEPH_REQ(ceph_conf_read_file, req, 1, ans, 0);

	CEPH_STR_ADD(req, v0.path, path_list);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_CONF_READ_FILE, req, ans);
}

__public int ceph_conf_set(struct ceph_mount_info *cmount, const char *option,
			   const char *value)
{
	CEPH_REQ(ceph_conf_set, req, 2, ans, 0);

	CEPH_STR_ADD(req, v0.option, option);
	CEPH_STR_ADD(req, v0.value, value);

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
	ceph_mount->cwd_path = proxy_strdup("/");
	if (ceph_mount->cwd_path == NULL) {
		err = -ENOMEM;
		goto failed;
	}

	err = proxy_connect(&ceph_mount->link);
	if (err < 0) {
		goto failed;
	}
	sd = err;

	proxy_link_negotiate_init(&ceph_mount->neg, 0, PROXY_FEAT_ALL, 0,
				  PROXY_FEAT_ASYNC_IO |
				  PROXY_FEAT_EMBEDDED_PERMS,
				  PROXY_LINK_PROTOCOL_VERSION);

	err = proxy_link_handshake_client(&ceph_mount->link, sd,
					  &ceph_mount->neg,
					  proxy_negotiation_check);
	if (err < 0) {
		goto failed_link;
	}

	if ((ceph_mount->neg.v1.enabled & PROXY_FEAT_ASYNC_CBK) != 0) {
		err = proxy_async_client(&ceph_mount->async, &ceph_mount->link,
					 sd);
		if (err < 0) {
			goto failed_link;
		}
	}

	CEPH_STR_ADD(req, v0.id, id);

	err = CEPH_CALL(sd, LIBCEPHFSD_OP_CREATE, req, ans);
	if ((err < 0) || ((err = ans.header.result) < 0)) {
		goto failed_link;
	}

	ceph_mount->cmount = ans.v0.cmount;

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
	return cmount->cwd_path;
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

	req.v0.fh = ptr_value(filehandle);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_CLOSE, req, ans);
}

__public int ceph_ll_create(struct ceph_mount_info *cmount, Inode *parent,
			    const char *name, mode_t mode, int oflags,
			    Inode **outp, Fh **fhp, struct ceph_statx *stx,
			    unsigned want, unsigned lflags,
			    const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_create, req, 2, ans, 1);
	int32_t err;

	PROTO_VERSION(&cmount->neg, req, PROXY_PROTOCOL_V1);

	req.v0.parent = ptr_value(parent);
	req.v0.mode = mode;
	req.v0.oflags = oflags;
	req.v0.want = want;
	req.v0.flags = lflags;

	CEPH_STR_ADD(req, v0.name, name);
	PROXY_EMBED_PERMS(cmount, req, perms);

	CEPH_BUFF_ADD(ans, stx, sizeof(*stx));

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_CREATE, req, ans);
	if (err >= 0) {
		*outp = value_ptr(ans.v0.inode);
		*fhp = value_ptr(ans.v0.fh);
	}

	return err;
}

__public int ceph_ll_fallocate(struct ceph_mount_info *cmount, struct Fh *fh,
			       int mode, int64_t offset, int64_t length)
{
	CEPH_REQ(ceph_ll_fallocate, req, 0, ans, 0);

	req.v0.fh = ptr_value(fh);
	req.v0.mode = mode;
	req.v0.offset = offset;
	req.v0.length = length;

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_FALLOCATE, req, ans);
}

__public int ceph_ll_fsync(struct ceph_mount_info *cmount, struct Fh *fh,
			   int syncdataonly)
{
	CEPH_REQ(ceph_ll_fsync, req, 0, ans, 0);

	req.v0.fh = ptr_value(fh);
	req.v0.dataonly = syncdataonly;

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_FSYNC, req, ans);
}

__public int ceph_ll_getattr(struct ceph_mount_info *cmount, struct Inode *in,
			     struct ceph_statx *stx, unsigned int want,
			     unsigned int flags, const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_getattr, req, 1, ans, 1);

	PROTO_VERSION(&cmount->neg, req, PROXY_PROTOCOL_V1);

	req.v0.inode = ptr_value(in);
	req.v0.want = want;
	req.v0.flags = flags;

	PROXY_EMBED_PERMS(cmount, req, perms);

	CEPH_BUFF_ADD(ans, stx, sizeof(*stx));

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_GETATTR, req, ans);
}

__public int ceph_ll_getxattr(struct ceph_mount_info *cmount, struct Inode *in,
			      const char *name, void *value, size_t size,
			      const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_getxattr, req, 2, ans, 1);

	PROTO_VERSION(&cmount->neg, req, PROXY_PROTOCOL_V1);

	req.v0.inode = ptr_value(in);
	req.v0.size = size;

	CEPH_STR_ADD(req, v0.name, name);
	PROXY_EMBED_PERMS(cmount, req, perms);

	CEPH_BUFF_ADD(ans, value, size);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_GETXATTR, req, ans);
}

__public int ceph_ll_link(struct ceph_mount_info *cmount, struct Inode *in,
			  struct Inode *newparent, const char *name,
			  const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_link, req, 2, ans, 0);

	PROTO_VERSION(&cmount->neg, req, PROXY_PROTOCOL_V1);

	req.v0.inode = ptr_value(in);
	req.v0.parent = ptr_value(newparent);

	CEPH_STR_ADD(req, v0.name, name);
	PROXY_EMBED_PERMS(cmount, req, perms);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_LINK, req, ans);
}

__public int ceph_ll_listxattr(struct ceph_mount_info *cmount, struct Inode *in,
			       char *list, size_t buf_size, size_t *list_size,
			       const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_listxattr, req, 1, ans, 1);
	int32_t err;

	PROTO_VERSION(&cmount->neg, req, PROXY_PROTOCOL_V1);

	req.v0.inode = ptr_value(in);
	req.v0.size = buf_size;

	PROXY_EMBED_PERMS(cmount, req, perms);

	CEPH_BUFF_ADD(ans, list, buf_size);

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_LISTXATTR, req, ans);
	if (err >= 0) {
		*list_size = ans.v0.size;
	}

	return err;
}

__public int ceph_ll_lookup(struct ceph_mount_info *cmount, Inode *parent,
			    const char *name, Inode **out,
			    struct ceph_statx *stx, unsigned want,
			    unsigned flags, const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_lookup, req, 2, ans, 1);
	int32_t err;

	PROTO_VERSION(&cmount->neg, req, PROXY_PROTOCOL_V1);

	req.v0.parent = ptr_value(parent);
	req.v0.want = want;
	req.v0.flags = flags;

	CEPH_STR_ADD(req, v0.name, name);
	PROXY_EMBED_PERMS(cmount, req, perms);

	CEPH_BUFF_ADD(ans, stx, sizeof(*stx));

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_LOOKUP, req, ans);
	if (err >= 0) {
		*out = value_ptr(ans.v0.inode);
	}

	return err;
}

__public int ceph_ll_lookup_inode(struct ceph_mount_info *cmount,
				  struct inodeno_t ino, Inode **inode)
{
	CEPH_REQ(ceph_ll_lookup_inode, req, 0, ans, 0);
	int32_t err;

	req.v0.ino = ino;

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_LOOKUP_INODE, req, ans);
	if (err >= 0) {
		*inode = value_ptr(ans.v0.inode);
	}

	return err;
}

__public int ceph_ll_lookup_root(struct ceph_mount_info *cmount, Inode **parent)
{
	CEPH_REQ(ceph_ll_lookup_root, req, 0, ans, 0);
	int32_t err;

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_LOOKUP_ROOT, req, ans);
	if (err >= 0) {
		*parent = value_ptr(ans.v0.inode);
	}

	return err;
}

__public off_t ceph_ll_lseek(struct ceph_mount_info *cmount,
			     struct Fh *filehandle, off_t offset, int whence)
{
	CEPH_REQ(ceph_ll_lseek, req, 0, ans, 0);
	int32_t err;

	req.v0.fh = ptr_value(filehandle);
	req.v0.offset = offset;
	req.v0.whence = whence;

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_LSEEK, req, ans);
	if (err >= 0) {
		return ans.v0.offset;
	}

	return err;
}

__public int ceph_ll_mkdir(struct ceph_mount_info *cmount, Inode *parent,
			   const char *name, mode_t mode, Inode **out,
			   struct ceph_statx *stx, unsigned want,
			   unsigned flags, const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_mkdir, req, 2, ans, 1);
	int32_t err;

	PROTO_VERSION(&cmount->neg, req, PROXY_PROTOCOL_V1);

	req.v0.parent = ptr_value(parent);
	req.v0.mode = mode;
	req.v0.want = want;
	req.v0.flags = flags;

	CEPH_STR_ADD(req, v0.name, name);
	PROXY_EMBED_PERMS(cmount, req, perms);

	CEPH_BUFF_ADD(ans, stx, sizeof(*stx));

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_MKDIR, req, ans);
	if (err >= 0) {
		*out = value_ptr(ans.v0.inode);
	}

	return err;
}

__public int ceph_ll_mknod(struct ceph_mount_info *cmount, Inode *parent,
			   const char *name, mode_t mode, dev_t rdev,
			   Inode **out, struct ceph_statx *stx, unsigned want,
			   unsigned flags, const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_mknod, req, 2, ans, 1);
	int32_t err;

	PROTO_VERSION(&cmount->neg, req, PROXY_PROTOCOL_V1);

	req.v0.parent = ptr_value(parent);
	req.v0.mode = mode;
	req.v0.rdev = rdev;
	req.v0.want = want;
	req.v0.flags = flags;

	CEPH_STR_ADD(req, v0.name, name);
	PROXY_EMBED_PERMS(cmount, req, perms);

	CEPH_BUFF_ADD(ans, stx, sizeof(*stx));

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_MKNOD, req, ans);
	if (err >= 0) {
		*out = value_ptr(ans.v0.inode);
	}

	return err;
}

__public int ceph_ll_open(struct ceph_mount_info *cmount, struct Inode *in,
			  int flags, struct Fh **fh, const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_open, req, 1, ans, 0);
	int32_t err;

	PROTO_VERSION(&cmount->neg, req, PROXY_PROTOCOL_V1);

	req.v0.inode = ptr_value(in);
	req.v0.flags = flags;

	PROXY_EMBED_PERMS(cmount, req, perms);

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_OPEN, req, ans);
	if (err >= 0) {
		*fh = value_ptr(ans.v0.fh);
	}

	return err;
}

__public int ceph_ll_opendir(struct ceph_mount_info *cmount, struct Inode *in,
			     struct ceph_dir_result **dirpp,
			     const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_opendir, req, 1, ans, 0);
	int32_t err;

	PROTO_VERSION(&cmount->neg, req, PROXY_PROTOCOL_V1);

	req.v0.inode = ptr_value(in);

	PROXY_EMBED_PERMS(cmount, req, perms);

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_OPENDIR, req, ans);
	if (err >= 0) {
		*dirpp = value_ptr(ans.v0.dir);
	}

	return err;
}

__public int ceph_ll_put(struct ceph_mount_info *cmount, struct Inode *in)
{
	CEPH_REQ(ceph_ll_put, req, 0, ans, 0);

	req.v0.inode = ptr_value(in);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_PUT, req, ans);
}

__public int ceph_ll_read(struct ceph_mount_info *cmount, struct Fh *filehandle,
			  int64_t off, uint64_t len, char *buf)
{
	CEPH_REQ(ceph_ll_read, req, 0, ans, 1);

	req.v0.fh = ptr_value(filehandle);
	req.v0.offset = off;
	req.v0.len = len;

	CEPH_BUFF_ADD(ans, buf, len);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_READ, req, ans);
}

__public int ceph_ll_readlink(struct ceph_mount_info *cmount, struct Inode *in,
			      char *buf, size_t bufsize, const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_readlink, req, 1, ans, 1);

	PROTO_VERSION(&cmount->neg, req, PROXY_PROTOCOL_V1);

	req.v0.inode = ptr_value(in);
	req.v0.size = bufsize;

	PROXY_EMBED_PERMS(cmount, req, perms);

	CEPH_BUFF_ADD(ans, buf, bufsize);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_READLINK, req, ans);
}

__public int ceph_ll_releasedir(struct ceph_mount_info *cmount,
				struct ceph_dir_result *dir)
{
	CEPH_REQ(ceph_ll_releasedir, req, 0, ans, 0);

	req.v0.dir = ptr_value(dir);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_RELEASEDIR, req, ans);
}

__public int ceph_ll_removexattr(struct ceph_mount_info *cmount,
				 struct Inode *in, const char *name,
				 const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_removexattr, req, 2, ans, 0);

	PROTO_VERSION(&cmount->neg, req, PROXY_PROTOCOL_V1);

	req.v0.inode = ptr_value(in);

	CEPH_STR_ADD(req, v0.name, name);
	PROXY_EMBED_PERMS(cmount, req, perms);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_REMOVEXATTR, req, ans);
}

__public int ceph_ll_rename(struct ceph_mount_info *cmount,
			    struct Inode *parent, const char *name,
			    struct Inode *newparent, const char *newname,
			    const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_rename, req, 3, ans, 0);

	PROTO_VERSION(&cmount->neg, req, PROXY_PROTOCOL_V1);

	req.v0.old_parent = ptr_value(parent);
	req.v0.new_parent = ptr_value(newparent);

	CEPH_STR_ADD(req, v0.old_name, name);
	CEPH_STR_ADD(req, v0.new_name, newname);
	PROXY_EMBED_PERMS(cmount, req, perms);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_RENAME, req, ans);
}

__public void ceph_rewinddir(struct ceph_mount_info *cmount,
			     struct ceph_dir_result *dirp)
{
	CEPH_REQ(ceph_rewinddir, req, 0, ans, 0);

	req.v0.dir = ptr_value(dirp);

	CEPH_PROCESS(cmount, LIBCEPHFSD_OP_REWINDDIR, req, ans);
}

__public int ceph_ll_rmdir(struct ceph_mount_info *cmount, struct Inode *in,
			   const char *name, const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_rmdir, req, 2, ans, 0);

	PROTO_VERSION(&cmount->neg, req, PROXY_PROTOCOL_V1);

	req.v0.parent = ptr_value(in);

	CEPH_STR_ADD(req, v0.name, name);
	PROXY_EMBED_PERMS(cmount, req, perms);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_RMDIR, req, ans);
}

__public int ceph_ll_setattr(struct ceph_mount_info *cmount, struct Inode *in,
			     struct ceph_statx *stx, int mask,
			     const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_setattr, req, 2, ans, 0);

	PROTO_VERSION(&cmount->neg, req, PROXY_PROTOCOL_V1);

	req.v0.inode = ptr_value(in);
	req.v0.mask = mask;

	CEPH_BUFF_ADD(req, stx, sizeof(*stx));
	PROXY_EMBED_PERMS(cmount, req, perms);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_SETATTR, req, ans);
}

__public int ceph_ll_setxattr(struct ceph_mount_info *cmount, struct Inode *in,
			      const char *name, const void *value, size_t size,
			      int flags, const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_setxattr, req, 3, ans, 0);

	PROTO_VERSION(&cmount->neg, req, PROXY_PROTOCOL_V1);

	req.v0.inode = ptr_value(in);
	req.v0.size = size;
	req.v0.flags = flags;

	CEPH_STR_ADD(req, v0.name, name);
	CEPH_BUFF_ADD(req, value, size);
	PROXY_EMBED_PERMS(cmount, req, perms);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_SETXATTR, req, ans);
}

__public int ceph_ll_statfs(struct ceph_mount_info *cmount, struct Inode *in,
			    struct statvfs *stbuf)
{
	CEPH_REQ(ceph_ll_statfs, req, 0, ans, 1);

	req.v0.inode = ptr_value(in);

	CEPH_BUFF_ADD(ans, stbuf, sizeof(*stbuf));

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_STATFS, req, ans);
}

__public int ceph_ll_symlink(struct ceph_mount_info *cmount, Inode *in,
			     const char *name, const char *value, Inode **out,
			     struct ceph_statx *stx, unsigned want,
			     unsigned flags, const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_symlink, req, 3, ans, 1);
	int32_t err;

	PROTO_VERSION(&cmount->neg, req, PROXY_PROTOCOL_V1);

	req.v0.parent = ptr_value(in);
	req.v0.want = want;
	req.v0.flags = flags;

	CEPH_STR_ADD(req, v0.name, name);
	CEPH_STR_ADD(req, v0.target, value);
	PROXY_EMBED_PERMS(cmount, req, perms);

	CEPH_BUFF_ADD(req, stx, sizeof(*stx));

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_SYMLINK, req, ans);
	if (err >= 0) {
		*out = value_ptr(ans.v0.inode);
	}

	return err;
}

__public int ceph_ll_unlink(struct ceph_mount_info *cmount, struct Inode *in,
			    const char *name, const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_unlink, req, 2, ans, 0);

	PROTO_VERSION(&cmount->neg, req, PROXY_PROTOCOL_V1);

	req.v0.parent = ptr_value(in);

	CEPH_STR_ADD(req, v0.name, name);
	PROXY_EMBED_PERMS(cmount, req, perms);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_UNLINK, req, ans);
}

__public int ceph_ll_walk(struct ceph_mount_info *cmount, const char *name,
			  Inode **i, struct ceph_statx *stx, unsigned int want,
			  unsigned int flags, const UserPerm *perms)
{
	CEPH_REQ(ceph_ll_walk, req, 2, ans, 1);
	int32_t err;

	PROTO_VERSION(&cmount->neg, req, PROXY_PROTOCOL_V1);

	req.v0.want = want;
	req.v0.flags = flags;

	CEPH_STR_ADD(req, v0.path, name);
	PROXY_EMBED_PERMS(cmount, req, perms);

	CEPH_BUFF_ADD(ans, stx, sizeof(*stx));

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_WALK, req, ans);
	if (err >= 0) {
		*i = value_ptr(ans.v0.inode);
	}

	return err;
}

__public int ceph_ll_write(struct ceph_mount_info *cmount,
			   struct Fh *filehandle, int64_t off, uint64_t len,
			   const char *data)
{
	CEPH_REQ(ceph_ll_write, req, 1, ans, 0);

	req.v0.fh = ptr_value(filehandle);
	req.v0.offset = off;
	req.v0.len = len;
	CEPH_BUFF_ADD(req, data, len);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_WRITE, req, ans);
}

__public int ceph_mount(struct ceph_mount_info *cmount, const char *root)
{
	CEPH_REQ(ceph_mount, req, 1, ans, 0);

	CEPH_STR_ADD(req, v0.root, root);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_MOUNT, req, ans);
}

/* The return value of this function has the same meaning as the original
 * ceph_readdir_r():
 *
 * Returned values:
 *
 *    1 if we got a dirent
 *    0 for end of directory
 *   <0 for error
 */
__public int ceph_readdir_r(struct ceph_mount_info *cmount,
			    struct ceph_dir_result *dirp, struct dirent *de)
{
	int32_t err;

	CEPH_REQ(ceph_readdir, req, 0, ans, 1);

	req.v0.dir = ptr_value(dirp);

	CEPH_BUFF_ADD(ans, de, sizeof(struct dirent));

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_READDIR, req, ans);
	if (err < 0) {
		return err;
	}
	if (ans.v0.eod) {
		return 0;
	}

	return 1;
}

__public struct dirent *ceph_readdir(struct ceph_mount_info *cmount,
				     struct ceph_dir_result *dirp)
{
	static struct dirent de;
	int res;

	res = ceph_readdir_r(cmount, dirp, &de);
	if (res <= 0) {
		if (res < 0) {
			errno = -res;
		}
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

	CEPH_STR_ADD(req, v0.fs, fs_name);

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

	/* We need to make use of the negotiation of the global mount since we
	 * don't have access to cmount here. Both negotiations should have
	 * the same setting for the embedded perms feature. */
	if ((global_cmount.neg.v1.enabled & PROXY_FEAT_EMBEDDED_PERMS) != 0) {
		proxy_free(perms);
	} else {
		req.v0.userperm = ptr_value(perms);

		CEPH_RUN(&global_cmount, LIBCEPHFSD_OP_USERPERM_DESTROY, req,
			 ans);
	}
}

__public UserPerm *ceph_userperm_new(uid_t uid, gid_t gid, int ngids,
				     gid_t *gidlist)
{
	CEPH_REQ(ceph_userperm_new, req, 1, ans, 0);
	UserPerm *perms;
	int32_t err;

	err = proxy_global_connect();
	if (err < 0) {
		errno = -err;
		return NULL;
	}

	/* We need to make use of the negotiation of the global mount since we
	 * don't have access to cmount here. Both negotiations should have
	 * the same setting for the embedded perms feature. */
	if ((global_cmount.neg.v1.enabled & PROXY_FEAT_EMBEDDED_PERMS) != 0) {
		perms = proxy_malloc(sizeof(UserPerm) + sizeof(gid_t) * ngids);
		if (perms == NULL) {
			errno = -ENOMEM;
			return NULL;
		}

		perms->uid = uid;
		perms->gid = gid;
		perms->count = ngids;
		memcpy(perms->groups, gidlist, sizeof(gid_t) * ngids);

		return perms;
	}

	req.v0.uid = uid;
	req.v0.gid = gid;
	req.v0.groups = ngids;
	CEPH_BUFF_ADD(req, gidlist, sizeof(gid_t) * ngids);

	err = CEPH_RUN(&global_cmount, LIBCEPHFSD_OP_USERPERM_NEW, req, ans);
	if (err >= 0) {
		return value_ptr(ans.v0.userperm);
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

		cached_major = ans.v0.major;
		cached_minor = ans.v0.minor;
		cached_patch = ans.v0.patch;
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

	if ((cmount->neg.v1.enabled & PROXY_FEAT_EMBEDDED_PERMS) != 0) {
		errno = -EOPNOTSUPP;
		return NULL;
	}

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_MOUNT_PERMS, req, ans);
	if (err < 0) {
		errno = -err;
		return NULL;
	}

	return value_ptr(ans.v0.userperm);
}

__public int64_t ceph_ll_nonblocking_readv_writev(
	struct ceph_mount_info *cmount, struct ceph_ll_io_info *io_info)
{
	CEPH_REQ(ceph_ll_nonblocking_readv_writev, req,
		 io_info->write ? io_info->iovcnt : 0, ans, 0);
	int32_t i, err;

	if ((cmount->neg.v1.enabled & PROXY_FEAT_ASYNC_IO) == 0) {
		return -EOPNOTSUPP;
	}

	req.v0.info = ptr_checksum(&cmount->async.random, io_info);
	req.v0.fh = (uintptr_t)io_info->fh;
	req.v0.off = io_info->off;
	req.v0.size = 0;
	req.v0.write = io_info->write;
	req.v0.fsync = io_info->fsync;
	req.v0.syncdataonly = io_info->syncdataonly;

	for (i = 0; i < io_info->iovcnt; i++) {
		if (io_info->write) {
			CEPH_BUFF_ADD(req, io_info->iov[i].iov_base,
				      io_info->iov[i].iov_len);
		}
		req.v0.size += io_info->iov[i].iov_len;
	}

	err = CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_NONBLOCKING_RW, req, ans);
	if (err < 0) {
		return err;
	}

	return ans.v0.res;
}

__public int32_t ceph_add_fscrypt_key(struct ceph_mount_info *cmount,
				      const char *key_data, int32_t key_len,
				      struct ceph_fscrypt_key_identifier *kid,
				      int32_t user)
{
	CEPH_REQ(ceph_add_fscrypt_key, req, 1, ans, 1);

	req.v0.user = user;
	req.v0.kid = FSCRYPT_KEY_IDENTIFIER_SIZE;

	CEPH_BUFF_ADD(req, key_data, key_len);

	CEPH_BUFF_ADD(ans, kid, FSCRYPT_KEY_IDENTIFIER_SIZE);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_ADD_FSCRYPT_KEY, req, ans);
}

__public int32_t ceph_remove_fscrypt_key(struct ceph_mount_info *cmount,
					 struct fscrypt_remove_key_arg *arg,
					 int32_t user)
{
	CEPH_REQ(ceph_remove_fscrypt_key, req, 1, ans, 1);

	req.v0.user = user;
	req.v0.arg = sizeof(struct fscrypt_remove_key_arg);

	CEPH_BUFF_ADD(req, arg, sizeof(struct fscrypt_remove_key_arg));

	CEPH_BUFF_ADD(ans, arg, sizeof(struct fscrypt_remove_key_arg));

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_REMOVE_FSCRYPT_KEY, req, ans);
}

__public int32_t ceph_get_fscrypt_key_status(
	struct ceph_mount_info *cmount, struct fscrypt_get_key_status_arg *arg)
{
	CEPH_REQ(ceph_get_fscrypt_key_status, req, 1, ans, 1);

	req.v0.arg = sizeof(struct fscrypt_get_key_status_arg);

	CEPH_BUFF_ADD(req, arg, sizeof(struct fscrypt_get_key_status_arg));

	CEPH_BUFF_ADD(ans, arg, sizeof(struct fscrypt_get_key_status_arg));

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_REMOVE_FSCRYPT_KEY, req, ans);
}

__public int32_t ceph_ll_set_fscrypt_policy_v2(
	struct ceph_mount_info *cmount, Inode *in,
	const struct fscrypt_policy_v2 *policy)
{
	CEPH_REQ(ceph_ll_set_fscrypt_policy_v2, req, 1, ans, 0);

	req.v0.inode = ptr_value(in);
	req.v0.policy = sizeof(struct fscrypt_policy_v2);

	CEPH_BUFF_ADD(req, policy, sizeof(struct fscrypt_policy_v2));

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_SET_FSCRYPT_POLICY_V2, req,
			    ans);
}

__public int32_t ceph_ll_get_fscrypt_policy_v2(struct ceph_mount_info *cmount,
					       Inode *in,
					       struct fscrypt_policy_v2 *policy)
{
	CEPH_REQ(ceph_ll_get_fscrypt_policy_v2, req, 0, ans, 1);

	req.v0.inode = ptr_value(in);
	req.v0.policy = sizeof(struct fscrypt_policy_v2);

	CEPH_BUFF_ADD(ans, policy, sizeof(struct fscrypt_policy_v2));

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_GET_FSCRYPT_POLICY_V2, req,
			    ans);
}

__public int32_t ceph_ll_is_encrypted(struct ceph_mount_info *cmount,
				      Inode *in, char *enctag)
{
	CEPH_REQ(ceph_ll_is_encrypted, req, 1, ans, 0);

	req.v0.inode = ptr_value(in);

	CEPH_STR_ADD(req, v0.tag, enctag);

	return CEPH_PROCESS(cmount, LIBCEPHFSD_OP_LL_IS_ENCRYPTED, req, ans);
}
