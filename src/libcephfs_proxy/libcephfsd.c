
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/uio.h>
#include <getopt.h>
#include <endian.h>
#include <string.h>
#include <stdarg.h>

#include "include/cephfs/libcephfs.h"

#include "proxy_manager.h"
#include "proxy_link.h"
#include "proxy_helpers.h"
#include "proxy_log.h"
#include "proxy_requests.h"
#include "proxy_mount.h"
#include "proxy_async.h"

typedef struct _proxy_server {
	proxy_link_t link;
	proxy_manager_t *manager;
} proxy_server_t;

typedef struct _proxy_client {
	proxy_worker_t worker;
	proxy_link_negotiate_t neg;
	proxy_async_t async;
	proxy_link_t *link;
	proxy_random_t random;
	void *buffer;
	uint32_t buffer_size;
	int32_t sd;
} proxy_client_t;

typedef struct _proxy {
	proxy_manager_t manager;
	proxy_log_handler_t log_handler;
	const char *socket_path;
} proxy_t;

typedef int32_t (*proxy_handler_t)(proxy_client_t *, proxy_req_t *,
				   const void *data, int32_t data_size);

typedef struct _proxy_async_io {
	struct ceph_ll_io_info io_info;
	struct iovec iov;
	proxy_async_t *async;
} proxy_async_io_t;

/* This is used for requests that are not associated with a cmount. */
static proxy_random_t global_random;

static int32_t send_error(proxy_client_t *client, int32_t error)
{
	proxy_link_ans_t ans;
	struct iovec iov[1];

	iov[0].iov_base = &ans;
	iov[0].iov_len = sizeof(ans);

	return proxy_link_ans_send(client->sd, error, iov, 1);
}

/* Macro to simplify request handling. */
#define CEPH_COMPLETE(_client, _err, _ans)                          \
	({                                                          \
		int32_t __err = (_err);                             \
		if (__err < 0) {                                    \
			__err = send_error(_client, __err);         \
		} else {                                            \
			__err = CEPH_RET(_client->sd, __err, _ans); \
		}                                                   \
		__err;                                              \
	})

#ifdef PROXY_TRACE
#define TRACE(_fmt, _args...) printf(_fmt "\n", ##_args)
#else
#define TRACE(_fmt, _args...) do { } while (0)
#endif

static int32_t libcephfsd_version(proxy_client_t *client, proxy_req_t *req,
				  const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_version, ans, 1);
	const char *text;
	int32_t major, minor, patch;

	text = ceph_version(&major, &minor, &patch);
	TRACE("ceph_version(%d, %d, %d) -> %s", major, minor, patch, text);

	ans.major = major;
	ans.minor = minor;
	ans.patch = patch;

	CEPH_STR_ADD(ans, text, text);

	return CEPH_RET(client->sd, 0, ans);
}

static int32_t libcephfsd_userperm_new(proxy_client_t *client, proxy_req_t *req,
				       const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_userperm_new, ans, 0);
	UserPerm *userperm;
	int32_t err;

	userperm = ceph_userperm_new(req->userperm_new.uid,
				     req->userperm_new.gid,
				     req->userperm_new.groups, (gid_t *)data);
	TRACE("ceph_userperm_new(%u, %u, %u) -> %p", req->userperm_new.uid,
	      req->userperm_new.gid, req->userperm_new.groups, userperm);

	err = -ENOMEM;
	if (userperm != NULL) {
		ans.userperm = ptr_checksum(&global_random, userperm);
		err = 0;
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_userperm_destroy(proxy_client_t *client,
					   proxy_req_t *req, const void *data,
					   int32_t data_size)
{
	CEPH_DATA(ceph_userperm_destroy, ans, 0);
	UserPerm *perms;
	int32_t err;

	err = ptr_check(&global_random, req->userperm_destroy.userperm,
			(void **)&perms);

	if (err >= 0) {
		ceph_userperm_destroy(perms);
		TRACE("ceph_userperm_destroy(%p)", perms);
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_create(proxy_client_t *client, proxy_req_t *req,
				 const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_create, ans, 0);
	proxy_mount_t *mount;
	const char *id;
	int32_t err;

	id = CEPH_STR_GET(req->create, id, data);

	err = proxy_mount_create(&mount, id);
	TRACE("ceph_create(%p, '%s') -> %d", mount, id, err);

	if (err >= 0) {
		ans.cmount = ptr_checksum(&client->random, mount);
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_release(proxy_client_t *client, proxy_req_t *req,
				  const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_release, ans, 0);
	proxy_mount_t *mount;
	int32_t err;

	err = ptr_check(&client->random, req->release.cmount, (void **)&mount);
	if (err >= 0) {
		err = proxy_mount_release(mount);
		TRACE("ceph_release(%p) -> %d", mount, err);
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_conf_read_file(proxy_client_t *client,
					 proxy_req_t *req, const void *data,
					 int32_t data_size)
{
	CEPH_DATA(ceph_conf_read_file, ans, 0);
	proxy_mount_t *mount;
	const char *path;
	int32_t err;

	err = ptr_check(&client->random, req->conf_read_file.cmount,
			(void **)&mount);
	if (err >= 0) {
		path = CEPH_STR_GET(req->conf_read_file, path, data);

		err = proxy_mount_config(mount, path);
		TRACE("ceph_conf_read_file(%p, '%s') ->%d", mount, path, err);
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_conf_get(proxy_client_t *client, proxy_req_t *req,
				   const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_conf_get, ans, 1);
	proxy_mount_t *mount;
	const char *option;
	void *buffer;
	uint32_t size;
	int32_t err;

	buffer = client->buffer;
	size = client->buffer_size;
	if (req->conf_get.size < size) {
		size = req->conf_get.size;
	}
	err = ptr_check(&client->random, req->conf_get.cmount, (void **)&mount);
	if (err >= 0) {
		option = CEPH_STR_GET(req->conf_get, option, data);

		err = proxy_mount_get(mount, option, buffer, size);
		TRACE("ceph_conf_get(%p, '%s', '%s') -> %d", mount, option,
		      (char *)buffer, err);

		if (err >= 0) {
			CEPH_DATA_ADD(ans, value, buffer, strlen(buffer) + 1);
		}
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_conf_set(proxy_client_t *client, proxy_req_t *req,
				   const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_conf_set, ans, 0);
	proxy_mount_t *mount;
	const char *option, *value;
	int32_t err;

	err = ptr_check(&client->random, req->conf_set.cmount, (void **)&mount);
	if (err >= 0) {
		option = CEPH_STR_GET(req->conf_set, option, data);
		value = CEPH_STR_GET(req->conf_set, value,
				     (const char *)(data) + req->conf_set.option);

		err = proxy_mount_set(mount, option, value);
		TRACE("ceph_conf_set(%p, '%s', '%s') -> %d", mount, option,
		      value, err);
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_init(proxy_client_t *client, proxy_req_t *req,
			       const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_init, ans, 0);
	proxy_mount_t *mount;
	int32_t err;

	err = ptr_check(&client->random, req->init.cmount, (void **)&mount);
	if (err >= 0) {
		err = proxy_mount_init(mount);
		TRACE("ceph_init(%p) -> %d", mount, err);
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_select_filesystem(proxy_client_t *client,
					    proxy_req_t *req, const void *data,
					    int32_t data_size)
{
	CEPH_DATA(ceph_select_filesystem, ans, 0);
	proxy_mount_t *mount;
	const char *fs;
	int32_t err;

	err = ptr_check(&client->random, req->select_filesystem.cmount,
			(void **)&mount);
	if (err >= 0) {
		fs = CEPH_STR_GET(req->select_filesystem, fs, data);

		err = proxy_mount_select(mount, fs);
		TRACE("ceph_select_filesystem(%p, '%s') -> %d", mount, fs, err);
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_mount(proxy_client_t *client, proxy_req_t *req,
				const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_mount, ans, 0);
	proxy_mount_t *mount;
	const char *root;
	int32_t err;

	err = ptr_check(&client->random, req->mount.cmount, (void **)&mount);
	if (err >= 0) {
		root = CEPH_STR_GET(req->mount, root, data);

		err = proxy_mount_mount(mount, root);
		TRACE("ceph_mount(%p, '%s') -> %d", mount, root, err);
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_unmount(proxy_client_t *client, proxy_req_t *req,
				  const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_unmount, ans, 0);
	proxy_mount_t *mount;
	int32_t err;

	err = ptr_check(&client->random, req->unmount.cmount, (void **)&mount);

	if (err >= 0) {
		err = proxy_mount_unmount(mount);
		TRACE("ceph_unmount(%p) -> %d", mount, err);
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_statfs(proxy_client_t *client, proxy_req_t *req,
				    const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_ll_statfs, ans, 1);
	struct statvfs st;
	proxy_mount_t *mount;
	struct Inode *inode;
	int32_t err;

	err = ptr_check(&client->random, req->ll_statfs.cmount,
			(void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_statfs.inode,
				(void **)&inode);
	}

	if (err >= 0) {
		CEPH_BUFF_ADD(ans, &st, sizeof(st));

		err = ceph_ll_statfs(proxy_cmount(mount), inode, &st);
		TRACE("ceph_ll_statfs(%p, %p) -> %d", mount, inode, err);
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_lookup(proxy_client_t *client, proxy_req_t *req,
				    const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_ll_lookup, ans, 1);
	struct ceph_statx stx;
	proxy_mount_t *mount;
	struct Inode *parent, *out;
	const char *name;
	UserPerm *perms;
	uint32_t want, flags;
	int32_t err;

	err = ptr_check(&client->random, req->ll_lookup.cmount,
			(void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_lookup.parent,
				(void **)&parent);
	}
	if (err >= 0) {
		err = ptr_check(&global_random, req->ll_lookup.userperm,
				(void **)&perms);
	}
	if (err >= 0) {
		want = req->ll_lookup.want;
		flags = req->ll_lookup.flags;
		name = CEPH_STR_GET(req->ll_lookup, name, data);

		CEPH_BUFF_ADD(ans, &stx, sizeof(stx));

		if (name == NULL) {
			err = proxy_log(LOG_ERR, EINVAL,
					"NULL name passed to ceph_ll_lookup()");
		} else {
			// Forbid going outside of the root mount point
			if ((parent == mount->root) &&
			    (strcmp(name, "..") == 0)) {
				name = ".";
			}

			err = ceph_ll_lookup(proxy_cmount(mount), parent, name,
					     &out, &stx, want, flags, perms);
		}

		TRACE("ceph_ll_lookup(%p, %p, '%s', %p, %x, %x, %p) -> %d",
		      mount, parent, name, out, want, flags, perms, err);

		if (err >= 0) {
			ans.inode = ptr_checksum(&client->random, out);
		}
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_lookup_inode(proxy_client_t *client,
					  proxy_req_t *req, const void *data,
					  int32_t data_size)
{
	CEPH_DATA(ceph_ll_lookup_inode, ans, 0);
	proxy_mount_t *mount;
	struct Inode *inode;
	struct inodeno_t ino;
	int32_t err;

	err = ptr_check(&client->random, req->ll_lookup_inode.cmount,
			(void **)&mount);
	if (err >= 0) {
		ino = req->ll_lookup_inode.ino;

		err = ceph_ll_lookup_inode(proxy_cmount(mount), ino, &inode);
		TRACE("ceph_ll_lookup_inode(%p, %lu, %p) -> %d", mount, ino.val,
		      inode, err);

		if (err >= 0) {
			ans.inode = ptr_checksum(&client->random, inode);
		}
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_lookup_root(proxy_client_t *client,
					 proxy_req_t *req, const void *data,
					 int32_t data_size)
{
	CEPH_DATA(ceph_ll_lookup_root, ans, 0);
	proxy_mount_t *mount;
	int32_t err;

	err = ptr_check(&client->random, req->ll_lookup_root.cmount,
			(void **)&mount);
	if (err >= 0) {
		/* The libcephfs view of the root of the mount could be
		 * different than ours, so we can't rely on
		 * ceph_ll_lookup_root(). We fake it by returning the cached
		 * root inode at the time of mount. */
		err = proxy_inode_ref(mount, mount->root_ino);
		TRACE("ceph_ll_lookup_root(%p, %p) -> %d", mount, mount->root,
		      err);

		if (err >= 0) {
			ans.inode = ptr_checksum(&client->random, mount->root);
		}
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_put(proxy_client_t *client, proxy_req_t *req,
				 const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_ll_put, ans, 0);
	proxy_mount_t *mount;
	struct Inode *inode;
	int32_t err;

	err = ptr_check(&client->random, req->ll_put.cmount, (void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_put.inode,
				(void **)&inode);
	}

	if (err >= 0) {
		err = ceph_ll_put(proxy_cmount(mount), inode);
		TRACE("ceph_ll_put(%p, %p) -> %d", mount, inode, err);
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_walk(proxy_client_t *client, proxy_req_t *req,
				  const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_ll_walk, ans, 1);
	struct ceph_statx stx;
	proxy_mount_t *mount;
	struct Inode *inode;
	const char *path;
	UserPerm *perms;
	uint32_t want, flags;
	int32_t err;

	err = ptr_check(&client->random, req->ll_walk.cmount, (void **)&mount);
	if (err >= 0) {
		err = ptr_check(&global_random, req->ll_walk.userperm,
				(void **)&perms);
	}
	if (err >= 0) {
		want = req->ll_walk.want;
		flags = req->ll_walk.flags;
		path = CEPH_STR_GET(req->ll_walk, path, data);

		CEPH_BUFF_ADD(ans, &stx, sizeof(stx));

		err = proxy_path_resolve(mount, path, &inode, &stx, want, flags,
					 perms, NULL);
		TRACE("ceph_ll_walk(%p, '%s', %p, %x, %x, %p) -> %d", mount,
		      path, inode, want, flags, perms, err);

		if (err >= 0) {
			ans.inode = ptr_checksum(&client->random, inode);
		}
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_chdir(proxy_client_t *client, proxy_req_t *req,
				const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_chdir, ans, 0);
	struct ceph_statx stx;
	proxy_mount_t *mount;
	struct Inode *inode;
	const char *path;
	char *realpath;
	int32_t err;

	err = ptr_check(&client->random, req->chdir.cmount, (void **)&mount);
	if (err >= 0) {
		path = CEPH_STR_GET(req->chdir, path, data);

		/* Since the libcephfs mount may be shared, we can't really
		 * change the current directory to avoid interferences with
		 * other users, so we just lookup the new directory and keep an
		 * internal reference. */
		err = proxy_path_resolve(mount, path, &inode, &stx,
					 CEPH_STATX_INO, 0, mount->perms,
					 &realpath);
		TRACE("ceph_chdir(%p, '%s') -> %d", mount, path, err);
		if (err >= 0) {
			ceph_ll_put(proxy_cmount(mount), mount->cwd);
			mount->cwd = inode;
			mount->cwd_ino = stx.stx_ino;

			/* TODO: This path may become outdated if the parent
			 *       directories are moved, however this seems the
			 *       best we can do for now. */
			proxy_free(mount->cwd_path);
			mount->cwd_path = realpath;
			mount->cwd_path_len = strlen(realpath);
		}
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_getcwd(proxy_client_t *client, proxy_req_t *req,
				 const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_getcwd, ans, 1);
	proxy_mount_t *mount;
	const char *path;
	int32_t err;

	err = ptr_check(&client->random, req->getcwd.cmount, (void **)&mount);

	if (err >= 0) {
		/* We just return the cached name from the last chdir(). */
		path = mount->cwd_path;
		TRACE("ceph_getcwd(%p) -> '%s'", mount, path);
		CEPH_STR_ADD(ans, path, path);
		err = 0;
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_readdir(proxy_client_t *client, proxy_req_t *req,
				  const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_readdir, ans, 1);
	struct dirent de;
	proxy_mount_t *mount;
	struct ceph_dir_result *dirp;
	int32_t err;

	err = ptr_check(&client->random, req->readdir.cmount, (void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->readdir.dir,
				(void **)&dirp);
	}

	if (err >= 0) {
		err = ceph_readdir_r(proxy_cmount(mount), dirp, &de);
		TRACE("ceph_readdir_r(%p, %p, %p) -> %d", mount, dirp, &de,
		      err);
		ans.eod = true;
		if (err > 0) {
			ans.eod = false;
			CEPH_BUFF_ADD(ans, &de,
				      offset_of(struct dirent, d_name) +
					      strlen(de.d_name) + 1);
		}
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_rewinddir(proxy_client_t *client, proxy_req_t *req,
				    const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_rewinddir, ans, 0);
	proxy_mount_t *mount;
	struct ceph_dir_result *dirp;
	int32_t err;

	err = ptr_check(&client->random, req->rewinddir.cmount,
			(void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->rewinddir.dir,
				(void **)&dirp);
	}

	if (err >= 0) {
		ceph_rewinddir(proxy_cmount(mount), dirp);
		TRACE("ceph_rewinddir(%p, %p)", mount, dirp);
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_open(proxy_client_t *client, proxy_req_t *req,
				  const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_ll_open, ans, 0);
	proxy_mount_t *mount;
	struct Inode *inode;
	UserPerm *perms;
	struct Fh *fh;
	int32_t flags, err;

	err = ptr_check(&client->random, req->ll_open.cmount, (void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_open.inode,
				(void **)&inode);
	}
	if (err >= 0) {
		err = ptr_check(&global_random, req->ll_open.userperm,
				(void **)&perms);
	}
	if (err >= 0) {
		flags = req->ll_open.flags;

		err = ceph_ll_open(proxy_cmount(mount), inode, flags, &fh,
				   perms);
		TRACE("ceph_ll_open(%p, %p, %x, %p, %p) -> %d", mount, inode,
		      flags, fh, perms, err);

		if (err >= 0) {
			ans.fh = ptr_checksum(&client->random, fh);
		}
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_create(proxy_client_t *client, proxy_req_t *req,
				    const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_ll_create, ans, 1);
	struct ceph_statx stx;
	proxy_mount_t *mount;
	struct Inode *parent, *inode;
	struct Fh *fh;
	const char *name;
	UserPerm *perms;
	mode_t mode;
	uint32_t want, flags;
	int32_t oflags, err;

	err = ptr_check(&client->random, req->ll_create.cmount,
			(void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_create.parent,
				(void **)&parent);
	}
	if (err >= 0) {
		err = ptr_check(&global_random, req->ll_create.userperm,
				(void **)&perms);
	}
	if (err >= 0) {
		mode = req->ll_create.mode;
		oflags = req->ll_create.oflags;
		want = req->ll_create.want;
		flags = req->ll_create.flags;
		name = CEPH_STR_GET(req->ll_create, name, data);

		CEPH_BUFF_ADD(ans, &stx, sizeof(stx));

		err = ceph_ll_create(proxy_cmount(mount), parent, name, mode,
				     oflags, &inode, &fh, &stx, want, flags,
				     perms);
		TRACE("ceph_ll_create(%p, %p, '%s', %o, %x, %p, %p, %x, %x, "
		      "%p) -> %d",
		      mount, parent, name, mode, oflags, inode, fh, want, flags,
		      perms, err);

		if (err >= 0) {
			ans.fh = ptr_checksum(&client->random, fh);
			ans.inode = ptr_checksum(&client->random, inode);
		}
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_mknod(proxy_client_t *client, proxy_req_t *req,
				   const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_ll_mknod, ans, 1);
	struct ceph_statx stx;
	proxy_mount_t *mount;
	struct Inode *parent, *inode;
	const char *name;
	UserPerm *perms;
	dev_t rdev;
	mode_t mode;
	uint32_t want, flags;
	int32_t err;

	err = ptr_check(&client->random, req->ll_mknod.cmount, (void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_mknod.parent,
				(void **)&parent);
	}
	if (err >= 0) {
		err = ptr_check(&global_random, req->ll_mknod.userperm,
				(void **)&perms);
	}
	if (err >= 0) {
		mode = req->ll_mknod.mode;
		rdev = req->ll_mknod.rdev;
		want = req->ll_mknod.want;
		flags = req->ll_mknod.flags;
		name = CEPH_STR_GET(req->ll_mknod, name, data);

		CEPH_BUFF_ADD(ans, &stx, sizeof(stx));

		err = ceph_ll_mknod(proxy_cmount(mount), parent, name, mode,
				    rdev, &inode, &stx, want, flags, perms);
		TRACE("ceph_ll_mknod(%p, %p, '%s', %o, %lx, %p, %x, %x, %p) -> "
		      "%d",
		      mount, parent, name, mode, rdev, inode, want, flags,
		      perms, err);

		if (err >= 0) {
			ans.inode = ptr_checksum(&client->random, inode);
		}
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_close(proxy_client_t *client, proxy_req_t *req,
				   const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_ll_close, ans, 0);
	proxy_mount_t *mount;
	struct Fh *fh;
	int32_t err;

	err = ptr_check(&client->random, req->ll_close.cmount, (void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_close.fh,
				(void **)&fh);
	}

	if (err >= 0) {
		err = ceph_ll_close(proxy_cmount(mount), fh);
		TRACE("ceph_ll_close(%p, %p) -> %d", mount, fh, err);
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_rename(proxy_client_t *client, proxy_req_t *req,
				    const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_ll_rename, ans, 0);
	proxy_mount_t *mount;
	struct Inode *old_parent, *new_parent;
	const char *old_name, *new_name;
	UserPerm *perms;
	int32_t err;

	err = ptr_check(&client->random, req->ll_rename.cmount,
			(void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_rename.old_parent,
				(void **)&old_parent);
	}
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_rename.new_parent,
				(void **)&new_parent);
	}
	if (err >= 0) {
		err = ptr_check(&global_random, req->ll_rename.userperm,
				(void **)&perms);
	}
	if (err >= 0) {
		old_name = CEPH_STR_GET(req->ll_rename, old_name, data);
		new_name = CEPH_STR_GET(req->ll_rename, new_name,
					(const char *)data + req->ll_rename.old_name);

		err = ceph_ll_rename(proxy_cmount(mount), old_parent, old_name,
				     new_parent, new_name, perms);
		TRACE("ceph_ll_rename(%p, %p, '%s', %p, '%s', %p) -> %d", mount,
		      old_parent, old_name, new_parent, new_name, perms, err);
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_lseek(proxy_client_t *client, proxy_req_t *req,
				   const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_ll_lseek, ans, 0);
	proxy_mount_t *mount;
	struct Fh *fh;
	off_t offset, pos;
	int32_t whence, err;

	err = ptr_check(&client->random, req->ll_lseek.cmount, (void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_lseek.fh,
				(void **)&fh);
	}
	if (err >= 0) {
		offset = req->ll_lseek.offset;
		whence = req->ll_lseek.whence;

		pos = ceph_ll_lseek(proxy_cmount(mount), fh, offset, whence);
		err = -errno;
		TRACE("ceph_ll_lseek(%p, %p, %ld, %d) -> %ld (%d)", mount, fh,
		      offset, whence, pos, -err);

		if (pos >= 0) {
			ans.offset = pos;
			err = 0;
		}
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_read(proxy_client_t *client, proxy_req_t *req,
				  const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_ll_read, ans, 1);
	proxy_mount_t *mount;
	struct Fh *fh;
	void *buffer;
	uint64_t len;
	int64_t offset;
	uint32_t size;
	int32_t err;

	buffer = client->buffer;

	err = ptr_check(&client->random, req->ll_read.cmount, (void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_read.fh, (void **)&fh);
	}
	if (err >= 0) {
		offset = req->ll_read.offset;
		len = req->ll_read.len;

		size = client->buffer_size;
		if (len > size) {
			buffer = proxy_malloc(len);
			if (buffer == NULL) {
				err = -ENOMEM;
			}
		}
		if (err >= 0) {
			err = ceph_ll_read(proxy_cmount(mount), fh, offset, len,
					   buffer);
			TRACE("ceph_ll_read(%p, %p, %ld, %lu) -> %d", mount, fh,
			      offset, len, err);

			if (err >= 0) {
				CEPH_BUFF_ADD(ans, buffer, err);
			}
		}
	}

	err = CEPH_COMPLETE(client, err, ans);

	if (buffer != client->buffer) {
		proxy_free(buffer);
	}

	return err;
}

static int32_t libcephfsd_ll_write(proxy_client_t *client, proxy_req_t *req,
				   const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_ll_write, ans, 0);
	proxy_mount_t *mount;
	struct Fh *fh;
	uint64_t len;
	int64_t offset;
	int32_t err;

	err = ptr_check(&client->random, req->ll_write.cmount, (void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_write.fh,
				(void **)&fh);
	}
	if (err >= 0) {
		offset = req->ll_write.offset;
		len = req->ll_write.len;

		err = ceph_ll_write(proxy_cmount(mount), fh, offset, len, data);
		TRACE("ceph_ll_write(%p, %p, %ld, %lu) -> %d", mount, fh,
		      offset, len, err);
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_link(proxy_client_t *client, proxy_req_t *req,
				  const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_ll_link, ans, 0);
	proxy_mount_t *mount;
	struct Inode *parent, *inode;
	const char *name;
	UserPerm *perms;
	int32_t err;

	err = ptr_check(&client->random, req->ll_link.cmount, (void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_link.inode,
				(void **)&inode);
	}
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_link.parent,
				(void **)&parent);
	}
	if (err >= 0) {
		err = ptr_check(&global_random, req->ll_link.userperm,
				(void **)&perms);
	}
	if (err >= 0) {
		name = CEPH_STR_GET(req->ll_link, name, data);

		err = ceph_ll_link(proxy_cmount(mount), inode, parent, name,
				   perms);
		TRACE("ceph_ll_link(%p, %p, %p, '%s', %p) -> %d", mount, inode,
		      parent, name, perms, err);
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_unlink(proxy_client_t *client, proxy_req_t *req,
				    const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_ll_unlink, ans, 0);
	proxy_mount_t *mount;
	struct Inode *parent;
	const char *name;
	UserPerm *perms;
	int32_t err;

	err = ptr_check(&client->random, req->ll_unlink.cmount,
			(void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_unlink.parent,
				(void **)&parent);
	}
	if (err >= 0) {
		err = ptr_check(&global_random, req->ll_unlink.userperm,
				(void **)&perms);
	}
	if (err >= 0) {
		name = CEPH_STR_GET(req->ll_unlink, name, data);

		err = ceph_ll_unlink(proxy_cmount(mount), parent, name, perms);
		TRACE("ceph_ll_unlink(%p, %p, '%s', %p) -> %d", mount, parent,
		      name, perms, err);
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_getattr(proxy_client_t *client, proxy_req_t *req,
				     const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_ll_getattr, ans, 1);
	struct ceph_statx stx;
	proxy_mount_t *mount;
	struct Inode *inode;
	UserPerm *perms;
	uint32_t want, flags;
	int32_t err;

	err = ptr_check(&client->random, req->ll_getattr.cmount,
			(void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_getattr.inode,
				(void **)&inode);
	}
	if (err >= 0) {
		err = ptr_check(&global_random, req->ll_getattr.userperm,
				(void **)&perms);
	}
	if (err >= 0) {
		want = req->ll_getattr.want;
		flags = req->ll_getattr.flags;

		CEPH_BUFF_ADD(ans, &stx, sizeof(stx));

		err = ceph_ll_getattr(proxy_cmount(mount), inode, &stx, want,
				      flags, perms);
		TRACE("ceph_ll_getattr(%p, %p, %x, %x, %p) -> %d", mount, inode,
		      want, flags, perms, err);
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_setattr(proxy_client_t *client, proxy_req_t *req,
				     const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_ll_setattr, ans, 0);
	proxy_mount_t *mount;
	struct Inode *inode;
	UserPerm *perms;
	int32_t mask, err;

	err = ptr_check(&client->random, req->ll_setattr.cmount,
			(void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_setattr.inode,
				(void **)&inode);
	}
	if (err >= 0) {
		err = ptr_check(&global_random, req->ll_setattr.userperm,
				(void **)&perms);
	}
	if (err >= 0) {
		mask = req->ll_setattr.mask;

		err = ceph_ll_setattr(proxy_cmount(mount), inode, (void *)data,
				      mask, perms);
		TRACE("ceph_ll_setattr(%p, %p, %x, %p) -> %d", mount, inode,
		      mask, perms, err);
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_fallocate(proxy_client_t *client, proxy_req_t *req,
				       const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_ll_fallocate, ans, 0);
	proxy_mount_t *mount;
	struct Fh *fh;
	int64_t offset, len;
	mode_t mode;
	int32_t err;

	err = ptr_check(&client->random, req->ll_fallocate.cmount,
			(void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_fallocate.fh,
				(void **)&fh);
	}
	if (err >= 0) {
		mode = req->ll_fallocate.mode;
		offset = req->ll_fallocate.offset;
		len = req->ll_fallocate.length;

		err = ceph_ll_fallocate(proxy_cmount(mount), fh, mode, offset,
					len);
		TRACE("ceph_ll_fallocate(%p, %p, %o, %ld, %lu) -> %d", mount,
		      fh, mode, offset, len, err);
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_fsync(proxy_client_t *client, proxy_req_t *req,
				   const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_ll_fsync, ans, 0);
	proxy_mount_t *mount;
	struct Fh *fh;
	int32_t dataonly, err;

	err = ptr_check(&client->random, req->ll_fsync.cmount, (void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_fsync.fh,
				(void **)&fh);
	}
	if (err >= 0) {
		dataonly = req->ll_fsync.dataonly;

		err = ceph_ll_fsync(proxy_cmount(mount), fh, dataonly);
		TRACE("ceph_ll_fsync(%p, %p, %d) -> %d", mount, fh, dataonly,
		      err);
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_listxattr(proxy_client_t *client, proxy_req_t *req,
				       const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_ll_listxattr, ans, 1);
	proxy_mount_t *mount;
	struct Inode *inode;
	UserPerm *perms;
	size_t size;
	int32_t err;

	err = ptr_check(&client->random, req->ll_listxattr.cmount,
			(void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_listxattr.inode,
				(void **)&inode);
	}
	if (err >= 0) {
		err = ptr_check(&global_random, req->ll_listxattr.userperm,
				(void **)&perms);
	}
	if (err >= 0) {
		size = req->ll_listxattr.size;
		if (size > client->buffer_size) {
			size = client->buffer_size;
		}
		err = ceph_ll_listxattr(proxy_cmount(mount), inode,
					client->buffer, size, &size, perms);
		TRACE("ceph_ll_listxattr(%p, %p, %lu, %p) -> %d", mount, inode,
		      size, perms, err);

		if (err >= 0) {
			ans.size = size;
			CEPH_BUFF_ADD(ans, client->buffer, size);
		}
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_getxattr(proxy_client_t *client, proxy_req_t *req,
				      const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_ll_getxattr, ans, 1);
	proxy_mount_t *mount;
	struct Inode *inode;
	const char *name;
	UserPerm *perms;
	size_t size;
	int32_t err;

	err = ptr_check(&client->random, req->ll_getxattr.cmount,
			(void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_getxattr.inode,
				(void **)&inode);
	}
	if (err >= 0) {
		err = ptr_check(&global_random, req->ll_getxattr.userperm,
				(void **)&perms);
	}
	if (err >= 0) {
		size = req->ll_getxattr.size;
		name = CEPH_STR_GET(req->ll_getxattr, name, data);

		if (size > client->buffer_size) {
			size = client->buffer_size;
		}
		err = ceph_ll_getxattr(proxy_cmount(mount), inode, name,
				       client->buffer, size, perms);
		TRACE("ceph_ll_getxattr(%p, %p, '%s', %p) -> %d", mount, inode,
		      name, perms, err);

		if (err >= 0) {
			CEPH_BUFF_ADD(ans, client->buffer, err);
		}
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_setxattr(proxy_client_t *client, proxy_req_t *req,
				      const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_ll_setxattr, ans, 0);
	proxy_mount_t *mount;
	struct Inode *inode;
	const char *name, *value;
	UserPerm *perms;
	size_t size;
	int32_t flags, err;

	err = ptr_check(&client->random, req->ll_setxattr.cmount,
			(void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_setxattr.inode,
				(void **)&inode);
	}
	if (err >= 0) {
		err = ptr_check(&global_random, req->ll_setxattr.userperm,
				(void **)&perms);
	}
	if (err >= 0) {
		name = CEPH_STR_GET(req->ll_setxattr, name, data);
		value = (const char *)data + req->ll_setxattr.name;
		size = req->ll_setxattr.size;
		flags = req->ll_setxattr.flags;

		err = ceph_ll_setxattr(proxy_cmount(mount), inode, name, value,
				       size, flags, perms);
		TRACE("ceph_ll_setxattr(%p, %p, '%s', %p, %x, %p) -> %d", mount,
		      inode, name, value, flags, perms, err);
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_removexattr(proxy_client_t *client,
					 proxy_req_t *req, const void *data,
					 int32_t data_size)
{
	CEPH_DATA(ceph_ll_removexattr, ans, 0);
	proxy_mount_t *mount;
	struct Inode *inode;
	const char *name;
	UserPerm *perms;
	int32_t err;

	err = ptr_check(&client->random, req->ll_removexattr.cmount,
			(void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_removexattr.inode,
				(void **)&inode);
	}
	if (err >= 0) {
		err = ptr_check(&global_random, req->ll_removexattr.userperm,
				(void **)&perms);
	}
	if (err >= 0) {
		name = CEPH_STR_GET(req->ll_removexattr, name, data);

		err = ceph_ll_removexattr(proxy_cmount(mount), inode, name,
					  perms);
		TRACE("ceph_ll_removexattr(%p, %p, '%s', %p) -> %d", mount,
		      inode, name, perms, err);
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_readlink(proxy_client_t *client, proxy_req_t *req,
				      const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_ll_readlink, ans, 0);
	proxy_mount_t *mount;
	struct Inode *inode;
	UserPerm *perms;
	size_t size;
	int32_t err;

	err = ptr_check(&client->random, req->ll_readlink.cmount,
			(void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_readlink.inode,
				(void **)&inode);
	}
	if (err >= 0) {
		err = ptr_check(&global_random, req->ll_readlink.userperm,
				(void **)&perms);
	}
	if (err >= 0) {
		size = req->ll_readlink.size;

		if (size > client->buffer_size) {
			size = client->buffer_size;
		}
		err = ceph_ll_readlink(proxy_cmount(mount), inode,
				       client->buffer, size, perms);
		TRACE("ceph_ll_readlink(%p, %p, %p) -> %d", mount, inode, perms,
		      err);
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_symlink(proxy_client_t *client, proxy_req_t *req,
				     const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_ll_symlink, ans, 1);
	struct ceph_statx stx;
	proxy_mount_t *mount;
	struct Inode *parent, *inode;
	UserPerm *perms;
	const char *name, *value;
	uint32_t want, flags;
	int32_t err;

	err = ptr_check(&client->random, req->ll_symlink.cmount,
			(void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_symlink.parent,
				(void **)&parent);
	}
	if (err >= 0) {
		err = ptr_check(&global_random, req->ll_symlink.userperm,
				(void **)&perms);
	}
	if (err >= 0) {
		name = CEPH_STR_GET(req->ll_symlink, name, data);
		value = CEPH_STR_GET(req->ll_symlink, target,
				     (const char *)data + req->ll_symlink.name);
		want = req->ll_symlink.want;
		flags = req->ll_symlink.flags;

		CEPH_BUFF_ADD(ans, &stx, sizeof(stx));

		err = ceph_ll_symlink(proxy_cmount(mount), parent, name, value,
				      &inode, &stx, want, flags, perms);
		TRACE("ceph_ll_symlink(%p, %p, '%s', '%s', %p, %x, %x, %p) -> "
		      "%d",
		      mount, parent, name, value, inode, want, flags, perms,
		      err);

		if (err >= 0) {
			ans.inode = ptr_checksum(&client->random, inode);
		}
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_opendir(proxy_client_t *client, proxy_req_t *req,
				     const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_ll_opendir, ans, 0);
	proxy_mount_t *mount;
	struct Inode *inode;
	struct ceph_dir_result *dirp;
	UserPerm *perms;
	int32_t err;

	err = ptr_check(&client->random, req->ll_opendir.cmount,
			(void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_opendir.inode,
				(void **)&inode);
	}
	if (err >= 0) {
		err = ptr_check(&global_random, req->ll_opendir.userperm,
				(void **)&perms);
	}

	if (err >= 0) {
		err = ceph_ll_opendir(proxy_cmount(mount), inode, &dirp, perms);
		TRACE("ceph_ll_opendir(%p, %p, %p, %p) -> %d", mount, inode,
		      dirp, perms, err);

		if (err >= 0) {
			ans.dir = ptr_checksum(&client->random, dirp);
		}
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_mkdir(proxy_client_t *client, proxy_req_t *req,
				   const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_ll_mkdir, ans, 1);
	struct ceph_statx stx;
	proxy_mount_t *mount;
	struct Inode *parent, *inode;
	const char *name;
	UserPerm *perms;
	mode_t mode;
	uint32_t want, flags;
	int32_t err;

	err = ptr_check(&client->random, req->ll_mkdir.cmount, (void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_mkdir.parent,
				(void **)&parent);
	}
	if (err >= 0) {
		err = ptr_check(&global_random, req->ll_mkdir.userperm,
				(void **)&perms);
	}
	if (err >= 0) {
		mode = req->ll_mkdir.mode;
		want = req->ll_mkdir.want;
		flags = req->ll_mkdir.flags;
		name = CEPH_STR_GET(req->ll_mkdir, name, data);

		CEPH_BUFF_ADD(ans, &stx, sizeof(stx));

		err = ceph_ll_mkdir(proxy_cmount(mount), parent, name, mode,
				    &inode, &stx, want, flags, perms);
		TRACE("ceph_ll_mkdir(%p, %p, '%s', %o, %p, %x, %x, %p) -> %d",
		      mount, parent, name, mode, inode, want, flags, perms,
		      err);

		if (err >= 0) {
			ans.inode = ptr_checksum(&client->random, inode);
		}
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_rmdir(proxy_client_t *client, proxy_req_t *req,
				   const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_ll_rmdir, ans, 0);
	proxy_mount_t *mount;
	struct Inode *parent;
	const char *name;
	UserPerm *perms;
	int32_t err;

	err = ptr_check(&client->random, req->ll_rmdir.cmount, (void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_rmdir.parent,
				(void **)&parent);
	}
	if (err >= 0) {
		err = ptr_check(&global_random, req->ll_rmdir.userperm,
				(void **)&perms);
	}
	if (err >= 0) {
		name = CEPH_STR_GET(req->ll_rmdir, name, data);

		err = ceph_ll_rmdir(proxy_cmount(mount), parent, name, perms);
		TRACE("ceph_ll_rmdir(%p, %p, '%s', %p) -> %d", mount, parent,
		      name, perms, err);
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_ll_releasedir(proxy_client_t *client,
					proxy_req_t *req, const void *data,
					int32_t data_size)
{
	CEPH_DATA(ceph_ll_releasedir, ans, 0);
	proxy_mount_t *mount;
	struct ceph_dir_result *dirp;
	int32_t err;

	err = ptr_check(&client->random, req->ll_releasedir.cmount,
			(void **)&mount);
	if (err >= 0) {
		err = ptr_check(&client->random, req->ll_releasedir.dir,
				(void **)&dirp);
	}

	if (err >= 0) {
		err = ceph_ll_releasedir(proxy_cmount(mount), dirp);
		TRACE("ceph_ll_releasedir(%p, %p) -> %d", mount, dirp, err);
	}

	return CEPH_COMPLETE(client, err, ans);
}

static int32_t libcephfsd_mount_perms(proxy_client_t *client, proxy_req_t *req,
				      const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_mount_perms, ans, 0);
	proxy_mount_t *mount;
	UserPerm *perms;
	int32_t err;

	err = ptr_check(&client->random, req->mount_perms.cmount,
			(void **)&mount);
	if (err >= 0) {
		perms = ceph_mount_perms(proxy_cmount(mount));
		TRACE("ceph_mount_perms(%p) -> %p", mount, perms);

		ans.userperm = ptr_checksum(&global_random, perms);
	}

	return CEPH_COMPLETE(client, err, ans);
}

static void libcephfsd_ll_nonblocking_rw_cbk(struct ceph_ll_io_info *cb_info)
{
	CEPH_CBK(ceph_ll_nonblocking_readv_writev, cbk, 1);
	proxy_async_io_t *async_io;
	proxy_async_t *async;
	int32_t err;

	async_io = container_of(cb_info, proxy_async_io_t, io_info);
	async = async_io->async;

	cbk.info = (uintptr_t)cb_info->priv;
	cbk.res = cb_info->result;

	if ((cbk.res >= 0) && !cb_info->write) {
		CEPH_BUFF_ADD(cbk, cb_info->iov->iov_base, cbk.res);
	}

	err = CEPH_CALL_CBK(async->fd, LIBCEPHFSD_CBK_LL_NONBLOCKING_RW, cbk);
	if (err < 0) {
		proxy_log(LOG_ERR, -err,
			  "Failed to send nonblocking rw completion "
			  "notification");
	}

	if (!cb_info->write) {
		proxy_free(cb_info->iov->iov_base);
	}

	proxy_free(async_io);
}

static int32_t libcephfsd_ll_nonblocking_rw(proxy_client_t *client,
					    proxy_req_t *req,
					    const void *data, int32_t data_size)
{
	CEPH_DATA(ceph_ll_nonblocking_readv_writev, ans, 0);
	struct ceph_ll_io_info *io_info;
	proxy_mount_t *mount;
	proxy_async_io_t *async_io;
	int64_t res;
	int32_t err;

	if ((client->neg.v1.enabled & PROXY_FEAT_ASYNC_IO) == 0) {
		return -EOPNOTSUPP;
	}

	err = ptr_check(&client->random, req->ll_nonblocking_rw.cmount,
			(void **)&mount);
	if (err < 0) {
		goto done;
	}

	async_io = proxy_malloc(sizeof(proxy_async_io_t));
	if (async_io == NULL) {
		err = -ENOMEM;
		goto done;
	}
	io_info = &async_io->io_info;

	memset(io_info, 0, sizeof(struct ceph_ll_io_info));
	io_info->callback = libcephfsd_ll_nonblocking_rw_cbk;
	io_info->priv = (void *)(uintptr_t)req->ll_nonblocking_rw.info;
	io_info->iov = &async_io->iov;
	io_info->iovcnt = 1;
	io_info->off = req->ll_nonblocking_rw.off;
	io_info->write = req->ll_nonblocking_rw.write;
	io_info->fsync = req->ll_nonblocking_rw.fsync;
	io_info->syncdataonly = req->ll_nonblocking_rw.syncdataonly;

	err = ptr_check(&client->random, req->ll_nonblocking_rw.fh,
			(void **)&io_info->fh);
	if (err < 0) {
		goto done;
	}

	if (io_info->write) {
		async_io->iov.iov_len = data_size;
		async_io->iov.iov_base = (void *)data;
	} else {
		async_io->iov.iov_len = req->ll_nonblocking_rw.size;
		async_io->iov.iov_base = proxy_malloc(async_io->iov.iov_len);
		if (async_io->iov.iov_base == NULL) {
			proxy_free(async_io);
			err = -ENOMEM;
			goto done;
		}
	}

	async_io->async = &client->async;

	res = ceph_ll_nonblocking_readv_writev(proxy_cmount(mount), io_info);
	TRACE("ceph_ll_nonblocking_readv_writev(%p) -> %ld", mount, res);

	ans.res = res;
	if (res < 0) {
		if (!io_info->write) {
			proxy_free(async_io->iov.iov_base);
		}
		proxy_free(async_io);
	}

	err = 0;

done:
	return CEPH_COMPLETE(client, err, ans);
}

static proxy_handler_t libcephfsd_handlers[LIBCEPHFSD_OP_TOTAL_OPS] = {
	[LIBCEPHFSD_OP_VERSION] = libcephfsd_version,
	[LIBCEPHFSD_OP_USERPERM_NEW] = libcephfsd_userperm_new,
	[LIBCEPHFSD_OP_USERPERM_DESTROY] = libcephfsd_userperm_destroy,
	[LIBCEPHFSD_OP_CREATE] = libcephfsd_create,
	[LIBCEPHFSD_OP_RELEASE] = libcephfsd_release,
	[LIBCEPHFSD_OP_CONF_READ_FILE] = libcephfsd_conf_read_file,
	[LIBCEPHFSD_OP_CONF_GET] = libcephfsd_conf_get,
	[LIBCEPHFSD_OP_CONF_SET] = libcephfsd_conf_set,
	[LIBCEPHFSD_OP_INIT] = libcephfsd_init,
	[LIBCEPHFSD_OP_SELECT_FILESYSTEM] = libcephfsd_select_filesystem,
	[LIBCEPHFSD_OP_MOUNT] = libcephfsd_mount,
	[LIBCEPHFSD_OP_UNMOUNT] = libcephfsd_unmount,
	[LIBCEPHFSD_OP_LL_STATFS] = libcephfsd_ll_statfs,
	[LIBCEPHFSD_OP_LL_LOOKUP] = libcephfsd_ll_lookup,
	[LIBCEPHFSD_OP_LL_LOOKUP_INODE] = libcephfsd_ll_lookup_inode,
	[LIBCEPHFSD_OP_LL_LOOKUP_ROOT] = libcephfsd_ll_lookup_root,
	[LIBCEPHFSD_OP_LL_PUT] = libcephfsd_ll_put,
	[LIBCEPHFSD_OP_LL_WALK] = libcephfsd_ll_walk,
	[LIBCEPHFSD_OP_CHDIR] = libcephfsd_chdir,
	[LIBCEPHFSD_OP_GETCWD] = libcephfsd_getcwd,
	[LIBCEPHFSD_OP_READDIR] = libcephfsd_readdir,
	[LIBCEPHFSD_OP_REWINDDIR] = libcephfsd_rewinddir,
	[LIBCEPHFSD_OP_LL_OPEN] = libcephfsd_ll_open,
	[LIBCEPHFSD_OP_LL_CREATE] = libcephfsd_ll_create,
	[LIBCEPHFSD_OP_LL_MKNOD] = libcephfsd_ll_mknod,
	[LIBCEPHFSD_OP_LL_CLOSE] = libcephfsd_ll_close,
	[LIBCEPHFSD_OP_LL_RENAME] = libcephfsd_ll_rename,
	[LIBCEPHFSD_OP_LL_LSEEK] = libcephfsd_ll_lseek,
	[LIBCEPHFSD_OP_LL_READ] = libcephfsd_ll_read,
	[LIBCEPHFSD_OP_LL_WRITE] = libcephfsd_ll_write,
	[LIBCEPHFSD_OP_LL_LINK] = libcephfsd_ll_link,
	[LIBCEPHFSD_OP_LL_UNLINK] = libcephfsd_ll_unlink,
	[LIBCEPHFSD_OP_LL_GETATTR] = libcephfsd_ll_getattr,
	[LIBCEPHFSD_OP_LL_SETATTR] = libcephfsd_ll_setattr,
	[LIBCEPHFSD_OP_LL_FALLOCATE] = libcephfsd_ll_fallocate,
	[LIBCEPHFSD_OP_LL_FSYNC] = libcephfsd_ll_fsync,
	[LIBCEPHFSD_OP_LL_LISTXATTR] = libcephfsd_ll_listxattr,
	[LIBCEPHFSD_OP_LL_GETXATTR] = libcephfsd_ll_getxattr,
	[LIBCEPHFSD_OP_LL_SETXATTR] = libcephfsd_ll_setxattr,
	[LIBCEPHFSD_OP_LL_REMOVEXATTR] = libcephfsd_ll_removexattr,
	[LIBCEPHFSD_OP_LL_READLINK] = libcephfsd_ll_readlink,
	[LIBCEPHFSD_OP_LL_SYMLINK] = libcephfsd_ll_symlink,
	[LIBCEPHFSD_OP_LL_OPENDIR] = libcephfsd_ll_opendir,
	[LIBCEPHFSD_OP_LL_MKDIR] = libcephfsd_ll_mkdir,
	[LIBCEPHFSD_OP_LL_RMDIR] = libcephfsd_ll_rmdir,
	[LIBCEPHFSD_OP_LL_RELEASEDIR] = libcephfsd_ll_releasedir,
	[LIBCEPHFSD_OP_MOUNT_PERMS] = libcephfsd_mount_perms,
	[LIBCEPHFSD_OP_LL_NONBLOCKING_RW] = libcephfsd_ll_nonblocking_rw,
};

static void serve_binary(proxy_client_t *client)
{
	proxy_req_t req;
	struct iovec req_iov[2];
	void *buffer;
	uint32_t size;
	int32_t err;

	/* This buffer will be used by most of the requests. For requests that
	 * require more space (probably just some writes), a new temporary
	 * buffer will be allocated by proxy_link_req_recv() code. */
	size = 65536;
	buffer = proxy_malloc(size);
	if (buffer == NULL) {
		return;
	}

	while (true) {
		req_iov[0].iov_base = &req;
		req_iov[0].iov_len = sizeof(req);
		req_iov[1].iov_base = buffer;
		req_iov[1].iov_len = size;

		err = proxy_link_req_recv(client->sd, req_iov, 2);
		if (err <= 0) {
			break;
		}

		if (req.header.op >= LIBCEPHFSD_OP_TOTAL_OPS) {
			err = send_error(client, -ENOSYS);
		} else if (libcephfsd_handlers[req.header.op] == NULL) {
			err = send_error(client, -EOPNOTSUPP);
		} else {
			err = libcephfsd_handlers[req.header.op](
				client, &req, req_iov[1].iov_base,
				req.header.data_len);
		}
		if (err < 0) {
			break;
		}

		if (req_iov[1].iov_base != buffer) {
			/* Free the buffer if it was temporarily allocated. */
			proxy_free(req_iov[1].iov_base);
		}
	}

	if (req_iov[1].iov_base != buffer) {
		proxy_free(req_iov[1].iov_base);
	}
	proxy_free(buffer);
}

static int32_t server_negotiation_check(proxy_link_negotiate_t *neg)
{
	proxy_log(LOG_INFO, 0, "Features enabled: %08x", neg->v1.enabled);

	return 0;
}

static void serve_connection(proxy_worker_t *worker)
{
	proxy_client_t *client;
	int32_t err;

	client = container_of(worker, proxy_client_t, worker);

	proxy_link_negotiate_init(&client->neg, 0, PROXY_FEAT_ALL, 0, 0);

	err = proxy_link_handshake_server(client->link, client->sd,
					  &client->neg,
					  server_negotiation_check);
	if (err < 0) {
		goto done;
	}

	if ((client->neg.v1.enabled & PROXY_FEAT_ASYNC_CBK) != 0) {
		err = proxy_async_server(&client->async, client->sd);
		if (err < 0) {
			goto done;
		}
	}

	serve_binary(client);

done:
	close(client->sd);
}

static void destroy_connection(proxy_worker_t *worker)
{
	proxy_client_t *client;

	client = container_of(worker, proxy_client_t, worker);

	proxy_free(client->buffer);
	proxy_free(client);
}

static int32_t accept_connection(proxy_link_t *link, int32_t sd)
{
	proxy_server_t *server;
	proxy_client_t *client;
	int32_t err;

	server = container_of(link, proxy_server_t, link);

	client = proxy_malloc(sizeof(proxy_client_t));
	if (client == NULL) {
		err = -ENOMEM;
		goto failed_close;
	}

	client->buffer_size = 65536;
	client->buffer = proxy_malloc(client->buffer_size);
	if (client->buffer == NULL) {
		err = -ENOMEM;
		goto failed_client;
	}

	random_init(&client->random);
	client->sd = sd;
	client->link = link;

	/* TODO: Make request management asynchronous and avoid creating a
	 *       thread for each connection. */
	err = proxy_manager_launch(server->manager, &client->worker,
				   serve_connection, destroy_connection);
	if (err < 0) {
		goto failed_buffer;
	}

	return 0;

failed_buffer:
	proxy_free(client->buffer);

failed_client:
	proxy_free(client);

failed_close:
	close(sd);

	return err;
}

static bool check_stop(proxy_link_t *link)
{
	proxy_server_t *server;

	server = container_of(link, proxy_server_t, link);

	return proxy_manager_stop(server->manager);
}

static int32_t server_start(proxy_manager_t *manager)
{
	proxy_server_t server;
	proxy_t *proxy;

	proxy = container_of(manager, proxy_t, manager);

	server.manager = manager;

	return proxy_link_server(&server.link, proxy->socket_path,
				 accept_connection, check_stop);
}

static void log_format(struct iovec *iov, char *buffer, size_t size,
		       const char *fmt, const char *err, ...)
{
	va_list args;
	int32_t len;

	va_start(args, err);
	len = vsnprintf(buffer, size, fmt, args);
	va_end(args);

	if (len < 0) {
		iov->iov_base = (void *)err;
		iov->iov_len = strlen(err);
	} else {
		if (len >= size) {
			memcpy(buffer + size - 6, "[...]", 6);
			len = size - 1;
		}

		iov->iov_base = buffer;
		iov->iov_len = len;
	}
}

static void log_print(proxy_log_handler_t *handler, int32_t level, int32_t err,
		      const char *msg)
{
	static const char level_chars[] = "CEWID";

	char emsg[256];
	char header[8];
	struct iovec iov[3];

	log_format(&iov[0], header, sizeof(header), "[%c] ", "[?] ",
		   level_chars[level]);

	iov[1].iov_base = (void *)msg;
	iov[1].iov_len = strlen(msg);

	if (err != 0) {
		log_format(&iov[2], emsg, sizeof(emsg), " (error %d: %s)\n",
			   " (error ?)\n", err, strerror(err));
	} else {
		iov[2].iov_base = "\n";
		iov[2].iov_len = 1;
	}

	writev(STDOUT_FILENO, iov, 3);
}

static struct option main_opts[] = {
	{"socket", required_argument, NULL, 's'},
	{}
};

int32_t main(int32_t argc, char *argv[])
{
	struct timespec now;
	proxy_t proxy;
	char *env;
	int32_t err, val;

	clock_gettime(CLOCK_MONOTONIC, &now);
	srand(now.tv_nsec);

	random_init(&global_random);

	proxy_log_register(&proxy.log_handler, log_print);

	proxy.socket_path = PROXY_SOCKET;

	env = getenv(PROXY_SOCKET_ENV);
	if (env != NULL) {
		proxy.socket_path = env;
	}

	while ((val = getopt_long(argc, argv, ":s:", main_opts, NULL)) >= 0) {
		if (val == 's') {
			proxy.socket_path = optarg;
		} else if (val == ':') {
			proxy_log(LOG_ERR, ENODATA,
				  "Argument missing for '%s'\n", optopt);
			return 1;
		} else if (val == '?') {
			proxy_log(LOG_ERR, EINVAL,
				  "Unknown option '%s'\n", optopt);
			return 1;
		} else {
			proxy_log(LOG_ERR, EINVAL,
				  "Unexpected error parsing the options\n");
			return 1;
		}
	}
	if (optind < argc) {
		proxy_log(LOG_ERR, EINVAL,
			  "Unexpected arguments in command line");
		return 1;
	}

	err = proxy_manager_run(&proxy.manager, server_start);

	proxy_log_deregister(&proxy.log_handler);

	return err < 0 ? 1 : 0;
}
