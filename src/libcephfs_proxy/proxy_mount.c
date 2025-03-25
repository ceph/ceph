
#include "proxy_mount.h"
#include "proxy_helpers.h"

#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>

/* Maximum number of symlinks to visit while resolving a path before returning
 * ELOOP. */
#define PROXY_MAX_SYMLINKS 16

struct _proxy_linked_str;
typedef struct _proxy_linked_str proxy_linked_str_t;

/* This structure is used to handle symlinks found during the walk of a path.
 *
 * We'll start with an initial string representing a path. If one of the
 * components is found to be a symlink, a new proxy_linked_str_t will be
 * created with the content of the symlink. Then the new string will point
 * to the old string, which may still contain some additional path components.
 * The new string will be traversed resolving symlinks as they are found in the
 * same way. Once it finished, the old string is recovered and traversal
 * continues from the point it was left. */
struct _proxy_linked_str {
	proxy_linked_str_t *next;
	char *remaining;
	char data[];
};

/* This structure is used to traverse a path while resolving any symlink
 * found. At the end, it will contain the realpath of the entry and its
 * inode. */
typedef struct _proxy_path_iterator {
	struct ceph_statx stx;
	struct ceph_mount_info *cmount;
	proxy_linked_str_t *lstr;
	UserPerm *perms;
	struct Inode *root;
	struct Inode *base;
	char *realpath;
	uint64_t root_ino;
	uint64_t base_ino;
	uint32_t realpath_size;
	uint32_t realpath_len;
	uint32_t symlinks;
	bool release;
	bool follow;
} proxy_path_iterator_t;

typedef struct _proxy_config {
	int32_t src;
	int32_t dst;
	int32_t size;
	int32_t total;
	void *buffer;
} proxy_config_t;

typedef struct _proxy_change {
	list_t list;
	uint32_t size;
	char data[];
} proxy_change_t;

typedef struct _proxy_iter {
	proxy_instance_t *instance;
	list_t *item;
} proxy_iter_t;

typedef struct _proxy_instance_pool {
	pthread_mutex_t mutex;
	list_t hash[256];
} proxy_mount_pool_t;

static proxy_mount_pool_t instance_pool = {
	.mutex = PTHREAD_MUTEX_INITIALIZER,
};

/* Ceph client instance sharing
 *
 * The main purpose of the libcephfs proxy is to avoid the multiple independent
 * data caches that are created when libcephfs is used from different processes.
 * However the cache is not created per process but per client instance, so each
 * call to `ceph_create()` creates its own private data cache instance. Just
 * forwarding the libcephfs API calls to a single proxy process is not enough to
 * solve the problem.
 *
 * The proxy will try to reuse existing client instances to reduce the number of
 * independent caches. However it's not always possible to map all proxy clients
 * to a single libcephfs instance. When different settings are used, separate
 * Ceph instances are required to avoid unwanted behaviors.
 *
 * Even though it's possible that some Ceph options may be compatible even if
 * they have different values, the proxy won't try to handle these cases. It
 * will consider the configuration as a black box, and only 100% equal
 * configurations will share the Ceph client instance.
 */

/* Ceph configuration file management
 *
 * We won't try to parse Ceph configuration files. The proxy only wants to know
 * if a configuration is equal or not. To do so, when a configuration file is
 * passed to the proxy, it will create a private copy and compute an SHA256
 * hash. If the hash doesn't match, the configuration is considered different,
 * even if it's not a real difference (like additional empty lines or the order
 * of the options).
 *
 * The private copy is necessary to enforce that the settings are not changed
 * concurrently, which could make us believe that two configurations are equal
 * when they are not.
 *
 * Besides a configuration file, the user can also make manual configuration
 * changes by using `ceph_conf_set()`. These changes are also tracked and
 * compared to be sure that the active configuration matches. Only if the
 * configuration file is exactly equal and all the applied changes are the same,
 * and in the same order, the Ceph client instance will be shared.
 */

int32_t proxy_inode_ref(proxy_mount_t *mount, uint64_t inode)
{
	inodeno_t ino;
	struct Inode *tmp;
	int32_t err;

	/* There's no way to tell libcephfs to increase the reference counter of
	 * an inode, so we do a full lookup for now. */

	ino.val = inode;

	err = ceph_ll_lookup_inode(proxy_cmount(mount), ino, &tmp);
	if (err < 0) {
		proxy_log(LOG_ERR, -err, "ceph_ll_loolkup_inode() failed");
	}

	return err;
}

static proxy_linked_str_t *proxy_linked_str_create(const char *str,
						   proxy_linked_str_t *next)
{
	proxy_linked_str_t *lstr;
	uint32_t len;

	len = strlen(str) + 1;
	lstr = proxy_malloc(sizeof(proxy_linked_str_t) + len);
	if (lstr != NULL) {
		lstr->next = next;
		if (len > 1) {
			lstr->remaining = lstr->data;
			memcpy(lstr->data, str, len);
		} else {
			lstr->remaining = NULL;
		}
	}

	return lstr;
}

static proxy_linked_str_t *proxy_linked_str_next(proxy_linked_str_t *lstr)
{
	proxy_linked_str_t *next;

	next = lstr->next;
	proxy_free(lstr);

	return next;
}

static void proxy_linked_str_destroy(proxy_linked_str_t *lstr)
{
	while (lstr != NULL) {
		lstr = proxy_linked_str_next(lstr);
	}
}

static bool proxy_linked_str_empty(proxy_linked_str_t *lstr)
{
	return lstr->remaining == NULL;
}

static char *proxy_linked_str_scan(proxy_linked_str_t *lstr, char ch)
{
	char *current;

	current = lstr->remaining;
	lstr->remaining = strchr(lstr->remaining, ch);
	if (lstr->remaining != NULL) {
		*lstr->remaining++ = 0;
	}

	return current;
}

static int32_t 	proxy_path_iterator_init(proxy_path_iterator_t *iter,
					proxy_mount_t *mount, const char *path,
					UserPerm *perms, bool realpath,
					bool follow)
{
	uint32_t len;
	char ch;

	if (path == NULL) {
		return proxy_log(LOG_ERR, EINVAL, "NULL path received");
	}

	memset(&iter->stx, 0, sizeof(iter->stx));
	iter->cmount = proxy_cmount(mount);
	iter->perms = perms;
	iter->root = mount->root;
	iter->root_ino = mount->root_ino;
	iter->base = mount->cwd;
	iter->base_ino = mount->cwd_ino;
	iter->symlinks = 0;
	iter->release = false;
	iter->follow = follow;

	len = strlen(path) + 1;

	ch = *path;
	if (ch == '/') {
		iter->base = mount->root;
		iter->base_ino = mount->root_ino;
		path++;
	}

	iter->realpath = NULL;
	iter->realpath_len = 0;
	iter->realpath_size = 0;

	if (realpath) {
		if (ch != '/') {
			len += mount->cwd_path_len;
		}
		len = (len + 63) & ~63;
		iter->realpath_size = len;

		iter->realpath = proxy_malloc(len);
		if (iter->realpath == NULL) {
			return -ENOMEM;
		}
		if (ch != '/') {
			memcpy(iter->realpath, mount->cwd_path,
			       mount->cwd_path_len + 1);
			iter->realpath_len = mount->cwd_path_len;
		} else {
			iter->realpath[0] = '/';
			iter->realpath[1] = 0;
			iter->realpath_len = 1;
		}
	}

	iter->lstr = proxy_linked_str_create(path, NULL);
	if (iter->lstr == NULL) {
		proxy_free(iter->realpath);
		return -ENOMEM;
	}

	return 0;
}

static char *proxy_path_iterator_next(proxy_path_iterator_t *iter)
{
	while (proxy_linked_str_empty(iter->lstr)) {
		iter->lstr = proxy_linked_str_next(iter->lstr);
		if (iter->lstr == NULL) {
			return NULL;
		}
	}

	return proxy_linked_str_scan(iter->lstr, '/');
}

static bool proxy_path_iterator_is_last(proxy_path_iterator_t *iter)
{
	proxy_linked_str_t *lstr;

	lstr = iter->lstr;
	while (proxy_linked_str_empty(iter->lstr)) {
		lstr = lstr->next;
		if (lstr == NULL) {
			return true;
		}
	}

	return false;
}

static void proxy_path_iterator_destroy(proxy_path_iterator_t *iter)
{
	if (iter->release) {
		ceph_ll_put(iter->cmount, iter->base);
	}

	proxy_free(iter->realpath);
	proxy_linked_str_destroy(iter->lstr);
}

static int32_t proxy_path_iterator_resolve(proxy_path_iterator_t *iter)
{
	static __thread char path[PATH_MAX];
	proxy_linked_str_t *lstr;
	char *ptr;
	int32_t err;

	if (++iter->symlinks > PROXY_MAX_SYMLINKS) {
		return proxy_log(LOG_ERR, ELOOP, "Too many symbolic links");
	}

	err = ceph_ll_readlink(iter->cmount, iter->base, path, sizeof(path),
			       iter->perms);
	if (err < 0) {
		return proxy_log(LOG_ERR, -err, "ceph_ll_readlink() failed");
	}

	ptr = path;
	if (*ptr == '/') {
		if (iter->release) {
			ceph_ll_put(iter->cmount, iter->base);
		}
		iter->base = iter->root;
		iter->base_ino = iter->root_ino;
		iter->release = false;
		if (iter->realpath != NULL) {
			iter->realpath[1] = 0;
			iter->realpath_len = 1;
		}

		ptr++;
	}

	lstr = proxy_linked_str_create(ptr, iter->lstr);
	if (lstr == NULL) {
		return -ENOMEM;
	}
	iter->lstr = lstr;

	return 0;
}

static int32_t proxy_path_iterator_append(proxy_path_iterator_t *iter,
					  const char *name)
{
	uint32_t len, size;
	int32_t err;

	len = strlen(name) + 1;
	size = iter->realpath_size;
	if (iter->realpath_len + len >= size) {
		do {
			size <<= 1;
		} while (iter->realpath_len + len >= size);
		err = proxy_realloc((void **)&iter->realpath, size);
		if (err < 0) {
			return err;
		}
		iter->realpath_size = size;
	}

	if (iter->realpath_len > 1) {
		iter->realpath[iter->realpath_len++] = '/';
	}
	memcpy(iter->realpath + iter->realpath_len, name, len);
	iter->realpath_len += len - 1;

	return 0;
}

static void proxy_path_iterator_remove(proxy_path_iterator_t *iter)
{
	while ((iter->realpath_len > 0) &&
	       (iter->realpath[--iter->realpath_len] != '/')) {
	}
}

static int32_t proxy_path_lookup(struct ceph_mount_info *cmount,
				 struct Inode *parent, const char *name,
				 struct Inode **inode, struct ceph_statx *stx,
				 uint32_t want, uint32_t flags, UserPerm *perms)
{
	int32_t err;

	err = ceph_ll_lookup(cmount, parent, name, inode, stx, want, flags,
			     perms);
	if (err < 0) {
		return proxy_log(LOG_ERR, -err, "ceph_ll_lookup() failed");
	}

	return err;
}

static int32_t proxy_path_iterator_lookup(proxy_path_iterator_t *iter,
					  const char *name)
{
	struct Inode *inode;
	int32_t err;

	if (S_ISLNK(iter->stx.stx_mode)) {
		return proxy_path_iterator_resolve(iter);
	}

	err = proxy_path_lookup(iter->cmount, iter->base, name, &inode,
				&iter->stx, CEPH_STATX_INO | CEPH_STATX_MODE,
				AT_SYMLINK_NOFOLLOW, iter->perms);
	if (err < 0) {
		return err;
	}

	if (iter->realpath != NULL) {
		if ((name[0] == '.') && (name[1] == '.') && (name[2] == 0)) {
			proxy_path_iterator_remove(iter);
		} else {
			err = proxy_path_iterator_append(iter, name);
			if (err < 0) {
				ceph_ll_put(iter->cmount, inode);
				return err;
			}
		}
	}

	if (iter->release) {
		ceph_ll_put(iter->cmount, iter->base);
	}
	iter->base = inode;
	iter->base_ino = iter->stx.stx_ino;
	iter->release = true;

	if (iter->follow && S_ISLNK(iter->stx.stx_mode) &&
	    proxy_path_iterator_is_last(iter)) {
		return proxy_path_iterator_resolve(iter);
	}

	return 0;
}

/* Implements a path walk ensuring that it's not possible to go higher than the
 * root mount point used in ceph_mount(). This means that it handles absolute
 * paths and ".." entries in a special way, including paths found in symbolic
 * links. */
int32_t proxy_path_resolve(proxy_mount_t *mount, const char *path,
			   struct Inode **inode, struct ceph_statx *stx,
			   uint32_t want, uint32_t flags, UserPerm *perms,
			   char **realpath)
{
	proxy_path_iterator_t iter;
	char *name, c;
	int32_t err;

	err = proxy_path_iterator_init(&iter, mount, path, perms,
				       realpath != NULL,
				       (flags & AT_SYMLINK_NOFOLLOW) == 0);
	if (err < 0) {
		return err;
	}

	while ((err >= 0) &&
	       ((name = proxy_path_iterator_next(&iter)) != NULL)) {
		c = *name;
		if (c == '.') {
			c = name[1];
			if ((c == '.') && (iter.base == mount->root)) {
				c = name[2];
			}
		}
		if (c == 0) {
			continue;
		}

		err = proxy_path_iterator_lookup(&iter, name);
	}

	if (err >= 0) {
		err = proxy_path_lookup(proxy_cmount(mount), iter.base, "",
					inode, stx, want, flags, iter.perms);
	}

	if ((err >= 0) && (realpath != NULL)) {
		*realpath = iter.realpath;
		iter.realpath = NULL;
	}

	proxy_path_iterator_destroy(&iter);

	return err;
}

static int32_t proxy_config_source_prepare(const char *config, struct stat *st)
{
	int32_t fd, err;

	fd = open(config, O_RDONLY);
	if (fd < 0) {
		return proxy_log(LOG_ERR, errno, "open() failed");
	}

	if (fstat(fd, st) < 0) {
		err = proxy_log(LOG_ERR, errno, "fstat() failed");
		goto failed;
	}

	if (!S_ISREG(st->st_mode)) {
		err = proxy_log(LOG_ERR, EINVAL,
				"Configuration file is not a regular file");
		goto failed;
	}

	return fd;

failed:
	close(fd);

	return err;
}

static void proxy_config_source_close(int32_t fd)
{
	close(fd);
}

static int32_t proxy_config_source_read(int32_t fd, void *buffer, size_t size)
{
	ssize_t len;

	len = read(fd, buffer, size);
	if (len < 0) {
		return proxy_log(LOG_ERR, errno, "read() failed");
	}

	return len;
}

static int32_t proxy_config_source_validate(int32_t fd, struct stat *before,
					    int32_t size)
{
	struct stat after;

	if (fstat(fd, &after) < 0) {
		return proxy_log(LOG_ERR, errno, "fstat() failed");
	}

	if ((before->st_size != size) || (before->st_size != after.st_size) ||
	    (before->st_blocks != after.st_blocks) ||
	    (before->st_ctim.tv_sec != after.st_ctim.tv_sec) ||
	    (before->st_ctim.tv_nsec != after.st_ctim.tv_nsec) ||
	    (before->st_mtim.tv_sec != after.st_mtim.tv_sec) ||
	    (before->st_mtim.tv_nsec != after.st_mtim.tv_nsec)) {
		proxy_log(LOG_WARN, 0,
			  "Configuration file has been modified while "
			  "reading it");

		return 0;
	}

	return 1;
}

static int32_t proxy_config_destination_prepare(void)
{
	int32_t fd;

	fd = openat(AT_FDCWD, ".", O_TMPFILE | O_WRONLY, 0600);
	if (fd < 0) {
		return proxy_log(LOG_ERR, errno, "openat() failed");
	}

	return fd;
}

static void proxy_config_destination_close(int32_t fd)
{
	close(fd);
}

static int32_t proxy_config_destination_write(int32_t fd, void *data,
					      size_t size)
{
	ssize_t len;

	len = write(fd, data, size);
	if (len < 0) {
		return proxy_log(LOG_ERR, errno, "write() failed");
	}
	if (len != size) {
		return proxy_log(LOG_ERR, ENOSPC, "Partial write");
	}

	return size;
}

static int32_t proxy_config_destination_commit(int32_t fd, const char *name)
{
	char path[32];

	if (fsync(fd) < 0) {
		return proxy_log(LOG_ERR, errno, "fsync() failed");
	}

	if (linkat(fd, "", AT_FDCWD, name, AT_EMPTY_PATH) < 0) {
		if (errno == EEXIST) {
			return 0;
		}

		/* This may fail if the user doesn't have CAP_DAC_READ_SEARCH.
		 * In this case we attempt to link it using the /proc
		 * filesystem. */
	}

	snprintf(path, sizeof(path), "/proc/self/fd/%d", fd);
	if (linkat(AT_FDCWD, path, AT_FDCWD, name, AT_SYMLINK_FOLLOW) < 0) {
		if (errno != EEXIST) {
			return proxy_log(LOG_ERR, errno, "linkat() failed");
		}
	}

	return 0;
}

static int32_t proxy_config_transfer(void **ptr, void *data, int32_t idx)
{
	proxy_config_t *cfg;
	int32_t len, err;

	cfg = data;

	len = proxy_config_source_read(cfg->src, cfg->buffer, cfg->size);
	if (len <= 0) {
		return len;
	}

	err = proxy_config_destination_write(cfg->dst, cfg->buffer, len);
	if (err < 0) {
		return err;
	}

	cfg->total += len;

	*ptr = cfg->buffer;

	return len;
}

/* Copies and checksums a given configuration to a file and makes sure that it
 * has not been modified. */
static int32_t proxy_config_prepare(const char *config, char *path,
				    int32_t size)
{
	char hash[65];
	proxy_config_t cfg;
	struct stat before;
	int32_t err;

	cfg.size = 4096;
	cfg.buffer = proxy_malloc(cfg.size);
	if (cfg.buffer == NULL) {
		return -ENOMEM;
	}
	cfg.total = 0;

	cfg.src = proxy_config_source_prepare(config, &before);
	if (cfg.src < 0) {
		err = cfg.src;
		goto done_mem;
	}

	cfg.dst = proxy_config_destination_prepare();
	if (cfg.dst < 0) {
		err = cfg.dst;
		goto done_src;
	}

	err = proxy_hash_hex(hash, sizeof(hash), proxy_config_transfer, &cfg);
	if (err < 0) {
		goto done_dst;
	}

	err = proxy_config_source_validate(cfg.src, &before, cfg.total);
	if (err < 0) {
		goto done_dst;
	}

	err = snprintf(path, size, "ceph-%s.conf", hash);
	if (err < 0) {
		err = proxy_log(LOG_ERR, errno, "snprintf() failed");
		goto done_dst;
	}
	if (err >= size) {
		err = proxy_log(LOG_ERR, ENOBUFS,
				"Insufficient space to store the name");
		goto done_dst;
	}

	err = proxy_config_destination_commit(cfg.dst, path);

done_dst:
	proxy_config_destination_close(cfg.dst);

done_src:
	proxy_config_source_close(cfg.src);

done_mem:
	proxy_free(cfg.buffer);

	return err;
}

/* Record changes to the configuration. */
static int32_t proxy_instance_change_add(proxy_instance_t *instance,
					 const char *arg1, const char *arg2,
					 const char *arg3)
{
	proxy_change_t *change;
	int32_t len[3], total;

	len[0] = strlen(arg1) + 1;
	if (arg2 == NULL) {
		arg2 = "<null>";
	}
	len[1] = strlen(arg2) + 1;
	len[2] = 0;
	if (arg3 != NULL) {
		len[2] = strlen(arg3) + 1;
	}

	total = len[0] + len[1] + len[2];

	change = proxy_malloc(sizeof(proxy_change_t) + total);
	if (change == NULL) {
		return -ENOMEM;
	}
	change->size = total;

	memcpy(change->data, arg1, len[0]);
	memcpy(change->data + len[0], arg2, len[1]);
	if (arg3 != NULL) {
		memcpy(change->data + len[0] + len[1], arg3, len[2]);
	}

	list_add_tail(&change->list, &instance->changes);

	return 0;
}

static void proxy_instance_change_del(proxy_instance_t *instance)
{
	proxy_change_t *change;

	change = list_last_entry(&instance->changes, proxy_change_t, list);
	list_del(&change->list);

	proxy_free(change);
}

/* Destroy a Ceph client instance */
static void proxy_instance_destroy(proxy_instance_t *instance)
{
	if (instance->mounted) {
		ceph_unmount(instance->cmount);
	}

	if (instance->cmount != NULL) {
		ceph_release(instance->cmount);
	}

	while (!list_empty(&instance->changes)) {
		proxy_instance_change_del(instance);
	}

	proxy_free(instance);
}

/* Create a new Ceph client instance with the provided id */
static int32_t proxy_instance_create(proxy_instance_t **pinstance,
				     const char *id)
{
	struct ceph_mount_info *cmount;
	proxy_instance_t *instance;
	int32_t err;

	instance = proxy_malloc(sizeof(proxy_instance_t));
	if (instance == NULL) {
		return -ENOMEM;
	}

	list_init(&instance->siblings);
	list_init(&instance->changes);
	instance->cmount = NULL;
	instance->inited = false;
	instance->mounted = false;

	err = proxy_instance_change_add(instance, "id", id, NULL);
	if (err < 0) {
		goto failed;
	}

	err = ceph_create(&cmount, id);
	if (err < 0) {
		proxy_log(LOG_ERR, -err, "ceph_create() failed");
		goto failed;
	}

	instance->cmount = cmount;

	*pinstance = instance;

	return 0;

failed:
	proxy_instance_destroy(instance);

	return err;
}

static int32_t proxy_instance_release(proxy_instance_t *instance)
{
	if (instance->mounted) {
		return proxy_log(LOG_ERR, EISCONN,
				 "Cannot release an active connection");
	}

	proxy_instance_destroy(instance);

	return 0;
}

/* Assign a configuration file to the instance. */
static int32_t proxy_instance_config(proxy_instance_t *instance,
				     const char *config)
{
	char path[128], *ppath;
	int32_t err;

	if (instance->mounted) {
		return proxy_log(LOG_ERR, EISCONN,
				 "Cannot configure a mounted instance");
	}

	ppath = NULL;
	if (config != NULL) {
		err = proxy_config_prepare(config, path, sizeof(path));
		if (err < 0) {
			return err;
		}
		ppath = path;
	}

	err = proxy_instance_change_add(instance, "conf", ppath, NULL);
	if (err < 0) {
		return err;
	}

	err = ceph_conf_read_file(instance->cmount, ppath);
	if (err < 0) {
		proxy_instance_change_del(instance);
	}

	return err;
}

static int32_t proxy_instance_option_get(proxy_instance_t *instance,
					 const char *name, char *value,
					 size_t size)
{
	int32_t err, res;

	if (name == NULL) {
		return proxy_log(LOG_ERR, EINVAL, "NULL option name");
	}

	res = ceph_conf_get(instance->cmount, name, value, size);
	if (res < 0) {
		return proxy_log(
			LOG_ERR, -res,
			"Failed to get configuration from a client instance");
	}

	err = proxy_instance_change_add(instance, "get", name, value);
	if (err < 0) {
		return err;
	}

	return res;
}

static int32_t proxy_instance_option_set(proxy_instance_t *instance,
					 const char *name, const char *value)
{
	int32_t err;

	if ((name == NULL) || (value == NULL)) {
		return proxy_log(LOG_ERR, EINVAL, "NULL value or option name");
	}

	if (instance->mounted) {
		return proxy_log(LOG_ERR, EISCONN,
				 "Cannot configure a mounted instance");
	}

	err = proxy_instance_change_add(instance, "set", name, value);
	if (err < 0) {
		return err;
	}

	err = ceph_conf_set(instance->cmount, name, value);
	if (err < 0) {
		proxy_log(LOG_ERR, -err,
			  "Failed to configure a client instance");
		proxy_instance_change_del(instance);
	}

	return err;
}

static int32_t proxy_instance_select(proxy_instance_t *instance, const char *fs)
{
	int32_t err;

	if (instance->mounted) {
		return proxy_log(
			LOG_ERR, EISCONN,
			"Cannot select a filesystem on a mounted instance");
	}

	err = proxy_instance_change_add(instance, "fs", fs, NULL);
	if (err < 0) {
		return err;
	}

	err = ceph_select_filesystem(instance->cmount, fs);
	if (err < 0) {
		proxy_log(LOG_ERR, -err,
			  "Failed to select a filesystem on a client instance");
		proxy_instance_change_del(instance);
	}

	return err;
}

static int32_t proxy_instance_init(proxy_instance_t *instance)
{
	if (instance->mounted || instance->inited) {
		return 0;
	}

	/* ceph_init() does start several internal threads. However, an instance
         * may not end up being mounted if the configuration matches with
	 * another mounted instance. Since ceph_mount() also calls ceph_init()
	 * if not already done, we avoid initializing it here to reduce resource
         * consumption. */

	instance->inited = true;

	return 0;
}

static int32_t proxy_instance_hash(void **ptr, void *data, int32_t idx)
{
	proxy_iter_t *iter;
	proxy_change_t *change;

	iter = data;

	if (iter->item == &iter->instance->changes) {
		return 0;
	}

	change = list_entry(iter->item, proxy_change_t, list);
	iter->item = iter->item->next;

	*ptr = change->data;

	return change->size;
}

/* Check if an existing instance matches the configuration used for the current
 * one. If so, share the mount. Otherwise, create a new mount. */
static int32_t proxy_instance_mount(proxy_instance_t **pinstance)
{
	proxy_instance_t *instance, *existing;
	proxy_iter_t iter;
	list_t *list;
	int32_t err;

	instance = *pinstance;

	if (instance->mounted) {
		return proxy_log(LOG_ERR, EISCONN,
				 "Cannot mount and already mounted instance");
	}

	iter.instance = instance;
	iter.item = instance->changes.next;

	/* Create a hash that includes all settings. */
	err = proxy_hash(instance->hash, sizeof(instance->hash),
			 proxy_instance_hash, &iter);
	if (err < 0) {
		return err;
	}

	list = &instance_pool.hash[instance->hash[0]];

	proxy_mutex_lock(&instance_pool.mutex);

	if (list->next == NULL) {
		list_init(list);
	} else {
		list_for_each_entry(existing, list, list) {
			if (memcmp(existing->hash, instance->hash, 32) == 0) {
				/* A match has been found. Instead of destroying
				 * the current instance, it's stored as a
				 * sibling of the one found. It will be
				 * reassigned to an instance when someone
				 * unmounts. */
				list_add(&instance->list, &existing->siblings);
				goto found;
			}
		}
	}

	/* No matching instance has been found. Just create a new one. The root
	 * is always "/". Each virtual mount point will locally store its root
	 * path. */
	err = ceph_mount(instance->cmount, "/");
	if (err >= 0) {
		err = ceph_ll_lookup_root(instance->cmount, &instance->root);
		if (err >= 0) {
			instance->inited = true;
			instance->mounted = true;
			list_add(&instance->list, list);
		} else {
			ceph_unmount(instance->cmount);
		}
	}

	existing = NULL;

found:
	proxy_mutex_unlock(&instance_pool.mutex);

	if (err < 0) {
		return proxy_log(LOG_ERR, -err, "ceph_mount() failed");
	}

	if (existing != NULL) {
		proxy_log(LOG_INFO, 0, "Shared a client instance (%p)",
			  existing);
		*pinstance = existing;
	} else {
		proxy_log(LOG_INFO, 0, "Created a new client instance (%p)",
			  instance);
	}

	return 0;
}

static int32_t proxy_instance_unmount(proxy_instance_t **pinstance)
{
	proxy_instance_t *instance, *sibling;
	int32_t err;

	instance = *pinstance;

	if (!instance->mounted) {
		return proxy_log(LOG_ERR, ENOTCONN,
				 "Cannot unmount an already unmount instance");
	}

	sibling = NULL;

	proxy_mutex_lock(&instance_pool.mutex);

	if (list_empty(&instance->siblings)) {
		/* This is the last mount using this instance. We unmount it. */
		list_del(&instance->list);
		instance->mounted = false;
	} else {
		/* There are other mounts sharing this instance. Take one of the
		 * saved siblings, which share the exact same configuration but
		 * are not mounted, to assign it to the current mount. */
		sibling = list_first_entry(&instance->siblings,
					   proxy_instance_t, list);
		list_del_init(&sibling->list);
	}

	proxy_mutex_unlock(&instance_pool.mutex);

	if (sibling == NULL) {
		ceph_ll_put(instance->cmount, instance->root);

		err = ceph_unmount(instance->cmount);
		if (err < 0) {
			return proxy_log(LOG_ERR, -err,
					 "ceph_unmount() failed");
		}
	} else {
		*pinstance = sibling;
	}

	return 0;
}

int32_t proxy_mount_create(proxy_mount_t **pmount, const char *id)
{
	proxy_mount_t *mount;
	int32_t err;

	mount = proxy_malloc(sizeof(proxy_mount_t));
	if (mount == NULL) {
		return -ENOMEM;
	}
	mount->root = NULL;

	err = proxy_instance_create(&mount->instance, id);
	if (err < 0) {
		proxy_free(mount);
		return err;
	}

	*pmount = mount;

	return 0;
}

int32_t proxy_mount_config(proxy_mount_t *mount, const char *config)
{
	return proxy_instance_config(mount->instance, config);
}

int32_t proxy_mount_set(proxy_mount_t *mount, const char *name,
			const char *value)
{
	return proxy_instance_option_set(mount->instance, name, value);
}

int32_t proxy_mount_get(proxy_mount_t *mount, const char *name, char *value,
			size_t size)
{
	return proxy_instance_option_get(mount->instance, name, value, size);
}

int32_t proxy_mount_select(proxy_mount_t *mount, const char *fs)
{
	return proxy_instance_select(mount->instance, fs);
}

int32_t proxy_mount_init(proxy_mount_t *mount)
{
	return proxy_instance_init(mount->instance);
}

int32_t proxy_mount_mount(proxy_mount_t *mount, const char *root)
{
	struct ceph_statx stx;
	struct ceph_mount_info *cmount;
	int32_t err;

	err = proxy_instance_mount(&mount->instance);
	if (err < 0) {
		return err;
	}

	cmount = proxy_cmount(mount);

	mount->perms = ceph_mount_perms(cmount);

	if (root == NULL) {
		root = "/";
	}

	/* Temporarily set the root and cwd inodes to make proxy_path_resolve()
	 * to work correctly. */
	mount->root = mount->instance->root;
	mount->root_ino = CEPH_INO_ROOT;

	mount->cwd = mount->instance->root;
	mount->cwd_ino = CEPH_INO_ROOT;

	/* Resolve the desired root directory. */
	err = proxy_path_resolve(mount, root, &mount->root, &stx,
				 CEPH_STATX_ALL_STATS, 0, mount->perms, NULL);
	if (err < 0) {
		goto failed;
	}
	if (!S_ISDIR(stx.stx_mode)) {
		err = proxy_log(LOG_ERR, ENOTDIR,
				"The root path is not a directory");
		goto failed_root;
	}

	mount->cwd_path = proxy_strdup("/");
	if (mount->cwd_path == NULL) {
		err = -ENOMEM;
		goto failed_root;
	}
	mount->cwd_path_len = 1;

	mount->root_ino = stx.stx_ino;

	err = proxy_inode_ref(mount, stx.stx_ino);
	if (err < 0) {
		goto failed_path;
	}

	mount->cwd = mount->root;
	mount->cwd_ino = stx.stx_ino;

	return 0;

failed_path:
	proxy_free(mount->cwd_path);

failed_root:
	ceph_ll_put(proxy_cmount(mount), mount->root);

failed:
	proxy_instance_unmount(&mount->instance);

	return err;
}

int32_t proxy_mount_unmount(proxy_mount_t *mount)
{
	ceph_ll_put(proxy_cmount(mount), mount->root);
	mount->root = NULL;
	mount->root_ino = 0;

	ceph_ll_put(proxy_cmount(mount), mount->cwd);
	mount->cwd = NULL;
	mount->cwd_ino = 0;

	proxy_free(mount->cwd_path);

	return proxy_instance_unmount(&mount->instance);
}

int32_t proxy_mount_release(proxy_mount_t *mount)
{
	int32_t err;

	err = proxy_instance_release(mount->instance);
	if (err >= 0) {
		proxy_free(mount);
	}

	return err;
}
