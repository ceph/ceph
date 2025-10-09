/*
 * rbd-fuse
 */
#include "include/int_types.h"

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <getopt.h>
#include <assert.h>
#include <string>
#include <mutex>
#include <limits.h>

#if defined(__FreeBSD__)
#include <sys/param.h>
#endif

#include "include/compat.h"
#include "include/rbd/librbd.h"
#include "include/ceph_assert.h"

#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "include/ceph_fuse.h"

#include "global/global_init.h"
#include "global/global_context.h"

static int gotrados = 0;
char *pool_name;
char *nspace_name;
char *mount_image_name;
rados_t cluster;
rados_ioctx_t ioctx;

std::mutex readdir_lock;

struct rbd_stat {
	u_char valid;
	rbd_image_info_t rbd_info;
};

struct rbd_options {
	char *pool_name;
	char *nspace_name;
	char *image_name;
};

struct rbd_image {
	char *image_name;
	struct rbd_image *next;
};
struct rbd_image_data {
	struct rbd_image *images;
	rbd_image_spec_t *image_specs;
	size_t image_spec_count;
};
struct rbd_image_data rbd_image_data;

struct rbd_openimage {
	char *image_name;
	rbd_image_t image;
	struct rbd_stat rbd_stat;
};
#define MAX_RBD_IMAGES		128
struct rbd_openimage opentbl[MAX_RBD_IMAGES];

struct rbd_options rbd_options = {(char*) "rbd", (char*) "", NULL};

#define rbdsize(fd)	opentbl[fd].rbd_stat.rbd_info.size
#define rbdblksize(fd)	opentbl[fd].rbd_stat.rbd_info.obj_size
#define rbdblkcnt(fd)	opentbl[fd].rbd_stat.rbd_info.num_objs

uint64_t imagesize = 1024ULL * 1024 * 1024;
uint64_t imageorder = 22ULL;
uint64_t imagefeatures = 1ULL;

// Minimize calls to rbd_list: marks bracketing of opendir/<ops>/releasedir
int in_opendir;

/* prototypes */
int connect_to_cluster(rados_t *pcluster);
void enumerate_images(struct rbd_image_data *data);
int open_rbd_image(const char *image_name);
int find_openrbd(const char *path);

void simple_err(const char *msg, int err);

void
enumerate_images(struct rbd_image_data *data)
{
	struct rbd_image **head = &data->images;
	struct rbd_image *im, *next;
	int ret;

	if (*head != NULL) {
		for (im = *head; im != NULL;) {
			next = im->next;
			free(im);
			im = next;
		}
		*head = NULL;
		rbd_image_spec_list_cleanup(data->image_specs,
					    data->image_spec_count);
		free(data->image_specs);
		data->image_specs = NULL;
		data->image_spec_count = 0;
	}

	while (true) {
		ret = rbd_list2(ioctx, data->image_specs,
				&data->image_spec_count);
		if (ret == -ERANGE) {
			data->image_specs = static_cast<rbd_image_spec_t *>(
				realloc(data->image_specs,
					sizeof(rbd_image_spec_t) * data->image_spec_count));
		} else if (ret < 0) {
			simple_err("Failed to list images", ret);
		} else {
			break;
		}
	}

	if (*nspace_name != '\0') {
	    fprintf(stderr, "pool/namespace %s/%s: ", pool_name, nspace_name);
	} else {
	    fprintf(stderr, "pool %s: ", pool_name);
	}
	for (size_t idx = 0; idx < data->image_spec_count; ++idx) {
		if ((mount_image_name == NULL) ||
		    ((strlen(mount_image_name) > 0) &&
		    (strcmp(data->image_specs[idx].name, mount_image_name) == 0))) {
			fprintf(stderr, "%s, ", data->image_specs[idx].name);
			im = static_cast<rbd_image*>(malloc(sizeof(*im)));
			im->image_name = data->image_specs[idx].name;
			im->next = *head;
			*head = im;
		}
	}
	fprintf(stderr, "\n");
}

int
find_openrbd(const char *path)
{
	int i;

	/* find in opentbl[] entry if already open */
	for (i = 0; i < MAX_RBD_IMAGES; i++) {
		if ((opentbl[i].image_name != NULL) &&
		    (strcmp(opentbl[i].image_name, path) == 0)) {
			return i;
		}
	}
	return -1;
}

int
open_rbd_image(const char *image_name)
{
	struct rbd_image *im;
	struct rbd_openimage *rbd = NULL;
	int fd;

	if (image_name == (char *)NULL) 
		return -1;

	// relies on caller to keep rbd_image_data up to date
	for (im = rbd_image_data.images; im != NULL; im = im->next) {
		if (strcmp(im->image_name, image_name) == 0) {
			break;
		}
	}
	if (im == NULL)
		return -1;

	/* find in opentbl[] entry if already open */
	if ((fd = find_openrbd(image_name)) != -1) {
		rbd = &opentbl[fd];
	} else {
		int i;
		// allocate an opentbl[] and open the image
		for (i = 0; i < MAX_RBD_IMAGES; i++) {
			if (opentbl[i].image == NULL) {
				fd = i;
				rbd = &opentbl[fd];
				rbd->image_name = strdup(image_name);
				break;
			}
		}
		if (i == MAX_RBD_IMAGES || !rbd)
			return -1;
		int ret = rbd_open(ioctx, rbd->image_name, &(rbd->image), NULL);
		if (ret < 0) {
			simple_err("open_rbd_image: can't open: ", ret);
			return ret;
		}
	}
	rbd_stat(rbd->image, &(rbd->rbd_stat.rbd_info),
		 sizeof(rbd_image_info_t));
	rbd->rbd_stat.valid = 1;
	return fd;
}

static void
iter_images(void *cookie,
	    void (*iter)(void *cookie, const char *image))
{
	struct rbd_image *im;

	readdir_lock.lock();

	for (im = rbd_image_data.images; im != NULL; im = im->next)
		iter(cookie, im->image_name);
	readdir_lock.unlock();
}

static void count_images_cb(void *cookie, const char *image)
{
	(*((unsigned int *)cookie))++;
}

static int count_images(void)
{
	unsigned int count = 0;

	readdir_lock.lock();
	enumerate_images(&rbd_image_data);
	readdir_lock.unlock();

	iter_images(&count, count_images_cb);
	return count;
}

extern "C" {

static int rbdfs_getattr(const char *path, struct stat *stbuf
#if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 0)
                         , struct fuse_file_info *fi
#endif
                        )
{
	int fd;
	time_t now;

	if (!gotrados)
		return -ENXIO;

	if (path[0] == 0)
		return -ENOENT;

	memset(stbuf, 0, sizeof(struct stat));

	if (strcmp(path, "/") == 0) {

		now = time(NULL);
		stbuf->st_mode = S_IFDIR + 0755;
		stbuf->st_nlink = 2+count_images();
		stbuf->st_uid = getuid();
		stbuf->st_gid = getgid();
		stbuf->st_size = 1024;
		stbuf->st_blksize = 1024;
		stbuf->st_blocks = 1;
		stbuf->st_atime = now;
		stbuf->st_mtime = now;
		stbuf->st_ctime = now;

		return 0;
	}

	if (!in_opendir) {
		readdir_lock.lock();
		enumerate_images(&rbd_image_data);
		readdir_lock.unlock();
	}
	fd = open_rbd_image(path + 1);
	if (fd < 0)
		return -ENOENT;

	now = time(NULL);
	stbuf->st_mode = S_IFREG | 0666;
	stbuf->st_nlink = 1;
	stbuf->st_uid = getuid();
	stbuf->st_gid = getgid();
	stbuf->st_size = rbdsize(fd);
	stbuf->st_blksize = rbdblksize(fd);
	stbuf->st_blocks = rbdblkcnt(fd);
	stbuf->st_atime = now;
	stbuf->st_mtime = now;
	stbuf->st_ctime = now;

	return 0;
}


static int rbdfs_open(const char *path, struct fuse_file_info *fi)
{
	int fd;

	if (!gotrados)
		return -ENXIO;

	if (path[0] == 0)
		return -ENOENT;

	readdir_lock.lock();
	enumerate_images(&rbd_image_data);
	readdir_lock.unlock();
	fd = open_rbd_image(path + 1);
	if (fd < 0)
		return -ENOENT;

	fi->fh = fd;
	return 0;
}

static int rbdfs_read(const char *path, char *buf, size_t size,
			off_t offset, struct fuse_file_info *fi)
{
	size_t numread;
	struct rbd_openimage *rbd;

	if (!gotrados)
		return -ENXIO;

	rbd = &opentbl[fi->fh];
	numread = 0;
	while (size > 0) {
		ssize_t ret;

		ret = rbd_read(rbd->image, offset, size, buf);

		if (ret <= 0)
			break;
		buf += ret;
		size -= ret;
		offset += ret;
		numread += ret;
	}

	return numread;
}

static int rbdfs_write(const char *path, const char *buf, size_t size,
			 off_t offset, struct fuse_file_info *fi)
{
	size_t numwritten;
	struct rbd_openimage *rbd;

	if (!gotrados)
		return -ENXIO;

	rbd = &opentbl[fi->fh];
	numwritten = 0;
	while (size > 0) {
		ssize_t ret;

		if ((size_t)(offset + size) > rbdsize(fi->fh)) {
			int r;
			fprintf(stderr, "rbdfs_write resizing %s to 0x%" PRIxMAX "\n",
				path, offset+size);
			r = rbd_resize(rbd->image, offset+size);
			if (r < 0)
				return r;

			r = rbd_stat(rbd->image, &(rbd->rbd_stat.rbd_info),
				 sizeof(rbd_image_info_t));
			if (r < 0)
				return r;
		}
		ret = rbd_write(rbd->image, offset, size, buf);

		if (ret < 0)
			break;
		buf += ret;
		size -= ret;
		offset += ret;
		numwritten += ret;
	}

	return numwritten;
}

static void rbdfs_statfs_image_cb(void *num, const char *image)
{
	int	fd;

	((uint64_t *)num)[0]++;

	fd = open_rbd_image(image);
	if (fd >= 0)
		((uint64_t *)num)[1] += rbdsize(fd);
}

static int rbdfs_statfs(const char *path, struct statvfs *buf)
{
	uint64_t num[2];

	if (!gotrados)
		return -ENXIO;

	num[0] = 1;
	num[1] = 0;
	readdir_lock.lock();
	enumerate_images(&rbd_image_data);
	readdir_lock.unlock();
	iter_images(num, rbdfs_statfs_image_cb);

#define	RBDFS_BSIZE	4096
	buf->f_bsize = RBDFS_BSIZE;
	buf->f_frsize = RBDFS_BSIZE;
	buf->f_blocks = num[1] / RBDFS_BSIZE;
	buf->f_bfree = 0;
	buf->f_bavail = 0;
	buf->f_files = num[0];
	buf->f_ffree = 0;
	buf->f_favail = 0;
	buf->f_fsid = 0;
	buf->f_flag = 0;
	buf->f_namemax = PATH_MAX;

	return 0;
}

static int rbdfs_fsync(const char *path, int datasync,
			 struct fuse_file_info *fi)
{
	if (!gotrados)
		return -ENXIO;
	rbd_flush(opentbl[fi->fh].image);
	return 0;
}

static int rbdfs_opendir(const char *path, struct fuse_file_info *fi)
{
	// only one directory, so global "in_opendir" flag should be fine
	readdir_lock.lock();
	in_opendir++;
	enumerate_images(&rbd_image_data);
	readdir_lock.unlock();
	return 0;
}

struct rbdfs_readdir_info {
	void *buf;
	fuse_fill_dir_t filler;
};

static void rbdfs_readdir_cb(void *_info, const char *name)
{
	struct rbdfs_readdir_info *info = (struct rbdfs_readdir_info*) _info;

        filler_compat(info->filler, info->buf, name, NULL, 0);
}

static int rbdfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
			 off_t offset, struct fuse_file_info *fi
#if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 0)
                         , enum fuse_readdir_flags
#endif
                         )
{
	struct rbdfs_readdir_info info = { buf, filler };

	if (!gotrados)
		return -ENXIO;
	if (!in_opendir)
		fprintf(stderr, "in readdir, but not inside opendir?\n");

	if (strcmp(path, "/") != 0)
		return -ENOENT;

	filler_compat(filler, buf, ".", NULL, 0);
	filler_compat(filler, buf, "..", NULL, 0);
	iter_images(&info, rbdfs_readdir_cb);

	return 0;
}
static int rbdfs_releasedir(const char *path, struct fuse_file_info *fi)
{
	// see opendir comments
	readdir_lock.lock();
	in_opendir--;
	readdir_lock.unlock();
	return 0;
}

void *
rbdfs_init(struct fuse_conn_info *conn
#if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 0)
           , struct fuse_config *cfg
#endif
          )
{
	int ret;

	// init cannot fail, so if we fail here, gotrados remains at 0,
	// causing other operations to fail immediately with ENXIO

	ret = connect_to_cluster(&cluster);
	if (ret < 0)
		exit(90);

	pool_name = rbd_options.pool_name;
	nspace_name = rbd_options.nspace_name;
	mount_image_name = rbd_options.image_name;
	ret = rados_ioctx_create(cluster, pool_name, &ioctx);
	if (ret < 0)
		exit(91);
#if FUSE_VERSION >= FUSE_MAKE_VERSION(2, 8) && FUSE_VERSION < FUSE_MAKE_VERSION(3, 0)
	conn->want |= FUSE_CAP_BIG_WRITES;
#endif
	rados_ioctx_set_namespace(ioctx, nspace_name);
	gotrados = 1;

	// init's return value shows up in fuse_context.private_data,
	// also to void (*destroy)(void *); useful?
	return NULL;
}

void
rbdfs_destroy(void *unused)
{
	if (!gotrados)
		return;
	for (int i = 0; i < MAX_RBD_IMAGES; ++i) {
		if (opentbl[i].image) {
			rbd_close(opentbl[i].image);
			opentbl[i].image = NULL;
		}
	}
	rados_ioctx_destroy(ioctx);
	rados_shutdown(cluster);
}

int
rbdfs_checkname(const char *checkname)
{
    const char *extra[] = {"@", "/"};
    std::string strCheckName(checkname);
    
    if (strCheckName.empty())
        return -EINVAL;

    unsigned int sz = sizeof(extra) / sizeof(const char*);
    for (unsigned int i = 0; i < sz; i++)
    {
        std::string ex(extra[i]);
        if (std::string::npos != strCheckName.find(ex))
            return -EINVAL;
    }

    return 0;
}

// return -errno on error.  fi->fh is not set until open time

int
rbdfs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
         int r;
         int order = imageorder;

         r = rbdfs_checkname(path+1);
         if (r != 0)
         {
            return r;  
         }

         r = rbd_create2(ioctx, path+1, imagesize, imagefeatures, &order);
         return r;
}

int
rbdfs_rename(const char *path, const char *destname
#if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 0)
             , unsigned int flags
#endif
            )
{
    int r;

    r = rbdfs_checkname(destname+1);
    if (r != 0)
    {
      return r;
    }

    if (strcmp(path, "/") == 0)
        return -EINVAL;

    return rbd_rename(ioctx, path+1, destname+1);
}

int
rbdfs_utimens(const char *path, const struct timespec tv[2]
#if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 0)
              , struct fuse_file_info *fi
#endif
             )
{
	// called on create; not relevant
	return 0;
}

int
rbdfs_unlink(const char *path)
{
	int fd = find_openrbd(path+1);
	if (fd != -1) {
		struct rbd_openimage *rbd = &opentbl[fd];
		rbd_close(rbd->image);
		rbd->image = 0;
		free(rbd->image_name);
		rbd->rbd_stat.valid = 0;
	}
	return rbd_remove(ioctx, path+1);
}


int
rbdfs_truncate(const char *path, off_t size
#if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 0)
               , struct fuse_file_info *fi
#endif
              )
{
	int fd;
	int r;
	struct rbd_openimage *rbd;

	if ((fd = open_rbd_image(path+1)) < 0)
		return -ENOENT;

	rbd = &opentbl[fd];
	fprintf(stderr, "truncate %s to %" PRIdMAX " (0x%" PRIxMAX ")\n",
          path, size, size);
	r = rbd_resize(rbd->image, size);
	if (r < 0)
		return r;

	r = rbd_stat(rbd->image, &(rbd->rbd_stat.rbd_info),
		 sizeof(rbd_image_info_t));
	if (r < 0)
		return r;
	return 0;
}

/**
 * set an xattr on path, with name/value, length size.
 * Presumably flags are from Linux, as in XATTR_CREATE or 
 * XATTR_REPLACE (both "set", but fail if exist vs fail if not exist.
 *
 * We accept xattrs only on the root node.
 *
 * All values converted with strtoull, so can be expressed in any base
 */

struct rbdfuse_attr {
	char *attrname;
	uint64_t *attrvalp;
} attrs[] = {
    { (char*) "user.rbdfuse.imagesize", &imagesize },
    { (char*) "user.rbdfuse.imageorder", &imageorder },
    { (char*) "user.rbdfuse.imagefeatures", &imagefeatures },
    { NULL, NULL }
};

int
rbdfs_setxattr(const char *path, const char *name, const char *value,
	       size_t size,
	       int flags
#if defined(DARWIN)
	       ,uint32_t pos
#endif
    )
{
	struct rbdfuse_attr *ap;
	if (strcmp(path, "/") != 0)
		return -EINVAL;

	for (ap = attrs; ap->attrname != NULL; ap++) {
		if (strcmp(name, ap->attrname) == 0) {
			*ap->attrvalp = strtoull(value, NULL, 0);
			fprintf(stderr, "rbd-fuse: %s set to 0x%" PRIx64 "\n",
				ap->attrname, *ap->attrvalp);
			return 0;
		}
	}
	return -EINVAL;
}

int
rbdfs_getxattr(const char *path, const char *name, char *value,
		 size_t size
#if defined(DARWIN)
	       ,uint32_t position
#endif
  )
{
	struct rbdfuse_attr *ap;
	char buf[128];
	// allow gets on other files; ls likes to ask for things like
	// security.*

	for (ap = attrs; ap->attrname != NULL; ap++) {
		if (strcmp(name, ap->attrname) == 0) {
			sprintf(buf, "%" PRIu64, *ap->attrvalp);
			if (value != NULL && size >= strlen(buf))
				strcpy(value, buf);
			fprintf(stderr, "rbd-fuse: get %s\n", ap->attrname);
			return (strlen(buf));
		}
	}
	return 0;
}

int
rbdfs_listxattr(const char *path, char *list, size_t len)
{
	struct rbdfuse_attr *ap;
	size_t required_len = 0;

	if (strcmp(path, "/") != 0)
		return -EINVAL;

	for (ap = attrs; ap->attrname != NULL; ap++)
		required_len += strlen(ap->attrname) + 1;
	if (len >= required_len) {
		for (ap = attrs; ap->attrname != NULL; ap++) {
			sprintf(list, "%s", ap->attrname);
			list += strlen(ap->attrname) + 1;
		}
	}
	return required_len;
}

const static struct fuse_operations rbdfs_oper = {
  getattr:    rbdfs_getattr,
  readlink:   0,
#if FUSE_VERSION < FUSE_MAKE_VERSION(3, 0)
  getdir:     0,
#endif
  mknod:      0,
  mkdir:      0,
  unlink:     rbdfs_unlink,
  rmdir:      0,
  symlink:    0,
  rename:     rbdfs_rename,
  link:       0,
  chmod:      0,
  chown:      0,
  truncate:   rbdfs_truncate,
#if FUSE_VERSION < FUSE_MAKE_VERSION(3, 0)
  utime:      0,
#endif
  open:	      rbdfs_open,
  read:	      rbdfs_read,
  write:      rbdfs_write,
  statfs:     rbdfs_statfs,
  flush:      0,
  release:    0,
  fsync:      rbdfs_fsync,
  setxattr:   rbdfs_setxattr,
  getxattr:   rbdfs_getxattr,
  listxattr:  rbdfs_listxattr,
  removexattr: 0,
  opendir:    rbdfs_opendir,
  readdir:    rbdfs_readdir,
  releasedir: rbdfs_releasedir,
  fsyncdir:   0,
  init:	      rbdfs_init,
  destroy:    rbdfs_destroy,
  access:     0,
  create:     rbdfs_create,
#if FUSE_VERSION < FUSE_MAKE_VERSION(3, 0)
  ftruncate:  0,
  fgetattr:   0,
#endif
  lock:       0,
  utimens:    rbdfs_utimens,
  /* skip unimplemented */
};

} /* extern "C" */

enum {
	KEY_HELP,
	KEY_VERSION,
	KEY_RADOS_POOLNAME,
	KEY_RADOS_POOLNAME_LONG,
	KEY_RADOS_NSPACENAME,
	KEY_RADOS_NSPACENAME_LONG,
	KEY_RBD_IMAGENAME,
	KEY_RBD_IMAGENAME_LONG
};

static struct fuse_opt rbdfs_opts[] = {
	FUSE_OPT_KEY("-h", KEY_HELP),
	FUSE_OPT_KEY("--help", KEY_HELP),
	FUSE_OPT_KEY("-V", KEY_VERSION),
	FUSE_OPT_KEY("--version", KEY_VERSION),
	{"-p %s", offsetof(struct rbd_options, pool_name), KEY_RADOS_POOLNAME},
	{"--poolname=%s", offsetof(struct rbd_options, pool_name),
	 KEY_RADOS_POOLNAME_LONG},
	{"-s %s", offsetof(struct rbd_options, nspace_name), KEY_RADOS_NSPACENAME},
	{"--namespace=%s", offsetof(struct rbd_options, nspace_name),
	 KEY_RADOS_NSPACENAME_LONG},
	{"-r %s", offsetof(struct rbd_options, image_name), KEY_RBD_IMAGENAME},
	{"--image=%s", offsetof(struct rbd_options, image_name),
	KEY_RBD_IMAGENAME_LONG},
	FUSE_OPT_END
};

static void usage(const char *progname)
{
	fprintf(stderr,
"Usage: %s mountpoint [options]\n"
"\n"
"General options:\n"
"    -h   --help            print help\n"
"    -V   --version         print version\n"
"    -c   --conf            ceph configuration file [/etc/ceph/ceph.conf]\n"
"    -p   --poolname        rados pool name [rbd]\n"
"    -s   --namespace       rados namespace name []\n"
"    -r   --image           RBD image name\n"
"\n", progname);
}

static int rbdfs_opt_proc(void *data, const char *arg, int key,
			    struct fuse_args *outargs)
{
	if (key == KEY_HELP) {
		usage(outargs->argv[0]);
		fuse_opt_add_arg(outargs, "-ho");
		fuse_main(outargs->argc, outargs->argv, &rbdfs_oper, NULL);
		exit(1);
	}

	if (key == KEY_VERSION) {
		fuse_opt_add_arg(outargs, "--version");
		fuse_main(outargs->argc, outargs->argv, &rbdfs_oper, NULL);
		exit(0);
	}

	if (key == KEY_RADOS_POOLNAME) {
		if (rbd_options.pool_name != NULL) {
			free(rbd_options.pool_name);
			rbd_options.pool_name = NULL;
		}
		rbd_options.pool_name = strdup(arg+2);
		return 0;
	}

	if (key == KEY_RADOS_NSPACENAME) {
		if (rbd_options.nspace_name != NULL) {
			free(rbd_options.nspace_name);
			rbd_options.nspace_name = NULL;
		}
		rbd_options.nspace_name = strdup(arg+2);
		return 0;
	}

	if (key == KEY_RBD_IMAGENAME) {
		if (rbd_options.image_name!= NULL) {
			free(rbd_options.image_name);
			rbd_options.image_name = NULL;
		}
		rbd_options.image_name = strdup(arg+2);
		return 0;
	}

	return 1;
}

void
simple_err(const char *msg, int err)
{
	fprintf(stderr, "%s: %s\n", msg, strerror(-err));
	return;
}

int
connect_to_cluster(rados_t *pcluster)
{
	int r;
	global_init_postfork_start(g_ceph_context);
	common_init_finish(g_ceph_context);
	global_init_postfork_finish(g_ceph_context);

	r = rados_create_with_context(pcluster, g_ceph_context);
	if (r < 0) {
		simple_err("Could not create cluster handle", r);
		return r;
	}

	r = rados_connect(*pcluster);
	if (r < 0) {
		simple_err("Error connecting to cluster", r);
		rados_shutdown(*pcluster);
		return r;
	}

	return 0;
}

#define OPT_NOT_FOUND -1

int main(int argc, const char *argv[])
{
	memset(&rbd_image_data, 0, sizeof(rbd_image_data));

	// librados will filter out -r/-f/-d options from command-line
	std::map<std::string, int> filter_options = {
		{"-r", OPT_NOT_FOUND},
		{"-f", OPT_NOT_FOUND},
		{"-d", OPT_NOT_FOUND}};

	std::set<std::string> require_arg_options = {"-r"};

	std::vector<const char*> arg_vector;
	for (auto idx = 0; idx < argc; ++idx) {
		auto it = filter_options.find(argv[idx]);
		if (it != filter_options.end()) {
			it->second = idx;
		}
		arg_vector.push_back(argv[idx]);
	}

	auto cct = global_init(NULL, arg_vector, CEPH_ENTITY_TYPE_CLIENT,
			       CODE_ENVIRONMENT_DAEMON,
			       CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);
	g_ceph_context->_conf.set_val_or_die("pid_file", "");
	g_ceph_context->_conf.set_val_or_die("daemonize", "true");

	if (global_init_prefork(g_ceph_context) < 0) {
		fprintf(stderr, "Failed to initialize librados\n");
		exit(1);
	}

	for (auto& it : filter_options) {
		if (it.second != OPT_NOT_FOUND) {
			arg_vector.push_back(it.first.c_str());
			if (require_arg_options.count(it.first) &&
			    it.second + 1 < argc) {
				arg_vector.push_back(argv[it.second + 1]);
			}
		}
	}

	struct fuse_args args = FUSE_ARGS_INIT((int)arg_vector.size(),
					       (char**)&arg_vector.front());
	if (fuse_opt_parse(&args, &rbd_options, rbdfs_opts,
			   rbdfs_opt_proc) == -1) {
		exit(1);
	}

	return fuse_main(args.argc, args.argv, &rbdfs_oper, NULL);
}
