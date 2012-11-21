/*
 * rbd-fuse
 * 
 * compile with
 * cc -D_FILE_OFFSET_BITS=64 rbd-fuse.c -o rbd-fuse -lfuse -lrbd
 */
#define PACKAGE_VERSION "0.1"

#define _GNU_SOURCE
#define FUSE_USE_VERSION 26

#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <pthread.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <getopt.h>

#include <rbd/librbd.h>

static pthread_mutex_t readdir_lock;

void dump_stat(char *label, struct stat *st);

/* access to rbd */
struct radosStat {
	/* fill with 	int rados_stat(rados_ioctx_t io, const char *o, uint64_t *psize, time_t *pmtime); */
	u_char		valid;
	char objname[RBD_MAX_BLOCK_NAME_SIZE];   /* object name */
	uint64_t	size;       /* object size */
	time_t      mtime;      /* modification time */
};

struct rbdStat {
	u_char		valid;
	/* filll with int rbd_stat(rbd_image_t image, rbd_image_info_t *info, size_t infosize); */
	rbd_image_info_t	rbd_info;
};

struct rbdOptions {
	char	*ceph_config;		/* ceph configuration file */
	char	*pool_name;			/* name of the pool our test image is in */
	char	*image_name;		/* name of rbd image */
};

rados_t cluster;

struct radosPool {
	char	*pool_name;				/* name of the pool */
	rados_t			cluster;
	rados_ioctx_t	ioctx;			/* handle for our test pool */
	struct radosStat rados_stat;
	struct radosPool	*next;
};

struct rbdImage {
	int		rbd_in_use;
	struct radosPool *pool;
	char	*image_name;
	rbd_image_t	image;			/* handle for our test image */
	struct rbdStat   rbd_stat;
	struct rbdImage *next;
};

struct rbdOptions rbdOptions;

#define RADOS_MAX_STRING	1024
#define MAX_RADOS_POOLS		4
struct radosPool radosPools[MAX_RADOS_POOLS];

#define MAX_RBD_IMAGES		16

struct rbdFd {
	int fd;
	struct rbdImage *rbd;
};

struct rbdFd rbdFds[MAX_RBD_IMAGES];
struct rbdImage rbdImages[MAX_RBD_IMAGES];

/* prototypes */
void radosPools_init(void);
void rbdImages_init(void);
void simple_err(const char *msg, int err);
int connect_to_cluster();
int count_rbd_devices() { return 1; }

int radosPool_start(rados_t cluster, char *pool_name);
int lookup_radosPool(char *pool_name);
int allocate_radosPool(rados_t cluster, char *pool_name);
void deallocate_radosPool(int poolid) { return; }   // stub
void radosPool_initImages(struct radosPool *pool);

int lookup_rbdImage(char *image_name);
int allocate_rbdImage(struct radosPool *pool);
void deallocate_rbdImage(int imageId);

static int open_volume_dir(const char *volume)
{
	int dirfd;
	dirfd = lookup_rbdImage((char *)volume);

	return dirfd;
}

static void
iter_volumes(void *cookie, void (*iter)(void *cookie, char *volume, int dirfd))
{
	int fd;
	DIR *dirp;
	struct dirent entry;
	struct dirent *result;
	struct rbdImage *rbdList;

	pthread_mutex_lock(&readdir_lock);

	for (rbdList = &(rbdImages[0]); rbdList != NULL; rbdList = rbdList->next){
		if (rbdList->image_name == NULL) {
			continue;
		}
		if (rbdList->rbd_stat.valid == 0) {
			continue;
		}

		iter(cookie, rbdList->image_name, -1);
	}

out:
	pthread_mutex_unlock(&readdir_lock);
}

static int read_property(int dirfd, char *name, unsigned int *_value)
{
	int fd, errors;
	/* FILE *f; */
	unsigned int value;
	struct rbdImage *rbd;


	errors = 0;
	rbd = rbdFds[dirfd].rbd;

	if (rbd == (struct rbdImage *)NULL) {
		errors++;
	}
	if (!errors) {
		if (rbd->rbd_in_use == 0) {
			errors++;
		}
	}
	if (!errors) {
		if (strncmp(name, "obj_siz", 7) == 0) {
			value = rbd->rbd_stat.rbd_info.obj_size;
		} else if (strncmp(name, "num_obj", 7) == 0) {
			value = rbd->rbd_stat.rbd_info.num_objs;
		} else {
			errors++;
		}
	}

	if (!errors) {
		*_value = value;
		return 0;
	} else {
		return -1;
	}
}

static void count_volumes_cb(void *cookie, char *volume, int dirfd)
{
	(*((unsigned int *)cookie))++;
}

static int count_volumes(void)
{
	unsigned int count = 0;

	iter_volumes(&count, count_volumes_cb);
	return count;
}

struct blockfs_getattr_info {
	unsigned int part_size;
	int part_size_mismatches;
	struct timespec st_atim;
	struct timespec st_mtim;
	struct timespec st_ctim;
};

static int timespec_cmp(struct timespec *a, struct timespec *b)
{
	if (a->tv_sec > b->tv_sec)
		return 1;

	if (a->tv_sec < b->tv_sec)
		return -1;

	if (a->tv_nsec > b->tv_nsec)
		return 1;

	if (a->tv_nsec < b->tv_nsec)
		return -1;

	return 0;
}

static int blockfs_getattr(const char *path, struct stat *stbuf)
{
	struct stat buf;
	int dirfd;
	time_t now;
	unsigned int num_parts;
	unsigned int part_size;
	struct blockfs_getattr_info info;

	if (path[0] == 0)
		return -ENOENT;

	memset(stbuf, 0, sizeof(struct stat));

	if (strcmp(path, "/") == 0) {
		int ret;

		now = time(NULL);
		stbuf->st_mode = S_IFDIR + 0755;
		stbuf->st_nlink = 2+count_rbd_devices();
		stbuf->st_uid = getuid();
		stbuf->st_gid = getgid();
		stbuf->st_size = 1024;
		stbuf->st_blksize = 1024;
		stbuf->st_blocks = 1;
		stbuf->st_atime = now;
		stbuf->st_mtime = now;
		stbuf->st_ctime = now;

		dump_stat("derived stat", stbuf);

		return 0;
	}

	dirfd = open_volume_dir(path + 1);
	if (dirfd < 0)
		return -ENOENT;

	if (read_property(dirfd, "num_objs", &num_parts) < 0) {
		return -EINVAL;
	}

	if (read_property(dirfd, "obj_size", &part_size) < 0) {
		return -EINVAL;
	}

	now = time(NULL);
	stbuf->st_mode = S_IFREG | 0666;
	stbuf->st_nlink = 1;
	stbuf->st_uid = getuid();
	stbuf->st_gid = getgid();
	stbuf->st_size = (uint64_t)num_parts * part_size;
	stbuf->st_blksize = part_size;
	stbuf->st_blocks = ((uint64_t)num_parts * part_size + 511) / 512;
	stbuf->st_atime = now;
	stbuf->st_mtime = now;
	stbuf->st_ctime = now;

	dump_stat("derived stat", stbuf);

	/* close(dirfd); */

	return 0;
}

static int blockfs_truncate(const char *path, off_t length)
{
	/* Silently fail.  */
	return 0;
}

static int blockfs_open(const char *path, struct fuse_file_info *fi)
{
	int dirfd;
	unsigned int num_parts;
	unsigned int part_size;
	int mode;
	int i;

	if (path[0] == 0)
		return -ENOENT;

	dirfd = open_volume_dir(path + 1);
	if (dirfd < 0)
		return -ENOENT;

	if (read_property(dirfd, "num_objs", &num_parts) < 0) {
		return -EINVAL;
	}

	if (read_property(dirfd, "obj_size", &part_size) < 0) {
		return -EINVAL;
	}

	return 0;
}

static int blockfs_read(const char *path, char *buf, size_t size,
			off_t offset, struct fuse_file_info *fi)
{
	int dirfd;
	size_t numread;
	unsigned int num_parts;
	unsigned int part_size;
	struct rbdImage *rbd;

	if (path[0] == 0)
		return -ENOENT;

	dirfd = open_volume_dir(path + 1);
	if (dirfd < 0)
		return -ENOENT;

	if (read_property(dirfd, "num_objs", &num_parts) < 0) {
		return -EINVAL;
	}

	if (read_property(dirfd, "obj_size", &part_size) < 0) {
		return -EINVAL;
	}

	/*
	if (read_property(dirfd, "num_parts", &num_parts) < 0) {
		close(dirfd);
		return -EINVAL;
	}

	if (read_property(dirfd, "part_size", &part_size) < 0) {
		close(dirfd);
		return -EINVAL;
	}
	*/
	rbd = rbdFds[dirfd].rbd;
	numread = 0;
	while (size > 0) {
		ssize_t ret;

		/* ssize_t rbd_read(rbd_image_t image, uint64_t ofs, size_t len, char *buf); */
		ret = rbd_read(rbd->image, offset, size, buf);

		buf += ret;
		size -= ret;
		offset += ret;
		numread += ret;
	}

	/* close(dirfd); */

	return numread;
}

static int blockfs_write(const char *path, const char *buf, size_t size,
			 off_t offset, struct fuse_file_info *fi)
{
	int dirfd;
	size_t numwritten;
	unsigned int num_parts;
	unsigned int part_size;
	struct rbdImage *rbd;

	if (path[0] == 0)
		return -ENOENT;

	dirfd = open_volume_dir(path + 1);
	if (dirfd < 0)
		return -ENOENT;

	if (read_property(dirfd, "num_objs", &num_parts) < 0) {
		return -EINVAL;
	}

	if (read_property(dirfd, "obj_size", &part_size) < 0) {
		return -EINVAL;
	}

	rbd = rbdFds[dirfd].rbd;
	numwritten = 0;
	while (size > 0) {
		ssize_t ret;

		/* ssize_t rbd_write(rbd_image_t image, uint64_t ofs, size_t len, const char *buf); */
		ret = rbd_write(rbd->image, offset, size, buf);
		if (ret < 0) {
			if (numwritten == 0)
				numwritten = ret;
			break;
		}
		buf += ret;
		size -= ret;
		offset += ret;
		numwritten += ret;
	}

	return numwritten;
}

static void blockfs_statfs_volume_cb(void *num, char *volume, int dirfd)
{
	unsigned int num_parts, part_size;
	int	rbdfd;

	((uint64_t *)num)[0]++;

	rbdfd = lookup_rbdImage(volume);
	if (rbdfd >= 0) {
		if (read_property(dirfd, "num_objs", &num_parts) >= 0) {
			if (read_property(dirfd, "obj_size", &part_size) >= 0) {
				((uint64_t *)num)[1] += (uint64_t)num_parts * (uint64_t)part_size;
			}
		}
	}
}

static int blockfs_statfs(const char *path, struct statvfs *buf)
{
	uint64_t num[2];

	num[0] = 1;
	num[1] = 0;
	iter_volumes(num, blockfs_statfs_volume_cb);

	buf->f_bsize = 4096;
	buf->f_frsize = 4096;
	buf->f_blocks = num[1] / 4096;
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

static int blockfs_fsync(const char *path, int datasync,
			 struct fuse_file_info *fi)
{
	/* FIXME.  */
	sync();

	return 0;
}

struct blockfs_readdir_info {
	void *buf;
	fuse_fill_dir_t filler;
};

static void blockfs_readdir_cb(void *_info, char *name, int dirfd)
{
	struct blockfs_readdir_info *info = _info;

	info->filler(info->buf, name, NULL, 0);
}

static int blockfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
			   off_t offset, struct fuse_file_info *fi)
{
	struct blockfs_readdir_info info = { buf, filler };

	if (strcmp(path, "/") != 0)
		return -ENOENT;

	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);
	iter_volumes(&info, blockfs_readdir_cb);

	return 0;
}

static struct fuse_operations blockfs_oper = {
	.getattr	= blockfs_getattr,
	.truncate	= blockfs_truncate,
	.open		= blockfs_open,
	.read		= blockfs_read,
	.write		= blockfs_write,
	.statfs		= blockfs_statfs,
	.fsync		= blockfs_fsync,
	.readdir	= blockfs_readdir,
};

enum {
	KEY_HELP,
	KEY_VERSION,
	KEY_CEPH_CONFIG,
	KEY_CEPH_CONFIG_LONG,
	KEY_RADOS_POOLNAME,
	KEY_RADOS_POOLNAME_LONG
};

static struct fuse_opt blockfs_opts[] = {
		FUSE_OPT_KEY("-h",		  KEY_HELP),
		FUSE_OPT_KEY("--help",	  KEY_HELP),
		FUSE_OPT_KEY("-V",		  KEY_VERSION),
		FUSE_OPT_KEY("--version", KEY_VERSION),
		{"-c %s",           offsetof(struct rbdOptions, ceph_config), KEY_CEPH_CONFIG},
		{"--configfile=%s", offsetof(struct rbdOptions, ceph_config), KEY_CEPH_CONFIG_LONG},
		{"-p %s",	        offsetof(struct rbdOptions, pool_name),   KEY_RADOS_POOLNAME},
		{"--poolname=%s",   offsetof(struct rbdOptions, pool_name),   KEY_RADOS_POOLNAME_LONG},
};

static void usage(const char *progname)
{
	fprintf(stderr,
"Usage: %s backingdir mountpoint [options]\n"
"\n"
"General options:\n"
"    -h   --help            print help\n"
"    -V   --version         print version\n"
"    -c   --configfile      ceph configuration file [/etc/ceph/ceph.conf]\n"
"    -p   --poolname        rados pool name [rbd]\n"
"\n", progname);
}

static int blockfs_opt_proc(void *data, const char *arg, int key,
			    struct fuse_args *outargs)
{
	if (key == FUSE_OPT_KEY_NONOPT) {
		if (rbdOptions.image_name == NULL) {
			rbdOptions.image_name = strdup(arg);
			return 0;
		}
		return 1;
	}

	if (key == KEY_HELP) {
		usage(outargs->argv[0]);
		fuse_opt_add_arg(outargs, "-ho");
		fuse_main(outargs->argc, outargs->argv, &blockfs_oper, NULL);
		exit(1);
	}

	if (key == KEY_VERSION) {
		fuse_opt_add_arg(outargs, "--version");
		fuse_main(outargs->argc, outargs->argv, &blockfs_oper, NULL);
		exit(0);
	}

	if (key == KEY_CEPH_CONFIG) {
		if (rbdOptions.ceph_config != NULL) {
			free(rbdOptions.ceph_config);
			rbdOptions.ceph_config = NULL;
		}
		rbdOptions.ceph_config = strdup(arg+2);
		return 0;
	}

	if (key == KEY_RADOS_POOLNAME) {
		if (rbdOptions.pool_name != NULL) {
			free(rbdOptions.pool_name);
			rbdOptions.pool_name = NULL;
		}
		rbdOptions.pool_name = strdup(arg+2);
		return 0;
	}

	return 1;
}

void
simple_err(const char *msg, int err)
{
    fprintf(stderr, "%s: %s", msg, strerror(-err));
    return;
}

int
connect_to_cluster()
{
	int r;
	int order = 0;


	r = rados_create(&(cluster), NULL);
	if (r < 0) {
		simple_err("Could not create cluster handle", r);
		return r;
	}
	rados_conf_parse_env(cluster, NULL);
	r = rados_conf_read_file(cluster, rbdOptions.ceph_config);
	if (r < 0) {
		simple_err("Error reading ceph config file", r);
		goto failed_shutdown;
	}
	r = rados_connect(cluster);
	if (r < 0) {
		simple_err("Error connecting to cluster", r);
		goto failed_shutdown;
	}

	return 0;

failed_shutdown:
	rados_shutdown(cluster);
	return r;
}

int
radosPool_start(rados_t cluster, char *pool_name)
{
	int r = 0;
	int poolfd;
	struct radosPool *pool;
	struct radosStat *sbuf;

	poolfd = lookup_radosPool(pool_name);
	pool = NULL;

	if (poolfd >= 0) {
		simple_err("Pool already started", poolfd);
		poolfd = -EEXIST;
	} else {
		poolfd = allocate_radosPool(cluster, pool_name);
		if (poolfd >= 0) {
			pool = &(radosPools[poolfd]);
		} else {
			poolfd = -ENOSPC;
		}
	}

	if (pool != (struct radosPool *)NULL) {
		r = rados_ioctx_create(pool->cluster, pool_name, &(pool->ioctx));
		if (r < 0) {
			simple_err("Error creating ioctx", r);
			poolfd = r;
		} else {
			sbuf = &(pool->rados_stat);
			rados_stat(pool->ioctx, &(sbuf->objname[0]), &(sbuf->size), &(sbuf->mtime));
			pool->rados_stat.valid = 1;
		}

	}
	return poolfd;
}

void
rbdOptions_init(void)
{
	rbdOptions.ceph_config = strdup("/etc/ceph/ceph.conf");
	rbdOptions.pool_name = strdup("rbd");
	return;
}

void
radosPools_init(void)
{
	int i;

	for (i = 0; i < MAX_RADOS_POOLS; i++) {
		radosPools[i].pool_name = NULL;
		radosPools[i].rados_stat.valid = 0;
		if ((i+1) < MAX_RADOS_POOLS) {
			radosPools[i].next = &(radosPools[i+1]);
		} else {
			radosPools[i].next = (struct radosPool *)NULL;
		}
	}
	return;
}

void
radosPool_initImages(struct radosPool *pool)
{
	char *imagenames;
	char *iname, *p, *fullname;
	int  name_len, ret;
	size_t	expected_size;
	int  rbd_fd;
	struct rbdImage *rbd_image;
	struct rbdStat  *sbuf;


	expected_size = 0;
	rbd_list(pool->ioctx, imagenames, &expected_size);
	imagenames = (char *)malloc(expected_size);
	if (imagenames == (char *) NULL) {
		return;
	}
	rbd_list(pool->ioctx, imagenames, &expected_size);
	p = imagenames;
	while (p < &(imagenames[expected_size])) {
		iname = p;
		if (strlen(iname) == 0)	break;
		rbd_fd = allocate_rbdImage(pool);
		if (rbd_fd >= 0) {
			rbd_image = rbdFds[rbd_fd].rbd;
			name_len = strlen(iname) + 1;
			fullname = malloc(name_len);
			snprintf(fullname, name_len, "%s", iname);
			rbd_image->image_name = fullname;
			ret = rbd_open(pool->ioctx, rbd_image->image_name, &(rbd_image->image), NULL);
			if (ret < 0) {
				simple_err("radosPool_initImages: error opening image", ret);
				deallocate_rbdImage(rbd_fd);
			} else {
				sbuf = &(rbd_image->rbd_stat);
				rbd_stat(rbd_image->image, &(sbuf->rbd_info), sizeof(rbd_image_info_t));
				sbuf->valid = 1;

				fprintf(stderr, "DEBUG(andy): rbd_stat (prefix %s, parent_poolid %d, parent_name %s)\n", sbuf->rbd_info.block_name_prefix, sbuf->rbd_info.parent_pool, sbuf->rbd_info.parent_name ? sbuf->rbd_info.parent_name : "NULL");
			}
		} else {
			simple_err("radosPool_initImages: failed to allocate rbd Image", rbd_fd);
			break;
		}
		p += strlen(iname) + 1;
	}
	free(imagenames);
	return;
}

void
rbdImages_init(void)
{
	int i;

	for (i = 0; i < MAX_RBD_IMAGES; i++) {
		rbdFds[i].fd = -1;
		rbdFds[i].rbd = (struct rbdImage *)NULL;
		rbdImages[i].rbd_stat.valid = 0;
		if (i+1 < MAX_RBD_IMAGES) {
			rbdImages[i].next = &(rbdImages[i+1]);
		} else {
			rbdImages[i].next = (struct rbdImage *)NULL;
		}
		rbdImages[i].rbd_in_use = 0;
	}
	return;
}

int
lookup_radosPool(char *pool_name)
{
	int i, poolfd;

	poolfd = -1;

	for (i = 0; i < MAX_RADOS_POOLS; i++) {
		if (radosPools[i].pool_name == NULL) {
			continue;
		}
		if (strcmp(pool_name, radosPools[i].pool_name) == 0) {
			poolfd = i;
			break;
		}
	}

	return poolfd;
}

int
allocate_radosPool(rados_t cluster, char *pool_name)
{
	int i, poolfd, errors;

	poolfd = -1;
	errors = 0;

	if (lookup_radosPool(pool_name) < 0) {
		for (i = 0; i < MAX_RADOS_POOLS; i++) {
			if (radosPools[i].pool_name == NULL) {
				radosPools[i].pool_name = strdup(pool_name);
				radosPools[i].cluster = cluster;
				poolfd = i;
				break;
			}
		}
	}

	return poolfd;
}

int lookup_rbdImage(char *image_name)
{
	int fd, errors;
	struct rbdImage *rbd;

	errors = 0;
	fd = -1;
	if (image_name == (char *)NULL) {
		errors++;
	}

	if (!errors) {
		for (fd = 0; fd < MAX_RBD_IMAGES; fd++) {
			rbd = rbdFds[fd].rbd;
			if (rbd == NULL) {
				continue;
			}
			if (rbd->rbd_in_use == 0) {
				continue;
			}
			if (rbd->image_name == (char *)NULL)
			{
				continue;
			}
			if (strcmp(rbd->image_name, image_name) == 0) {
				break;
			}
		}
		if (fd >= MAX_RBD_IMAGES) {
			fd = -1;
		}
	}
	return fd;
}

int
allocate_rbdImage(struct radosPool *pool)
{
	int fd;

	for (fd = 0; fd < MAX_RBD_IMAGES; fd++) {
		if (rbdFds[fd].fd == -1) {
			rbdFds[fd].fd = fd;
			rbdFds[fd].rbd = &(rbdImages[fd]);
			rbdImages[fd].rbd_in_use = 1;
			rbdImages[fd].pool = pool;
			break;
		}
	}

	if (fd >= MAX_RBD_IMAGES) {
		return -1;
	} else {
		return fd;
	}
}

void
deallocate_rbdImage(int imageId)
{
	struct rbdImage *rbd;
	if (imageId < 0 || imageId >= MAX_RBD_IMAGES) {
		return;
	}
	rbdFds[imageId].fd = -1;
	rbd = rbdFds[imageId].rbd;
	rbdFds[imageId].rbd= (struct rbdImage *)NULL;

	if (rbd->image_name != NULL) {
		free(rbd->image_name);
		rbd->image_name = NULL;
	}
	rbd->rbd_in_use = 0;

	return;
}

void
dump_stat(char *label, struct stat *st)
{
	if (st == NULL) return;
	// fprintf(stderr, "dumpstat for '%s'\n", label ? label : "unknown");
	// fprintf(stderr, "\tdev %d ino %d mode %d\n", st->st_dev, st->st_ino, st->st_mode);
	// fprintf(stderr, "\tlinks %d uid %d gid %d\n", st->st_nlink, st->st_uid, st->st_gid);
	// fprintf(stderr, "\tspecial dev %ld size %d blksize %d blkcnt %d\n", (uint64_t)(st->st_rdev), st->st_size, st->st_blksize, st->st_blocks);
	// fprintf(stderr, "\tatime %ld mtime %ld ctime %ld\n", (uint64_t)(st->st_atime), (uint64_t)(st->st_mtime), (uint64_t)(st->st_ctime));
   
	return;
}

int main(int argc, char *argv[])
{
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
	int ret, poolfd;
	struct radosPool *rados_pool;

	rbdOptions_init();
	radosPools_init();
	rbdImages_init();

	if (fuse_opt_parse(&args, &rbdOptions, blockfs_opts, blockfs_opt_proc) == -1)
		exit(1);

	ret = connect_to_cluster();
	if (ret < 0) {
		simple_err("Error accessing ceph cluster", ret);
		exit(90);
	}

	poolfd = radosPool_start(cluster, rbdOptions.pool_name);
	if (poolfd < 0) {
		simple_err("error starting pool", poolfd);
		exit(91);
	}

	rados_pool = &(radosPools[poolfd]);
	radosPool_initImages(rados_pool);

	pthread_mutex_init(&readdir_lock, NULL);

	return fuse_main(args.argc, args.argv, &blockfs_oper, NULL);
}
