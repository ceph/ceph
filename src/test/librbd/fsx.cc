// -*- mode:C++; tab-width:8; c-basic-offset:8; indent-tabs-mode:t -*-
// vim: ts=8 sw=8 smarttab
/*
 *	Copyright (C) 1991, NeXT Computer, Inc.  All Rights Reserverd.
 *
 *	File:	fsx.cc
 *	Author:	Avadis Tevanian, Jr.
 *
 *	File system exerciser.
 *
 *	Rewritten 8/98 by Conrad Minshall.
 *
 *	Small changes to work under Linux -- davej.
 *
 *	Checks for mmap last-page zero fill.
 */

#include <sys/types.h>
#include <unistd.h>
#include <limits.h>
#include <time.h>
#include <strings.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#ifdef HAVE_ERR_H
#include <err.h>
#endif
#include <signal.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <assert.h>
#include <errno.h>
#include <math.h>
#include <fcntl.h>
#include <random>

#include "include/intarith.h"
#include "include/krbd.h"
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.h"
#include "include/rbd/librbd.hpp"
#include "common/Cond.h"
#include "common/SubProcess.h"
#include "common/safe_io.h"
#include "journal/Journaler.h"
#include "journal/ReplayEntry.h"
#include "journal/ReplayHandler.h"

#define NUMPRINTCOLUMNS 32	/* # columns of data to print on each line */

/*
 *	A log entry is an operation and a bunch of arguments.
 */

struct log_entry {
	int	operation;
	int	args[3];
};

#define	LOGSIZE	1000

struct log_entry	oplog[LOGSIZE];	/* the log */
int			logptr = 0;	/* current position in log */
int			logcount = 0;	/* total ops */

/*
 * The operation matrix is complex due to conditional execution of different
 * features. Hence when we come to deciding what operation to run, we need to
 * be careful in how we select the different operations. The active operations
 * are mapped to numbers as follows:
 *
 *		lite	!lite
 * READ:	0	0
 * WRITE:	1	1
 * MAPREAD:	2	2
 * MAPWRITE:	3	3
 * TRUNCATE:	-	4
 * FALLOCATE:	-	5
 * PUNCH HOLE:	-	6
 *
 * When mapped read/writes are disabled, they are simply converted to normal
 * reads and writes. When fallocate/fpunch calls are disabled, they are
 * converted to OP_SKIPPED. Hence OP_SKIPPED needs to have a number higher than
 * the operation selction matrix, as does the OP_CLOSEOPEN which is an
 * operation modifier rather than an operation in itself.
 *
 * Because of the "lite" version, we also need to have different "maximum
 * operation" defines to allow the ops to be selected correctly based on the
 * mode being run.
 */

/* common operations */
#define	OP_READ		0
#define OP_WRITE	1
#define OP_MAPREAD	2
#define OP_MAPWRITE	3
#define OP_MAX_LITE	4

/* !lite operations */
#define OP_TRUNCATE	4
#define OP_FALLOCATE	5
#define OP_PUNCH_HOLE	6
/* rbd-specific operations */
#define OP_CLONE        7
#define OP_FLATTEN	8
#define OP_MAX_FULL	9

/* operation modifiers */
#define OP_CLOSEOPEN	100
#define OP_SKIPPED	101

#undef PAGE_SIZE
#define PAGE_SIZE       getpagesize()
#undef PAGE_MASK
#define PAGE_MASK       (PAGE_SIZE - 1)


char	*original_buf;			/* a pointer to the original data */
char	*good_buf;			/* a pointer to the correct data */
char	*temp_buf;			/* a pointer to the current data */

char	dirpath[1024];

off_t		file_size = 0;
off_t		biggest = 0;
unsigned long	testcalls = 0;		/* calls to function "test" */

unsigned long	simulatedopcount = 0;	/* -b flag */
int	closeprob = 0;			/* -c flag */
int	debug = 0;			/* -d flag */
unsigned long	debugstart = 0;		/* -D flag */
int	flush_enabled = 0;		/* -f flag */
int	holebdy = 1;			/* -h flag */
bool    journal_replay = false;         /* -j flah */
int	keep_on_success = 0;		/* -k flag */
int	do_fsync = 0;			/* -y flag */
unsigned long	maxfilelen = 256 * 1024;	/* -l flag */
int	sizechecks = 1;			/* -n flag disables them */
int	maxoplen = 64 * 1024;		/* -o flag */
int	quiet = 0;			/* -q flag */
unsigned long progressinterval = 0;	/* -p flag */
int	readbdy = 1;			/* -r flag */
int	style = 0;			/* -s flag */
int	prealloc = 0;			/* -x flag */
int	truncbdy = 1;			/* -t flag */
int	writebdy = 1;			/* -w flag */
long	monitorstart = -1;		/* -m flag */
long	monitorend = -1;		/* -m flag */
int	lite = 0;			/* -L flag */
long	numops = -1;			/* -N flag */
int	randomoplen = 1;		/* -O flag disables it */
int	seed = 1;			/* -S flag */
int     mapped_writes = 0;              /* -W flag disables */
int     fallocate_calls = 0;            /* -F flag disables */
int     punch_hole_calls = 1;           /* -H flag disables */
int	clone_calls = 1;                /* -C flag disables */
int	randomize_striping = 1;		/* -U flag disables */
int	randomize_parent_overlap = 1;
int 	mapped_reads = 0;		/* -R flag disables it */
int	fsxgoodfd = 0;
int	o_direct = 0;			/* -Z flag */
int	aio = 0;

int num_clones = 0;

int page_size;
int page_mask;
int mmap_mask;
#ifdef AIO
int aio_rw(int rw, int fd, char *buf, unsigned len, unsigned offset);
#define READ 0
#define WRITE 1
#define fsxread(a,b,c,d)	aio_rw(READ, a,b,c,d)
#define fsxwrite(a,b,c,d)	aio_rw(WRITE, a,b,c,d)
#else
#define fsxread(a,b,c,d)	read(a,b,c)
#define fsxwrite(a,b,c,d)	write(a,b,c)
#endif

FILE *	fsxlogf = NULL;
int badoff = -1;
int closeopen = 0;

void
vwarnc(int code, const char *fmt, va_list ap) {
  fprintf(stderr, "fsx: ");
  if (fmt != NULL) {
	vfprintf(stderr, fmt, ap);
	fprintf(stderr, ": ");
  }
  fprintf(stderr, "%s\n", strerror(code));
}

void
warn(const char * fmt, ...)  {
	va_list ap;
	va_start(ap, fmt);
	vwarnc(errno, fmt, ap);
	va_end(ap);
}

#define BUF_SIZE 1024

void
prt(const char *fmt, ...)
{
	va_list args;
	char buffer[BUF_SIZE];

	va_start(args, fmt);
	vsnprintf(buffer, BUF_SIZE, fmt, args);
	va_end(args);
	fprintf(stdout, "%s", buffer);
	if (fsxlogf)
		fprintf(fsxlogf, "%s", buffer);
}

void
prterr(const char *prefix)
{
	prt("%s%s%s\n", prefix, prefix ? ": " : "", strerror(errno));
}

void
prterrcode(const char *prefix, int code)
{
	prt("%s%s%s\n", prefix, prefix ? ": " : "", strerror(-code));
}

void
simple_err(const char *msg, int err)
{
    fprintf(stderr, "%s: %s\n", msg, strerror(-err));
}

/*
 * random
 */
std::mt19937 random_generator;

uint_fast32_t
get_random(void)
{
	return random_generator();
}

void replay_imagename(char *buf, size_t len, int clones);

namespace {

static const std::string JOURNAL_CLIENT_ID("fsx");

struct ReplayHandler : public journal::ReplayHandler {
        journal::Journaler *journaler;
        journal::Journaler *replay_journaler;
        Context *on_finish;

        ReplayHandler(journal::Journaler *journaler,
                      journal::Journaler *replay_journaler, Context *on_finish)
                : journaler(journaler), replay_journaler(replay_journaler),
                  on_finish(on_finish) {
        }

        virtual void get() {
        }
        virtual void put() {
        }

        virtual void handle_entries_available() {
                while (true) {
                        journal::ReplayEntry replay_entry;
                        if (!journaler->try_pop_front(&replay_entry)) {
                                return;
                        }

                        replay_journaler->append(0, replay_entry.get_data());
                }
        }

        virtual void handle_complete(int r) {
                on_finish->complete(r);
        }
};

int get_image_id(librados::IoCtx &io_ctx, const char *image_name,
                 std::string *image_id) {
        librbd::RBD rbd;
        librbd::Image image;
        int r = rbd.open(io_ctx, image, image_name);
        if (r < 0) {
                simple_err("failed to open image", r);
                return r;
        }

        rbd_image_info_t info;
        r = image.stat(info, sizeof(info));
        if (r < 0) {
                simple_err("failed to stat image", r);
                return r;
        }

        *image_id = std::string(&info.block_name_prefix[strlen(RBD_DATA_PREFIX)]);
        return 0;
}

int register_journal(rados_ioctx_t ioctx, const char *image_name) {
        librados::IoCtx io_ctx;
        librados::IoCtx::from_rados_ioctx_t(ioctx, io_ctx);

        std::string image_id;
        int r = get_image_id(io_ctx, image_name, &image_id);
        if (r < 0) {
                return r;
        }

        journal::Journaler journaler(io_ctx, image_id, JOURNAL_CLIENT_ID, 0);
        r = journaler.register_client(bufferlist());
        if (r < 0) {
                simple_err("failed to register journal client", r);
                return r;
        }
        return 0;
}

int unregister_journal(rados_ioctx_t ioctx, const char *image_name) {
        librados::IoCtx io_ctx;
        librados::IoCtx::from_rados_ioctx_t(ioctx, io_ctx);

        std::string image_id;
        int r = get_image_id(io_ctx, image_name, &image_id);
        if (r < 0) {
                return r;
        }

        journal::Journaler journaler(io_ctx, image_id, JOURNAL_CLIENT_ID, 0);
        r = journaler.unregister_client();
        if (r < 0) {
                simple_err("failed to unregister journal client", r);
                return r;
        }
        return 0;
}

int create_replay_image(rados_ioctx_t ioctx, int order,
                        uint64_t stripe_unit, int stripe_count,
                        const char *replay_image_name,
                        const char *last_replay_image_name) {
        librados::IoCtx io_ctx;
        librados::IoCtx::from_rados_ioctx_t(ioctx, io_ctx);

        int r;
        librbd::RBD rbd;
        if (last_replay_image_name == nullptr) {
                r = rbd.create2(io_ctx, replay_image_name, 0,
                                RBD_FEATURES_ALL, &order);
        } else {
                r = rbd.clone2(io_ctx, last_replay_image_name, "snap",
                               io_ctx, replay_image_name, RBD_FEATURES_ALL,
                               &order, stripe_unit, stripe_count);
        }

        if (r < 0) {
                simple_err("failed to create replay image", r);
                return r;
        }

        return 0;
}

int replay_journal(rados_ioctx_t ioctx, const char *image_name,
                   const char *replay_image_name) {
        librados::IoCtx io_ctx;
        librados::IoCtx::from_rados_ioctx_t(ioctx, io_ctx);

        std::string image_id;
        int r = get_image_id(io_ctx, image_name, &image_id);
        if (r < 0) {
                return r;
        }

        std::string replay_image_id;
        r = get_image_id(io_ctx, replay_image_name, &replay_image_id);
        if (r < 0) {
                return r;
        }

        journal::Journaler journaler(io_ctx, image_id, JOURNAL_CLIENT_ID, 0);
        C_SaferCond init_ctx;
        journaler.init(&init_ctx);
        r = init_ctx.wait();
        if (r < 0) {
                simple_err("failed to initialize journal", r);
                return r;
        }

        journal::Journaler replay_journaler(io_ctx, replay_image_id, "", 0);
        C_SaferCond replay_init_ctx;
        replay_journaler.init(&replay_init_ctx);
        r = replay_init_ctx.wait();
        if (r < 0) {
                simple_err("failed to initialize replay journal", r);
                return r;
        }

        replay_journaler.start_append(0, 0, 0);

        C_SaferCond replay_ctx;
        ReplayHandler replay_handler(&journaler, &replay_journaler,
                                     &replay_ctx);

        // copy journal events from source image to replay image
        journaler.start_replay(&replay_handler);
        r = replay_ctx.wait();

        journaler.stop_replay();

        C_SaferCond stop_ctx;
        replay_journaler.stop_append(&stop_ctx);
        int stop_r = stop_ctx.wait();
        if (r == 0 && stop_r < 0) {
                r = stop_r;
        }

        if (r < 0) {
                simple_err("failed to replay journal", r);
                return r;
        }

        librbd::RBD rbd;
        librbd::Image image;
        r = rbd.open(io_ctx, image, replay_image_name);
        if (r < 0) {
                simple_err("failed to open replay image", r);
                return r;
        }

        // perform an IO op to initiate the journal replay
        bufferlist bl;
        r = static_cast<ssize_t>(image.write(0, 0, bl));
        if (r < 0) {
                simple_err("failed to write to replay image", r);
                return r;
        }
        return 0;
}

int finalize_journal(rados_ioctx_t ioctx, const char *imagename, int clones,
                     int order, uint64_t stripe_unit, int stripe_count) {
        char replayimagename[1024];
        replay_imagename(replayimagename, sizeof(replayimagename), clones);

        char lastreplayimagename[1024];
        if (clones > 0) {
                replay_imagename(lastreplayimagename,
                                 sizeof(lastreplayimagename), clones - 1);
        }

        int ret = create_replay_image(ioctx, order, stripe_unit,
                                      stripe_count, replayimagename,
                                      clones > 0 ? lastreplayimagename :
                                                   nullptr);
        if (ret < 0) {
                exit(EXIT_FAILURE);
        }

        ret = replay_journal(ioctx, imagename, replayimagename);
        if (ret < 0) {
                exit(EXIT_FAILURE);
        }
        return 0;
}

} // anonymous namespace

/*
 * rbd
 */

struct rbd_ctx {
	const char *name;	/* image name */
	rbd_image_t image;	/* image handle */
	const char *krbd_name;	/* image /dev/rbd<id> name */ /* reused for nbd test */
	int krbd_fd;		/* image /dev/rbd<id> fd */ /* reused for nbd test */
};

#define RBD_CTX_INIT	(struct rbd_ctx) { NULL, NULL, NULL, -1 }

struct rbd_operations {
	int (*open)(const char *name, struct rbd_ctx *ctx);
	int (*close)(struct rbd_ctx *ctx);
	ssize_t (*read)(struct rbd_ctx *ctx, uint64_t off, size_t len, char *buf);
	ssize_t (*write)(struct rbd_ctx *ctx, uint64_t off, size_t len, const char *buf);
	int (*flush)(struct rbd_ctx *ctx);
	int (*discard)(struct rbd_ctx *ctx, uint64_t off, uint64_t len);
	int (*get_size)(struct rbd_ctx *ctx, uint64_t *size);
	int (*resize)(struct rbd_ctx *ctx, uint64_t size);
	int (*clone)(struct rbd_ctx *ctx, const char *src_snapname,
		     const char *dst_imagename, int *order, int stripe_unit,
		     int stripe_count);
	int (*flatten)(struct rbd_ctx *ctx);
};

char *pool;			/* name of the pool our test image is in */
char *iname;			/* name of our test image */
rados_t cluster;		/* handle for our test cluster */
rados_ioctx_t ioctx;		/* handle for our test pool */
struct krbd_ctx *krbd;		/* handle for libkrbd */

/*
 * librbd/krbd rbd_operations handlers.  Given the rest of fsx.c, no
 * attempt to do error handling is made in these handlers.
 */

int
__librbd_open(const char *name, struct rbd_ctx *ctx)
{
	rbd_image_t image;
	int ret;

	assert(!ctx->name && !ctx->image &&
	       !ctx->krbd_name && ctx->krbd_fd < 0);

	ret = rbd_open(ioctx, name, &image, NULL);
	if (ret < 0) {
		prt("rbd_open(%s) failed\n", name);
		return ret;
	}

	ctx->name = strdup(name);
	ctx->image = image;
	ctx->krbd_name = NULL;
	ctx->krbd_fd = -1;

	return 0;
}

int
librbd_open(const char *name, struct rbd_ctx *ctx)
{
	return __librbd_open(name, ctx);
}

int
__librbd_close(struct rbd_ctx *ctx)
{
	int ret;

	assert(ctx->name && ctx->image);

	ret = rbd_close(ctx->image);
	if (ret < 0) {
		prt("rbd_close(%s) failed\n", ctx->name);
		return ret;
	}

	free((void *)ctx->name);

	ctx->name = NULL;
	ctx->image = NULL;

	return 0;
}

int
librbd_close(struct rbd_ctx *ctx)
{
	return __librbd_close(ctx);
}

int
librbd_verify_object_map(struct rbd_ctx *ctx)
{
	int n;
	uint64_t flags;
	n = rbd_get_flags(ctx->image, &flags);
	if (n < 0) {
		prt("rbd_get_flags() failed\n");
		return n;
	}

	if ((flags & RBD_FLAG_OBJECT_MAP_INVALID) != 0) {
		prt("rbd_get_flags() indicates object map is invalid\n");
		return -EINVAL;
	}
	return 0;
}

ssize_t
librbd_read(struct rbd_ctx *ctx, uint64_t off, size_t len, char *buf)
{
	ssize_t n;

	n = rbd_read(ctx->image, off, len, buf);
	if (n < 0)
		prt("rbd_read(%llu, %zu) failed\n", off, len);

	return n;
}

ssize_t
librbd_write(struct rbd_ctx *ctx, uint64_t off, size_t len, const char *buf)
{
	ssize_t n;
	int ret;

	n = rbd_write(ctx->image, off, len, buf);
	if (n < 0) {
		prt("rbd_write(%llu, %zu) failed\n", off, len);
		return n;
	}

	ret = librbd_verify_object_map(ctx);
	if (ret < 0) {
		return ret;
	}
	return n;
}

int
librbd_flush(struct rbd_ctx *ctx)
{
	int ret;

	ret = rbd_flush(ctx->image);
	if (ret < 0) {
		prt("rbd_flush failed\n");
		return ret;
	}

	return librbd_verify_object_map(ctx);
}

int
librbd_discard(struct rbd_ctx *ctx, uint64_t off, uint64_t len)
{
	int ret;

	ret = rbd_discard(ctx->image, off, len);
	if (ret < 0) {
		prt("rbd_discard(%llu, %llu) failed\n", off, len);
		return ret;
	}

	return librbd_verify_object_map(ctx);
}

int
librbd_get_size(struct rbd_ctx *ctx, uint64_t *size)
{
	rbd_image_info_t info;
	int ret;

	ret = rbd_stat(ctx->image, &info, sizeof(info));
	if (ret < 0) {
		prt("rbd_stat failed\n");
		return ret;
	}

	*size = info.size;

	return 0;
}

int
__librbd_resize(struct rbd_ctx *ctx, uint64_t size)
{
	int ret;

	ret = rbd_resize(ctx->image, size);
	if (ret < 0) {
		prt("rbd_resize(%llu) failed\n", size);
		return ret;
	}

	return librbd_verify_object_map(ctx);
}

int
librbd_resize(struct rbd_ctx *ctx, uint64_t size)
{
	return __librbd_resize(ctx, size);
}

int
__librbd_clone(struct rbd_ctx *ctx, const char *src_snapname,
	       const char *dst_imagename, int *order, int stripe_unit,
	       int stripe_count, bool krbd)
{
	int ret;

	ret = rbd_snap_create(ctx->image, src_snapname);
	if (ret < 0) {
		prt("rbd_snap_create(%s@%s) failed\n", ctx->name,
		    src_snapname);
		return ret;
	}

	ret = rbd_snap_protect(ctx->image, src_snapname);
	if (ret < 0) {
		prt("rbd_snap_protect(%s@%s) failed\n", ctx->name,
		    src_snapname);
		return ret;
	}

	uint64_t features = RBD_FEATURES_ALL;
	if (krbd) {
		features &= ~(RBD_FEATURE_EXCLUSIVE_LOCK |
		              RBD_FEATURE_OBJECT_MAP     |
                              RBD_FEATURE_FAST_DIFF      |
                              RBD_FEATURE_DEEP_FLATTEN   |
                              RBD_FEATURE_JOURNALING);
	}
	ret = rbd_clone2(ioctx, ctx->name, src_snapname, ioctx,
			 dst_imagename, features, order,
			 stripe_unit, stripe_count);
	if (ret < 0) {
		prt("rbd_clone2(%s@%s -> %s) failed\n", ctx->name,
		    src_snapname, dst_imagename);
		return ret;
	}

	return 0;
}

int
librbd_clone(struct rbd_ctx *ctx, const char *src_snapname,
	     const char *dst_imagename, int *order, int stripe_unit,
	     int stripe_count)
{
	return __librbd_clone(ctx, src_snapname, dst_imagename, order,
			      stripe_unit, stripe_count, false);
}

int
__librbd_flatten(struct rbd_ctx *ctx)
{
	int ret;

	ret = rbd_flatten(ctx->image);
	if (ret < 0) {
		prt("rbd_flatten failed\n");
		return ret;
	}

	return librbd_verify_object_map(ctx);
}

int
librbd_flatten(struct rbd_ctx *ctx)
{
	return __librbd_flatten(ctx);
}

const struct rbd_operations librbd_operations = {
	librbd_open,
	librbd_close,
	librbd_read,
	librbd_write,
	librbd_flush,
	librbd_discard,
	librbd_get_size,
	librbd_resize,
	librbd_clone,
	librbd_flatten
};

int
krbd_open(const char *name, struct rbd_ctx *ctx)
{
	char *devnode;
	int fd;
	int ret;

	ret = __librbd_open(name, ctx);
	if (ret < 0)
		return ret;

	ret = krbd_map(krbd, pool, name, "", "", &devnode);
	if (ret < 0) {
		prt("krbd_map(%s) failed\n", name);
		return ret;
	}

	fd = open(devnode, O_RDWR | o_direct);
	if (fd < 0) {
		ret = -errno;
		prt("open(%s) failed\n", devnode);
		return ret;
	}

	ctx->krbd_name = devnode;
	ctx->krbd_fd = fd;

	return 0;
}

int
krbd_close(struct rbd_ctx *ctx)
{
	int ret;

	assert(ctx->krbd_name && ctx->krbd_fd >= 0);

	if (close(ctx->krbd_fd) < 0) {
		ret = -errno;
		prt("close(%s) failed\n", ctx->krbd_name);
		return ret;
	}

	ret = krbd_unmap(krbd, ctx->krbd_name);
	if (ret < 0) {
		prt("krbd_unmap(%s) failed\n", ctx->krbd_name);
		return ret;
	}

	free((void *)ctx->krbd_name);

	ctx->krbd_name = NULL;
	ctx->krbd_fd = -1;

	return __librbd_close(ctx);
}

ssize_t
krbd_read(struct rbd_ctx *ctx, uint64_t off, size_t len, char *buf)
{
	ssize_t n;

	n = pread(ctx->krbd_fd, buf, len, off);
	if (n < 0) {
		n = -errno;
		prt("pread(%llu, %zu) failed\n", off, len);
		return n;
	}

	return n;
}

ssize_t
krbd_write(struct rbd_ctx *ctx, uint64_t off, size_t len, const char *buf)
{
	ssize_t n;

	n = pwrite(ctx->krbd_fd, buf, len, off);
	if (n < 0) {
		n = -errno;
		prt("pwrite(%llu, %zu) failed\n", off, len);
		return n;
	}

	return n;
}

int
__krbd_flush(struct rbd_ctx *ctx, bool invalidate)
{
	int ret;

	if (o_direct)
		return 0;

	/*
	 * BLKFLSBUF will sync the filesystem on top of the device (we
	 * don't care about that here, since we write directly to it),
	 * write out any dirty buffers and invalidate the buffer cache.
	 * It won't do a hardware cache flush.
	 *
	 * fsync() will write out any dirty buffers and do a hardware
	 * cache flush (which we don't care about either, because for
	 * krbd it's a noop).  It won't try to empty the buffer cache
	 * nor poke the filesystem before writing out.
	 *
	 * Given that, for our purposes, fsync is a flush, while
	 * BLKFLSBUF is a flush+invalidate.
	 */
        if (invalidate)
		ret = ioctl(ctx->krbd_fd, BLKFLSBUF, NULL);
	else
		ret = fsync(ctx->krbd_fd);
	if (ret < 0) {
		ret = -errno;
		prt("%s failed\n", invalidate ? "BLKFLSBUF" : "fsync");
		return ret;
	}

	return 0;
}

int
krbd_flush(struct rbd_ctx *ctx)
{
	return __krbd_flush(ctx, false);
}

int
krbd_discard(struct rbd_ctx *ctx, uint64_t off, uint64_t len)
{
	uint64_t range[2] = { off, len };
	int ret;

	/*
	 * BLKDISCARD goes straight to disk and doesn't do anything
	 * about dirty buffers.  This means we need to flush so that
	 *
	 *   write 0..3M
	 *   discard 1..2M
	 *
	 * results in "data 0000 data" rather than "data data data" on
	 * disk and invalidate so that
	 *
	 *   discard 1..2M
	 *   read 0..3M
	 *
	 * returns "data 0000 data" rather than "data data data" in
	 * case 1..2M was cached.
	 */
	ret = __krbd_flush(ctx, true);
	if (ret < 0)
		return ret;

	/*
	 * off and len must be 512-byte aligned, otherwise BLKDISCARD
	 * will fail with -EINVAL.  This means that -K (enable krbd
	 * mode) requires -h 512 or similar.
	 */
	if (ioctl(ctx->krbd_fd, BLKDISCARD, &range) < 0) {
		ret = -errno;
		prt("BLKDISCARD(%llu, %llu) failed\n", off, len);
		return ret;
	}

	return 0;
}

int
krbd_get_size(struct rbd_ctx *ctx, uint64_t *size)
{
	uint64_t bytes;

	if (ioctl(ctx->krbd_fd, BLKGETSIZE64, &bytes) < 0) {
		int ret = -errno;
		prt("BLKGETSIZE64 failed\n");
		return ret;
	}

	*size = bytes;

	return 0;
}

int
krbd_resize(struct rbd_ctx *ctx, uint64_t size)
{
	int ret;

	assert(size % truncbdy == 0);

	/*
	 * When krbd detects a size change, it calls revalidate_disk(),
	 * which ends up calling invalidate_bdev(), which invalidates
	 * clean pages and does nothing about dirty pages beyond the
	 * new size.  The preceding cache flush makes sure those pages
	 * are invalidated, which is what we need on shrink so that
	 *
	 *  write 0..1M
	 *  resize 0
	 *  resize 2M
	 *  read 0..2M
	 *
	 * returns "0000 0000" rather than "data 0000".
	 */
	ret = __krbd_flush(ctx, false);
	if (ret < 0)
		return ret;

	return __librbd_resize(ctx, size);
}

int
krbd_clone(struct rbd_ctx *ctx, const char *src_snapname,
	   const char *dst_imagename, int *order, int stripe_unit,
	   int stripe_count)
{
	int ret;

	ret = __krbd_flush(ctx, false);
	if (ret < 0)
		return ret;

	return __librbd_clone(ctx, src_snapname, dst_imagename, order,
			      stripe_unit, stripe_count, true);
}

int
krbd_flatten(struct rbd_ctx *ctx)
{
	int ret;

	ret = __krbd_flush(ctx, false);
	if (ret < 0)
		return ret;

	return __librbd_flatten(ctx);
}

const struct rbd_operations krbd_operations = {
	krbd_open,
	krbd_close,
	krbd_read,
	krbd_write,
	krbd_flush,
	krbd_discard,
	krbd_get_size,
	krbd_resize,
	krbd_clone,
	krbd_flatten,
};

int
nbd_open(const char *name, struct rbd_ctx *ctx)
{
	int r;
	int fd;
	char dev[4096];
	char *devnode;

	SubProcess process("rbd-nbd", SubProcess::KEEP, SubProcess::PIPE);
	process.add_cmd_arg("map");
	std::string img;
	img.append(pool);
	img.append("/");
	img.append(name);
	process.add_cmd_arg(img.c_str());

	r = __librbd_open(name, ctx);
	if (r < 0)
		return r;

        r = process.spawn();
        if (r < 0) {
		prt("nbd_open failed to run rbd-nbd error: %s\n", process.err().c_str());
		return r;
        }
	r = safe_read(process.get_stdout(), dev, sizeof(dev));
	if (r < 0) {
		prt("nbd_open failed to get nbd device path\n");
		return r;
	}
	for (int i = 0; i < r; ++i)
	  if (dev[i] == 10 || dev[i] == 13)
	    dev[i] = 0;
	dev[r] = 0;
	r = process.join();
	if (r) {
		prt("rbd-nbd failed with error: %s", process.err().c_str());
		return -EINVAL;
	}

	devnode = strdup(dev);
	if (!devnode)
		return -ENOMEM;

	fd = open(devnode, O_RDWR | o_direct);
	if (fd < 0) {
		r = -errno;
		prt("open(%s) failed\n", devnode);
		return r;
	}

	ctx->krbd_name = devnode;
	ctx->krbd_fd = fd;

	return 0;
}

int
nbd_close(struct rbd_ctx *ctx)
{
	int r;

	assert(ctx->krbd_name && ctx->krbd_fd >= 0);

	if (close(ctx->krbd_fd) < 0) {
		r = -errno;
		prt("close(%s) failed\n", ctx->krbd_name);
		return r;
	}

	SubProcess process("rbd-nbd");
	process.add_cmd_arg("unmap");
	process.add_cmd_arg(ctx->krbd_name);

        r = process.spawn();
        if (r < 0) {
		prt("nbd_close failed to run rbd-nbd error: %s\n", process.err().c_str());
		return r;
        }
	r = process.join();
	if (r) {
		prt("rbd-nbd failed with error: %d", process.err().c_str());
		return -EINVAL;
	}

	free((void *)ctx->krbd_name);

	ctx->krbd_name = NULL;
	ctx->krbd_fd = -1;

	return __librbd_close(ctx);
}

int
nbd_clone(struct rbd_ctx *ctx, const char *src_snapname,
	  const char *dst_imagename, int *order, int stripe_unit,
	  int stripe_count)
{
	int ret;

	ret = __krbd_flush(ctx, false);
	if (ret < 0)
		return ret;

	return __librbd_clone(ctx, src_snapname, dst_imagename, order,
			      stripe_unit, stripe_count, false);
}

const struct rbd_operations nbd_operations = {
	nbd_open,
	nbd_close,
	krbd_read,
	krbd_write,
	krbd_flush,
	krbd_discard,
	krbd_get_size,
	krbd_resize,
	nbd_clone,
	krbd_flatten,
};

struct rbd_ctx ctx = RBD_CTX_INIT;
const struct rbd_operations *ops = &librbd_operations;

static bool rbd_image_has_parent(struct rbd_ctx *ctx)
{
	int ret;

	ret = rbd_get_parent_info(ctx->image, NULL, 0, NULL, 0, NULL, 0);
	if (ret < 0 && ret != -ENOENT) {
		prterrcode("rbd_get_parent_info", ret);
		exit(1);
	}

	return !ret;
}

/*
 * fsx
 */

void
log4(int operation, int arg0, int arg1, int arg2)
{
	struct log_entry *le;

	le = &oplog[logptr];
	le->operation = operation;
	if (closeopen)
		le->operation = ~ le->operation;
	le->args[0] = arg0;
	le->args[1] = arg1;
	le->args[2] = arg2;
	logptr++;
	logcount++;
	if (logptr >= LOGSIZE)
		logptr = 0;
}

void
logdump(void)
{
	int	i, count, down;
	struct log_entry	*lp;
	const char *falloc_type[3] = {"PAST_EOF", "EXTENDING", "INTERIOR"};

	prt("LOG DUMP (%d total operations):\n", logcount);
	if (logcount < LOGSIZE) {
		i = 0;
		count = logcount;
	} else {
		i = logptr;
		count = LOGSIZE;
	}
	for ( ; count > 0; count--) {
		int opnum;

		opnum = i+1 + (logcount/LOGSIZE)*LOGSIZE;
		prt("%d(%3d mod 256): ", opnum, opnum%256);
		lp = &oplog[i];
		if ((closeopen = lp->operation < 0))
			lp->operation = ~ lp->operation;
			
		switch (lp->operation) {
		case OP_MAPREAD:
			prt("MAPREAD  0x%x thru 0x%x\t(0x%x bytes)",
			    lp->args[0], lp->args[0] + lp->args[1] - 1,
			    lp->args[1]);
			if (badoff >= lp->args[0] && badoff <
						     lp->args[0] + lp->args[1])
				prt("\t***RRRR***");
			break;
		case OP_MAPWRITE:
			prt("MAPWRITE 0x%x thru 0x%x\t(0x%x bytes)",
			    lp->args[0], lp->args[0] + lp->args[1] - 1,
			    lp->args[1]);
			if (badoff >= lp->args[0] && badoff <
						     lp->args[0] + lp->args[1])
				prt("\t******WWWW");
			break;
		case OP_READ:
			prt("READ     0x%x thru 0x%x\t(0x%x bytes)",
			    lp->args[0], lp->args[0] + lp->args[1] - 1,
			    lp->args[1]);
			if (badoff >= lp->args[0] &&
			    badoff < lp->args[0] + lp->args[1])
				prt("\t***RRRR***");
			break;
		case OP_WRITE:
			prt("WRITE    0x%x thru 0x%x\t(0x%x bytes)",
			    lp->args[0], lp->args[0] + lp->args[1] - 1,
			    lp->args[1]);
			if (lp->args[0] > lp->args[2])
				prt(" HOLE");
			else if (lp->args[0] + lp->args[1] > lp->args[2])
				prt(" EXTEND");
			if ((badoff >= lp->args[0] || badoff >=lp->args[2]) &&
			    badoff < lp->args[0] + lp->args[1])
				prt("\t***WWWW");
			break;
		case OP_TRUNCATE:
			down = lp->args[0] < lp->args[1];
			prt("TRUNCATE %s\tfrom 0x%x to 0x%x",
			    down ? "DOWN" : "UP", lp->args[1], lp->args[0]);
			if (badoff >= lp->args[!down] &&
			    badoff < lp->args[!!down])
				prt("\t******WWWW");
			break;
		case OP_FALLOCATE:
			/* 0: offset 1: length 2: where alloced */
			prt("FALLOC   0x%x thru 0x%x\t(0x%x bytes) %s",
				lp->args[0], lp->args[0] + lp->args[1],
				lp->args[1], falloc_type[lp->args[2]]);
			if (badoff >= lp->args[0] &&
			    badoff < lp->args[0] + lp->args[1])
				prt("\t******FFFF");
			break;
		case OP_PUNCH_HOLE:
			prt("PUNCH    0x%x thru 0x%x\t(0x%x bytes)",
			    lp->args[0], lp->args[0] + lp->args[1] - 1,
			    lp->args[1]);
			if (badoff >= lp->args[0] && badoff <
						     lp->args[0] + lp->args[1])
				prt("\t******PPPP");
			break;
		case OP_CLONE:
			prt("CLONE");
			break;
		case OP_FLATTEN:
			prt("FLATTEN");
			break;
		case OP_SKIPPED:
			prt("SKIPPED (no operation)");
			break;
		default:
			prt("BOGUS LOG ENTRY (operation code = %d)!",
			    lp->operation);
		}
		if (closeopen)
			prt("\n\t\tCLOSE/OPEN");
		prt("\n");
		i++;
		if (i == LOGSIZE)
			i = 0;
	}
}

void
save_buffer(char *buffer, off_t bufferlength, int fd)
{
	off_t ret;
	ssize_t byteswritten;

	if (fd <= 0 || bufferlength == 0)
		return;

	if (bufferlength > SSIZE_MAX) {
		prt("fsx flaw: overflow in save_buffer\n");
		exit(67);
	}

	ret = lseek(fd, (off_t)0, SEEK_SET);
	if (ret == (off_t)-1)
		prterr("save_buffer: lseek 0");
	
	byteswritten = write(fd, buffer, (size_t)bufferlength);
	if (byteswritten != bufferlength) {
		if (byteswritten == -1)
			prterr("save_buffer write");
		else
			warn("save_buffer: short write, 0x%x bytes instead of 0x%llx\n",
			     (unsigned)byteswritten,
			     (unsigned long long)bufferlength);
	}
}


void
report_failure(int status)
{
	logdump();
	
	if (fsxgoodfd) {
		if (good_buf) {
			save_buffer(good_buf, file_size, fsxgoodfd);
			prt("Correct content saved for comparison\n");
			prt("(maybe hexdump \"%s\" vs \"%s.fsxgood\")\n",
			    iname, iname);
		}
		close(fsxgoodfd);
	}
	sleep(3);   // so the log can flush to disk.  KLUDGEY!
	exit(status);
}

#define short_at(cp) ((unsigned short)((*((unsigned char *)(cp)) << 8) | \
				        *(((unsigned char *)(cp)) + 1)))

void
check_buffers(char *good_buf, char *temp_buf, unsigned offset, unsigned size)
{
	if (memcmp(good_buf + offset, temp_buf, size) != 0) {
		unsigned i = 0;
		unsigned n = 0;

		prt("READ BAD DATA: offset = 0x%x, size = 0x%x, fname = %s\n",
		    offset, size, iname);
		prt("OFFSET\tGOOD\tBAD\tRANGE\n");
		while (size > 0) {
			unsigned char c = good_buf[offset];
			unsigned char t = temp_buf[i];
			if (c != t) {
			        if (n < 16) {
					unsigned bad = short_at(&temp_buf[i]);
				        prt("0x%5x\t0x%04x\t0x%04x", offset,
				            short_at(&good_buf[offset]), bad);
					unsigned op = temp_buf[(offset & 1) ? i+1 : i];
				        prt("\t0x%5x\n", n);
					if (op)
						prt("operation# (mod 256) for "
						  "the bad data may be %u\n",
						((unsigned)op & 0xff));
					else
						prt("operation# (mod 256) for "
						  "the bad data unknown, check"
						  " HOLE and EXTEND ops\n");
				}
				n++;
				badoff = offset;
			}
			offset++;
			i++;
			size--;
		}
		report_failure(110);
	}
}


void
check_size(void)
{
	uint64_t size;
	int ret;

	ret = ops->get_size(&ctx, &size);
	if (ret < 0)
		prterrcode("check_size: ops->get_size", ret);

	if ((uint64_t)file_size != size) {
		prt("Size error: expected 0x%llx stat 0x%llx\n",
		    (unsigned long long)file_size,
		    (unsigned long long)size);
		report_failure(120);
	}
}

#define TRUNC_HACK_SIZE	(200ULL << 9)	/* 512-byte aligned for krbd */

void
check_trunc_hack(void)
{
	uint64_t size;
	int ret;

	ret = ops->resize(&ctx, 0ULL);
	if (ret < 0)
		prterrcode("check_trunc_hack: ops->resize pre", ret);

	ret = ops->resize(&ctx, TRUNC_HACK_SIZE);
	if (ret < 0)
		prterrcode("check_trunc_hack: ops->resize actual", ret);

	ret = ops->get_size(&ctx, &size);
	if (ret < 0)
		prterrcode("check_trunc_hack: ops->get_size", ret);

	if (size != TRUNC_HACK_SIZE) {
		prt("no extend on truncate! not posix!\n");
		exit(130);
	}

	ret = ops->resize(&ctx, 0ULL);
	if (ret < 0)
		prterrcode("check_trunc_hack: ops->resize post", ret);
}

int
create_image()
{
	int r;
	int order = 0;

	r = rados_create(&cluster, NULL);
	if (r < 0) {
		simple_err("Could not create cluster handle", r);
		return r;
	}
	rados_conf_parse_env(cluster, NULL);
	r = rados_conf_read_file(cluster, NULL);
	if (r < 0) {
		simple_err("Error reading ceph config file", r);
		goto failed_shutdown;
	}
	r = rados_connect(cluster);
	if (r < 0) {
		simple_err("Error connecting to cluster", r);
		goto failed_shutdown;
	}
	r = krbd_create_from_context(rados_cct(cluster), &krbd);
	if (r < 0) {
		simple_err("Could not create libkrbd handle", r);
		goto failed_shutdown;
	}

	r = rados_pool_create(cluster, pool);
	if (r < 0 && r != -EEXIST) {
		simple_err("Error creating pool", r);
		goto failed_krbd;
	}
	r = rados_ioctx_create(cluster, pool, &ioctx);
	if (r < 0) {
		simple_err("Error creating ioctx", r);
		goto failed_krbd;
	}
	if (clone_calls || journal_replay) {
                uint64_t features = 0;
                if (clone_calls) {
                        features |= RBD_FEATURE_LAYERING;
                }
                if (journal_replay) {
                        features |= (RBD_FEATURE_EXCLUSIVE_LOCK |
                                     RBD_FEATURE_JOURNALING);
                }
		r = rbd_create2(ioctx, iname, 0, features, &order);
	} else {
		r = rbd_create(ioctx, iname, 0, &order);
	}
	if (r < 0) {
		simple_err("Error creating image", r);
		goto failed_open;
	}

        if (journal_replay) {
                r = register_journal(ioctx, iname);
                if (r < 0) {
                        goto failed_open;
                }
        }
	return 0;

 failed_open:
	rados_ioctx_destroy(ioctx);
 failed_krbd:
	krbd_destroy(krbd);
 failed_shutdown:
	rados_shutdown(cluster);
	return r;
}

void
doflush(unsigned offset, unsigned size)
{
	int ret;

	if (o_direct)
		return;

	ret = ops->flush(&ctx);
	if (ret < 0)
		prterrcode("doflush: ops->flush", ret);
}

void
doread(unsigned offset, unsigned size)
{
	int ret;

	offset -= offset % readbdy;
	if (o_direct)
		size -= size % readbdy;
	if (size == 0) {
		if (!quiet && testcalls > simulatedopcount && !o_direct)
			prt("skipping zero size read\n");
		log4(OP_SKIPPED, OP_READ, offset, size);
		return;
	}
	if (size + offset > file_size) {
		if (!quiet && testcalls > simulatedopcount)
			prt("skipping seek/read past end of file\n");
		log4(OP_SKIPPED, OP_READ, offset, size);
		return;
	}

	log4(OP_READ, offset, size, 0);

	if (testcalls <= simulatedopcount)
		return;

	if (!quiet &&
		((progressinterval && testcalls % progressinterval == 0)  ||
		(debug &&
		       (monitorstart == -1 ||
			(offset + size > monitorstart &&
			(monitorend == -1 || offset <= monitorend))))))
		prt("%lu read\t0x%x thru\t0x%x\t(0x%x bytes)\n", testcalls,
		    offset, offset + size - 1, size);

	ret = ops->read(&ctx, offset, size, temp_buf);
	if (ret != (int)size) {
		if (ret < 0)
			prterrcode("doread: ops->read", ret);
		else
			prt("short read: 0x%x bytes instead of 0x%x\n",
			    ret, size);
		report_failure(141);
	}

	check_buffers(good_buf, temp_buf, offset, size);
}


void
check_eofpage(char *s, unsigned offset, char *p, int size)
{
	unsigned long last_page, should_be_zero;

	if (offset + size <= (file_size & ~page_mask))
		return;
	/*
	 * we landed in the last page of the file
	 * test to make sure the VM system provided 0's 
	 * beyond the true end of the file mapping
	 * (as required by mmap def in 1996 posix 1003.1)
	 */
	last_page = ((unsigned long)p + (offset & page_mask) + size) & ~page_mask;

	for (should_be_zero = last_page + (file_size & page_mask);
	     should_be_zero < last_page + page_size;
	     should_be_zero++)
		if (*(char *)should_be_zero) {
			prt("Mapped %s: non-zero data past EOF (0x%llx) page offset 0x%x is 0x%04x\n",
			    s, file_size - 1, should_be_zero & page_mask,
			    short_at(should_be_zero));
			report_failure(205);
		}
}


void
gendata(char *original_buf, char *good_buf, unsigned offset, unsigned size)
{
	while (size--) {
		good_buf[offset] = testcalls % 256; 
		if (offset % 2)
			good_buf[offset] += original_buf[offset];
		offset++;
	}
}


void
dowrite(unsigned offset, unsigned size)
{
	ssize_t ret;
	off_t newsize;

	offset -= offset % writebdy;
	if (o_direct)
		size -= size % writebdy;
	if (size == 0) {
		if (!quiet && testcalls > simulatedopcount && !o_direct)
			prt("skipping zero size write\n");
		log4(OP_SKIPPED, OP_WRITE, offset, size);
		return;
	}

	log4(OP_WRITE, offset, size, file_size);

	gendata(original_buf, good_buf, offset, size);
	if (file_size < offset + size) {
		newsize = ceil(((double)offset + size) / truncbdy) * truncbdy;
		if (file_size < newsize)
			memset(good_buf + file_size, '\0', newsize - file_size);
		file_size = newsize;
		if (lite) {
			warn("Lite file size bug in fsx!");
			report_failure(149);
		}
		ret = ops->resize(&ctx, newsize);
		if (ret < 0) {
			prterrcode("dowrite: ops->resize", ret);
			report_failure(150);
		}
	}

	if (testcalls <= simulatedopcount)
		return;

	if (!quiet &&
		((progressinterval && testcalls % progressinterval == 0) ||
		       (debug &&
		       (monitorstart == -1 ||
			(offset + size > monitorstart &&
			 (monitorend == -1 || (long)offset <= monitorend))))))
		prt("%lu write\t0x%x thru\t0x%x\t(0x%x bytes)\n", testcalls,
		    offset, offset + size - 1, size);

	ret = ops->write(&ctx, offset, size, good_buf + offset);
	if (ret != (ssize_t)size) {
		if (ret < 0)
			prterrcode("dowrite: ops->write", ret);
		else
			prt("short write: 0x%x bytes instead of 0x%x\n",
			    ret, size);
		report_failure(151);
	}

	if (flush_enabled)
		doflush(offset, size);
}


void
dotruncate(unsigned size)
{
	int oldsize = file_size;
	int ret;

	size -= size % truncbdy;
	if (size > biggest) {
		biggest = size;
		if (!quiet && testcalls > simulatedopcount)
			prt("truncating to largest ever: 0x%x\n", size);
	}

	log4(OP_TRUNCATE, size, (unsigned)file_size, 0);

	if (size > file_size)
		memset(good_buf + file_size, '\0', size - file_size);
	else if (size < file_size)
		memset(good_buf + size, '\0', file_size - size);
	file_size = size;

	if (testcalls <= simulatedopcount)
		return;

	if ((progressinterval && testcalls % progressinterval == 0) ||
	    (debug && (monitorstart == -1 || monitorend == -1 ||
		       (long)size <= monitorend)))
		prt("%lu trunc\tfrom 0x%x to 0x%x\n", testcalls, oldsize, size);

	ret = ops->resize(&ctx, size);
	if (ret < 0) {
		prterrcode("dotruncate: ops->resize", ret);
		report_failure(160);
	}
}

void
do_punch_hole(unsigned offset, unsigned length)
{
	unsigned end_offset;
	int max_offset = 0;
	int max_len = 0;
	int ret;

	offset -= offset % holebdy;
	length -= length % holebdy;
	if (length == 0) {
		if (!quiet && testcalls > simulatedopcount)
			prt("skipping zero length punch hole\n");
		log4(OP_SKIPPED, OP_PUNCH_HOLE, offset, length);
		return;
	}

	if (file_size <= (loff_t)offset) {
		if (!quiet && testcalls > simulatedopcount)
			prt("skipping hole punch off the end of the file\n");
		log4(OP_SKIPPED, OP_PUNCH_HOLE, offset, length);
		return;
	}

	end_offset = offset + length;

	log4(OP_PUNCH_HOLE, offset, length, 0);

	if (testcalls <= simulatedopcount)
		return;

	if ((progressinterval && testcalls % progressinterval == 0) ||
	    (debug && (monitorstart == -1 || monitorend == -1 ||
		       (long)end_offset <= monitorend))) {
		prt("%lu punch\tfrom 0x%x to 0x%x, (0x%x bytes)\n", testcalls,
			offset, offset+length, length);
	}

	ret = ops->discard(&ctx, (unsigned long long)offset,
			   (unsigned long long)length);
	if (ret < 0) {
		prterrcode("do_punch_hole: ops->discard", ret);
		report_failure(161);
	}

	max_offset = offset < file_size ? offset : file_size;
	max_len = max_offset + length <= file_size ? length :
			file_size - max_offset;
	memset(good_buf + max_offset, '\0', max_len);
}

void clone_filename(char *buf, size_t len, int clones)
{
	snprintf(buf, len, "%s/fsx-%s-parent%d",
		 dirpath, iname, clones);
}

void clone_imagename(char *buf, size_t len, int clones)
{
	if (clones > 0)
		snprintf(buf, len, "%s-clone%d", iname, clones);
	else
		strncpy(buf, iname, len);
        buf[len - 1] = '\0';
}

void replay_imagename(char *buf, size_t len, int clones)
{
        clone_imagename(buf, len, clones);
        strncat(buf, "-replay", len - strlen(buf));
        buf[len - 1] = '\0';
}

void check_clone(int clonenum, bool replay_image);

void
do_clone()
{
	char filename[1024];
	char imagename[1024];
	char lastimagename[1024];
	int ret, fd;
	int order = 0, stripe_unit = 0, stripe_count = 0;
	uint64_t newsize = file_size;

	log4(OP_CLONE, 0, 0, 0);
	++num_clones;

	if (randomize_striping) {
		order = 18 + get_random() % 8;
		stripe_unit = 1ull << (order - 1 - (get_random() % 8));
		stripe_count = 2 + get_random() % 14;
	}

	prt("%lu clone\t%d order %d su %d sc %d\n", testcalls, num_clones,
	    order, stripe_unit, stripe_count);

	clone_imagename(imagename, sizeof(imagename), num_clones);
	clone_imagename(lastimagename, sizeof(lastimagename),
			num_clones - 1);
	assert(strcmp(lastimagename, ctx.name) == 0);

	ret = ops->clone(&ctx, "snap", imagename, &order, stripe_unit,
			 stripe_count);
	if (ret < 0) {
		prterrcode("do_clone: ops->clone", ret);
		exit(165);
	}

	if (randomize_parent_overlap && rbd_image_has_parent(&ctx)) {
		int rand = get_random() % 16 + 1; // [1..16]

		if (rand < 13) {
			uint64_t overlap;

			ret = rbd_get_overlap(ctx.image, &overlap);
			if (ret < 0) {
				prterrcode("do_clone: rbd_get_overlap", ret);
				exit(1);
			}

			if (rand < 10) {	// 9/16
				newsize = overlap * ((double)rand / 10);
				newsize -= newsize % truncbdy;
			} else {		// 3/16
				newsize = 0;
			}

			assert(newsize != (uint64_t)file_size);
			prt("truncating image %s from 0x%llx (overlap 0x%llx) to 0x%llx\n",
			    ctx.name, file_size, overlap, newsize);

			ret = ops->resize(&ctx, newsize);
			if (ret < 0) {
				prterrcode("do_clone: ops->resize", ret);
				exit(1);
			}
		} else if (rand < 15) {		// 2/16
			prt("flattening image %s\n", ctx.name);

			ret = ops->flatten(&ctx);
			if (ret < 0) {
				prterrcode("do_clone: ops->flatten", ret);
				exit(1);
			}
		} else {			// 2/16
			prt("leaving image %s intact\n", ctx.name);
		}
	}

	clone_filename(filename, sizeof(filename), num_clones);
	if ((fd = open(filename, O_WRONLY|O_CREAT|O_TRUNC, 0666)) < 0) {
		simple_err("do_clone: open", -errno);
		exit(162);
	}
	save_buffer(good_buf, newsize, fd);
	if ((ret = close(fd)) < 0) {
		simple_err("do_clone: close", -errno);
		exit(163);
	}

	/*
	 * Close parent.
	 */
	if ((ret = ops->close(&ctx)) < 0) {
		prterrcode("do_clone: ops->close", ret);
		exit(174);
	}

        if (journal_replay) {
                ret = finalize_journal(ioctx, lastimagename, num_clones - 1,
                                       order, stripe_unit, stripe_count);
                if (ret < 0) {
                        exit(EXIT_FAILURE);
                }

                ret = register_journal(ioctx, imagename);
                if (ret < 0) {
                        exit(EXIT_FAILURE);
                }
        }

	/*
	 * Open freshly made clone.
	 */
	if ((ret = ops->open(imagename, &ctx)) < 0) {
		prterrcode("do_clone: ops->open", ret);
		exit(166);
	}

	if (num_clones > 1) {
                if (journal_replay) {
		        check_clone(num_clones - 2, true);
                }
		check_clone(num_clones - 2, false);
        }
}

void
check_clone(int clonenum, bool replay_image)
{
	char filename[128];
	char imagename[128];
	int ret, fd;
	struct rbd_ctx cur_ctx = RBD_CTX_INIT;
	struct stat file_info;
	char *good_buf, *temp_buf;

        if (replay_image) {
                replay_imagename(imagename, sizeof(imagename), clonenum);
        } else {
        	clone_imagename(imagename, sizeof(imagename), clonenum);
        }

	if ((ret = ops->open(imagename, &cur_ctx)) < 0) {
		prterrcode("check_clone: ops->open", ret);
		exit(167);
	}

	clone_filename(filename, sizeof(filename), clonenum + 1);
	if ((fd = open(filename, O_RDONLY)) < 0) {
		simple_err("check_clone: open", -errno);
		exit(168);
	}

	prt("checking clone #%d, image %s against file %s\n",
	    clonenum, imagename, filename);
	if ((ret = fstat(fd, &file_info)) < 0) {
		simple_err("check_clone: fstat", -errno);
		exit(169);
	}

	good_buf = NULL;
	ret = posix_memalign((void **)&good_buf,
			     MAX(writebdy, (int)sizeof(void *)),
			     file_info.st_size);
	if (ret > 0) {
		prterrcode("check_clone: posix_memalign(good_buf)", -ret);
		exit(96);
	}

	temp_buf = NULL;
	ret = posix_memalign((void **)&temp_buf,
			     MAX(readbdy, (int)sizeof(void *)),
			     file_info.st_size);
	if (ret > 0) {
		prterrcode("check_clone: posix_memalign(temp_buf)", -ret);
		exit(97);
	}

	if ((ret = pread(fd, good_buf, file_info.st_size, 0)) < 0) {
		simple_err("check_clone: pread", -errno);
		exit(170);
	}
	if ((ret = ops->read(&cur_ctx, 0, file_info.st_size, temp_buf)) < 0) {
		prterrcode("check_clone: ops->read", ret);
		exit(171);
	}
	close(fd);
	if ((ret = ops->close(&cur_ctx)) < 0) {
		prterrcode("check_clone: ops->close", ret);
		exit(174);
	}
	check_buffers(good_buf, temp_buf, 0, file_info.st_size);

        if (!replay_image) {
	        unlink(filename);
        }

	free(good_buf);
	free(temp_buf);
}

void
writefileimage()
{
	ssize_t ret;

	ret = ops->write(&ctx, 0, file_size, good_buf);
	if (ret != file_size) {
		if (ret < 0)
			prterrcode("writefileimage: ops->write", ret);
		else
			prt("short write: 0x%x bytes instead of 0x%llx\n",
			    ret, (unsigned long long)file_size);
		report_failure(172);
	}

	if (!lite) {
		ret = ops->resize(&ctx, file_size);
		if (ret < 0) {
			prterrcode("writefileimage: ops->resize", ret);
			report_failure(173);
		}
	}
}

void
do_flatten()
{
	int ret;

	if (!rbd_image_has_parent(&ctx)) {
		log4(OP_SKIPPED, OP_FLATTEN, 0, 0);
		return;
	}
	log4(OP_FLATTEN, 0, 0, 0);
	prt("%lu flatten\n", testcalls);

	ret = ops->flatten(&ctx);
	if (ret < 0) {
		prterrcode("writefileimage: ops->flatten", ret);
		exit(177);
	}
}

void
docloseopen(void)
{
	char *name;
	int ret;

	if (testcalls <= simulatedopcount)
		return;

	name = strdup(ctx.name);

	if (debug)
		prt("%lu close/open\n", testcalls);

	ret = ops->close(&ctx);
	if (ret < 0) {
		prterrcode("docloseopen: ops->close", ret);
		report_failure(180);
	}

	ret = ops->open(name, &ctx);
	if (ret < 0) {
		prterrcode("docloseopen: ops->open", ret);
		report_failure(181);
	}

	free(name);
}

#define TRIM_OFF_LEN(off, len, size)	\
do {					\
	if (size)			\
		(off) %= (size);	\
	else				\
		(off) = 0;		\
	if ((unsigned)(off) + (unsigned)(len) > (unsigned)(size))	\
		(len) = (size) - (off);	\
} while (0)

void
test(void)
{
	unsigned long	offset;
	unsigned long	size = maxoplen;
	unsigned long	rv = get_random();
	unsigned long	op;

	if (simulatedopcount > 0 && testcalls == simulatedopcount)
		writefileimage();

	testcalls++;

	if (closeprob)
		closeopen = (rv >> 3) < (1u << 28) / (unsigned)closeprob;

	if (debugstart > 0 && testcalls >= debugstart)
		debug = 1;

	if (!quiet && testcalls < simulatedopcount && testcalls % 100000 == 0)
		prt("%lu...\n", testcalls);

	offset = get_random();
	if (randomoplen)
		size = get_random() % (maxoplen + 1);

	/* calculate appropriate op to run */
	if (lite)
		op = rv % OP_MAX_LITE;
	else
		op = rv % OP_MAX_FULL;

	switch (op) {
	case OP_MAPREAD:
		if (!mapped_reads)
			op = OP_READ;
		break;
	case OP_MAPWRITE:
		if (!mapped_writes)
			op = OP_WRITE;
		break;
	case OP_FALLOCATE:
		if (!fallocate_calls) {
			log4(OP_SKIPPED, OP_FALLOCATE, offset, size);
			goto out;
		}
		break;
	case OP_PUNCH_HOLE:
		if (!punch_hole_calls) {
			log4(OP_SKIPPED, OP_PUNCH_HOLE, offset, size);
			goto out;
		}
		break;
	case OP_CLONE:
		/* clone, 8% chance */
		if (!clone_calls || file_size == 0 || get_random() % 100 >= 8) {
			log4(OP_SKIPPED, OP_CLONE, 0, 0);
			goto out;
		}
		break;
	case OP_FLATTEN:
		/* flatten four times as rarely as clone, 2% chance */
		if (get_random() % 100 >= 2) {
			log4(OP_SKIPPED, OP_FLATTEN, 0, 0);
			goto out;
		}
		break;
	}

	switch (op) {
	case OP_READ:
		TRIM_OFF_LEN(offset, size, file_size);
		doread(offset, size);
		break;

	case OP_WRITE:
		TRIM_OFF_LEN(offset, size, maxfilelen);
		dowrite(offset, size);
		break;

	case OP_MAPREAD:
		TRIM_OFF_LEN(offset, size, file_size);
		exit(183);
		break;

	case OP_MAPWRITE:
		TRIM_OFF_LEN(offset, size, maxfilelen);
		exit(182);
		break;

	case OP_TRUNCATE:
		if (!style)
			size = get_random() % maxfilelen;
		dotruncate(size);
		break;

	case OP_PUNCH_HOLE:
		TRIM_OFF_LEN(offset, size, file_size);
		do_punch_hole(offset, size);
		break;

	case OP_CLONE:
		do_clone();
		break;

	case OP_FLATTEN:
		do_flatten();
		break;

	default:
		prterr("test: unknown operation");
		report_failure(42);
		break;
	}

out:
	if (sizechecks && testcalls > simulatedopcount)
		check_size();
	if (closeopen)
		docloseopen();
}


void
cleanup(int sig)
{
	if (sig)
		prt("signal %d\n", sig);
	prt("testcalls = %lu\n", testcalls);
	exit(sig);
}


void
usage(void)
{
	fprintf(stdout, "usage: %s",
		"fsx [-dfjknqxyACFHKLORUWZ] [-b opnum] [-c Prob] [-h holebdy] [-l flen] [-m start:end] [-o oplen] [-p progressinterval] [-r readbdy] [-s style] [-t truncbdy] [-w writebdy] [-D startingop] [-N numops] [-P dirpath] [-S seed] pname iname\n\
	-b opnum: beginning operation number (default 1)\n\
	-c P: 1 in P chance of file close+open at each op (default infinity)\n\
	-d: debug output for all operations\n\
	-f: flush and invalidate cache after I/O\n\
	-h holebdy: 4096 would make discards page aligned (default 1)\n\
	-j: journal replay stress test\n\
	-k: keep data on success (default 0)\n\
	-l flen: the upper bound on file size (default 262144)\n\
	-m startop:endop: monitor (print debug output) specified byte range (default 0:infinity)\n\
	-n: no verifications of file size\n\
	-o oplen: the upper bound on operation size (default 65536)\n\
	-p progressinterval: debug output at specified operation interval\n\
	-q: quieter operation\n\
	-r readbdy: 4096 would make reads page aligned (default 1)\n\
	-s style: 1 gives smaller truncates (default 0)\n\
	-t truncbdy: 4096 would make truncates page aligned (default 1)\n\
	-w writebdy: 4096 would make writes page aligned (default 1)\n\
	-x: preallocate file space before starting, XFS only (default 0)\n\
	-y: synchronize changes to a file\n"

#ifdef AIO
"	-A: Use the AIO system calls\n"
#endif
"	-C: do not use clone calls\n\
	-D startingop: debug output starting at specified operation\n"
#ifdef FALLOCATE
"	-F: Do not use fallocate (preallocation) calls\n"
#endif
"	-H: do not use punch hole calls\n\
	-K: enable krbd mode (use -t and -h too)\n\
	-M: enable rbd-nbd mode (use -t and -h too)\n\
	-L: fsxLite - no file creations & no file size changes\n\
	-N numops: total # operations to do (default infinity)\n\
	-O: use oplen (see -o flag) for every op (default random)\n\
	-P dirpath: save .fsxlog and .fsxgood files in dirpath (default ./)\n\
	-R: read() system calls only (mapped reads disabled)\n\
	-S seed: for random # generator (default 1) 0 gets timestamp\n\
	-U: disable randomized striping\n\
	-W: mapped write operations DISabled\n\
	-Z: O_DIRECT (use -R, -W, -r and -w too)\n\
	poolname: this is REQUIRED (no default)\n\
	imagename: this is REQUIRED (no default)\n");
	exit(89);
}


int
getnum(char *s, char **e)
{
	int ret;

	*e = (char *) 0;
	ret = strtol(s, e, 0);
	if (*e)
		switch (**e) {
		case 'b':
		case 'B':
			ret *= 512;
			*e = *e + 1;
			break;
		case 'k':
		case 'K':
			ret *= 1024;
			*e = *e + 1;
			break;
		case 'm':
		case 'M':
			ret *= 1024*1024;
			*e = *e + 1;
			break;
		case 'w':
		case 'W':
			ret *= 4;
			*e = *e + 1;
			break;
		}
	return (ret);
}

#ifdef AIO

#define QSZ     1024
io_context_t	io_ctx;
struct iocb 	iocb;

int aio_setup()
{
	int ret;
	ret = io_queue_init(QSZ, &io_ctx);
	if (ret != 0) {
		fprintf(stderr, "aio_setup: io_queue_init failed: %s\n",
                        strerror(ret));
                return(-1);
        }
        return(0);
}

int
__aio_rw(int rw, int fd, char *buf, unsigned len, unsigned offset)
{
	struct io_event event;
	static struct timespec ts;
	struct iocb *iocbs[] = { &iocb };
	int ret;
	long res;

	if (rw == READ) {
		io_prep_pread(&iocb, fd, buf, len, offset);
	} else {
		io_prep_pwrite(&iocb, fd, buf, len, offset);
	}

	ts.tv_sec = 30;
	ts.tv_nsec = 0;
	ret = io_submit(io_ctx, 1, iocbs);
	if (ret != 1) {
		fprintf(stderr, "errcode=%d\n", ret);
		fprintf(stderr, "aio_rw: io_submit failed: %s\n",
				strerror(ret));
		goto out_error;
	}

	ret = io_getevents(io_ctx, 1, 1, &event, &ts);
	if (ret != 1) {
		if (ret == 0)
			fprintf(stderr, "aio_rw: no events available\n");
		else {
			fprintf(stderr, "errcode=%d\n", -ret);
			fprintf(stderr, "aio_rw: io_getevents failed: %s\n",
				 	strerror(-ret));
		}
		goto out_error;
	}
	if (len != event.res) {
		/*
		 * The b0rked libaio defines event.res as unsigned.
		 * However the kernel strucuture has it signed,
		 * and it's used to pass negated error value.
		 * Till the library is fixed use the temp var.
		 */
		res = (long)event.res;
		if (res >= 0)
			fprintf(stderr, "bad io length: %lu instead of %u\n",
					res, len);
		else {
			fprintf(stderr, "errcode=%ld\n", -res);
			fprintf(stderr, "aio_rw: async io failed: %s\n",
					strerror(-res));
			ret = res;
			goto out_error;
		}

	}
	return event.res;

out_error:
	/*
	 * The caller expects error return in traditional libc
	 * convention, i.e. -1 and the errno set to error.
	 */
	errno = -ret;
	return -1;
}

int aio_rw(int rw, int fd, char *buf, unsigned len, unsigned offset)
{
	int ret;

	if (aio) {
		ret = __aio_rw(rw, fd, buf, len, offset);
	} else {
		if (rw == READ)
			ret = read(fd, buf, len);
		else
			ret = write(fd, buf, len);
	}
	return ret;
}

#endif

void
test_fallocate()
{
#ifdef FALLOCATE
	if (!lite && fallocate_calls) {
		if (fallocate(fd, 0, 0, 1) && errno == EOPNOTSUPP) {
			if(!quiet)
				warn("main: filesystem does not support fallocate, disabling\n");
			fallocate_calls = 0;
		} else {
			ftruncate(fd, 0);
		}
	}
#else /* ! FALLOCATE */
	fallocate_calls = 0;
#endif

}

void remove_image(rados_ioctx_t ioctx, char *imagename, bool remove_snap,
                  bool unregister) {
	rbd_image_t image;
	char errmsg[128];
        int ret;

	if ((ret = rbd_open(ioctx, imagename, &image, NULL)) < 0) {
		sprintf(errmsg, "rbd_open %s", imagename);
		prterrcode(errmsg, ret);
		report_failure(101);
	}
	if (remove_snap) {
		if ((ret = rbd_snap_unprotect(image, "snap")) < 0) {
			sprintf(errmsg, "rbd_snap_unprotect %s@snap",
				imagename);
			prterrcode(errmsg, ret);
			report_failure(102);
		}
		if ((ret = rbd_snap_remove(image, "snap")) < 0) {
			sprintf(errmsg, "rbd_snap_remove %s@snap",
				imagename);
			prterrcode(errmsg, ret);
			report_failure(103);
		}
	}
	if ((ret = rbd_close(image)) < 0) {
		sprintf(errmsg, "rbd_close %s", imagename);
		prterrcode(errmsg, ret);
		report_failure(104);
	}

        if (unregister &&
            (ret = unregister_journal(ioctx, imagename)) < 0) {
                report_failure(105);
        }

	if ((ret = rbd_remove(ioctx, imagename)) < 0) {
		sprintf(errmsg, "rbd_remove %s", imagename);
		prterrcode(errmsg, ret);
		report_failure(106);
	}
}

int
main(int argc, char **argv)
{
	int	i, style, ch, ret;
	char	*endp;
	char goodfile[1024];
	char logfile[1024];

	goodfile[0] = 0;
	logfile[0] = 0;

	page_size = getpagesize();
	page_mask = page_size - 1;
	mmap_mask = page_mask;

	setvbuf(stdout, (char *)0, _IOLBF, 0); /* line buffered stdout */

	while ((ch = getopt(argc, argv, "b:c:dfh:jkl:m:no:p:qr:s:t:w:xyACD:FHKMLN:OP:RS:UWZ"))
	       != EOF)
		switch (ch) {
		case 'b':
			simulatedopcount = getnum(optarg, &endp);
			if (!quiet)
				fprintf(stdout, "Will begin at operation %lu\n",
					simulatedopcount);
			if (simulatedopcount == 0)
				usage();
			simulatedopcount -= 1;
			break;
		case 'c':
			closeprob = getnum(optarg, &endp);
			if (!quiet)
				fprintf(stdout,
					"Chance of close/open is 1 in %d\n",
					closeprob);
			if (closeprob <= 0)
				usage();
			break;
		case 'd':
			debug = 1;
			break;
		case 'f':
			flush_enabled = 1;
			break;
		case 'h':
			holebdy = getnum(optarg, &endp);
			if (holebdy <= 0)
				usage();
			break;
                case 'j':
                        journal_replay = true;
                        break;
		case 'k':
			keep_on_success = 1;
			break;
		case 'l':
			{
				int _num = getnum(optarg, &endp);
				if (_num <= 0)
					usage();
				maxfilelen = _num;
			}
			break;
		case 'm':
			monitorstart = getnum(optarg, &endp);
			if (monitorstart < 0)
				usage();
			if (!endp || *endp++ != ':')
				usage();
			monitorend = getnum(endp, &endp);
			if (monitorend < 0)
				usage();
			if (monitorend == 0)
				monitorend = -1; /* aka infinity */
			debug = 1;
			break;
		case 'n':
			sizechecks = 0;
			break;
		case 'o':
			maxoplen = getnum(optarg, &endp);
			if (maxoplen <= 0)
				usage();
			break;
		case 'p':
			progressinterval = getnum(optarg, &endp);
			if (progressinterval == 0)
				usage();
			break;
		case 'q':
			quiet = 1;
			break;
		case 'r':
			readbdy = getnum(optarg, &endp);
			if (readbdy <= 0)
				usage();
			break;
		case 's':
			style = getnum(optarg, &endp);
			if (style < 0 || style > 1)
				usage();
			break;
		case 't':
			truncbdy = getnum(optarg, &endp);
			if (truncbdy <= 0)
				usage();
			break;
		case 'w':
			writebdy = getnum(optarg, &endp);
			if (writebdy <= 0)
				usage();
			break;
		case 'x':
			prealloc = 1;
			break;
		case 'y':
			do_fsync = 1;
			break;
		case 'A':
		        aio = 1;
			break;
		case 'C':
			clone_calls = 0;
			break;
		case 'D':
			debugstart = getnum(optarg, &endp);
			if (debugstart < 1)
				usage();
			break;
		case 'F':
			fallocate_calls = 0;
			break;
		case 'H':
			punch_hole_calls = 0;
			break;
		case 'K':
			prt("krbd mode enabled\n");
			ops = &krbd_operations;
			break;
		case 'M':
			prt("rbd-nbd mode enabled\n");
			ops = &nbd_operations;
			break;
		case 'L':
			prt("lite mode not supported for rbd\n");
			exit(1);
			break;
		case 'N':
			numops = getnum(optarg, &endp);
			if (numops < 0)
				usage();
			break;
		case 'O':
			randomoplen = 0;
			break;
		case 'P':
			strncpy(dirpath, optarg, sizeof(dirpath)-1);
			dirpath[sizeof(dirpath)-1] = '\0';
			strncpy(goodfile, dirpath, sizeof(goodfile)-1);
			goodfile[sizeof(goodfile)-1] = '\0';
			if (strlen(goodfile) < sizeof(goodfile)-2) {
				strcat(goodfile, "/");
			} else {
				prt("file name to long\n");
				exit(1);
			}
			strncpy(logfile, dirpath, sizeof(logfile)-1);
			logfile[sizeof(logfile)-1] = '\0';
			if (strlen(logfile) < sizeof(logfile)-2) {
				strcat(logfile, "/");
			} else {
				prt("file path to long\n");
				exit(1);
			}
			break;
                case 'R':
                        mapped_reads = 0;
			if (!quiet)
				fprintf(stdout, "mapped reads DISABLED\n");
                        break;
		case 'S':
                        seed = getnum(optarg, &endp);
			if (seed == 0)
				seed = time(0) % 10000;
			if (!quiet)
				fprintf(stdout, "Seed set to %d\n", seed);
			if (seed < 0)
				usage();
			break;
		case 'U':
			randomize_striping = 0;
			break;
		case 'W':
		        mapped_writes = 0;
			if (!quiet)
				fprintf(stdout, "mapped writes DISABLED\n");
			break;
		case 'Z':
			o_direct = O_DIRECT;
			break;
		default:
			usage();
			/* NOTREACHED */
		}
	argc -= optind;
	argv += optind;
	if (argc != 2)
		usage();
	pool = argv[0];
	iname = argv[1];

	signal(SIGHUP,	cleanup);
	signal(SIGINT,	cleanup);
	signal(SIGPIPE,	cleanup);
	signal(SIGALRM,	cleanup);
	signal(SIGTERM,	cleanup);
	signal(SIGXCPU,	cleanup);
	signal(SIGXFSZ,	cleanup);
	signal(SIGVTALRM,	cleanup);
	signal(SIGUSR1,	cleanup);
	signal(SIGUSR2,	cleanup);

	random_generator.seed(seed);

	ret = create_image();
	if (ret < 0) {
		prterrcode(iname, ret);
		exit(90);
	}
	ret = ops->open(iname, &ctx);
	if (ret < 0) {
		simple_err("Error opening image", ret);
		exit(91);
	}
	if (!dirpath[0])
		strcat(dirpath, ".");
	strncat(goodfile, iname, 256);
	strcat (goodfile, ".fsxgood");
	fsxgoodfd = open(goodfile, O_RDWR|O_CREAT|O_TRUNC, 0666);
	if (fsxgoodfd < 0) {
		prterr(goodfile);
		exit(92);
	}
	strncat(logfile, iname, 256);
	strcat (logfile, ".fsxlog");
	fsxlogf = fopen(logfile, "w");
	if (fsxlogf == NULL) {
		prterr(logfile);
		exit(93);
	}

#ifdef AIO
	if (aio) 
		aio_setup();
#endif

	original_buf = (char *) malloc(maxfilelen);
	for (i = 0; i < (int)maxfilelen; i++)
		original_buf[i] = get_random() % 256;

	ret = posix_memalign((void **)&good_buf,
			     MAX(writebdy, (int)sizeof(void *)), maxfilelen);
	if (ret > 0) {
		if (ret == EINVAL)
			prt("writebdy is not a suitable power of two\n");
		else
			prterrcode("main: posix_memalign(good_buf)", -ret);
		exit(94);
	}
	memset(good_buf, '\0', maxfilelen);

	ret = posix_memalign((void **)&temp_buf,
			     MAX(readbdy, (int)sizeof(void *)), maxfilelen);
	if (ret > 0) {
		if (ret == EINVAL)
			prt("readbdy is not a suitable power of two\n");
		else
			prterrcode("main: posix_memalign(temp_buf)", -ret);
		exit(95);
	}
	memset(temp_buf, '\0', maxfilelen);

	if (lite) {	/* zero entire existing file */
		ssize_t written;

		written = ops->write(&ctx, 0, (size_t)maxfilelen, good_buf);
		if (written != (ssize_t)maxfilelen) {
			if (written < 0) {
				prterrcode(iname, written);
				warn("main: error on write");
			} else
				warn("main: short write, 0x%x bytes instead "
					"of 0x%lx\n",
					(unsigned)written,
					maxfilelen);
			exit(98);
		}
	} else
		check_trunc_hack();

	//test_fallocate();

	while (numops == -1 || numops--)
		test();

	ret = ops->close(&ctx);
	if (ret < 0) {
		prterrcode("ops->close", ret);
		report_failure(99);
	}

        if (journal_replay) {
                char imagename[1024];
	        clone_imagename(imagename, sizeof(imagename), num_clones);
                ret = finalize_journal(ioctx, imagename, num_clones, 0, 0, 0);
                if (ret < 0) {
                        report_failure(100);
                }
        }

	if (num_clones > 0) {
                if (journal_replay) {
		        check_clone(num_clones - 1, true);
                }
		check_clone(num_clones - 1, false);
        }

	if (!keep_on_success) {
		while (num_clones >= 0) {
			static bool remove_snap = false;

			if (journal_replay) {
				char replayimagename[1024];
				replay_imagename(replayimagename,
						 sizeof(replayimagename),
						 num_clones);
				remove_image(ioctx, replayimagename,
					     remove_snap,
					     false);
			}

			char clonename[128];
			clone_imagename(clonename, 128, num_clones);
			remove_image(ioctx, clonename, remove_snap,
				     journal_replay);

			remove_snap = true;
			num_clones--;
		}
	}

	prt("All operations completed A-OK!\n");
	fclose(fsxlogf);

	rados_ioctx_destroy(ioctx);
	krbd_destroy(krbd);
	rados_shutdown(cluster);

	free(original_buf);
	free(good_buf);
	free(temp_buf);

	exit(0);
	return 0;
}
