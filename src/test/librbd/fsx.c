// -*- mode:C; tab-width:8; c-basic-offset:8; indent-tabs-mode:t -*- 
/*
 *	Copyright (C) 1991, NeXT Computer, Inc.  All Rights Reserverd.
 *
 *	File:	fsx.c
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
#ifdef HAVE_ERR_H
#include <err.h>
#endif
#include <signal.h>
#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <errno.h>
#include <math.h>

#include "include/rados/librados.h"
#include "include/rbd/librbd.h"

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
char	*pool;				/* name of the pool our test image is in */
char	*iname;				/* name of our test image */
rados_t cluster;                        /* handle for our test cluster */
rados_ioctx_t	ioctx;			/* handle for our test pool */
rbd_image_t	image;			/* handle for our test image */

char	dirpath[1024];

off_t		file_size = 0;
off_t		biggest = 0;
char		state[256];
unsigned long	testcalls = 0;		/* calls to function "test" */

unsigned long	simulatedopcount = 0;	/* -b flag */
int	closeprob = 0;			/* -c flag */
int	debug = 0;			/* -d flag */
unsigned long	debugstart = 0;		/* -D flag */
int	flush = 0;			/* -f flag */
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
int     randomize_striping = 1;
int 	mapped_reads = 0;		/* -R flag disables it */
int	fsxgoodfd = 0;
int	o_direct;			/* -Z */
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

static void *round_ptr_up(void *ptr, unsigned long align, unsigned long offset)
{
	unsigned long ret = (unsigned long)ptr;

	ret = ((ret + align - 1) & ~(align - 1));
	ret += offset;
	return (void *)ret;
}

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
prt(char *fmt, ...)
{
	va_list args;
	char buffer[BUF_SIZE];

	va_start(args, fmt);
	vsnprintf(buffer, BUF_SIZE, fmt, args);
	va_end(args);
	fprintf(stdout, buffer);
	if (fsxlogf)
		fprintf(fsxlogf, buffer);
}

void
prterr(char *prefix)
{
	prt("%s%s%s\n", prefix, prefix ? ": " : "", strerror(errno));
}

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
simple_err(const char *msg, int err)
{
    fprintf(stderr, "%s: %s\n", msg, strerror(-err));
}

void
logdump(void)
{
	int	i, count, down;
	struct log_entry	*lp;
	char *falloc_type[3] = {"PAST_EOF", "EXTENDING", "INTERIOR"};

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
prterrcode(char *prefix, int code)
{
	prt("%s%s%s\n", prefix, prefix ? ": " : "", strerror(-code));
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
		unsigned char c, t;
		unsigned i = 0;
		unsigned n = 0;
		unsigned op = 0;
		unsigned bad = 0;

		prt("READ BAD DATA: offset = 0x%x, size = 0x%x, fname = %s\n",
		    offset, size, iname);
		prt("OFFSET\tGOOD\tBAD\tRANGE\n");
		while (size > 0) {
			c = good_buf[offset];
			t = temp_buf[i];
			if (c != t) {
			        if (n < 16) {
					bad = short_at(&temp_buf[i]);
				        prt("0x%5x\t0x%04x\t0x%04x", offset,
				            short_at(&good_buf[offset]), bad);
					op = temp_buf[offset & 1 ? i+1 : i];
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
	rbd_image_info_t statbuf;
	int ret;

	if ((ret = rbd_stat(image, &statbuf, sizeof(statbuf))) < 0) {
		prterrcode("check_size: fstat", ret);
	}
	if ((uint64_t)file_size != statbuf.size) {
		prt("Size error: expected 0x%llx stat 0x%llx\n",
		    (unsigned long long)file_size,
		    (unsigned long long)statbuf.size);
		report_failure(120);
	}
}


void
check_trunc_hack(void)
{
	rbd_image_info_t statbuf;

	rbd_resize(image, (off_t)0);
	rbd_resize(image, (off_t)100000);
	rbd_stat(image, &statbuf, sizeof(statbuf));
	if (statbuf.size != (off_t)100000) {
 		prt("no extend on truncate! not posix!\n");
 		exit(130);
 	}
	rbd_resize(image, (off_t)0);
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
	r = rados_pool_create(cluster, pool);
	if (r < 0 && r != -EEXIST) {
		simple_err("Error creating pool", r);
		goto failed_shutdown;
	}
	r = rados_ioctx_create(cluster, pool, &ioctx);
	if (r < 0) {
		simple_err("Error creating ioctx", r);
		goto failed_shutdown;
	}
	if (clone_calls) {
		r = rbd_create2(ioctx, iname, 0, RBD_FEATURE_LAYERING, &order);
	} else {
		r = rbd_create(ioctx, iname, 0, &order);
	}
	if (r < 0) {
		simple_err("Error creating image", r);
		goto failed_open;
	}

	return 0;

 failed_open:
	rados_ioctx_destroy(ioctx);
 failed_shutdown:
	rados_shutdown(cluster);
	return r;
}

void
doflush(unsigned offset, unsigned size)
{
	if (o_direct == O_DIRECT)
		return;

	rbd_flush(image);
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
	ret = rbd_read(image, offset, size, temp_buf);
	if (ret != (int)size) {
		if (ret < 0)
			prterrcode("doread: read", ret);
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
		ret = rbd_resize(image, newsize);
		if (ret < 0) {
			prterrcode("dowrite: resize", ret);
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
			(monitorend == -1 || offset <= monitorend))))))
		prt("%lu write\t0x%x thru\t0x%x\t(0x%x bytes)\n", testcalls,
		    offset, offset + size - 1, size);

	ret = rbd_write(image, offset, size, good_buf + offset);
	if (ret != size) {
		if (ret < 0)
			prterrcode("dowrite: rbd_write", ret);
		else
			prt("short write: 0x%x bytes instead of 0x%x\n",
			    ret, size);
		report_failure(151);
	}
	if (flush) {
		doflush(offset, size);
	}
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
		      size <= monitorend)))
		prt("%lu trunc\tfrom 0x%x to 0x%x\n", testcalls, oldsize, size);
	if ((ret = rbd_resize(image, size)) < 0) {
		prt("rbd_resize: %x\n", size);
		prterrcode("dotruncate: ftruncate", ret);
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
		      end_offset <= monitorend))) {
		prt("%lu punch\tfrom 0x%x to 0x%x, (0x%x bytes)\n", testcalls,
			offset, offset+length, length);
	}
	if ((ret = rbd_discard(image, (unsigned long long) offset,
			       (unsigned long long) length)) < 0) {
		prt("%punch hole: %x to %x\n", offset, length);
		prterrcode("do_punch_hole: discard", ret);
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
}

void check_clone(int clonenum);

void
do_clone()
{
	char filename[1024];
	char imagename[1024];
	char lastimagename[1024];
	int ret, fd;
	int order = 0, stripe_unit = 0, stripe_count = 0;

	if (randomize_striping) {
		order = 18 + rand() % 8;
		stripe_unit = 1ull << (order - 1 - (rand() % 8));
		stripe_count = 2 + rand() % 14;
	}

	log4(OP_CLONE, 0, 0, 0);
	++num_clones;
	prt("%lu clone\t%d order %d su %d sc %d\n", testcalls, num_clones, order, stripe_unit, stripe_count);

	clone_filename(filename, sizeof(filename), num_clones);
	if ((fd = open(filename, O_WRONLY|O_CREAT|O_TRUNC, 0666)) < 0) {
		simple_err("do_clone: open", -errno);
		exit(162);
	}
	save_buffer(good_buf, file_size, fd);
	if ((ret = close(fd)) < 0) {
		simple_err("do_clone: close", -errno);
		exit(163);
	}

	clone_imagename(imagename, sizeof(imagename), num_clones);
	clone_imagename(lastimagename, sizeof(lastimagename),
			num_clones - 1);

	if ((ret = rbd_snap_create(image, "snap")) < 0) {
		simple_err("do_clone: rbd create snap", ret);
		exit(164);
	}

	if ((ret = rbd_snap_protect(image, "snap")) < 0) {
		simple_err("do_clone: rbd protect snap", ret);
		exit(164);
	}

	ret = rbd_clone2(ioctx, lastimagename, "snap", ioctx, imagename,
			 RBD_FEATURES_ALL, &order, stripe_unit, stripe_count);
	if (ret < 0) {
		simple_err("do_clone: rbd clone", ret);
		exit(165);
	}

	if ((ret = rbd_close(image)) < 0) {
		simple_err("do_clone: rbd close", ret);
		exit(174);
	}

	if ((ret = rbd_open(ioctx, imagename, &image, NULL)) < 0) {
		simple_err("do_clone: rbd open", ret);
		exit(166);
	}

	if (num_clones > 1)
		check_clone(num_clones - 2);
}

void
check_clone(int clonenum)
{
	char filename[128];
	char imagename[128];
	int ret, fd;
	rbd_image_t cur_image;
	struct stat file_info;
	char *good_buf, *temp_buf;

	clone_imagename(imagename, sizeof(imagename), clonenum);
	if ((ret = rbd_open(ioctx, imagename, &cur_image, NULL)) < 0) {
		simple_err("check_clone: rbd open", ret);
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

	good_buf = malloc(file_info.st_size);
	temp_buf = malloc(file_info.st_size);

	if ((ret = pread(fd, good_buf, file_info.st_size, 0)) < 0) {
		simple_err("check_clone: pread", -errno);
		exit(170);
	}
	if ((ret = rbd_read(cur_image, 0, file_info.st_size, temp_buf)) < 0) {
		simple_err("check_clone: rbd_read", ret);
		exit(171);
	}
	close(fd);
	if ((ret = rbd_close(cur_image)) < 0) {
		simple_err("check_clone: rbd close", ret);
		exit(174);
	}
	check_buffers(good_buf, temp_buf, 0, file_info.st_size);

	unlink(filename);

	free(good_buf);
	free(temp_buf);
}

void
writefileimage()
{
	ssize_t ret;

	ret = rbd_write(image, 0, file_size, good_buf);
	if (ret != file_size) {
		if (ret < 0)
			prterrcode("writefileimage: write", ret);
		else
			prt("short write: 0x%x bytes instead of 0x%llx\n",
			    ret, (unsigned long long)file_size);
		report_failure(172);
	}
	if (lite ? 0 : (ret = rbd_resize(image, file_size)) < 0) {
		prt("rbd_resize: %llx\n", (unsigned long long)file_size);
		prterrcode("writefileimage: rbd_resize", ret);
		report_failure(173);
	}
}

void
do_flatten()
{
	int ret;

	if (num_clones == 0 ||
	    (rbd_get_parent_info(image, NULL, 0, NULL, 0, NULL, 0)
	    == -ENOENT)) {
		log4(OP_SKIPPED, OP_FLATTEN, 0, 0);
		return;
	}
	log4(OP_FLATTEN, 0, 0, 0);
	prt("%lu flatten\n", testcalls);

	if ((ret = rbd_flatten(image)) < 0) {
		simple_err("do_flatten: rbd flatten", ret);
		exit(177);
	}
}

void
docloseopen(void)
{
	int ret;

	if (testcalls <= simulatedopcount)
		return;

	if (debug)
		prt("%lu close/open\n", testcalls);
	if ((ret = rbd_close(image)) < 0) {
		prterrcode("docloseopen: close", ret);
		report_failure(180);
	}
	ret = rbd_open(ioctx, iname, &image, NULL);
	if (ret < 0) {
		prterrcode("docloseopen: open", ret);
		report_failure(181);
	}
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
	unsigned long	rv = random();
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

	offset = random();
	if (randomoplen)
		size = random() % (maxoplen + 1);

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
		if (!clone_calls || random() % 100 > 5 || file_size == 0) {
			log4(OP_SKIPPED, OP_CLONE, 0, 0);
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
			size = random() % maxfilelen;
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
cleanup(sig)
	int	sig;
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
		"fsx [-dnqxAFLOWZ] [-b opnum] [-c Prob] [-l flen] [-m start:end] [-o oplen] [-p progressinterval] [-r readbdy] [-s style] [-t truncbdy] [-w writebdy] [-D startingop] [-N numops] [-P dirpath] [-S seed] pname iname\n\
	-b opnum: beginning operation number (default 1)\n\
	-c P: 1 in P chance of file close+open at each op (default infinity)\n\
	-d: debug output for all operations\n\
	-f flush and invalidate cache after I/O\n\
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
	-y synchronize changes to a file\n"

#ifdef AIO
"	-A: Use the AIO system calls\n"
#endif
"	-D startingop: debug output starting at specified operation\n"
#ifdef FALLOCATE
"	-F: Do not use fallocate (preallocation) calls\n"
#endif
"        -H: Do not use punch hole calls\n"
"        -C: Do not use clone calls\n"
"	-L: fsxLite - no file creations & no file size changes\n\
	-N numops: total # operations to do (default infinity)\n\
	-O: use oplen (see -o flag) for every op (default random)\n\
	-P: save .fsxlog and .fsxgood files in dirpath (default ./)\n\
	-S seed: for random # generator (default 1) 0 gets timestamp\n\
	-W: mapped write operations DISabled\n\
        -R: read() system calls only (mapped reads disabled)\n\
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

	while ((ch = getopt(argc, argv, "b:c:dfl:m:no:p:qr:s:t:w:xyACD:FHLN:OP:RS:WZ"))
	       != EOF)
		switch (ch) {
		case 'b':
			simulatedopcount = getnum(optarg, &endp);
			if (!quiet)
				fprintf(stdout, "Will begin at operation %ld\n",
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
			flush = 1;
			break;
		case 'l':
			maxfilelen = getnum(optarg, &endp);
			if (maxfilelen == 0)
				usage();
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
			strncpy(dirpath, optarg, sizeof(dirpath));
			strncpy(goodfile, dirpath, sizeof(goodfile));
			strcat(goodfile, "/");
			strncpy(logfile, dirpath, sizeof(logfile));
			strcat(logfile, "/");
			break;
                case 'R':
                        mapped_reads = 0;
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

	initstate(seed, state, 256);
	setstate(state);

	ret = create_image();
	if (ret < 0) {
		prterrcode(iname, ret);
		exit(90);
	}
	ret = rbd_open(ioctx, iname, &image, NULL);
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
		original_buf[i] = random() % 256;
	good_buf = (char *) malloc(maxfilelen + writebdy);
	good_buf = round_ptr_up(good_buf, writebdy, 0);
	memset(good_buf, '\0', maxfilelen);
	temp_buf = (char *) malloc(maxfilelen + writebdy);
	temp_buf = round_ptr_up(temp_buf, writebdy, 0);
	memset(temp_buf, '\0', maxfilelen);
	if (lite) {	/* zero entire existing file */
		ssize_t written;

		written = rbd_write(image, 0, (size_t)maxfilelen, good_buf);
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

	if ((ret = rbd_close(image)) < 0) {
		prterrcode("rbd_close", ret);
		report_failure(99);
	}

	if (num_clones > 0)
		check_clone(num_clones - 1);

	while (num_clones >= 0) {
		static int first = 1;
		char clonename[128];
		char errmsg[128];
		clone_imagename(clonename, 128, num_clones);
		if ((ret = rbd_open(ioctx, clonename, &image, NULL)) < 0) {
			sprintf(errmsg, "rbd_open %s", clonename);
			prterrcode(errmsg, ret);
			report_failure(101);
		}
		if (!first) {
			if ((ret = rbd_snap_unprotect(image, "snap")) < 0) {
				sprintf(errmsg, "rbd_snap_unprotect %s@snap",
					clonename);
				prterrcode(errmsg, ret);
			report_failure(102);
			}
			if ((ret = rbd_snap_remove(image, "snap")) < 0) {
				sprintf(errmsg, "rbd_snap_remove %s@snap",
					clonename);
				prterrcode(errmsg, ret);
				report_failure(103);
			}
		}
		if ((ret = rbd_close(image)) < 0) {
			sprintf(errmsg, "rbd_close %s", clonename);
			prterrcode(errmsg, ret);
			report_failure(104);
		}

		if ((ret = rbd_remove(ioctx, clonename)) < 0) {
			sprintf(errmsg, "rbd_remove %s", clonename);
			prterrcode(errmsg, ret);
			report_failure(105);
		}

		first = 0;
		num_clones--;
	}

	rados_ioctx_destroy(ioctx);
	rados_shutdown(cluster);

	free(original_buf);
	free(good_buf);
	free(temp_buf);

	prt("All operations completed A-OK!\n");
	fclose(fsxlogf);

	exit(0);
	return 0;
}
