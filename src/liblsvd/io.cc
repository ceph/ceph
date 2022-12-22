/*
 * file:        io.cc
 * description: implementation of libaio-based async I/O
 * author:      Peter Desnoyers, Northeastern University
 * Copyright 2021, 2022 Peter Desnoyers
 * license:     GNU LGPL v2.1 or newer
 *              LGPL-2.1-or-later
 */

#include <libaio.h>
#include <sys/uio.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <linux/fs.h>

#include <shared_mutex>
#include <condition_variable>
#include <thread>

#include "lsvd_types.h"
#include "smartiov.h"
#include "extent.h"

#include <unistd.h>
#include "io.h"
#include "misc_cache.h"

#include <valgrind/drd.h>

size_t getsize64(int fd)
{
    struct stat sb;
    size_t size;
    
    if (fstat(fd, &sb) < 0)
	throw_fs_error("stat");
    if (S_ISBLK(sb.st_mode)) {
	if (ioctl(fd, BLKGETSIZE64, &size) < 0)
	    throw_fs_error("ioctl");
    }
    else
	size = sb.st_size;
    return size;
}

/* libaio helpers */

void e_iocb_cb(io_context_t ctx, iocb *io, long res, long res2)
{
    assert(res2 == 0);
    auto iocb = (e_iocb*)io;
    iocb->cb(iocb->ptr, res);
}

int io_queue_wait(io_context_t ctx, struct timespec *timeout)
{
    return io_getevents(ctx, 1, 10, NULL, timeout);
}

bool __lsvd_dbg_reverse = false;

/* https://lwn.net/Articles/39285/
 */
#define IO_BATCH_EVENTS 8               /* number of events to batch up */
int io_queue_run2(io_context_t ctx, struct timespec *timeout)
{
    struct io_event events[IO_BATCH_EVENTS];
    //struct io_event *ep;
    int ret = 0;                /* total number of events processed */
    int n;

    /*
     * Process io events and call the callbacks.
     * Try to batch the events up to IO_BATCH_EVENTS at a time.
     * Loop until we have read all the available events and called the callbacks.
     */
    do {
        int i;

        if ((n = io_getevents(ctx, 1, IO_BATCH_EVENTS, events, timeout)) < 0)
            break;
        ret += n;
        //for (ep = events, i = n; i-- > 0; ep++) {
	struct io_event *ep = events;
	if (__lsvd_dbg_reverse) 
	    for (i = n-1; i >= 0; i--) {
		io_callback_t cb = (io_callback_t)ep[i].data;
		struct iocb *iocb = ep[i].obj;
		ANNOTATE_HAPPENS_AFTER(iocb);
		cb(ctx, iocb, ep[i].res, ep[i].res2);
	    }
	else
	    for (i = 0; i < n; i++) {
		io_callback_t cb = (io_callback_t)ep[i].data;
		struct iocb *iocb = ep[i].obj;
		ANNOTATE_HAPPENS_AFTER(iocb);
		cb(ctx, iocb, ep[i].res, ep[i].res2);
	    }
    } while (n >= 0);

    return ret ? ret : n;               /* return number of events or error */
}

void e_iocb_runner(io_context_t ctx, bool *running, const char *name)
{
    struct timespec timeout = {0, 1000*100}; // 100 microseconds
    pthread_setname_np(pthread_self(), name);
    while (*running) 
	io_queue_run2(ctx, &timeout);
}

void e_io_prep_pwrite(e_iocb *io, int fd, void *buf, size_t len, size_t offset,
		      void (*cb)(void*,long), void *arg)
{
    io_prep_pwrite(&io->io, fd, buf, len, offset);
    io->cb = cb;
    io->ptr = arg;
    io_set_callback(&io->io, e_iocb_cb);
}

void e_io_prep_pread(e_iocb *io, int fd, void *buf, size_t len, size_t offset,
		     void (*cb)(void*,long), void *arg)
{
    io_prep_pread(&io->io, fd, buf, len, offset);
    io->cb = cb;
    io->ptr = arg;
    io_set_callback(&io->io, e_iocb_cb);
}

void e_io_prep_pwritev(e_iocb *io, int fd, const struct iovec *iov, int iovcnt,
		       size_t offset, void (*cb)(void*,long), void *arg)
{
    io_prep_pwritev(&io->io, fd, iov, iovcnt, offset);
    io->cb = cb;
    io->ptr = arg;
    io_set_callback(&io->io, e_iocb_cb);
}

void e_io_prep_preadv(e_iocb *eio, int fd, const struct iovec *iov, int iovcnt,
		      size_t offset, void (*cb)(void*,long), void *arg)
{
    io_prep_preadv(&eio->io, fd, iov, iovcnt, offset);
    eio->cb = cb;
    eio->ptr = arg;
    io_set_callback(&eio->io, e_iocb_cb);
}

// see https://chromium.googlesource.com/chromium/src/base/+/
//  f86dd125dc43d6dba04b61e73ac8a2bd4636c3f8/dynamic_annotations.h

int e_io_submit(io_context_t ctx, e_iocb *eio)
{
    iocb *io = &eio->io;
    ANNOTATE_HAPPENS_BEFORE(&eio->io);
    return io_submit(ctx, 1, &io);
}

