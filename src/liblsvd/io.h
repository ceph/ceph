/*
 * file:        io.h
 * description: interface for libaio-based async I/O
 * author:      Peter Desnoyers, Northeastern University
 * Copyright 2021, 2022 Peter Desnoyers
 * license:     GNU LGPL v2.1 or newer
 *              LGPL-2.1-or-later
 */

#ifndef IO_H
#define IO_H

#include <stddef.h>
#include <libaio.h>

/* TODO: why is this here???
 */
size_t getsize64(int fd);



/* adapt libaio for generic callbacks
 */
//void e_iocb_cb(io_context_t ctx, iocb *io, long res, long res2);
struct e_iocb {
    iocb io;
    void (*cb)(void*,long) = NULL;
    void *ptr = NULL;
    e_iocb() {}
};

/* this is *supposed* to be implemented in libaio
 */
int io_queue_wait(io_context_t ctx, struct timespec *timeout);

/* sets thread name, runs queue. Polls with 100uS timeout, since
 * blocking doesn't seem to work correctly
 */
void e_iocb_runner(io_context_t ctx, bool *running, const char *name);

/* each e_io_xyz corresponds io_prep_xyz in libaio.h
 * see man page for more info
 */
void e_io_prep_pwrite(e_iocb *io, int fd, void *buf, size_t len, size_t offset,
		      void (*cb)(void*,long), void *arg);
void e_io_prep_pread(e_iocb *io, int fd, void *buf, size_t len, size_t offset,
		     void (*cb)(void*,long), void *arg);
void e_io_prep_pwritev(e_iocb *io, int fd, const struct iovec *iov, int iovcnt,
                       size_t offset, void (*cb)(void*,long), void *arg);
void e_io_prep_preadv(e_iocb *eio, int fd, const struct iovec *iov, int iovcnt,
                      size_t offset, void (*cb)(void*,long), void *arg);
int e_io_submit(io_context_t ctx, e_iocb *eio);

#endif
