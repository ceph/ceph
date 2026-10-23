// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM Corporation
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef CEPH_CEPH_BUFFER_H
#define CEPH_CEPH_BUFFER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/uio.h>

#ifdef __cplusplus
extern "C" {
#endif

struct ceph_mount_info;
struct Fh;

/**
 * Opaque handle owning a ceph::bufferlist.
 *
 * Ownership / lifetime:
 * - create / alloc / wrap_iov / async_read completion: caller owns the handle
 *   and must call ceph_bufferlist_put() when done (e.g. after socket send).
 * - ceph_ll_async_write: consumes the handle at submit (do not put after a
 *   successful submit; put only if submit fails and you still own it).
 * - ceph_bufferlist_to_iovec(): fills iovec views into the handle's pages;
 *   the handle must outlive use of those iovecs (no byte copy).
 *
 * This is the C surface for zero-copy I/O. Do not use ceph_ll_io_info
 * zerocopy/release fields (that approach is discarded).
 */
typedef struct ceph_bufferlist ceph_bufferlist;

ceph_bufferlist* ceph_bufferlist_create(void);
void ceph_bufferlist_put(ceph_bufferlist* bl);
size_t ceph_bufferlist_length(const ceph_bufferlist* bl);

/**
 * Map bufferlist segments into @p iov without copying bytes.
 * @return iovec count on success, 0 if empty, or -errno (-ERANGE if more
 *         non-empty segments than @p max_iov).
 */
int ceph_bufferlist_to_iovec(const ceph_bufferlist* bl, struct iovec* iov,
                             int max_iov);

/** Page-aligned Ceph-owned buffers (ceph::ZeroCopyClient::alloc_buffers). */
ceph_bufferlist* ceph_bufferlist_alloc(size_t len);

/**
 * Wrap caller iovecs without copying (ceph::ZeroCopyClient::wrap_static).
 * Caller memory must remain valid until write completion if submitted.
 */
ceph_bufferlist* ceph_bufferlist_wrap_iov(const struct iovec* iov, int iovcnt);

struct ceph_io_completion {
  void (*cb)(int64_t result, ceph_bufferlist* data, void* priv);
  void* priv;
};

/**
 * Zero-copy async read/write (ceph::ZeroCopyClient). Not implemented until
 * the Filer path lands; currently returns -ENOTSUP.
 *
 * async_read: on success, @c cb receives an owned @p data handle (put after use).
 * async_write: consumes @p data on successful submit.
 */
int ceph_ll_async_read(struct ceph_mount_info* cmount, struct Fh* fh,
                       uint64_t off, uint64_t len,
                       struct ceph_io_completion* c);
int ceph_ll_async_write(struct ceph_mount_info* cmount, struct Fh* fh,
                        uint64_t off, ceph_bufferlist* data, bool fsync,
                        struct ceph_io_completion* c);

#ifdef __cplusplus
}
#endif

#endif /* CEPH_CEPH_BUFFER_H */
