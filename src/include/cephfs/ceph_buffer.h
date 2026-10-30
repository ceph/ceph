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
 * - create / alloc / wrap_iov / claim_iov / async_read completion: caller owns
 *   the handle and must call ceph_bufferlist_put() when done (e.g. after
 *   socket send), unless the handle was consumed by ceph_ll_async_write.
 * - ceph_ll_async_write: consumes the handle at submit (do not put after a
 *   successful submit; put only if submit fails and you still own it).
 * - ceph_bufferlist_to_iovec(): fills iovec views into the handle's pages;
 *   the handle must outlive use of those iovecs (no byte copy). Views are
 *   writable so the caller may readv/memcpy the NFS payload into alloc'd
 *   pages before submit.
 *
 * WRITE buffer sources (prefer in this order for Ganesha ZC):
 * - alloc (primary / upper-layer): Ceph-owned page-aligned pages; fill via
 *   to_iovec then ceph_ll_async_write. No claim/donate.
 * - claim_iov (fallback): donate ntirpc/RPC memory when recv cannot be
 *   deferred into Ceph pages (compound/GSS/failure/RDMA).
 * - wrap_iov: non-owning borrow until write CB; Objecter-only, unsafe for
 *   ObjectCacher dirty hold.
 *
 * This is the C surface for zero-copy I/O. Do not use ceph_ll_io_info
 * zerocopy/release fields (that approach is discarded).
 */
typedef struct ceph_bufferlist ceph_bufferlist;

/**
 * Called once per claimed iovec segment when Ceph releases that raw.
 * @p buf / @p len are the segment passed to claim_iov. May run off the
 * submit thread; must be thread-safe.
 */
typedef void (*ceph_buffer_deleter_t)(void* buf, size_t len, void* priv);

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

/**
 * Page-aligned Ceph-owned buffers (ceph::ZeroCopyClient::alloc_buffers).
 * Preferred source for upper-layer WRITE: FSAL/ntirpc readv into these
 * pages, then ceph_ll_async_write consumes the handle.
 */
ceph_bufferlist* ceph_bufferlist_alloc(size_t len);

/**
 * Wrap caller iovecs without copying (ceph::ZeroCopyClient::wrap_static).
 * Caller memory must remain valid until write completion if submitted.
 * Do not use with ObjectCacher — prefer alloc (upper-layer) or claim_iov.
 */
ceph_bufferlist* ceph_bufferlist_wrap_iov(const struct iovec* iov, int iovcnt);

/**
 * Claim ownership of caller iovecs without copying
 * (ceph::ZeroCopyClient::claim_iov via buffer::claim_buffer).
 *
 * Fallback when upper-layer alloc is unavailable. After success the caller
 * must not free the bytes. @p del is invoked once per non-empty segment when
 * that segment's raw is destroyed; if @p del is NULL, each base is passed to
 * free(3). @p priv is passed through to @p del.
 */
ceph_bufferlist* ceph_bufferlist_claim_iov(const struct iovec* iov, int iovcnt,
                                           ceph_buffer_deleter_t del,
                                           void* priv);

struct ceph_io_completion {
  void (*cb)(int64_t result, ceph_bufferlist* data, void* priv);
  void* priv;
};

/** Flags for @c ceph_ll_async_write (@p flags). */
enum {
  /** Flush file data (and metadata) after the write completes. */
  CEPH_LL_WRITE_FSYNC = (1 << 0),
  /**
   * With @c CEPH_LL_WRITE_FSYNC, flush data only (skip metadata wait).
   * Ignored if @c CEPH_LL_WRITE_FSYNC is not set.
   */
  CEPH_LL_WRITE_SYNCDATAONLY = (1 << 1),
};

/**
 * Zero-copy async read/write (ceph::ZeroCopyClient, Filer → Objecter).
 *
 * Bufferlists at this API are plaintext. On FSCrypt-enabled inodes the client
 * encrypts/decrypts internally (buffer copies); callers still put/fill
 * plaintext handles as usual.
 *
 * async_read: on success, @c cb receives an owned @p data handle (put after use).
 * async_write: consumes @p data on successful submit. @p flags is a bitwise-OR
 * of @c CEPH_LL_WRITE_* (0 = no post-write fsync).
 */
int ceph_ll_async_read(struct ceph_mount_info* cmount, struct Fh* fh,
                       uint64_t off, uint64_t len,
                       struct ceph_io_completion* c);
int ceph_ll_async_write(struct ceph_mount_info* cmount, struct Fh* fh,
                        uint64_t off, ceph_bufferlist* data, int flags,
                        struct ceph_io_completion* c);

#ifdef __cplusplus
}
#endif

#endif /* CEPH_CEPH_BUFFER_H */
