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

#pragma once

#include <sys/uio.h>

#include <cstdint>
#include <functional>

#include "include/buffer.h"

class Client;
struct Fh;

namespace ceph {

/**
 * Result of a zero-copy ZeroCopyClient I/O completion.
 *
 * Ownership:
 * - On a successful read, @c data holds Ceph @c bufferlist pages (typically
 *   from Objecter). The caller owns @c data after the callback returns and
 *   must keep it alive for as long as any @c iovec view into it is used
 *   (e.g. until NFS/XDR send completes). Dropping the last @c bufferlist
 *   releases the underlying @c buffer::raw pages.
 * - On write completion, @c data is empty; write payload ownership was
 *   transferred into ZeroCopyClient at submit (see async_write).
 */
struct IoResult {
  int64_t result = 0; ///< bytes transferred, or -errno
  bufferlist data; ///< read payload; empty on write
};

using IoCallback = std::function<void(IoResult&&)>;

/**
 * Zero-copy async CephFS data plane (Ganesha-oriented).
 *
 * This API uses @c ceph::bufferlist as the unit of payload ownership. It does
 * not extend @c ceph_ll_io_info with zerocopy/release fields; callers that need
 * a C ABI should use the opaque @c ceph_bufferlist helpers in ceph_buffer.h.
 *
 * Data path is Filer → Objecter only today (ObjectCacher routing is separate).
 *
 * FSCrypt: the API is always plaintext. On encrypted inodes, @c async_write
 * runs WriteEncMgr RMW + @c encrypt_bl (allocates a ciphertext bufferlist —
 * a copy) before Filer; @c async_read expands to FSCrypt block bounds, reads
 * ciphertext, then @c decrypt_bl (another copy) before the user callback.
 * ObjectCacher, when wired later, caches ciphertext under the same boundary.
 *
 * Write ownership rule: @c async_write consumes the caller's @c bufferlist at
 * submit. Prefer @c alloc_buffers (upper-layer / Ceph-owned pages) so the
 * payload can live in ObjectCacher later without claim/donate. @c claim_iov is
 * the fallback when memory was already allocated by the RPC layer. For
 * @c wrap_static buffers, the wrapped memory must remain valid until the write
 * callback runs. For @c claim / @c claim_iov, ownership transfers at claim
 * time; the deleter runs when the last @c buffer::ptr drops the @c raw.
 */
class ZeroCopyClient {
public:
  /**
   * Per-segment release callback for @c claim / @c claim_iov.
   * Invoked with the claimed @p buf / @p len when that raw is destroyed.
   */
  using ClaimDeleter = void (*)(void* buf, size_t len, void* priv);

  explicit ZeroCopyClient(Client* client);

  Client* get_client() const { return client; }

  /**
   * Page-aligned Ceph-owned buffers for upper-layer WRITE.
   * Caller fills via @c bufferlist_to_iovec (writable views), then passes the
   * list to @c async_write which consumes it. Single contiguous page-aligned
   * ptr so FSAL can map with one iovec under IOV_MAX.
   */
  static bufferlist alloc_buffers(size_t len);

  /**
   * Wrap caller memory without copying (non-owning).
   * Lifetime: @p buf / @p iov bases must remain valid until write completion
   * (or until the returned list is destroyed if never submitted). Unsafe to
   * hold in ObjectCacher after the caller frees the memory.
   */
  static bufferlist wrap_static(char* buf, size_t len);
  static bufferlist wrap_static(const struct iovec* iov, int iovcnt);

  /**
   * Claim ownership of caller memory without copying (@c buffer::claim_buffer).
   * After return the caller must not free @p buf; @p del runs when the last
   * ptr drops the raw (may be off-thread). If @p del is nullptr, uses free(3).
   */
  static bufferlist claim(char* buf, size_t len, ClaimDeleter del, void* priv);

  /**
   * Claim each non-empty iovec segment (same deleter / priv for all).
   * Empty / null segments are skipped. @p del may be nullptr (free(3) each).
   */
  static bufferlist claim_iov(const struct iovec* iov, int iovcnt,
                              ClaimDeleter del, void* priv);

  /**
   * Fill @p iov with views into @p bl segments (no byte copy).
   * @return number of iovecs written, 0 if @p bl is empty, or -ERANGE if
   *         @p bl has more non-empty segments than @p max_iov.
   * @p bl must outlive use of @p iov.
   */
  static int bufferlist_to_iovec(const bufferlist& bl, struct iovec* iov,
                                 int max_iov);

  /// Async read: on success, callback receives owned @c IoResult::data.
  int async_read(Fh* fh, uint64_t off, uint64_t len, IoCallback cb);

  /// Async write: consumes @p data at submit.
  int async_write(Fh* fh, uint64_t off, bufferlist data, IoCallback cb,
                  bool fsync = false, bool syncdataonly = false);

  /// Sync wrappers for tests / non-async embeds (not the Ganesha hot path).
  int64_t read(Fh* fh, uint64_t off, uint64_t len, bufferlist* out);
  int64_t write(Fh* fh, uint64_t off, bufferlist data, bool fsync = false);

private:
  friend class C_ZC_Read;
  friend class C_ZC_Write;
  friend class C_ZC_Fsync;

  Client* client;

  /// Deliver @p cb on objecter_finisher (never store ZeroCopyClient* in I/O ctxs —
  /// callers often construct a stack ZeroCopyClient for a single submit).
  static void queue_user_cb(Client* clnt, IoCallback cb, int64_t result,
                            bufferlist data);
};

} // namespace ceph
