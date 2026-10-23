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
 * Data path is Filer → Objecter only (no ObjectCacher). Buffer helpers and
 * async/sync I/O are implemented; FSCrypt is identity (passthrough) for v1.
 *
 * Write ownership rule: @c async_write consumes the caller's @c bufferlist at
 * submit. If the caller needs a view after submit, it must retain its own
 * share (copy/share the list) before calling. For @c wrap_static buffers, the
 * wrapped memory must remain valid until the write callback runs.
 */
class ZeroCopyClient {
public:
  explicit ZeroCopyClient(Client* client);

  Client* get_client() const { return client; }

  /// Page-aligned Ceph-owned buffers the caller may fill, then pass to write.
  static bufferlist alloc_buffers(size_t len);

  /**
   * Wrap caller memory without copying.
   * Lifetime: @p buf / @p iov bases must remain valid until write completion
   * (or until the returned list is destroyed if never submitted).
   */
  static bufferlist wrap_static(char* buf, size_t len);
  static bufferlist wrap_static(const struct iovec* iov, int iovcnt);

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
