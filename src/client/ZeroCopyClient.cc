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

#include "client/ZeroCopyClient.h"

#include <errno.h>

namespace ceph {

ZeroCopyClient::ZeroCopyClient(Client* client) : client(client) {}

bufferlist ZeroCopyClient::alloc_buffers(size_t len)
{
  bufferlist bl;
  if (len == 0) {
    return bl;
  }
  bl.push_back(bufferptr(buffer::create_page_aligned(len)));
  return bl;
}

bufferlist ZeroCopyClient::wrap_static(char* buf, size_t len)
{
  if (len == 0 || buf == nullptr) {
    return bufferlist();
  }
  return bufferlist::static_from_mem(buf, len);
}

bufferlist ZeroCopyClient::wrap_static(const struct iovec* iov, int iovcnt)
{
  bufferlist bl;
  if (iov == nullptr || iovcnt <= 0) {
    return bl;
  }
  for (int i = 0; i < iovcnt; ++i) {
    if (iov[i].iov_base == nullptr || iov[i].iov_len == 0) {
      continue;
    }
    bl.push_back(buffer::create_static(iov[i].iov_len,
                                       static_cast<char*>(iov[i].iov_base)));
  }
  return bl;
}

int ZeroCopyClient::bufferlist_to_iovec(const bufferlist& bl, struct iovec* iov,
                                        int max_iov)
{
  if (iov == nullptr && bl.get_num_buffers() > 0) {
    return -EINVAL;
  }
  if (max_iov < 0) {
    return -EINVAL;
  }

  int n = 0;
  for (const auto& p : bl.buffers()) {
    if (p.length() == 0) {
      continue;
    }
    if (n >= max_iov) {
      return -ERANGE;
    }
    iov[n].iov_base = const_cast<char*>(p.c_str());
    iov[n].iov_len = p.length();
    ++n;
  }
  return n;
}

int ZeroCopyClient::async_read(Fh* fh, uint64_t off, uint64_t len, IoCallback cb)
{
  // Phase 1: Filer/Objecter path
  (void)fh;
  (void)off;
  (void)len;
  (void)cb;
  return -ENOTSUP;
}

int ZeroCopyClient::async_write(Fh* fh, uint64_t off, bufferlist data,
                                IoCallback cb, bool fsync, bool syncdataonly)
{
  (void)fh;
  (void)off;
  (void)data;
  (void)cb;
  (void)fsync;
  (void)syncdataonly;
  return -ENOTSUP;
}

int64_t ZeroCopyClient::read(Fh* fh, uint64_t off, uint64_t len, bufferlist* out)
{
  (void)fh;
  (void)off;
  (void)len;
  (void)out;
  return -ENOTSUP;
}

int64_t ZeroCopyClient::write(Fh* fh, uint64_t off, bufferlist data, bool fsync)
{
  (void)fh;
  (void)off;
  (void)data;
  (void)fsync;
  return -ENOTSUP;
}

} // namespace ceph
