// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_putobj.h"

namespace rgw::putobj {

int ChunkProcessor::process(bufferlist&& data, uint64_t offset)
{
  ceph_assert(offset >= chunk.length());
  uint64_t position = offset - chunk.length();

  const bool flush = (data.length() == 0);
  if (flush) {
    if (chunk.length() > 0) {
      int r = Pipe::process(std::move(chunk), position);
      if (r < 0) {
        return r;
      }
    }
    return Pipe::process({}, offset);
  }
  chunk.claim_append(data);

  // write each full chunk
  while (chunk.length() >= chunk_size) {
    bufferlist bl;
    chunk.splice(0, chunk_size, &bl);

    int r = Pipe::process(std::move(bl), position);
    if (r < 0) {
      return r;
    }
    position += chunk_size;
  }
  return 0;
}

} // namespace rgw::putobj
