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


int StripeProcessor::process(bufferlist&& data, uint64_t offset)
{
  ceph_assert(offset >= bounds.first);

  const bool flush = (data.length() == 0);
  if (flush) {
    return Pipe::process({}, offset - bounds.first);
  }

  auto max = bounds.second - offset;
  while (data.length() > max) {
    if (max > 0) {
      bufferlist bl;
      data.splice(0, max, &bl);

      int r = Pipe::process(std::move(bl), offset - bounds.first);
      if (r < 0) {
        return r;
      }
      offset += max;
    }

    // flush the current chunk
    int r = Pipe::process({}, offset - bounds.first);
    if (r < 0) {
      return r;
    }
    // generate the next stripe
    uint64_t stripe_size;
    r = gen->next(offset, &stripe_size);
    if (r < 0) {
      return r;
    }
    ceph_assert(stripe_size > 0);

    bounds.first = offset;
    bounds.second = offset + stripe_size;

    max = stripe_size;
  }

  if (data.length() == 0) { // don't flush the chunk here
    return 0;
  }
  return Pipe::process(std::move(data), offset - bounds.first);
}

} // namespace rgw::putobj
