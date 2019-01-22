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

#pragma once

#include "include/buffer.h"

namespace rgw::putobj {

// a simple streaming data processing abstraction
class DataProcessor {
 public:
  virtual ~DataProcessor() {}

  // consume a bufferlist in its entirety at the given object offset. an
  // empty bufferlist is given to request that any buffered data be flushed,
  // though this doesn't wait for completions
  virtual int process(bufferlist&& data, uint64_t offset) = 0;
};

// for composing data processors into a pipeline
class Pipe : public DataProcessor {
  DataProcessor *next;
 public:
  explicit Pipe(DataProcessor *next) : next(next) {}

  // passes the data on to the next processor
  int process(bufferlist&& data, uint64_t offset) override {
    return next->process(std::move(data), offset);
  }
};

// pipe that writes to the next processor in discrete chunks
class ChunkProcessor : public Pipe {
  uint64_t chunk_size;
  bufferlist chunk; // leftover bytes from the last call to process()
 public:
  ChunkProcessor(DataProcessor *next, uint64_t chunk_size)
    : Pipe(next), chunk_size(chunk_size)
  {}

  int process(bufferlist&& data, uint64_t offset) override;
};


// interface to generate the next stripe description
class StripeGenerator {
 public:
  virtual ~StripeGenerator() {}

  virtual int next(uint64_t offset, uint64_t *stripe_size) = 0;
};

// pipe that respects stripe boundaries and restarts each stripe at offset 0
class StripeProcessor : public Pipe {
  StripeGenerator *gen;
  std::pair<uint64_t, uint64_t> bounds; // bounds of current stripe
 public:
  StripeProcessor(DataProcessor *next, StripeGenerator *gen,
                  uint64_t first_stripe_size)
    : Pipe(next), gen(gen), bounds(0, first_stripe_size)
  {}

  int process(bufferlist&& data, uint64_t data_offset) override;
};

} // namespace rgw::putobj
