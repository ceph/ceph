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

} // namespace rgw::putobj
