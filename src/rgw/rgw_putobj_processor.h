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

#include <optional>

#include "rgw_putobj.h"
#include "rgw_rados.h"

namespace rgw::putobj {

// a data consumer that writes an object in a bucket
class ObjectProcessor : public DataProcessor {
 public:
  // prepare to start processing object data
  virtual int prepare() = 0;

  // complete the operation and make its result visible to clients
  virtual int complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled) = 0;
};

// an object processor with special handling for the first chunk of the head.
// the virtual process_first_chunk() function returns a processor to handle the
// rest of the object
class HeadObjectProcessor : public ObjectProcessor {
  uint64_t head_chunk_size;
  // buffer to capture the first chunk of the head object
  bufferlist head_data;
  // initialized after process_first_chunk() to process everything else
  DataProcessor *processor = nullptr;
  uint64_t data_offset = 0; // maximum offset of data written (ie compressed)
 protected:
  uint64_t get_actual_size() const { return data_offset; }

  // process the first chunk of data and return a processor for the rest
  virtual int process_first_chunk(bufferlist&& data,
                                  DataProcessor **processor) = 0;
 public:
  HeadObjectProcessor(uint64_t head_chunk_size)
    : head_chunk_size(head_chunk_size)
  {}

  void set_head_chunk_size(uint64_t size) { head_chunk_size = size; }

  // cache first chunk for process_first_chunk(), then forward everything else
  // to the returned processor
  int process(bufferlist&& data, uint64_t logical_offset) final override;
};

} // namespace rgw::putobj
