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


class Aio;
using RawObjSet = std::set<rgw_raw_obj>;

// a data sink that writes to rados objects and deletes them on cancelation
class RadosWriter : public DataProcessor {
  Aio *const aio;
  RGWRados *const store;
  const RGWBucketInfo& bucket_info;
  RGWObjectCtx& obj_ctx;
  const rgw_obj& head_obj;
  rgw_rados_ref stripe_ref; // current stripe ref
  rgw_raw_obj stripe_obj; // current stripe object
  RawObjSet written; // set of written objects for deletion

 public:
  RadosWriter(Aio *aio, RGWRados *store, const RGWBucketInfo& bucket_info,
              RGWObjectCtx& obj_ctx, const rgw_obj& head_obj)
    : aio(aio), store(store), bucket_info(bucket_info),
      obj_ctx(obj_ctx), head_obj(head_obj)
  {}
  ~RadosWriter();

  // change the current stripe object
  int set_stripe_obj(rgw_raw_obj&& obj);

  // write the data at the given offset of the current stripe object
  int process(bufferlist&& data, uint64_t stripe_offset) override;

  // write the data as an exclusive create and wait for it to complete
  int write_exclusive(const bufferlist& data);

  int drain();

  // when the operation completes successfully, clear the set of written objects
  // so they aren't deleted on destruction
  void clear_written() { written.clear(); }
};

} // namespace rgw::putobj
