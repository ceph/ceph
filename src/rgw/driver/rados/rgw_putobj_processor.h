// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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
#include "services/svc_tier_rados.h"
#include "rgw_sal.h"
#include "rgw_obj_manifest.h"

namespace rgw {

namespace sal {
  class RadosStore;
}

class Aio;

namespace putobj {

// an object processor with special handling for the first chunk of the head.
// the virtual process_first_chunk() function returns a processor to handle the
// rest of the object
class HeadObjectProcessor : public rgw::sal::ObjectProcessor {
  uint64_t head_chunk_size;
  // buffer to capture the first chunk of the head object
  bufferlist head_data;
  // initialized after process_first_chunk() to process everything else
  rgw::sal::DataProcessor *processor = nullptr;
  uint64_t data_offset = 0; // maximum offset of data written (ie compressed)
 protected:
  uint64_t get_actual_size() const { return data_offset; }

  // process the first chunk of data and return a processor for the rest
  virtual int process_first_chunk(bufferlist&& data,
                                  rgw::sal::DataProcessor **processor) = 0;
 public:
  HeadObjectProcessor(uint64_t head_chunk_size)
    : head_chunk_size(head_chunk_size)
  {}

  void set_head_chunk_size(uint64_t size) { head_chunk_size = size; }

  // cache first chunk for process_first_chunk(), then forward everything else
  // to the returned processor
  int process(bufferlist&& data, uint64_t logical_offset) final override;
};

using RawObjSet = std::set<rgw_raw_obj>;

// a data sink that writes to rados objects and deletes them on cancelation
class RadosWriter : public rgw::sal::DataProcessor {
  Aio *const aio;
  RGWRados *const store;
  const RGWBucketInfo& bucket_info;
  RGWObjectCtx& obj_ctx;
  const rgw_obj head_obj;
  rgw_rados_ref stripe_obj; // current stripe object
  RawObjSet written; // set of written objects for deletion
  const DoutPrefixProvider *dpp;
  optional_yield y;
  jspan_context& trace;

 public:
  RadosWriter(Aio *aio, RGWRados *store,
              const RGWBucketInfo& bucket_info,
              RGWObjectCtx& obj_ctx, const rgw_obj& _head_obj,
              const DoutPrefixProvider *dpp, optional_yield y, jspan_context& _trace)
    : aio(aio), store(store), bucket_info(bucket_info),
      obj_ctx(obj_ctx), head_obj(_head_obj), dpp(dpp), y(y), trace(_trace)
  {}
  ~RadosWriter();

  // add alloc hint to osd
  void add_write_hint(librados::ObjectWriteOperation& op);

  // change the current stripe object
  int set_stripe_obj(const rgw_raw_obj& obj);

  // write the data at the given offset of the current stripe object
  int process(bufferlist&& data, uint64_t stripe_offset) override;

  // write the data as an exclusive create and wait for it to complete
  int write_exclusive(const bufferlist& data);

  int drain();

  // when the operation completes successfully, clear the set of written objects
  // so they aren't deleted on destruction
  void clear_written() { written.clear(); }

  jspan_context& get_trace() { return trace; }
};


// a rados object processor that stripes according to RGWObjManifest
class ManifestObjectProcessor : public HeadObjectProcessor,
                                public StripeGenerator {
 protected:
  RGWRados* const store;
  RGWBucketInfo& bucket_info;
  rgw_placement_rule tail_placement_rule;
  rgw_user owner;
  RGWObjectCtx& obj_ctx;
  rgw_obj head_obj;

  RadosWriter writer;
  RGWObjManifest manifest;
  RGWObjManifest::generator manifest_gen;
  ChunkProcessor chunk;
  StripeProcessor stripe;
  const DoutPrefixProvider *dpp;

  // implements StripeGenerator
  int next(uint64_t offset, uint64_t *stripe_size) override;

 public:
  ManifestObjectProcessor(Aio *aio, RGWRados* store,
                          RGWBucketInfo& bucket_info,
                          const rgw_placement_rule *ptail_placement_rule,
                          const rgw_user& owner, RGWObjectCtx& _obj_ctx,
                          const rgw_obj& _head_obj,
                          const DoutPrefixProvider* dpp,
                          optional_yield y,
                          jspan_context& trace)
    : HeadObjectProcessor(0),
      store(store), bucket_info(bucket_info),
      owner(owner),
      obj_ctx(_obj_ctx), head_obj(_head_obj),
      writer(aio, store, bucket_info, obj_ctx, head_obj, dpp, y, trace),
      chunk(&writer, 0), stripe(&chunk, this, 0), dpp(dpp) {
        if (ptail_placement_rule) {
          tail_placement_rule = *ptail_placement_rule;
        }
      }

  void set_owner(const rgw_user& _owner) {
    owner = _owner;
  }

  void set_tail_placement(const rgw_placement_rule& tpr) {
    tail_placement_rule = tpr;
  }
  void set_tail_placement(const rgw_placement_rule&& tpr) {
    tail_placement_rule = tpr;
  }

};


// a processor that completes with an atomic write to the head object as part of
// a bucket index transaction
class AtomicObjectProcessor : public ManifestObjectProcessor {
  const std::optional<uint64_t> olh_epoch;
  const std::string unique_tag;
  bufferlist first_chunk; // written with the head in complete()

  int process_first_chunk(bufferlist&& data, rgw::sal::DataProcessor **processor) override;
 public:
  AtomicObjectProcessor(Aio *aio, RGWRados* store,
                        RGWBucketInfo& bucket_info,
                        const rgw_placement_rule *ptail_placement_rule,
                        const rgw_user& owner,
                        RGWObjectCtx& obj_ctx, const rgw_obj& _head_obj,
                        std::optional<uint64_t> olh_epoch,
                        const std::string& unique_tag,
                        const DoutPrefixProvider *dpp, optional_yield y, jspan_context& trace)
    : ManifestObjectProcessor(aio, store, bucket_info, ptail_placement_rule,
                              owner, obj_ctx, _head_obj, dpp, y, trace),
      olh_epoch(olh_epoch), unique_tag(unique_tag)
  {}

  // prepare a trivial manifest
  int prepare(optional_yield y) override;
  // write the head object atomically in a bucket index transaction
  int complete(size_t accounted_size, const std::string& etag,
               ceph::real_time *mtime, ceph::real_time set_mtime,
               std::map<std::string, bufferlist>& attrs,
               ceph::real_time delete_at,
               const char *if_match, const char *if_nomatch,
               const std::string *user_data,
               rgw_zone_set *zones_trace, bool *canceled,
               const req_context& rctx,
               uint32_t flags) override;

};


// a processor for multipart parts, which don't require atomic completion. the
// part's head is written with an exclusive create to detect racing uploads of
// the same part/upload id, which are restarted with a random oid prefix
class MultipartObjectProcessor : public ManifestObjectProcessor {
  const rgw_obj target_obj; // target multipart object
  const std::string upload_id;
  const int part_num;
  const std::string part_num_str;
  RGWMPObj mp;

  // write the first chunk and wait on aio->drain() for its completion.
  // on EEXIST, retry with random prefix
  int process_first_chunk(bufferlist&& data, rgw::sal::DataProcessor **processor) override;
  // prepare the head stripe and manifest
  int prepare_head();
 public:
  MultipartObjectProcessor(Aio *aio, RGWRados* store,
                           RGWBucketInfo& bucket_info,
                           const rgw_placement_rule *ptail_placement_rule,
                           const rgw_user& owner, RGWObjectCtx& obj_ctx,
                           const rgw_obj& _head_obj,
                           const std::string& upload_id, uint64_t part_num,
                           const std::string& part_num_str,
                           const DoutPrefixProvider *dpp, optional_yield y, jspan_context& trace)
    : ManifestObjectProcessor(aio, store, bucket_info, ptail_placement_rule,
                              owner, obj_ctx, _head_obj, dpp, y, trace),
      target_obj(head_obj), upload_id(upload_id),
      part_num(part_num), part_num_str(part_num_str),
      mp(head_obj.key.name, upload_id)
  {}

  // prepare a multipart manifest
  int prepare(optional_yield y) override;
  // write the head object attributes in a bucket index transaction, then
  // register the completed part with the multipart meta object
  int complete(size_t accounted_size, const std::string& etag,
               ceph::real_time *mtime, ceph::real_time set_mtime,
               std::map<std::string, bufferlist>& attrs,
               ceph::real_time delete_at,
               const char *if_match, const char *if_nomatch,
               const std::string *user_data,
               rgw_zone_set *zones_trace, bool *canceled,
               const req_context& rctx,
               uint32_t flags) override;

};

  class AppendObjectProcessor : public ManifestObjectProcessor {
    uint64_t cur_part_num;
    uint64_t position;
    uint64_t cur_size;
    uint64_t *cur_accounted_size;
    std::string cur_etag;
    const std::string unique_tag;

    RGWObjManifest *cur_manifest;

    int process_first_chunk(bufferlist&& data, rgw::sal::DataProcessor **processor) override;

  public:
    AppendObjectProcessor(Aio *aio, RGWRados* store,
                          RGWBucketInfo& bucket_info,
                          const rgw_placement_rule *ptail_placement_rule,
                          const rgw_user& owner, RGWObjectCtx& obj_ctx,
                          const rgw_obj& _head_obj,
                          const std::string& unique_tag, uint64_t position,
                          uint64_t *cur_accounted_size,
                          const DoutPrefixProvider *dpp, optional_yield y, jspan_context& trace)
            : ManifestObjectProcessor(aio, store, bucket_info, ptail_placement_rule,
                                      owner, obj_ctx, _head_obj, dpp, y, trace),
              position(position), cur_size(0), cur_accounted_size(cur_accounted_size),
              unique_tag(unique_tag), cur_manifest(nullptr)
    {}
    int prepare(optional_yield y) override;
    int complete(size_t accounted_size, const std::string& etag,
                 ceph::real_time *mtime, ceph::real_time set_mtime,
                 std::map<std::string, bufferlist>& attrs, ceph::real_time delete_at,
                 const char *if_match, const char *if_nomatch, const std::string *user_data,
                 rgw_zone_set *zones_trace, bool *canceled,
                 const req_context& rctx,
                 uint32_t flags) override;
  };

} // namespace putobj
} // namespace rgw

