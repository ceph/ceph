
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */


#pragma once

#include "rgw/rgw_service.h"
#include "rgw/rgw_tools.h"

#include "svc_bi.h"
#include "svc_rados.h"

struct rgw_bucket_dir_header;

class RGWSI_BILog_RADOS;
class RGWSI_DataLog_RADOS;

#define RGW_NO_SHARD -1

#define RGW_SHARDS_PRIME_0 7877
#define RGW_SHARDS_PRIME_1 65521

class RGWSI_BucketIndex_RADOS : public RGWSI_BucketIndex
{
  friend class RGWSI_BILog_RADOS;

  int open_pool(const rgw_pool& pool,
                RGWSI_RADOS::Pool *index_pool,
                bool mostly_omap, const Span& parent_span = nullptr);

  int open_bucket_index_pool(const RGWBucketInfo& bucket_info,
                            RGWSI_RADOS::Pool *index_pool, const Span& parent_span = nullptr);
  int open_bucket_index_base(const RGWBucketInfo& bucket_info,
                             RGWSI_RADOS::Pool *index_pool,
                             string *bucket_oid_base);

  void get_bucket_index_object(const string& bucket_oid_base,
                               uint32_t num_shards,
                               int shard_id,
                               uint64_t gen_id,
                               string *bucket_obj);
  int get_bucket_index_object(const string& bucket_oid_base, const string& obj_key,
                              uint32_t num_shards, rgw::BucketHashType hash_type,
                              string *bucket_obj, int *shard_id);

  int cls_bucket_head(const RGWBucketInfo& bucket_info,
                      int shard_id,
                      vector<rgw_bucket_dir_header> *headers,
                      map<int, string> *bucket_instance_ids,
                      optional_yield y);

public:

  struct Svc {
    RGWSI_Zone *zone{nullptr};
    RGWSI_RADOS *rados{nullptr};
    RGWSI_BILog_RADOS *bilog{nullptr};
    RGWSI_DataLog_RADOS *datalog_rados{nullptr};
  } svc;

  RGWSI_BucketIndex_RADOS(CephContext *cct);

  void init(RGWSI_Zone *zone_svc,
            RGWSI_RADOS *rados_svc,
            RGWSI_BILog_RADOS *bilog_svc,
            RGWSI_DataLog_RADOS *datalog_rados_svc);

  static int shards_max() {
    return RGW_SHARDS_PRIME_1;
  }

  static int shard_id(const string& key, int max_shards) {
    return rgw_shard_id(key, max_shards);
  }

  static uint32_t bucket_shard_index(const std::string& key,
                                     int num_shards) {
    uint32_t sid = ceph_str_hash_linux(key.c_str(), key.size());
    uint32_t sid2 = sid ^ ((sid & 0xFF) << 24);
    return rgw_shards_mod(sid2, num_shards);
  }

  int init_index(RGWBucketInfo& bucket_info, const Span& parent_span = nullptr);
  int clean_index(RGWBucketInfo& bucket_info);


  /* RADOS specific */

  int read_stats(const RGWBucketInfo& bucket_info,
                 RGWBucketEnt *stats,
                 optional_yield y) override;

  int get_reshard_status(const RGWBucketInfo& bucket_info,
                         std::list<cls_rgw_bucket_instance_entry> *status);

  int handle_overwrite(const RGWBucketInfo& info,
                       const RGWBucketInfo& orig_info) override;

  int open_bucket_index_shard(const RGWBucketInfo& bucket_info,
                              const string& obj_key,
                              RGWSI_RADOS::Obj *bucket_obj,
                              int *shard_id);

  int open_bucket_index_shard(const RGWBucketInfo& bucket_info,
                              int shard_id,
                              const rgw::bucket_index_layout_generation& idx_layout,
                              RGWSI_RADOS::Obj *bucket_obj);

  int open_bucket_index(const RGWBucketInfo& bucket_info,
                        RGWSI_RADOS::Pool *index_pool,
                        string *bucket_oid);

  int open_bucket_index(const RGWBucketInfo& bucket_info,
                        std::optional<int> shard_id,
                        RGWSI_RADOS::Pool *index_pool,
                        map<int, string> *bucket_objs,
                        map<int, string> *bucket_instance_ids, const Span& parent_span = nullptr);
};


