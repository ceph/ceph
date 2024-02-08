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

#include "rgw_datalog.h"
#include "rgw_service.h"
#include "rgw_tools.h"

#include "svc_bi.h"
#include "svc_tier_rados.h"

struct rgw_bucket_dir_header;

class RGWSI_BILog_RADOS;

#define RGW_NO_SHARD -1

#define RGW_SHARDS_PRIME_0 7877
#define RGW_SHARDS_PRIME_1 65521

/*
 * Defined Bucket Index Namespaces
 */
#define RGW_OBJ_NS_MULTIPART "multipart"
#define RGW_OBJ_NS_SHADOW    "shadow"

class RGWSI_BucketIndex_RADOS : public RGWSI_BucketIndex
{
  friend class RGWSI_BILog_RADOS;

  int open_pool(const DoutPrefixProvider *dpp,
                const rgw_pool& pool,
                librados::IoCtx* index_pool,
                bool mostly_omap);

  int open_bucket_index_pool(const DoutPrefixProvider *dpp,
			     const RGWBucketInfo& bucket_info,
			     librados::IoCtx* index_pool);
  int open_bucket_index_base(const DoutPrefixProvider *dpp,
                             const RGWBucketInfo& bucket_info,
                             librados::IoCtx* index_pool,
                             std::string *bucket_oid_base);

  // return the index oid for the given shard id
  void get_bucket_index_object(const std::string& bucket_oid_base,
                               const rgw::bucket_index_normal_layout& normal,
                               uint64_t gen_id, int shard_id,
                               std::string* bucket_obj);
  // return the index oid and shard id for the given object name
  int get_bucket_index_object(const std::string& bucket_oid_base,
                              const rgw::bucket_index_normal_layout& normal,
                              uint64_t gen_id, const std::string& obj_key,
                              std::string* bucket_obj, int* shard_id);

  int cls_bucket_head(const DoutPrefixProvider *dpp,
		      const RGWBucketInfo& bucket_info,
                      const rgw::bucket_index_layout_generation& idx_layout,
                      int shard_id,
                      std::vector<rgw_bucket_dir_header> *headers,
                      std::map<int, std::string> *bucket_instance_ids,
                      optional_yield y);

public:

  librados::Rados* rados{nullptr};

  struct Svc {
    RGWSI_Zone *zone{nullptr};
    RGWSI_BILog_RADOS *bilog{nullptr};
    RGWDataChangesLog *datalog_rados{nullptr};
  } svc;

  RGWSI_BucketIndex_RADOS(CephContext *cct);

  void init(RGWSI_Zone *zone_svc,
            librados::Rados* rados_,
            RGWSI_BILog_RADOS *bilog_svc,
            RGWDataChangesLog *datalog_rados_svc);

  static int shards_max() {
    return RGW_SHARDS_PRIME_1;
  }

  static int shard_id(const std::string& key, int max_shards) {
    return rgw_shard_id(key, max_shards);
  }

  static uint32_t bucket_shard_index(const std::string& key,
                                     int num_shards) {
    uint32_t sid = ceph_str_hash_linux(key.c_str(), key.size());
    uint32_t sid2 = sid ^ ((sid & 0xFF) << 24);
    return rgw_shards_mod(sid2, num_shards);
  }

  static uint32_t bucket_shard_index(const rgw_obj_key& obj_key,
				     int num_shards)
  {
    std::string sharding_key;
    if (obj_key.ns == RGW_OBJ_NS_MULTIPART) {
      RGWMPObj mp;
      mp.from_meta(obj_key.name);
      sharding_key = mp.get_key();
    } else {
      sharding_key = obj_key.name;
    }

    return bucket_shard_index(sharding_key, num_shards);
  }

  int init_index(const DoutPrefixProvider *dpp, RGWBucketInfo& bucket_info,const rgw::bucket_index_layout_generation& idx_layout) override;
  int clean_index(const DoutPrefixProvider *dpp, RGWBucketInfo& bucket_info, const rgw::bucket_index_layout_generation& idx_layout) override;

  /* RADOS specific */

  int read_stats(const DoutPrefixProvider *dpp,
                 const RGWBucketInfo& bucket_info,
                 RGWBucketEnt *stats,
                 optional_yield y) override;

  int get_reshard_status(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info,
                         std::list<cls_rgw_bucket_instance_entry> *status);

  int handle_overwrite(const DoutPrefixProvider *dpp, const RGWBucketInfo& info,
                       const RGWBucketInfo& orig_info,
		       optional_yield y) override;

  int open_bucket_index_shard(const DoutPrefixProvider *dpp,
                              const RGWBucketInfo& bucket_info,
                              const std::string& obj_key,
                              rgw_rados_ref* bucket_obj,
                              int *shard_id);

  int open_bucket_index_shard(const DoutPrefixProvider *dpp,
                              const RGWBucketInfo& bucket_info,
                              const rgw::bucket_index_layout_generation& index,
                              int shard_id, rgw_rados_ref* bucket_obj);

  int open_bucket_index(const DoutPrefixProvider *dpp,
                        const RGWBucketInfo& bucket_info,
                        librados::IoCtx* index_pool,
                        std::string *bucket_oid);

  int open_bucket_index(const DoutPrefixProvider *dpp,
                        const RGWBucketInfo& bucket_info,
                        std::optional<int> shard_id,
                        const rgw::bucket_index_layout_generation& idx_layout,
                        librados::IoCtx* index_pool,
                        std::map<int, std::string> *bucket_objs,
                        std::map<int, std::string> *bucket_instance_ids);
};
