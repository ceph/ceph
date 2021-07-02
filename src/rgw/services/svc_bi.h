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

#include <string>
#include <string_view>
#include <vector>

#include <fmt/format.h>

#include "include/rados/librados.hpp"

#include "rgw/rgw_datalog.h"
#include "rgw/rgw_service.h"
#include "rgw/rgw_tools.h"

struct rgw_bucket_dir_header;

class RGWSI_BILog;

#define RGW_NO_SHARD -1

#define RGW_SHARDS_PRIME_0 7877
#define RGW_SHARDS_PRIME_1 65521

class RGWSI_BucketIndex final : public RGWServiceInstance
{
  friend RGWSI_BILog;

  int open_pool(const DoutPrefixProvider* dpp,
                const rgw_pool& pool,
                librados::IoCtx* index_pool,
                bool mostly_omap);

  int open_bucket_index_pool(const DoutPrefixProvider* dpp,
			     const RGWBucketInfo& bucket_info,
			     librados::IoCtx* index_pool);
  int open_bucket_index_base(const DoutPrefixProvider* dpp,
                             const RGWBucketInfo& bucket_info,
			     librados::IoCtx* index_pool,
                             std::string* bucket_oid_base);

  void get_bucket_index_object(std::string_view bucket_oid_base,
                               uint32_t num_shards,
                               int shard_id,
                               uint64_t gen_id,
                               std::string* bucket_obj);
  int get_bucket_index_object(std::string_view bucket_oid_base,
			      std::string_view obj_key,
                              uint32_t num_shards,
			      rgw::BucketHashType hash_type,
                              std::string* bucket_obj, int* shard_id);

  int cls_bucket_head(const DoutPrefixProvider* dpp,
		      const RGWBucketInfo& bucket_info,
		      int shard_id,
		      std::vector<rgw_bucket_dir_header>* headers,
		      std::map<int, std::string>* bucket_instance_ids,
		      optional_yield y);

public:

  librados::Rados* rados;

  struct Svc {
    RGWSI_Zone* zone{nullptr};
    RGWSI_BILog* bilog{nullptr};
    RGWDataChangesLog* datalog{nullptr};
  } svc;

  RGWSI_BucketIndex(CephContext* cct);

  void init(librados::Rados* rados,
	    RGWSI_Zone* zone_svc,
            RGWSI_BILog* bilog_svc,
            RGWDataChangesLog* datalog_svc);

  static int shards_max() {
    return RGW_SHARDS_PRIME_1;
  }

  static int shard_id(std::string_view key, int max_shards) {
    return rgw_shard_id(key, max_shards);
  }

  static uint32_t bucket_shard_index(std::string_view key,
                                     int num_shards) {
    uint32_t sid = ceph_str_hash_linux(key.data(), key.size());
    uint32_t sid2 = sid ^ ((sid & 0xFF) << 24);
    return rgw_shards_mod(sid2, num_shards);
  }

  int init_index(const DoutPrefixProvider* dpp, RGWBucketInfo& bucket_info);
  int clean_index(const DoutPrefixProvider* dpp, RGWBucketInfo& bucket_info);


  /* RADOS specific */

  int read_stats(const DoutPrefixProvider* dpp,
                 const RGWBucketInfo& bucket_info,
                 RGWBucketEnt* stats,
                 optional_yield y);

  int get_reshard_status(const DoutPrefixProvider* dpp,
			 const RGWBucketInfo& bucket_info,
                         std::vector<cls_rgw_bucket_instance_entry>* status);

  int handle_overwrite(const DoutPrefixProvider* dpp, const RGWBucketInfo& info,
                       const RGWBucketInfo& orig_info);

  int open_bucket_index_shard(const DoutPrefixProvider* dpp,
			      const RGWBucketInfo& bucket_info,
			      std::string_view obj_key,
			      librados::IoCtx* pool,
			      std::string* bucket_obj,
			      int* shard_id);


  int open_bucket_index_shard(const DoutPrefixProvider* dpp,
                              const RGWBucketInfo& bucket_info,
                              int shard_id,
                              const rgw::bucket_index_layout_generation& idx_layout,
			      librados::IoCtx* index_pool,
                              std::string* bucket_oid);

  int open_bucket_index(const DoutPrefixProvider* dpp,
                        const RGWBucketInfo& bucket_info,
			librados::IoCtx* index_pool,
                        std::string* bucket_oid);

  int open_bucket_index(const DoutPrefixProvider* dpp,
			const RGWBucketInfo& bucket_info,
			std::optional<int> _shard_id,
			librados::IoCtx* index_pool,
			std::map<int, string>* bucket_objs,
			std::map<int, string>* bucket_instance_ids);

};
