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

#include "include/expected.hpp"
#include "rgw/rgw_service.h"
#include "rgw/rgw_tools.h"

#include "svc_bi.h"
#include "svc_rados.h"

struct rgw_bucket_dir_header;

class RGWSI_BILog_RADOS;

#define RGW_NO_SHARD -1

#define RGW_SHARDS_PRIME_0 7877
#define RGW_SHARDS_PRIME_1 65521

class RGWSI_BucketIndex_RADOS : public RGWSI_BucketIndex
{
  friend class RGWSI_BILog_RADOS;

  tl::expected<RGWSI_RADOS::Pool, boost::system::error_code>
  open_bucket_index_pool(const RGWBucketInfo& bucket_info,
                         optional_yield y);

  tl::expected<std::pair<RGWSI_RADOS::Pool, std::string>,
	       boost::system::error_code>
  open_bucket_index_base(const RGWBucketInfo& bucket_info,
                         optional_yield y);


public:
  tl::expected<rgw_pool, boost::system::error_code>
  get_bucket_index_pool(const RGWBucketInfo& bucket_info);
  std::string get_bucket_index_object(const string& bucket_oid_base,
                                      uint32_t num_shards,
                                      int shard_id);
  tl::expected<std::pair<std::string, int>, boost::system::error_code>
  get_bucket_index_object(const string& bucket_oid_base,
                          const string& obj_key,
                          uint32_t num_shards,
                          RGWBucketInfo::BIShardsHashType hash_type);

private:
  tl::expected<std::pair<std::vector<rgw_bucket_dir_header>,
			 boost::container::flat_map<int, string>>,
	       boost::system::error_code>
  cls_bucket_head(const RGWBucketInfo& bucket_info, int shard_id,
		  optional_yield y);

public:

  struct Svc {
    RGWSI_Zone *zone{nullptr};
    RGWSI_RADOS *rados{nullptr};
    RGWSI_BILog_RADOS *bilog{nullptr};
    RGWDataChangesLog* datalog_rados{nullptr};
  } svc;

  RGWSI_BucketIndex_RADOS(CephContext *cct, boost::asio::io_context& ioc);

  void init(RGWSI_Zone *zone_svc,
            RGWSI_RADOS *rados_svc,
            RGWSI_BILog_RADOS *bilog_svc,
            RGWDataChangesLog *_datalog_rados);

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

  boost::system::error_code init_index(RGWBucketInfo& bucket_info,
				       optional_yield y) override;
  boost::system::error_code clean_index(RGWBucketInfo& bucket_info,
					optional_yield y) override;


  /* RADOS specific */

  tl::expected<RGWBucketEnt, boost::system::error_code>
  read_stats(const RGWBucketInfo& bucket_info, optional_yield y) override;

  tl::expected<std::vector<cls_rgw_bucket_instance_entry>,
	       boost::system::error_code>
  get_reshard_status(const RGWBucketInfo& bucket_info, optional_yield y);

  boost::system::error_code
  handle_overwrite(const RGWBucketInfo& info,
		   const RGWBucketInfo& orig_info,
		   optional_yield y) override;

  tl::expected<std::pair<RGWSI_RADOS::Obj, int>, boost::system::error_code>
  open_bucket_index_shard(const RGWBucketInfo& bucket_info,
                          const string& obj_key,
                          optional_yield y);

  tl::expected<RGWSI_RADOS::Obj, boost::system::error_code>
  open_bucket_index_shard(const RGWBucketInfo& bucket_info,
                          int shard_id,
                          optional_yield y);


  tl::expected<std::pair<RGWSI_RADOS::Pool, std::string>,
	       boost::system::error_code>
  open_bucket_index(const RGWBucketInfo& bucket_info, optional_yield y);

  tl::expected<std::tuple<RGWSI_RADOS::Pool,
			  boost::container::flat_map<int, string>,
			  boost::container::flat_map<int, string>>,
	       boost::system::error_code>
  open_bucket_index(const RGWBucketInfo& bucket_info,
                    std::optional<int> _shard_id,
                    optional_yield y);
};
