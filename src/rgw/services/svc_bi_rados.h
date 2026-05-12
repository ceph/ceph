// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

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

#include "driver/rados/rgw_datalog.h"
#include "driver/rados/rgw_service.h"
#include "driver/rados/rgw_tools.h"
#include "rgw_bucket.h"

#include "svc_bi.h"
#include "svc_tier_rados.h"
#include "svc_bindexer_rados.h"


struct rgw_bucket_dir_header;
struct rgw_cls_list_ret;

class RGWSI_BILog_RADOS;

#define RGW_NO_SHARD -1

#define RGW_SHARDS_PRIME_0 7877
#define RGW_SHARDS_PRIME_1 65521

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

public:

  int cls_bucket_head(const DoutPrefixProvider *dpp,
		      const RGWBucketInfo& bucket_info,
                      const rgw::bucket_index_layout_generation& idx_layout,
                      rgw::BIShardIndex shard_id,
                      std::vector<rgw_bucket_dir_header> *headers,
                      std::map<int, std::string> *bucket_instance_ids,
                      optional_yield y);

  librados::Rados* rados{nullptr};

  struct Svc {
    RGWSI_Zone *zone{nullptr};
    RGWSI_BILog_RADOS *bilog{nullptr};
    RGWDataChangesLog *datalog_rados{nullptr};
    RGWSI_Bucket_SObj* bucket_sobj{nullptr};
  } svc;

  RGWSI_BucketIndex_RADOS(CephContext *cct);

  void init(RGWSI_Zone *zone_svc,
            librados::Rados* rados_,
            RGWSI_BILog_RADOS *bilog_svc,
            RGWDataChangesLog *datalog_rados_svc,
            RGWSI_Bucket_SObj *bucket_sobj
    );

  static int shards_max() {
    return RGW_SHARDS_PRIME_1;
  }

  static int shard_id(const std::string& key, int max_shards) {
    return rgw_shard_id(key, max_shards);
  }

// OBI: this should be converted into returning an index object name
// and be based on index type
  static int32_t bucket_shard_index(const rgw_obj_key& obj_key,
				    int num_shards);

  int init_index(const DoutPrefixProvider *dpp, optional_yield y,
                 std::unique_ptr<rgw::rados::BIndexer>& bindexer,
		 std::map<std::string, bufferlist>* binfo_map_data,
                 bool judge_support_logrecord = false) override;

#if 1 // OBI deprecate??
  int clean_index(const DoutPrefixProvider *dpp, optional_yield y,
                  const RGWBucketInfo& bucket_info,
                  const rgw::bucket_index_layout_generation& idx_layout) override;
#endif
  int clean_index(const DoutPrefixProvider *dpp, optional_yield y,
                  std::unique_ptr<rgw::rados::BIndexer>& bindexer) override;

  /* RADOS specific */

  int read_stats(const DoutPrefixProvider *dpp,
                 const RGWBucketInfo& bucket_info,
                 RGWBucketEnt *stats,
                 optional_yield y) override;

  int get_reshard_status(const DoutPrefixProvider *dpp, optional_yield y,
                         const RGWBucketInfo& bucket_info,
                         std::list<cls_rgw_bucket_instance_entry> *status);
  int set_reshard_status(const DoutPrefixProvider *dpp, optional_yield y,
                         const RGWBucketInfo& bucket_info,
                         cls_rgw_reshard_status status);
  int trim_reshard_log(const DoutPrefixProvider* dpp, optional_yield,
                       const RGWBucketInfo& bucket_info);

  int set_tag_timeout(const DoutPrefixProvider *dpp, optional_yield y,
                      const RGWBucketInfo& bucket_info, uint64_t timeout);

  int check_index(const DoutPrefixProvider *dpp, optional_yield y,
                  const RGWBucketInfo& bucket_info,
                  std::map<int, bufferlist>& buffers);

  int rebuild_index(const DoutPrefixProvider *dpp, optional_yield y,
                    const RGWBucketInfo& bucket_info);

  /// Read the requested number of entries from each index shard object.
  int list_objects(const DoutPrefixProvider* dpp, optional_yield y,
                   librados::IoCtx& index_pool,
                   const std::map<int, std::string>& bucket_objs,
                   const cls_rgw_obj_key& start_obj,
                   const std::string& prefix,
                   const std::string& delimiter,
                   uint32_t num_entries, bool list_versions,
                   std::map<int, rgw_cls_list_ret>& results);

  int handle_overwrite(const DoutPrefixProvider *dpp, const RGWBucketInfo& info,
                       const RGWBucketInfo& orig_info,
		       optional_yield y) override;

  int open_bucket_index_shard(const DoutPrefixProvider *dpp,
                              RGWSI_Bucket_SObj* bucket_sobj,
                              const RGWBucketInfo& bucket_info,
                              const std::string& obj_key,
                              rgw_rados_ref* bucket_obj,
                              std::unique_ptr<rgw::BIShardIdent>& shard_ident);
  int open_bucket_index_shard(const DoutPrefixProvider *dpp,
                              const RGWBucketInfo& bucket_info,
                              const rgw::bucket_index_layout_generation& layout_gen,
                              const rgw::BIShardIdent& shard_ident,
			      rgw_rados_ref* bucket_obj);
  int open_bucket_index_shard(const DoutPrefixProvider *dpp,
                              const rgw::rados::BIndexer::ShardIterator& shard_it,
                              rgw_rados_ref* bucket_obj);

  int open_bucket_index(const DoutPrefixProvider *dpp,
                        const RGWBucketInfo& bucket_info,
                        librados::IoCtx* index_pool,
                        std::string *bucket_oid);

  int open_bucket_index(const DoutPrefixProvider *dpp,
                        const RGWBucketInfo& bucket_info,
                        rgw::BIShardIndex shard_id,
                        const rgw::bucket_index_layout_generation& idx_layout,
                        librados::IoCtx* index_pool,
                        std::map<int, std::string> *bucket_objs,
                        std::map<int, std::string> *bucket_instance_ids);

  std::unique_ptr<rgw::rados::BIndexer> create_bindexer(
    const DoutPrefixProvider* dpp,
    const RGWBucketInfo& bucket_info)
  {
    return rgw::rados::BIndexer::create_unique(cct, dpp, bucket_info);
  }
};
