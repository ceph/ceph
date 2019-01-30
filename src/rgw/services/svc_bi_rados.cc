// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <fmt/format.h>

#include "common/expected.h"

#include "svc_bi_rados.h"
#include "svc_bilog_rados.h"
#include "svc_zone.h"

#include "rgw/rgw_bucket.h"
#include "rgw/rgw_cls.h"
#include "rgw/rgw_zone.h"
#include "rgw/rgw_error_code.h"

#include "RADOS/cls/rgw.h"

#define dout_subsys ceph_subsys_rgw

using namespace std::literals;

namespace bc = boost::container;
namespace bs = boost::system;

namespace RCr = RADOS::CLS::rgw;

static auto dir_oid_prefix = ".dir."sv;

RGWSI_BucketIndex_RADOS::RGWSI_BucketIndex_RADOS(CephContext *cct,
                                                 boost::asio::io_context& ioc)
  : RGWSI_BucketIndex(cct, ioc)
{
}

void RGWSI_BucketIndex_RADOS::init(RGWSI_Zone *zone_svc,
                                   RGWSI_RADOS *rados_svc,
                                   RGWSI_BILog_RADOS *bilog_svc,
				   RGWDataChangesLog *_datalog_rados)
{
  svc.zone = zone_svc;
  svc.rados = rados_svc;
  svc.bilog = bilog_svc;
  svc.datalog_rados = _datalog_rados;
}

tl::expected<rgw_pool, bs::error_code>
RGWSI_BucketIndex_RADOS::get_bucket_index_pool(const RGWBucketInfo& bucket_info)
{
  const rgw_pool& explicit_pool
    = bucket_info.bucket.explicit_placement.index_pool;

  if (!explicit_pool.empty()) {
    return explicit_pool;
  }

  auto& zonegroup = svc.zone->get_zonegroup();
  auto& zone_params = svc.zone->get_zone_params();

  const rgw_placement_rule *rule = &bucket_info.placement_rule;
  if (rule->empty()) {
    rule = &zonegroup.default_placement;
  }
  auto iter = zone_params.placement_pools.find(rule->name);
  if (iter == zone_params.placement_pools.end()) {
    ldout(cct, 0) << "could not find placement rule " << *rule
                  << " within zonegroup " << dendl;
    return tl::unexpected(
      rgw_errc::zone_does_not_contain_placement_rule_present_in_zone_group);
  }
  return iter->second.index_pool;
}


tl::expected<RGWSI_RADOS::Pool, bs::error_code>
RGWSI_BucketIndex_RADOS::open_bucket_index_pool(const RGWBucketInfo& bucket_info,
                                                optional_yield y)
{
  return svc.rados->pool(TRY(get_bucket_index_pool(bucket_info)), y, true);
}

tl::expected<std::pair<RGWSI_RADOS::Pool, std::string>, bs::error_code>
RGWSI_BucketIndex_RADOS::open_bucket_index_base(const RGWBucketInfo& bucket_info,
                                                optional_yield y)
{
  const rgw_bucket& bucket = bucket_info.bucket;
  if (bucket.bucket_id.empty()) {
    ldout(cct, 0) << "ERROR: empty bucket_id for bucket operation" << dendl;
    return tl::unexpected(rgw_errc::no_bucket_for_bucket_operation);
  }
  std::string bucket_oid_base = std::string(dir_oid_prefix) + bucket.bucket_id;
  return std::make_pair(TRY(open_bucket_index_pool(bucket_info, y)),
			std::move(bucket_oid_base));
}

tl::expected<std::pair<RGWSI_RADOS::Pool, std::string>,
	     bs::error_code>
RGWSI_BucketIndex_RADOS::open_bucket_index(const RGWBucketInfo& bucket_info,
                                           optional_yield y)
{
  const rgw_bucket& bucket = bucket_info.bucket;
  if (bucket.bucket_id.empty()) {
    ldout(cct, 0) << "ERROR: empty bucket id for bucket operation" << dendl;
    return tl::unexpected(rgw_errc::no_bucket_for_bucket_operation);
  }
  return std::make_pair(TRY(open_bucket_index_pool(bucket_info, y)),
			std::string(dir_oid_prefix) + bucket.bucket_id);
}

namespace {
bc::flat_map<int, std::string>
get_bucket_index_objects(const string& bucket_oid_base,
			 uint32_t num_shards,
			 int shard_id = -1)
{
  bc::flat_map<int, std::string> bucket_objects;
  if (!num_shards) {
    bucket_objects.emplace(0, bucket_oid_base);
  } else {
    if (shard_id < 0) {
      for (uint32_t i = 0; i < num_shards; ++i) {
        bucket_objects.emplace(i,
			       fmt::format("{}.{0:d}", bucket_oid_base, i));
      }
    } else if ((uint32_t)shard_id <= num_shards) {
      bucket_objects.emplace(shard_id,
			     fmt::format("{}.{0:d}",
					 bucket_oid_base, shard_id));
    }
  }
  return bucket_objects;
}

bc::flat_map<int, std::string>
get_bucket_instance_ids(const RGWBucketInfo& bucket_info,
			int shard_id)
{
  const rgw_bucket& bucket = bucket_info.bucket;
  bc::flat_map<int, std::string> result;
  std::string plain_id = bucket.name + ":" + bucket.bucket_id;
  if (!bucket_info.num_shards) {
    result.emplace(0, plain_id);
  } else {
    if (shard_id < 0) {
      for (uint32_t i = 0; i < bucket_info.num_shards; ++i) {
	result.emplace(i, fmt::format("{}:{0:d}", plain_id, i));
      }
    } else if ((uint32_t)shard_id <= bucket_info.num_shards) {
      result.emplace(shard_id, fmt::format("{}:{0:d}", plain_id, shard_id));
    }
  }
  return result;
}
}

tl::expected<std::tuple<RGWSI_RADOS::Pool, bc::flat_map<int, string>,
			bc::flat_map<int, string>>,
	     bs::error_code>
RGWSI_BucketIndex_RADOS::open_bucket_index(const RGWBucketInfo& bucket_info,
					   std::optional<int> _shard_id,
					   optional_yield y)

{
  int shard_id = _shard_id.value_or(-1);
  auto [pool, bucket_oid_base] = TRY(open_bucket_index_base(bucket_info, y));
  auto bucket_objs = get_bucket_index_objects(bucket_oid_base,
					      bucket_info.num_shards, shard_id);
  auto bucket_instance_ids = get_bucket_instance_ids(bucket_info, shard_id);
  return std::make_tuple(std::move(pool),
			 std::move(bucket_objs),
			 std::move(bucket_instance_ids));
}

std::string RGWSI_BucketIndex_RADOS::get_bucket_index_object(const string& bucket_oid_base,
                                                             uint32_t num_shards,
                                                             int shard_id)
{
  if (!num_shards) {
    // By default with no sharding, we use the bucket oid as itself
    return bucket_oid_base;
  } else {
    return fmt::format("{}.{0:d}", bucket_oid_base, shard_id);
  }
}

tl::expected<std::pair<std::string, int>, bs::error_code>
RGWSI_BucketIndex_RADOS::get_bucket_index_object(const string& bucket_oid_base,
                                                 const string& obj_key,
                                                 uint32_t num_shards,
                                                 RGWBucketInfo::BIShardsHashType hash_type)
{
  bs::error_code ec;
  if (hash_type != RGWBucketInfo::MOD) {
    return tl::unexpected(rgw_errc::unsupported_hash_type);
  }
  if (!num_shards) {
    // By default with no sharding, we use the bucket oid as itself
    return std::make_pair(std::move(bucket_oid_base), -1);
  }
  uint32_t sid = bucket_shard_index(obj_key, num_shards);
  return std::make_pair(fmt::format("{}.{0:d}", bucket_oid_base, sid),
                        static_cast<int>(sid));
}


tl::expected<std::pair<RGWSI_RADOS::Obj, int>, bs::error_code>
RGWSI_BucketIndex_RADOS::open_bucket_index_shard(const RGWBucketInfo& bucket_info,
                                                 const string& obj_key,
                                                 optional_yield y)
{
  auto [pool, bucket_oid_base] =
    TRY(open_bucket_index_base(bucket_info, y));

  auto [oid, shard_id] = TRY(
    get_bucket_index_object(
      bucket_oid_base, obj_key, bucket_info.num_shards,
      static_cast<RGWBucketInfo::BIShardsHashType>(
        bucket_info.bucket_index_shard_hash_type)));
  return std::make_pair(svc.rados->obj(pool, oid, {}), shard_id);
}

tl::expected<RGWSI_RADOS::Obj, bs::error_code>
RGWSI_BucketIndex_RADOS::open_bucket_index_shard(const RGWBucketInfo& bucket_info,
                                                 int shard_id,
                                                 optional_yield y)
{
  auto [index_pool, bucket_oid_base] = TRY(open_bucket_index_base(bucket_info,
								  y));
  auto oid = get_bucket_index_object(bucket_oid_base, bucket_info.num_shards,
                                     shard_id);

  return svc.rados->obj(index_pool, oid, {});
}

tl::expected<std::pair<std::vector<rgw_bucket_dir_header>,
		       bc::flat_map<int, string>>,
	     bs::error_code>
RGWSI_BucketIndex_RADOS::cls_bucket_head(const RGWBucketInfo& bucket_info,
					 int shard_id, optional_yield y)
{
  auto [index_pool, oids, bucket_instance_ids] =
    TRY(open_bucket_index(bucket_info, shard_id, y));

  std::vector<rgw_bucket_dir_header> headers;

  auto list_results =
    TRY(rgw::cls::get_dir_header(index_pool, oids,
				 cct->_conf->rgw_bucket_index_max_aio,
				 y));

  for ([[maybe_unused]] auto&& [n, d] : list_results)
    headers.push_back(std::move(d.dir.header));

  return std::make_pair(std::move(headers),
			std::move(bucket_instance_ids));
}


bs::error_code RGWSI_BucketIndex_RADOS::init_index(RGWBucketInfo& bucket_info,
						   optional_yield y)
{
  auto index_pool = TRYE(open_bucket_index_pool(bucket_info, y));
  auto dir_oid = std::string(dir_oid_prefix) + bucket_info.bucket.bucket_id;
  auto bucket_objs = get_bucket_index_objects(dir_oid, bucket_info.num_shards);

  return rgw::cls::bucket_index_init(index_pool, bucket_objs,
				     cct->_conf->rgw_bucket_index_max_aio, y);
}

bs::error_code RGWSI_BucketIndex_RADOS::clean_index(RGWBucketInfo& bucket_info,
						    optional_yield y)
{
  auto index_pool = TRYE(open_bucket_index_pool(bucket_info, y));
  auto dir_oid = std::string(dir_oid_prefix) + bucket_info.bucket.bucket_id;
  auto bucket_objs = get_bucket_index_objects(dir_oid, bucket_info.num_shards);

  return rgw::cls::bucket_index_cleanup(
    index_pool, bucket_objs, cct->_conf->rgw_bucket_index_max_aio, y);
}

tl::expected<RGWBucketEnt, bs::error_code>
RGWSI_BucketIndex_RADOS::read_stats(const RGWBucketInfo& bucket_info,
				    optional_yield y)
{
  RGWBucketEnt result;
  result.bucket = bucket_info.bucket;
  auto&& [headers, _] = TRY(cls_bucket_head(bucket_info, RGW_NO_SHARD, y));

  for (auto& header : headers) {
    RGWObjCategory category = RGWObjCategory::Main;
    auto i = header.stats.find(category);
    if (i != header.stats.end()) {
      auto stats = i->second;
      result.count += stats.num_entries;
      result.size += stats.total_size;
      result.size_rounded += stats.total_size_rounded;
    }
  }

  result.placement_rule = std::move(bucket_info.placement_rule);
  return result;
}

tl::expected<std::vector<cls_rgw_bucket_instance_entry>, bs::error_code>
RGWSI_BucketIndex_RADOS::get_reshard_status(const RGWBucketInfo& bucket_info,
					    optional_yield y)
{
  auto [index_pool, bucket_objs, _] = TRY(open_bucket_index(bucket_info,
							    std::nullopt, y));

  std::vector<cls_rgw_bucket_instance_entry> status;
  status.reserve(bucket_objs.size());
  for (auto i : bucket_objs) {
    auto obj = index_pool.obj(i.second);
    auto entry = rgw::cls::get_bucket_resharding(obj, y);
    if (!entry && entry.error() != bs::errc::no_such_file_or_directory) {
      lderr(cct) << "ERROR: " << __func__
		 << ": cls_rgw_get_bucket_resharding() returned ret="
		 << entry.error() << dendl;
      return tl::unexpected(entry.error());
    }
    status.push_back(std::move(*entry));
  }
  return status;
}

bs::error_code
RGWSI_BucketIndex_RADOS::handle_overwrite(const RGWBucketInfo& info,
					  const RGWBucketInfo& orig_info,
					  optional_yield y)
{
  bs::error_code ec;
  if (orig_info.datasync_flag_enabled() != info.datasync_flag_enabled()) {
    int shards_num = info.num_shards ? info.num_shards : 1;
    int shard_id = info.num_shards ? 0 : -1;

    if (!info.datasync_flag_enabled()) {
      ec = svc.bilog->log_stop(info, -1, y);
    } else {
      ec = svc.bilog->log_start(info, -1, y);
    }
    if (ec) {
      lderr(cct) << "ERROR: failed writing bilog (bucket=" << info.bucket
		 << "); ec=" << ec << dendl;
      return ec;
    }

    for (int i = 0; i < shards_num; ++i, ++shard_id) {
      ec = ceph::to_error_code(svc.datalog_rados->add_entry(info.bucket,
							    shard_id));
      if (ec) {
        lderr(cct) << "ERROR: failed writing data log (info.bucket="
		   << info.bucket << ", shard_id=" << shard_id << ")" << dendl;
	return ec;
      }
    }
  }
  return ec;
}
