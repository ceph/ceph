// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/bucket_sync.h"
#include <iostream>
#include <iomanip>
#include "common/ceph_json.h"
#include "fmt/format.h"
#include "driver/rados/rgw_rados.h"
#include "driver/rados/rgw_sal_rados.h"
#include "rgw_sync.h"
#include "cls/rgw/cls_rgw_client.h"
#include "rgw_data_sync.h"
#include "rgw_bucket_sync.h"
#include "rgw_sync_policy.h"
#include "services/svc_zone.h"
#include "driver/rados/rgw_bucket.h"
#include "radosgw-admin/bucket.h"
#include "radosgw-admin/sync_checkpoint.h"


using namespace rgw_admin;
using namespace std;

namespace {


static int rgw_admin_init_bucket_for_sync(const DoutPrefixProvider* dpp,
                                          rgw::sal::Driver* driver,
                                          const string& tenant,
                                          const string& bucket_name,
                                          const string& bucket_id,
                                          std::unique_ptr<rgw::sal::Bucket>* bucket)
{
  int ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, bucket);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  return 0;
}
struct indented {
  int w; // indent width
  std::string header;
  indented(int w, std::string header = "") : w(w), header(header) {}
};
std::ostream& operator<<(std::ostream& out, const indented& h) {
  return out << std::setw(h.w) << h.header << std::setw(1) << ' ';
}

struct bucket_source_sync_info {
  const RGWZone& _source;
  std::string error;
  std::map<int,std::string> shards_behind;
  int total_shards;
  std::string status;
  rgw_bucket bucket_source;

  bucket_source_sync_info(const RGWZone& source): _source(source) {}

  void _print_plaintext(std::ostream& out, int width) const {
    out << indented{width, "source zone"} << _source.id << " (" << _source.name << ")" << std::endl;
    if (!error.empty()) {
      out << indented{width} << error << std::endl;
      return;
    }
    out << indented{width, "source bucket"} << bucket_source << std::endl;
    if (!status.empty()) {
      out << indented{width} << status << std::endl;
      return;
    }
    out << indented{width} << "incremental sync on " << total_shards << " shards\n";
    if (!shards_behind.empty()) {
      out << indented{width} << "bucket is behind on " << shards_behind.size() << " shards\n";
      set<int> shard_ids;
      for (auto const& [shard_id, _] : shards_behind) {
        shard_ids.insert(shard_id);
      }
      out << indented{width} << "behind shards: [" << shard_ids << "]\n";
    } else {
      out << indented{width} << "bucket is caught up with source\n";
    }
  }

  void _print_formatter(std::ostream& out, ceph::Formatter* formatter) const {
    formatter->open_object_section("source");
    formatter->dump_string("source_zone", _source.id);
    formatter->dump_string("source_name", _source.name);

    if (!error.empty()) {
      formatter->dump_string("error", error);
      formatter->close_section();
      formatter->flush(out);
      return;
    }

    formatter->dump_string("source_bucket", bucket_source.name);
    formatter->dump_string("source_bucket_id", bucket_source.bucket_id);

    if (!status.empty()) {
      formatter->dump_string("status", status);
      formatter->close_section();
      formatter->flush(out);
      return;
    }

    formatter->dump_int("total_shards", total_shards);
    formatter->open_array_section("behind_shards");
    for (auto const& [id, marker] : shards_behind) {
      formatter->open_object_section("shard");
      formatter->dump_int("shard_id", id);
      formatter->dump_string("shard_marker", marker);
      formatter->close_section();
    }
    formatter->close_section();
    formatter->close_section();
    formatter->flush(out);
  }
};

#ifdef WITH_RADOSGW_RADOS
static int bucket_source_sync_status(const DoutPrefixProvider *dpp, rgw::sal::RadosStore* driver,
                                     const RGWZone& zone,
                                     const RGWZone& source, RGWRESTConn *conn,
                                     const RGWBucketInfo& bucket_info,
                                     rgw_sync_bucket_pipe pipe,
                                     bucket_source_sync_info& source_sync_info)
{
  // syncing from this zone?
  if (!driver->svc()->zone->zone_syncs_from(zone, source)) {
    source_sync_info.error = "does not sync from zone";
    return 0;
  }

  if (!pipe.source.bucket) {
    source_sync_info.error = fmt::format("{} (): missing source bucket", __func__);
    return -EINVAL;
  }

  std::unique_ptr<rgw::sal::Bucket> source_bucket;
  int r = rgw_admin_init_bucket(dpp, driver, *pipe.source.bucket, &source_bucket);
  if (r < 0) {
    source_sync_info.error = fmt::format("failed to read source bucket info: {}", cpp_strerror(r));
    return r;
  }

  source_sync_info.bucket_source = source_bucket->get_key();

  pipe.source.bucket = source_bucket->get_key();
  pipe.dest.bucket = bucket_info.bucket;

  uint64_t gen = 0;
  std::vector<rgw_bucket_shard_sync_info> shard_status;

  // check for full sync status
  rgw_bucket_sync_status full_status;
  r = rgw_read_bucket_full_sync_status(dpp, driver, pipe, &full_status, null_yield);
  if (r >= 0) {
    if (full_status.state == BucketSyncState::Init) {
      source_sync_info.status = "init: bucket sync has not started";
      return 0;
    }
    if (full_status.state == BucketSyncState::Stopped) {
      source_sync_info.status = "stopped: bucket sync is disabled";
      return 0;
    }
    if (full_status.state == BucketSyncState::Full) {
      source_sync_info.status = fmt::format("full sync: {} objects completed", full_status.full.count);
      return 0;
    }
    gen = full_status.incremental_gen;
    shard_status.resize(full_status.shards_done_with_gen.size());
  } else if (r == -ENOENT) {
    // no full status, but there may be per-shard status from before upgrade
    const auto& logs = source_bucket->get_info().layout.logs;
    if (logs.empty()) {
      source_sync_info.status = "init: bucket sync has not started";
      return 0;
    }
    const auto& log = logs.front();
    if (log.gen > 0) {
      // this isn't the backward-compatible case, so we just haven't started yet
      source_sync_info.status = "init: bucket sync has not started";
      return 0;
    }
    if (log.layout.type != rgw::BucketLogType::InIndex) {
      source_sync_info.error = fmt::format("unrecognized log layout type {}", to_string(log.layout.type));
      return -EINVAL;
    }
    // use shard count from our log gen=0
    shard_status.resize(rgw::num_shards(log.layout.in_index));
  } else {
    source_sync_info.error = fmt::format("failed to read bucket full sync status: {}", cpp_strerror(r));
    return r;
  }

  r = rgw_read_bucket_inc_sync_status(dpp, driver, pipe, gen, &shard_status);
  if (r < 0) {
    source_sync_info.error = fmt::format("failed to read bucket incremental sync status: {}", cpp_strerror(r));
    return r;
  }

  const int total_shards = shard_status.size();
  source_sync_info.total_shards = total_shards;

  rgw_bucket_index_marker_info remote_info;
  BucketIndexShardsManager remote_markers;
  r = rgw_read_remote_bilog_info(dpp, conn, source_bucket->get_key(),
                                 remote_info, remote_markers, null_yield);
  if (r < 0) {
    source_sync_info.error = fmt::format("failed to read remote log: {}", cpp_strerror(r));
    return r;
  }

  std::map<int, std::string> shards_behind;
  for (const auto& r : remote_markers.get()) {
    auto shard_id = r.first;
    if (r.second.empty()) {
      continue; // empty bucket index shard
    }
    if (shard_id >= total_shards) {
      // unexpected shard id. we don't have status for it, so we're behind
      shards_behind[shard_id] = r.second;
      continue;
    }
    auto& m = shard_status[shard_id];
    const auto pos = BucketIndexShardsManager::get_shard_marker(m.inc_marker.position);
    if (pos < r.second) {
      shards_behind[shard_id] = r.second;
    }
  }

  source_sync_info.shards_behind = std::move(shards_behind);
  return 0;
}
#endif

#ifdef WITH_RADOSGW_RADOS
static int bucket_sync_info(const DoutPrefixProvider* dpp,
                            rgw::sal::Driver* driver, const RGWBucketInfo& info,
                              std::ostream& out)
{
  const rgw::sal::ZoneGroup& zonegroup = driver->get_zone()->get_zonegroup();
  rgw::sal::Zone* zone = driver->get_zone();
  constexpr int width = 15;

  out << indented{width, "realm"} << zone->get_realm_id() << " (" << zone->get_realm_name() << ")\n";
  out << indented{width, "zonegroup"} << zonegroup.get_id() << " (" << zonegroup.get_name() << ")\n";
  out << indented{width, "zone"} << zone->get_id() << " (" << zone->get_name() << ")\n";
  out << indented{width, "bucket"} << info.bucket << "\n\n";

  if (!static_cast<rgw::sal::RadosStore*>(driver)->ctl()->bucket->bucket_imports_data(info.bucket, null_yield, dpp)) {
    out << "Sync is disabled for bucket " << info.bucket.name << '\n';
    return 0;
  }

  RGWBucketSyncPolicyHandlerRef handler;

  int r = driver->get_sync_policy_handler(dpp, std::nullopt, info.bucket, &handler, null_yield);
  if (r < 0) {
    ldpp_dout(dpp, -1) << "ERROR: failed to get policy handler for bucket (" << info.bucket << "): r=" << r << ": " << cpp_strerror(-r) << dendl;
    return r;
  }

  auto& sources = handler->get_sources();

  for (auto& m : sources) {
    auto& zone = m.first;
    out << indented{width, "source zone"} << zone << std::endl;
    for (auto& pipe_handler : m.second) {
      out << indented{width, "bucket"} << *pipe_handler.source.bucket << std::endl;
    }
  }

  return 0;
}
#endif

struct bucket_sync_status_info {
  std::vector<bucket_source_sync_info> source_status_info;
  rgw::sal::Zone* _zone;
  const rgw::sal::ZoneGroup* _zonegroup;
  const RGWBucketInfo& _bucket_info;
  const int width = 15;
  std::string error;

  bucket_sync_status_info(const RGWBucketInfo& bucket_info): _bucket_info(bucket_info) {}

  void print(std::ostream& out, bool use_formatter, ceph::Formatter* formatter) {
    if (use_formatter) {
      _print_formatter(out, formatter);
    } else {
      _print_plaintext(out);
    }
  }

  void _print_plaintext(std::ostream& out) {
    out << indented{width, "realm"} << _zone->get_realm_id() << " (" << _zone->get_realm_name() << ")" << std::endl;
    out << indented{width, "zonegroup"} << _zonegroup->get_id() << " (" << _zonegroup->get_name() << ")" << std::endl;
    out << indented{width, "zone"} << _zone->get_id() << " (" << _zone->get_name() << ")" << std::endl;
    out << indented{width, "bucket"} << _bucket_info.bucket << std::endl;
    out << indented{width, "current time"}
      << to_iso_8601(ceph::real_clock::now(), iso_8601_format::YMDhms) << "\n\n";

    if (!error.empty()){
      out << error << std::endl;
    }

    for (const auto &info : source_status_info) {
      info._print_plaintext(out, width);
    }
  }

  void _print_formatter(std::ostream& out, ceph::Formatter* formatter) {
    formatter->open_object_section("test");
    formatter->dump_string("realm", _zone->get_realm_id());
    formatter->dump_string("realm_name", _zone->get_realm_name());
    formatter->dump_string("zonegroup", _zonegroup->get_id());
    formatter->dump_string("zonegroup_name", _zonegroup->get_name());
    formatter->dump_string("zone", _zone->get_id());
    formatter->dump_string("zone_name", _zone->get_name());
    formatter->dump_string("bucket", _bucket_info.bucket.name);
    formatter->dump_string("bucket_instance_id", _bucket_info.bucket.bucket_id);
    formatter->dump_string("current_time", to_iso_8601(ceph::real_clock::now(), iso_8601_format::YMDhms));

    if (!error.empty()) {
      formatter->dump_string("error", error);
    }

    formatter->open_array_section("sources");
    for (const auto &info : source_status_info) {
      info._print_formatter(out, formatter);
    }
    formatter->close_section();

    formatter->close_section();
    formatter->flush(out);
  }

};

#ifdef WITH_RADOSGW_RADOS
static int bucket_sync_status(const DoutPrefixProvider* dpp,
                              rgw::sal::Driver* driver, const RGWBucketInfo& info,
                              const rgw_zone_id& source_zone_id,
			      const std::optional<rgw_bucket>& opt_source_bucket,
                              bucket_sync_status_info& bucket_sync_info)
{
  const rgw::sal::ZoneGroup& zonegroup = driver->get_zone()->get_zonegroup();
  rgw::sal::Zone* zone = driver->get_zone();

  bucket_sync_info._zone = zone;
  bucket_sync_info._zonegroup = &zonegroup;

  if (!static_cast<rgw::sal::RadosStore*>(driver)->ctl()->bucket->bucket_imports_data(info.bucket, null_yield, dpp)) {
    bucket_sync_info.error = fmt::format("Sync is disabled for bucket {} or bucket has no sync sources", info.bucket.name);
    return 0;
  }

  RGWBucketSyncPolicyHandlerRef handler;

  int r = driver->get_sync_policy_handler(dpp, std::nullopt, info.bucket, &handler, null_yield);
  if (r < 0) {
    bucket_sync_info.error = fmt::format("ERROR: failed to get policy handler for bucket ({}): r={}: {}", info.bucket.name, r, cpp_strerror(-r));
    return r;
  }

  auto sources = handler->get_all_sources();

  auto& zone_conn_map = static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->get_zone_conn_map();
  set<rgw_zone_id> zone_ids;

  if (!source_zone_id.empty()) {
    std::unique_ptr<rgw::sal::Zone> zone;
    int ret = driver->get_zone()->get_zonegroup().get_zone_by_id(source_zone_id.id, &zone);
    if (ret < 0) {
      bucket_sync_info.error = fmt::format("Source zone not found in zonegroup {}", zonegroup.get_name());
      return -EINVAL;
    }
    auto c = zone_conn_map.find(source_zone_id);
    if (c == zone_conn_map.end()) {
      bucket_sync_info.error = fmt::format("No connection to zone {}", zone->get_name());
      return -EINVAL;
    }
    zone_ids.insert(source_zone_id);
  } else {
    std::list<std::string> ids;
    int ret = driver->get_zone()->get_zonegroup().list_zones(ids);
    if (ret == 0) {
      for (const auto& entry : ids) {
	zone_ids.insert(entry);
      }
    }
  }

  for (auto& zone_id : zone_ids) {
    auto z = static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->get_zonegroup().zones.find(zone_id.id);
    if (z == static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->get_zonegroup().zones.end()) { /* shouldn't happen */
      continue;
    }
    auto c = zone_conn_map.find(zone_id.id);
    if (c == zone_conn_map.end()) { /* shouldn't happen */
      continue;
    }

    for (auto& entry : sources) {
      auto& pipe = entry.second;
      if (opt_source_bucket &&
	  pipe.source.bucket != opt_source_bucket) {
	continue;
      }
      if (pipe.source.zone.value_or(rgw_zone_id()) == z->second.id) {
        bucket_source_sync_info source_sync_info(z->second);
        bucket_source_sync_status(dpp, static_cast<rgw::sal::RadosStore*>(driver), static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->get_zone(), z->second,
				  c->second,
				  info, pipe,
				  source_sync_info);

        bucket_sync_info.source_status_info.emplace_back(std::move(source_sync_info));
      }
    }
  }

  return 0;
}
#endif

} // anonymous namespace

int rgw_admin_bucket_sync(const DoutPrefixProvider* dpp,
                            rgw::sal::Driver* driver,
                            ceph::Formatter* formatter,
                            RGWBucketAdminOpState& bucket_op,
                            std::unique_ptr<rgw::sal::Bucket>& bucket,
                            const rgw_admin_bucket_sync_options& opts)
{
  auto& command = opts.command;
  auto& tenant = opts.tenant;
  auto& bucket_name = opts.bucket_name;
  auto& bucket_id = opts.bucket_id;
  auto& source_zone = opts.source_zone;
  auto& opt_source_bucket = opts.opt_source_bucket;
  auto opt_retry_delay_ms = opts.opt_retry_delay_ms;
  auto opt_timeout_sec = opts.opt_timeout_sec;
  bool extra_info = opts.extra_info;
  bool format_arg_passed = opts.format_arg_passed;
  int ret = 0;

  if (command == OPT::BUCKET_SYNC_INIT) {
    if (source_zone.empty()) {
      cerr << "ERROR: source zone not specified" << std::endl;
      return EINVAL;
    }
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    int ret = rgw_admin_init_bucket_for_sync(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      return -ret;
    }
    auto opt_sb = opt_source_bucket;
    if (opt_sb && opt_sb->bucket_id.empty()) {
      string sbid;
      std::unique_ptr<rgw::sal::Bucket> sbuck;
      int ret = rgw_admin_init_bucket_for_sync(dpp, driver, opt_sb->tenant, opt_sb->name, sbid, &sbuck);
      if (ret < 0) {
        return -ret;
      }
      opt_sb = sbuck->get_key();
    }

    auto sync = RGWBucketPipeSyncStatusManager::construct(
      dpp, static_cast<rgw::sal::RadosStore*>(driver), source_zone, opt_sb,
      bucket->get_key(), extra_info ? &std::cout : nullptr);

    if (!sync) {
      cerr << "ERROR: sync.init() returned error=" << sync.error() << std::endl;
      return -sync.error();
    }
    ret = (*sync)->init_sync_status(dpp);
    if (ret < 0) {
      cerr << "ERROR: sync.init_sync_status() returned ret=" << ret << std::endl;
      return -ret;
    }
  }

  if (command == OPT::BUCKET_SYNC_CHECKPOINT) {
    std::optional<rgw_zone_id> opt_source_zone;
    if (!source_zone.empty()) {
      opt_source_zone = source_zone;
    }
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    int ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      return -ret;
    }

    if (!static_cast<rgw::sal::RadosStore*>(driver)->ctl()->bucket->bucket_imports_data(bucket->get_key(), null_yield, dpp)) {
      std::cout << "Sync is disabled for bucket " << bucket_name << std::endl;
      return 0;
    }

    RGWBucketSyncPolicyHandlerRef handler;
    ret = driver->get_sync_policy_handler(dpp, std::nullopt, bucket->get_key(), &handler, null_yield);
    if (ret < 0) {
      std::cerr << "ERROR: failed to get policy handler for bucket ("
          << bucket << "): r=" << ret << ": " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    auto timeout_at = ceph::coarse_mono_clock::now() + opt_timeout_sec;
    ret = rgw_bucket_sync_checkpoint(dpp, static_cast<rgw::sal::RadosStore*>(driver), *handler, bucket->get_info(),
                                     opt_source_zone, opt_source_bucket,
                                     opt_retry_delay_ms, timeout_at);
    if (ret < 0) {
      ldpp_dout(dpp, -1) << "bucket sync checkpoint failed: " << cpp_strerror(ret) << dendl;
      return -ret;
    }
  }

  if ((command == OPT::BUCKET_SYNC_DISABLE) || (command == OPT::BUCKET_SYNC_ENABLE)) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    } 
    if (command == OPT::BUCKET_SYNC_DISABLE) {
      bucket_op.set_sync_bucket(false);
    } else {
      bucket_op.set_sync_bucket(true);
    }
    bucket_op.set_tenant(tenant);
    string err_msg;
    ret = RGWBucketAdminOp::sync_bucket(driver, bucket_op, dpp, null_yield, &err_msg);
    if (ret < 0) {
      cerr << err_msg << std::endl;
      return -ret;
    }
  }


  if (command == OPT::BUCKET_SYNC_INFO) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    int ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      return -ret;
    }
    bucket_sync_info(dpp, driver, bucket->get_info(), std::cout);
  }

  if (command == OPT::BUCKET_SYNC_STATUS) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    int ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      return -ret;
    }

    auto bucket_info = bucket->get_info();
    bucket_sync_status_info bucket_sync_info(bucket_info);
 
    ret = bucket_sync_status(dpp, driver, bucket_info, source_zone,
        opt_source_bucket, bucket_sync_info);

    if (ret == 0) {
      bucket_sync_info.print(std::cout, format_arg_passed, formatter);
    } else {
      cerr << "failed to get bucket sync status. see logs for more info" << std::endl;
    }
  }

  if (command == OPT::BUCKET_SYNC_MARKERS) {
    if (source_zone.empty()) {
      cerr << "ERROR: source zone not specified" << std::endl;
      return EINVAL;
    }
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    int ret = rgw_admin_init_bucket_for_sync(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      return -ret;
    }
    auto sync = RGWBucketPipeSyncStatusManager::construct(
      dpp, static_cast<rgw::sal::RadosStore*>(driver), source_zone,
      opt_source_bucket, bucket->get_key(), nullptr);

    if (!sync) {
      cerr << "ERROR: sync.init() returned error=" << sync.error() << std::endl;
      return -sync.error();
    }

    auto sync_status = (*sync)->read_sync_status(dpp);
    if (!sync_status) {
      cerr << "ERROR: sync.read_sync_status() returned error="
	   << sync_status.error() << std::endl;
      return -sync_status.error();
    }

    encode_json("sync_status", *sync_status, formatter);
    formatter->flush(cout);
  }

  if (command == OPT::BUCKET_SYNC_RUN) {
    if (source_zone.empty()) {
      cerr << "ERROR: source zone not specified" << std::endl;
      return EINVAL;
    }
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    int ret = rgw_admin_init_bucket_for_sync(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      return -ret;
    }
    auto sync = RGWBucketPipeSyncStatusManager::construct(
      dpp, static_cast<rgw::sal::RadosStore*>(driver), source_zone,
      opt_source_bucket, bucket->get_key(), extra_info ? &std::cout : nullptr);

    if (!sync) {
      cerr << "ERROR: sync.init() returned error=" << sync.error() << std::endl;
      return -sync.error();
    }

    ret = (*sync)->run(dpp);
    if (ret < 0) {
      cerr << "ERROR: sync.run() returned ret=" << ret << std::endl;
      return -ret;
    }
  }

  return 0;
}

