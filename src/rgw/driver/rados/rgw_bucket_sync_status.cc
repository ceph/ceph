// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_bucket_sync_status.h"

#include <iomanip>
#include <list>
#include <set>

#include <fmt/format.h>

#include "cls/rgw/cls_rgw_client.h"
#include "common/errno.h"
#include "common/iso_8601.h"
#include "rgw_bucket_sync.h"
#include "rgw_sal.h"
#include "rgw_sal_rados.h"
#include "services/svc_zone.h"

std::ostream& operator<<(std::ostream& out, const indented& h)
{
  return out << std::setw(h.w) << h.header << std::setw(1) << ' ';
}

bucket_source_sync_info::bucket_source_sync_info(const RGWZone& source)
  : _source(source)
{
}

void bucket_source_sync_info::print_plaintext(std::ostream& out, int width) const
{
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
    std::set<int> shard_ids;
    for (auto const& [shard_id, _] : shards_behind) {
      shard_ids.insert(shard_id);
    }
    out << indented{width} << "behind shards: [" << shard_ids << "]\n";
  } else {
    out << indented{width} << "bucket is caught up with source\n";
  }
}

void bucket_source_sync_info::print_formatter(std::ostream& out, Formatter* formatter) const
{
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

bucket_sync_status_info::bucket_sync_status_info(const RGWBucketInfo& bucket_info)
  : _bucket_info(bucket_info)
{
}

void bucket_sync_status_info::print(std::ostream& out, bool use_formatter, Formatter* formatter)
{
  if (use_formatter) {
    print_formatter(out, formatter);
  } else {
    print_plaintext(out);
  }
}

void bucket_sync_status_info::print_plaintext(std::ostream& out)
{
  out << indented{width, "realm"} << _zone->get_realm_id() << " (" << _zone->get_realm_name() << ")" << std::endl;
  out << indented{width, "zonegroup"} << _zonegroup->get_id() << " (" << _zonegroup->get_name() << ")" << std::endl;
  out << indented{width, "zone"} << _zone->get_id() << " (" << _zone->get_name() << ")" << std::endl;
  out << indented{width, "bucket"} << _bucket_info.bucket << std::endl;
  out << indented{width, "current time"}
      << to_iso_8601(ceph::real_clock::now(), iso_8601_format::YMDhms) << "\n\n";

  if (!error.empty()) {
    out << error << std::endl;
  }

  for (const auto& info : source_status_info) {
    info.print_plaintext(out, width);
  }
}

void bucket_sync_status_info::print_formatter(std::ostream& out, Formatter* formatter)
{
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
  for (const auto& info : source_status_info) {
    info.print_formatter(out, formatter);
  }
  formatter->close_section();

  formatter->close_section();
  formatter->flush(out);
}

int bucket_source_sync_status(const DoutPrefixProvider* dpp,
                              rgw::sal::RadosStore* driver,
                              const RGWZone& zone,
                              const RGWZone& source,
                              RGWRESTConn* conn,
                              const RGWBucketInfo& bucket_info,
                              rgw_sync_bucket_pipe pipe,
                              bucket_source_sync_info& source_sync_info)
{
  if (!driver->svc()->zone->zone_syncs_from(zone, source)) {
    source_sync_info.error = "does not sync from zone";
    return 0;
  }

  if (!pipe.source.bucket) {
    source_sync_info.error = fmt::format("{} (): missing source bucket", __func__);
    return -EINVAL;
  }

  std::unique_ptr<rgw::sal::Bucket> source_bucket;
  int r = driver->load_bucket(dpp, *pipe.source.bucket, &source_bucket, null_yield);
  if (r < 0) {
    source_sync_info.error = fmt::format("failed to read source bucket info: {}", cpp_strerror(r));
    return r;
  }

  source_sync_info.bucket_source = source_bucket->get_key();

  pipe.source.bucket = source_bucket->get_key();
  pipe.dest.bucket = bucket_info.bucket;

  uint64_t gen = 0;
  std::vector<rgw_bucket_shard_sync_info> shard_status;

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
    const auto& logs = source_bucket->get_info().layout.logs;
    if (logs.empty()) {
      source_sync_info.status = "init: bucket sync has not started";
      return 0;
    }
    const auto& log = logs.front();
    if (log.gen > 0) {
      source_sync_info.status = "init: bucket sync has not started";
      return 0;
    }
    if (log.layout.type != rgw::BucketLogType::InIndex &&
        log.layout.type != rgw::BucketLogType::FIFO) {
      source_sync_info.error = fmt::format("unrecognized log layout type {}", to_string(log.layout.type));
      return -EINVAL;
    }
    if (log.layout.type == rgw::BucketLogType::FIFO) {
      shard_status.resize(rgw::num_shards(log.layout.fifo));
    } else {
      shard_status.resize(rgw::num_shards(log.layout.in_index));
    }
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
  for (const auto& rm : remote_markers.get()) {
    auto shard_id = rm.first;
    if (rm.second.empty()) {
      continue;
    }
    if (shard_id >= total_shards) {
      shards_behind[shard_id] = rm.second;
      continue;
    }
    auto& m = shard_status[shard_id];
    const auto pos = BucketIndexShardsManager::get_shard_marker(m.inc_marker.position);
    if (pos < rm.second) {
      shards_behind[shard_id] = rm.second;
    }
  }

  source_sync_info.shards_behind = std::move(shards_behind);
  return 0;
}

int bucket_sync_status(const DoutPrefixProvider* dpp,
                       rgw::sal::Driver* driver,
                       const RGWBucketInfo& info,
                       const rgw_zone_id& source_zone_id,
                       std::optional<rgw_bucket>& opt_source_bucket,
                       bucket_sync_status_info& bucket_sync_info)
{
  const rgw::sal::ZoneGroup& zonegroup = driver->get_zone()->get_zonegroup();
  rgw::sal::Zone* zone = driver->get_zone();

  bucket_sync_info._zone = zone;
  bucket_sync_info._zonegroup = &zonegroup;

  auto* rados = static_cast<rgw::sal::RadosStore*>(driver);
  if (!rados->ctl()->bucket->bucket_imports_data(info.bucket, null_yield, dpp)) {
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

  auto& zone_conn_map = rados->svc()->zone->get_zone_conn_map();
  std::set<rgw_zone_id> zone_ids;

  if (!source_zone_id.empty()) {
    std::unique_ptr<rgw::sal::Zone> z;
    int ret = driver->get_zone()->get_zonegroup().get_zone_by_id(source_zone_id.id, &z);
    if (ret < 0) {
      bucket_sync_info.error = fmt::format("Source zone not found in zonegroup {}", zonegroup.get_name());
      return -EINVAL;
    }
    auto c = zone_conn_map.find(source_zone_id);
    if (c == zone_conn_map.end()) {
      bucket_sync_info.error = fmt::format("No connection to zone {}", z->get_name());
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

  auto& zonegroup_zones = rados->svc()->zone->get_zonegroup().zones;
  const RGWZone& local_zone = rados->svc()->zone->get_zone();

  for (auto& zone_id : zone_ids) {
    auto z = zonegroup_zones.find(zone_id.id);
    if (z == zonegroup_zones.end()) {
      continue;
    }
    auto c = zone_conn_map.find(zone_id.id);
    if (c == zone_conn_map.end()) {
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
        bucket_source_sync_status(dpp, rados, local_zone, z->second,
                                  c->second,
                                  info, pipe,
                                  source_sync_info);

        bucket_sync_info.source_status_info.emplace_back(std::move(source_sync_info));
      }
    }
  }

  return 0;
}