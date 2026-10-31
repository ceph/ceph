// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#ifdef WITH_RADOSGW_RADOS

#include "radosgw-admin/sync_status_report.h"

#include <iomanip>
#include <list>
#include <sstream>

#include "common/Formatter.h"
#include "common/iso_8601.h"
#include "driver/rados/rgw_rados.h"
#include "driver/rados/rgw_sync.h"
#include "rgw_sal_rados.h"

namespace {

void dump_int_set(Formatter* formatter, const char* name, const std::set<int>& values)
{
  formatter->open_array_section(name);
  for (const int id : values) {
    formatter->dump_int("shard", id);
  }
  formatter->close_section();
}

} // anonymous namespace

void rgw_admin_collect_md_sync_report(const DoutPrefixProvider* dpp,
                                      rgw::sal::Driver* driver,
                                      rgw_admin_md_sync_report* out)
{
  out->error.clear();
  out->meta_master = driver->is_meta_master();
  if (out->meta_master) {
    return;
  }

  auto* rados_store = static_cast<rgw::sal::RadosStore*>(driver);
  RGWMetaSyncStatusManager sync(rados_store, rados_store->svc()->async_processor);

  int ret = sync.init(dpp);
  if (ret < 0) {
    out->error = string("failed to retrieve sync info: sync.init() failed: ") +
                 cpp_strerror(-ret);
    return;
  }

  rgw_meta_sync_status sync_status;
  ret = sync.read_sync_status(dpp, &sync_status);
  if (ret < 0) {
    out->error = string("failed to read sync status: ") + cpp_strerror(-ret);
    return;
  }

  switch (sync_status.sync_info.state) {
  case rgw_meta_sync_info::StateInit:
    out->status = "init";
    break;
  case rgw_meta_sync_info::StateBuildingFullSyncMaps:
    out->status = "preparing for full sync";
    break;
  case rgw_meta_sync_info::StateSync:
    out->status = "syncing";
    break;
  default:
    out->status = "unknown";
  }

  for (auto marker_iter : sync_status.sync_markers) {
    out->full_total += marker_iter.second.total_entries;
    out->total_shards++;
    if (marker_iter.second.state == rgw_meta_sync_marker::SyncState::FullSync) {
      out->num_full++;
      out->full_complete += marker_iter.second.pos;
      out->shards_behind.insert(marker_iter.first);
    } else {
      out->full_complete += marker_iter.second.total_entries;
    }
    if (marker_iter.second.state == rgw_meta_sync_marker::SyncState::IncrementalSync) {
      out->num_inc++;
    }
  }

  out->master_period = rados_store->svc()->zone->get_current_period_id();

  std::map<int, RGWMetadataLogInfo> master_shards_info;
  ret = sync.read_master_log_shards_info(dpp, out->master_period, &master_shards_info);
  if (ret < 0) {
    out->error = string("failed to fetch master sync status: ") + cpp_strerror(-ret);
    return;
  }

  std::map<int, string> shards_behind_map;
  if (sync_status.sync_info.period != out->master_period) {
    out->period_mismatch = true;
    out->local_period = sync_status.sync_info.period;
  } else {
    out->period_mismatch = false;
    for (auto local_iter : sync_status.sync_markers) {
      int shard_id = local_iter.first;
      auto iter = master_shards_info.find(shard_id);
      if (iter == master_shards_info.end()) {
        lderr(rados_store->ctx()) << "ERROR: could not find remote sync shard status for shard_id="
                                  << shard_id << dendl;
        continue;
      }
      auto master_marker = iter->second.marker;
      if (local_iter.second.state == rgw_meta_sync_marker::SyncState::IncrementalSync &&
          master_marker > local_iter.second.marker) {
        shards_behind_map[shard_id] = local_iter.second.marker;
        out->shards_behind.insert(shard_id);
      }
    }
  }

  if (!shards_behind_map.empty()) {
    std::map<int, rgw_mdlog_shard_data> master_pos;
    ret = sync.read_master_log_shards_next(dpp, sync_status.sync_info.period,
                                           shards_behind_map, &master_pos);
    if (ret < 0) {
      lderr(rados_store->ctx()) << "ERROR: failed to fetch master next positions ("
                                << cpp_strerror(-ret) << ")" << dendl;
    } else {
      for (auto iter : master_pos) {
        rgw_mdlog_shard_data& shard_data = iter.second;
        if (shard_data.entries.empty()) {
          shards_behind_map.erase(iter.first);
          out->shards_behind.erase(iter.first);
        } else {
          rgw_mdlog_entry& entry = shard_data.entries.front();
          if (!out->oldest) {
            out->oldest.emplace(iter.first, entry.timestamp);
          } else if (!ceph::real_clock::is_zero(entry.timestamp) &&
                     entry.timestamp < out->oldest->second) {
            out->oldest.emplace(iter.first, entry.timestamp);
          }
        }
      }
    }
  }

  out->total_behind = shards_behind_map.size() +
                      (sync_status.sync_info.num_shards - out->num_inc);
}

void rgw_admin_collect_data_sync_report(const DoutPrefixProvider* dpp,
                                        rgw::sal::Driver* driver,
                                        const rgw_zone_id& source_zone,
                                        rgw_admin_data_sync_report* out)
{
  out->error.clear();
  out->source_zone = source_zone;
  out->not_syncing = false;

  auto* rados_store = static_cast<rgw::sal::RadosStore*>(driver);

  std::unique_ptr<rgw::sal::Zone> sz_sal;
  if (driver->get_zone()->get_zonegroup().get_zone_by_id(source_zone.id, &sz_sal) == 0) {
    out->source_zone_name = sz_sal->get_name();
  }

  RGWZone* sz;
  if (!(sz = rados_store->svc()->zone->find_zone(source_zone))) {
    out->error = "zone not found";
    return;
  }

  if (!rados_store->svc()->zone->zone_syncs_from(*sz)) {
    out->not_syncing = true;
    return;
  }

  RGWDataSyncStatusManager sync(rados_store, rados_store->svc()->async_processor,
                                source_zone, nullptr);

  int ret = sync.init(dpp);
  if (ret < 0) {
    out->error = string("failed to retrieve sync info: ") + cpp_strerror(-ret);
    return;
  }

  rgw_data_sync_status sync_status;
  ret = sync.read_sync_status(dpp, &sync_status);
  if (ret < 0 && ret != -ENOENT) {
    out->error = string("failed read sync status: ") + cpp_strerror(-ret);
    return;
  }

  ret = sync.read_recovering_shards(dpp, sync_status.sync_info.num_shards,
                                    out->recovering_shards);
  if (ret < 0 && ret != ENOENT) {
    out->error = string("failed read recovering shards: ") + cpp_strerror(-ret);
    return;
  }

  switch (sync_status.sync_info.state) {
  case rgw_data_sync_info::StateInit:
    out->status = "init";
    break;
  case rgw_data_sync_info::StateBuildingFullSyncMaps:
    out->status = "preparing for full sync";
    break;
  case rgw_data_sync_info::StateSync:
    out->status = "syncing";
    break;
  default:
    out->status = "unknown";
  }

  for (auto marker_iter : sync_status.sync_markers) {
    out->full_total += marker_iter.second.total_entries;
    out->total_shards++;
    if (marker_iter.second.state == rgw_data_sync_marker::SyncState::FullSync) {
      out->num_full++;
      out->full_complete += marker_iter.second.pos;
      out->shards_behind.insert(marker_iter.first);
    } else {
      out->full_complete += marker_iter.second.total_entries;
    }
    if (marker_iter.second.state == rgw_data_sync_marker::SyncState::IncrementalSync) {
      out->num_inc++;
    }
  }

  std::map<int, RGWDataChangesLogInfo> source_shards_info;
  ret = sync.read_source_log_shards_info(dpp, &source_shards_info);
  if (ret < 0) {
    out->error = string("failed to fetch source sync status: ") + cpp_strerror(-ret);
    return;
  }

  std::map<int, string> shards_behind_map;
  for (auto local_iter : sync_status.sync_markers) {
    int shard_id = local_iter.first;
    auto iter = source_shards_info.find(shard_id);
    if (iter == source_shards_info.end()) {
      lderr(rados_store->ctx()) << "ERROR: could not find remote sync shard status for shard_id="
                                << shard_id << dendl;
      continue;
    }
    auto master_marker = iter->second.marker;
    if (local_iter.second.state == rgw_data_sync_marker::SyncState::IncrementalSync &&
        master_marker > local_iter.second.marker) {
      shards_behind_map[shard_id] = local_iter.second.marker;
      out->shards_behind.insert(shard_id);
    }
  }

  if (!shards_behind_map.empty()) {
    std::map<int, rgw_datalog_shard_data> master_pos;
    ret = sync.read_source_log_shards_next(dpp, shards_behind_map, &master_pos);
    if (ret < 0) {
      lderr(rados_store->ctx()) << "ERROR: failed to fetch next positions ("
                                << cpp_strerror(-ret) << ")" << dendl;
    } else {
      for (auto iter : master_pos) {
        rgw_datalog_shard_data& shard_data = iter.second;
        if (shard_data.entries.empty()) {
          shards_behind_map.erase(iter.first);
          out->shards_behind.erase(iter.first);
        } else {
          rgw_datalog_entry& entry = shard_data.entries.front();
          if (!out->oldest) {
            out->oldest.emplace(iter.first, entry.timestamp);
          } else if (!ceph::real_clock::is_zero(entry.timestamp) &&
                     entry.timestamp < out->oldest->second) {
            out->oldest.emplace(iter.first, entry.timestamp);
          }
        }
      }
    }
  }

  out->total_behind = shards_behind_map.size() +
                      (sync_status.sync_info.num_shards - out->num_inc);
  out->total_recovering = out->recovering_shards.size();
}

void rgw_admin_format_md_sync_plaintext(const rgw_admin_md_sync_report& report,
                                        std::list<std::string>& status)
{
  if (report.meta_master) {
    status.push_back("no sync (zone is master)");
    return;
  }
  if (!report.error.empty()) {
    status.push_back(report.error);
    return;
  }

  status.push_back(report.status);

  std::ostringstream ss;
  ss << "full sync: " << report.num_full << "/" << report.total_shards << " shards";
  status.push_back(ss.str());

  if (report.num_full > 0) {
    std::ostringstream ss2;
    ss2 << "full sync: " << report.full_total - report.full_complete
        << " entries to sync";
    status.push_back(ss2.str());
  }

  {
    std::ostringstream ss3;
    ss3 << "incremental sync: " << report.num_inc << "/" << report.total_shards
        << " shards";
    status.push_back(ss3.str());
  }

  if (report.period_mismatch) {
    status.push_back(string("master is on a different period: master_period=") +
                     report.master_period + " local_period=" + report.local_period);
  }

  if (report.total_behind == 0) {
    status.push_back("metadata is caught up with master");
  } else {
    std::ostringstream ss4;
    ss4 << "metadata is behind on " << report.total_behind << " shards";
    status.push_back(ss4.str());
    std::ostringstream ss5;
    ss5 << "behind shards: " << report.shards_behind;
    status.push_back(ss5.str());
    if (report.oldest) {
      std::ostringstream ss6;
      ss6 << "oldest incremental change not applied: " << report.oldest->second
          << " [" << report.oldest->first << ']';
      status.push_back(ss6.str());
    }
  }
}

void rgw_admin_format_md_sync_json(const rgw_admin_md_sync_report& report,
                                   Formatter* formatter)
{
  formatter->open_object_section("metadata_sync");

  if (report.meta_master) {
    formatter->dump_string("status", "no sync (zone is master)");
    formatter->close_section();
    return;
  }
  if (!report.error.empty()) {
    formatter->dump_string("error", report.error);
    formatter->close_section();
    return;
  }

  formatter->dump_string("status", report.status);

  formatter->open_object_section("full_sync");
  formatter->dump_int("shards", report.num_full);
  formatter->dump_int("total_shards", report.total_shards);
  formatter->dump_unsigned("total_entries", report.full_total);
  formatter->dump_unsigned("complete_entries", report.full_complete);
  if (report.num_full > 0) {
    formatter->dump_unsigned("entries_to_sync",
                             report.full_total - report.full_complete);
  }
  formatter->close_section();

  formatter->open_object_section("incremental_sync");
  formatter->dump_int("shards", report.num_inc);
  formatter->dump_int("total_shards", report.total_shards);
  formatter->close_section();

  if (report.period_mismatch) {
    formatter->dump_string("master_period", report.master_period);
    formatter->dump_string("local_period", report.local_period);
    formatter->dump_bool("period_mismatch", true);
  } else {
    formatter->dump_bool("period_mismatch", false);
  }

  formatter->dump_bool("caught_up", report.total_behind == 0);
  formatter->dump_int("shards_behind", report.total_behind);
  dump_int_set(formatter, "behind_shards", report.shards_behind);
  if (report.oldest) {
    formatter->open_object_section("oldest_incremental_change");
    formatter->dump_int("shard_id", report.oldest->first);
    formatter->dump_string("timestamp",
                           to_iso_8601(report.oldest->second,
                                       iso_8601_format::YMDhmsn));
    formatter->close_section();
  }

  formatter->close_section();
}

void rgw_admin_format_data_sync_plaintext(const rgw_admin_data_sync_report& report,
                                            int tab,
                                            std::list<std::string>& status)
{
  auto push_line = [&](const std::string& line) {
    if (tab > 0) {
      status.push_back(std::string(tab, ' ') + line);
    } else {
      status.push_back(line);
    }
  };

  if (!report.error.empty()) {
    push_line(report.error);
    return;
  }
  if (report.not_syncing) {
    push_line("not syncing from zone");
    return;
  }

  push_line(report.status);

  {
    std::ostringstream ss;
    ss << "full sync: " << report.num_full << "/" << report.total_shards
       << " shards";
    push_line(ss.str());
  }
  if (report.num_full > 0) {
    std::ostringstream ss;
    ss << "full sync: " << report.full_total - report.full_complete
       << " buckets to sync";
    push_line(ss.str());
  }
  {
    std::ostringstream ss;
    ss << "incremental sync: " << report.num_inc << "/" << report.total_shards
       << " shards";
    push_line(ss.str());
  }

  if (report.total_behind == 0 && report.total_recovering == 0) {
    push_line("data is caught up with source");
  } else if (report.total_behind > 0) {
    std::ostringstream ss;
    ss << "data is behind on " << report.total_behind << " shards";
    push_line(ss.str());
    std::ostringstream ss2;
    ss2 << "behind shards: " << report.shards_behind;
    push_line(ss2.str());
    if (report.oldest) {
      std::ostringstream ss3;
      ss3 << "oldest incremental change not applied: " << report.oldest->second
          << " [" << report.oldest->first << ']';
      push_line(ss3.str());
    }
  }

  if (report.total_recovering > 0) {
    std::ostringstream ss;
    ss << report.total_recovering << " shards are recovering";
    push_line(ss.str());
    std::ostringstream ss2;
    ss2 << "recovering shards: " << report.recovering_shards;
    push_line(ss2.str());
  }
}

void rgw_admin_format_data_sync_json(const rgw_admin_data_sync_report& report,
                                     Formatter* formatter)
{
  formatter->open_object_section("source");
  formatter->dump_string("source_zone", report.source_zone.id);
  if (!report.source_zone_name.empty()) {
    formatter->dump_string("source_zone_name", report.source_zone_name);
  }

  if (!report.error.empty()) {
    formatter->dump_string("error", report.error);
    formatter->close_section();
    return;
  }
  if (report.not_syncing) {
    formatter->dump_string("status", "not syncing from zone");
    formatter->close_section();
    return;
  }

  formatter->dump_string("status", report.status);

  formatter->open_object_section("full_sync");
  formatter->dump_int("shards", report.num_full);
  formatter->dump_int("total_shards", report.total_shards);
  formatter->dump_unsigned("total_buckets", report.full_total);
  formatter->dump_unsigned("complete_buckets", report.full_complete);
  if (report.num_full > 0) {
    formatter->dump_unsigned("buckets_to_sync",
                             report.full_total - report.full_complete);
  }
  formatter->close_section();

  formatter->open_object_section("incremental_sync");
  formatter->dump_int("shards", report.num_inc);
  formatter->dump_int("total_shards", report.total_shards);
  formatter->close_section();

  formatter->dump_bool("caught_up",
                       report.total_behind == 0 && report.total_recovering == 0);
  formatter->dump_int("shards_behind", report.total_behind);
  dump_int_set(formatter, "behind_shards", report.shards_behind);
  if (report.oldest) {
    formatter->open_object_section("oldest_incremental_change");
    formatter->dump_int("shard_id", report.oldest->first);
    formatter->dump_string("timestamp",
                           to_iso_8601(report.oldest->second,
                                       iso_8601_format::YMDhmsn));
    formatter->close_section();
  }
  formatter->dump_int("shards_recovering", report.total_recovering);
  dump_int_set(formatter, "recovering_shards", report.recovering_shards);

  formatter->close_section();
}

#endif
