// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/sync.h"
#include "radosgw-admin/bucket.h"

#include <iostream>
#include <iomanip>
#include <list>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "common/ceph_json.h"
#include "common/errno.h"
#include "driver/rados/rgw_rados.h"
#include "driver/rados/rgw_sal_rados.h"
#include "rgw_coroutine.h"
#include "rgw_data_sync.h"
#include "rgw_http_client.h"
#include "rgw_meta_sync_status.h"
#include "rgw_sync.h"
#include "rgw_sync_policy.h"
#include "rgw_trim_mdlog.h"
#include "rgw_zone.h"
#include "rgw_sal_config.h"
#include "services/svc_cls.h"
#include "services/svc_mdlog.h"
#include "services/svc_sync_modules.h"
#include "services/svc_zone.h"

using namespace rgw_admin;
using namespace std;

#define CHECK_TRUE(x, msg, err) \
  if (!(x)) { \
    cerr << msg << std::endl; \
    return err; \
  }

namespace {

void flush_ss(stringstream& ss, list<string>& l)
{
  if (!ss.str().empty()) {
    l.push_back(ss.str());
  }
  ss.str("");
}

stringstream& push_ss(stringstream& ss, list<string>& l, int tab = 0)
{
  flush_ss(ss, l);
  if (tab > 0) {
    ss << setw(tab) << "" << setw(1);
  }
  return ss;
}

#ifdef WITH_RADOSGW_RADOS
static void get_md_sync_status(const DoutPrefixProvider* dpp,
                               rgw::sal::Driver* driver,
                               list<string>& status)
{
  RGWMetaSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(driver), static_cast<rgw::sal::RadosStore*>(driver)->svc()->async_processor);

  int ret = sync.init(dpp);
  if (ret < 0) {
    status.push_back(string("failed to retrieve sync info: sync.init() failed: ") + cpp_strerror(-ret));
    return;
  }

  rgw_meta_sync_status sync_status;
  ret = sync.read_sync_status(dpp, &sync_status);
  if (ret < 0) {
    status.push_back(string("failed to read sync status: ") + cpp_strerror(-ret));
    return;
  }

  string status_str;
  switch (sync_status.sync_info.state) {
    case rgw_meta_sync_info::StateInit:
      status_str = "init";
      break;
    case rgw_meta_sync_info::StateBuildingFullSyncMaps:
      status_str = "preparing for full sync";
      break;
    case rgw_meta_sync_info::StateSync:
      status_str = "syncing";
      break;
    default:
      status_str = "unknown";
  }

  status.push_back(status_str);

  uint64_t full_total = 0;
  uint64_t full_complete = 0;

  int num_full = 0;
  int num_inc = 0;
  int total_shards = 0;
  set<int> shards_behind_set;

  for (auto marker_iter : sync_status.sync_markers) {
    full_total += marker_iter.second.total_entries;
    total_shards++;
    if (marker_iter.second.state == rgw_meta_sync_marker::SyncState::FullSync) {
      num_full++;
      full_complete += marker_iter.second.pos;
      int shard_id = marker_iter.first;
      shards_behind_set.insert(shard_id);
    } else {
      full_complete += marker_iter.second.total_entries;
    }
    if (marker_iter.second.state == rgw_meta_sync_marker::SyncState::IncrementalSync) {
      num_inc++;
    }
  }

  stringstream ss;
  push_ss(ss, status) << "full sync: " << num_full << "/" << total_shards << " shards";

  if (num_full > 0) {
    push_ss(ss, status) << "full sync: " << full_total - full_complete << " entries to sync";
  }

  push_ss(ss, status) << "incremental sync: " << num_inc << "/" << total_shards << " shards";

  map<int, RGWMetadataLogInfo> master_shards_info;
  string master_period = static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->get_current_period_id();

  ret = sync.read_master_log_shards_info(dpp, master_period, &master_shards_info);
  if (ret < 0) {
    status.push_back(string("failed to fetch master sync status: ") + cpp_strerror(-ret));
    return;
  }

  map<int, string> shards_behind;
  if (sync_status.sync_info.period != master_period) {
    status.push_back(string("master is on a different period: master_period=" +
                            master_period + " local_period=" + sync_status.sync_info.period));
  } else {
    for (auto local_iter : sync_status.sync_markers) {
      int shard_id = local_iter.first;
      auto iter = master_shards_info.find(shard_id);

      if (iter == master_shards_info.end()) {
        /* huh? */
        ldpp_dout(dpp, -1) << "ERROR: could not find remote sync shard status for shard_id=" << shard_id << dendl;
        continue;
      }
      auto master_marker = iter->second.marker;
      if (local_iter.second.state == rgw_meta_sync_marker::SyncState::IncrementalSync &&
          master_marker > local_iter.second.marker) {
        shards_behind[shard_id] = local_iter.second.marker;
        shards_behind_set.insert(shard_id);
      }
    }
  }

  // fetch remote log entries to determine the oldest change
  std::optional<std::pair<int, ceph::real_time>> oldest;
  if (!shards_behind.empty()) {
    map<int, rgw_mdlog_shard_data> master_pos;
    ret = sync.read_master_log_shards_next(dpp, sync_status.sync_info.period, shards_behind, &master_pos);
    if (ret < 0) {
      ldpp_dout(dpp, -1) << "ERROR: failed to fetch master next positions (" << cpp_strerror(-ret) << ")" << dendl;
    } else {
      for (auto iter : master_pos) {
        rgw_mdlog_shard_data& shard_data = iter.second;

        if (shard_data.entries.empty()) {
          // there aren't any entries in this shard, so we're not really behind
          shards_behind.erase(iter.first);
          shards_behind_set.erase(iter.first);
        } else {
          rgw_mdlog_entry& entry = shard_data.entries.front();
          if (!oldest) {
            oldest.emplace(iter.first, entry.timestamp);
          } else if (!ceph::real_clock::is_zero(entry.timestamp) && entry.timestamp < oldest->second) {
            oldest.emplace(iter.first, entry.timestamp);
          }
        }
      }
    }
  }

  int total_behind = shards_behind.size() + (sync_status.sync_info.num_shards - num_inc);
  if (total_behind == 0) {
    push_ss(ss, status) << "metadata is caught up with master";
  } else {
    push_ss(ss, status) << "metadata is behind on " << total_behind << " shards";
    push_ss(ss, status) << "behind shards: " << "[" << shards_behind_set << "]";
    if (oldest) {
      push_ss(ss, status) << "oldest incremental change not applied: "
          << oldest->second << " [" << oldest->first << ']';
    }
  }

  flush_ss(ss, status);
}

static void get_data_sync_status(const DoutPrefixProvider* dpp,
                               rgw::sal::Driver* driver,
                               const rgw_zone_id& source_zone, list<string>& status, int tab)
{
  stringstream ss;

  RGWZone *sz;

  if (!(sz = static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->find_zone(source_zone))) {
    push_ss(ss, status, tab) << string("zone not found");
    flush_ss(ss, status);
    return;
  }

  if (!static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->zone_syncs_from(*sz)) {
    push_ss(ss, status, tab) << string("not syncing from zone");
    flush_ss(ss, status);
    return;
  }
  RGWDataSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(driver), static_cast<rgw::sal::RadosStore*>(driver)->svc()->async_processor, source_zone, nullptr);

  int ret = sync.init(dpp);
  if (ret < 0) {
    push_ss(ss, status, tab) << string("failed to retrieve sync info: ") + cpp_strerror(-ret);
    flush_ss(ss, status);
    return;
  }

  rgw_data_sync_status sync_status;
  ret = sync.read_sync_status(dpp, &sync_status);
  if (ret < 0 && ret != -ENOENT) {
    push_ss(ss, status, tab) << string("failed read sync status: ") + cpp_strerror(-ret);
    flush_ss(ss, status);
    return;
  }

  set<int> recovering_shards;
  ret = sync.read_recovering_shards(dpp, sync_status.sync_info.num_shards, recovering_shards);
  if (ret < 0 && ret != ENOENT) {
    push_ss(ss, status, tab) << string("failed read recovering shards: ") + cpp_strerror(-ret);
    flush_ss(ss, status);
    return;
  }

  string status_str;
  switch (sync_status.sync_info.state) {
    case rgw_data_sync_info::StateInit:
      status_str = "init";
      break;
    case rgw_data_sync_info::StateBuildingFullSyncMaps:
      status_str = "preparing for full sync";
      break;
    case rgw_data_sync_info::StateSync:
      status_str = "syncing";
      break;
    default:
      status_str = "unknown";
  }

  push_ss(ss, status, tab) << status_str;

  uint64_t full_total = 0;
  uint64_t full_complete = 0;

  int num_full = 0;
  int num_inc = 0;
  int total_shards = 0;
  set<int> shards_behind_set;

  for (auto marker_iter : sync_status.sync_markers) {
    full_total += marker_iter.second.total_entries;
    total_shards++;
    if (marker_iter.second.state == rgw_data_sync_marker::SyncState::FullSync) {
      num_full++;
      full_complete += marker_iter.second.pos;
      int shard_id = marker_iter.first;
      shards_behind_set.insert(shard_id);
    } else {
      full_complete += marker_iter.second.total_entries;
    }
    if (marker_iter.second.state == rgw_data_sync_marker::SyncState::IncrementalSync) {
      num_inc++;
    }
  }

  push_ss(ss, status, tab) << "full sync: " << num_full << "/" << total_shards << " shards";

  if (num_full > 0) {
    push_ss(ss, status, tab) << "full sync: " << full_total - full_complete << " buckets to sync";
  }

  push_ss(ss, status, tab) << "incremental sync: " << num_inc << "/" << total_shards << " shards";

  map<int, RGWDataChangesLogInfo> source_shards_info;

  ret = sync.read_source_log_shards_info(dpp, &source_shards_info);
  if (ret < 0) {
    push_ss(ss, status, tab) << string("failed to fetch source sync status: ") + cpp_strerror(-ret);
    flush_ss(ss, status);
    return;
  }

  map<int, string> shards_behind;

  for (auto local_iter : sync_status.sync_markers) {
    int shard_id = local_iter.first;
    auto iter = source_shards_info.find(shard_id);

    if (iter == source_shards_info.end()) {
      /* huh? */
      ldpp_dout(dpp, -1) << "ERROR: could not find remote sync shard status for shard_id=" << shard_id << dendl;
      continue;
    }
    auto master_marker = iter->second.marker;
    if (local_iter.second.state == rgw_data_sync_marker::SyncState::IncrementalSync &&
        master_marker > local_iter.second.marker) {
      shards_behind[shard_id] = local_iter.second.marker;
      shards_behind_set.insert(shard_id);
    }
  }

  std::optional<std::pair<int, ceph::real_time>> oldest;
  if (!shards_behind.empty()) {
    map<int, rgw_datalog_shard_data> master_pos;
    ret = sync.read_source_log_shards_next(dpp, shards_behind, &master_pos);

    if (ret < 0) {
      ldpp_dout(dpp, -1) << "ERROR: failed to fetch next positions (" << cpp_strerror(-ret) << ")" << dendl;
    } else {
      for (auto iter : master_pos) {
        rgw_datalog_shard_data& shard_data = iter.second;
        if (shard_data.entries.empty()) {
          // there aren't any entries in this shard, so we're not really behind
          shards_behind.erase(iter.first);
          shards_behind_set.erase(iter.first);
        } else {
          rgw_datalog_entry& entry = shard_data.entries.front();
          if (!oldest) {
            oldest.emplace(iter.first, entry.timestamp);
          } else if (!ceph::real_clock::is_zero(entry.timestamp) && entry.timestamp < oldest->second) {
            oldest.emplace(iter.first, entry.timestamp);
          }
        }
      }
    }
  }

  int total_behind = shards_behind.size() + (sync_status.sync_info.num_shards - num_inc);
  int total_recovering = recovering_shards.size();

  if (total_behind == 0 && total_recovering == 0) {
    push_ss(ss, status, tab) << "data is caught up with source";
  } else if (total_behind > 0) {
    push_ss(ss, status, tab) << "data is behind on " << total_behind << " shards";
    push_ss(ss, status, tab) << "behind shards: " << "[" << shards_behind_set << "]";
    if (oldest) {
      push_ss(ss, status, tab) << "oldest incremental change not applied: "
          << oldest->second << " [" << oldest->first << ']';
    }
  }

  if (total_recovering > 0) {
    push_ss(ss, status, tab) << total_recovering << " shards are recovering";
    push_ss(ss, status, tab) << "recovering shards: " << "[" << recovering_shards << "]";
  }

  flush_ss(ss, status);
}

static void tab_dump(const string& header, int width, const list<string>& entries)
{
  string s = header;

  for (auto e : entries) {
    cout << std::setw(width) << s << std::setw(1) << " " << e << std::endl;
    s.clear();
  }
}

// return features that are supported but not enabled
static auto get_disabled_features(const rgw::zone_features::set& enabled) {
  auto features = rgw::zone_features::set{rgw::zone_features::supported.begin(),
                                          rgw::zone_features::supported.end()};
  for (const auto& feature : enabled) {
    features.erase(feature);
  }
  return features;
}


static void sync_status(const DoutPrefixProvider* dpp,
                        rgw::sal::Driver* driver,
                        ceph::Formatter *formatter)
{
  const rgw::sal::ZoneGroup& zonegroup = driver->get_zone()->get_zonegroup();
  rgw::sal::Zone* zone = driver->get_zone();

  int width = 15;

  cout << std::setw(width) << "realm" << std::setw(1) << " " << zone->get_realm_id() << " (" << zone->get_realm_name() << ")" << std::endl;
  cout << std::setw(width) << "zonegroup" << std::setw(1) << " " << zonegroup.get_id() << " (" << zonegroup.get_name() << ")" << std::endl;
  cout << std::setw(width) << "zone" << std::setw(1) << " " << zone->get_id() << " (" << zone->get_name() << ")" << std::endl;
  cout << std::setw(width) << "current time" << std::setw(1) << " "
       << to_iso_8601(ceph::real_clock::now(), iso_8601_format::YMDhms) << std::endl;

  const auto& rzg =
    static_cast<const rgw::sal::RadosZoneGroup&>(zonegroup).get_group();

  cout << std::setw(width) << "zonegroup features enabled: " << rzg.enabled_features << std::endl;
  if (auto d = get_disabled_features(rzg.enabled_features); !d.empty()) {
    cout << std::setw(width) << "                   disabled: " << d << std::endl;
  }

  list<string> md_status;

  if (driver->is_meta_master()) {
    md_status.push_back("no sync (zone is master)");
  } else {
    get_md_sync_status(dpp, driver, md_status);
  }

  tab_dump("metadata sync", width, md_status);

  list<string> data_status;

  auto& zone_conn_map = static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->get_zone_conn_map();

  for (auto iter : zone_conn_map) {
    const rgw_zone_id& source_id = iter.first;
    string source_str = "source: ";
    string s = source_str + source_id.id;
    std::unique_ptr<rgw::sal::Zone> sz;
    if (driver->get_zone()->get_zonegroup().get_zone_by_id(source_id.id, &sz) == 0) {
      s += string(" (") + sz->get_name() + ")";
    }
    data_status.push_back(s);
    get_data_sync_status(dpp, driver, source_id, data_status, source_str.size());
  }

  tab_dump("data sync", width, data_status);
}
#endif

static std::vector<string> convert_bucket_set_to_str_vec(const std::set<rgw_bucket>& bs)
{
  std::vector<string> result;
  result.reserve(bs.size());
  for (auto& b : bs) {
    result.push_back(b.get_key());
  }
  return result;
}

static void get_hint_entities(const DoutPrefixProvider* dpp,
                              rgw::sal::Driver* driver,
                              const std::set<rgw_zone_id>& zones,
                              const std::set<rgw_bucket>& buckets,
			      std::set<rgw_sync_bucket_entity> *hint_entities)
{
  for (auto& zone_id : zones) {
    for (auto& b : buckets) {
      std::unique_ptr<rgw::sal::Bucket> hint_bucket;
      int ret = rgw_admin_init_bucket(dpp, driver, b, &hint_bucket);
      if (ret < 0) {
	ldpp_dout(dpp, 20) << "could not init bucket info for hint bucket=" << b << " ... skipping" << dendl;
	continue;
      }

      hint_entities->insert(rgw_sync_bucket_entity(zone_id, hint_bucket->get_key()));
    }
  }
}

static rgw_zone_id resolve_zone_id(rgw::sal::Driver* driver, const string& s)
{
  std::unique_ptr<rgw::sal::Zone> zone;
  int ret = driver->get_zone()->get_zonegroup().get_zone_by_id(s, &zone);
  if (ret < 0)
    ret = driver->get_zone()->get_zonegroup().get_zone_by_name(s, &zone);
  if (ret < 0)
    return rgw_zone_id(s);

  return rgw_zone_id(zone->get_id());
}

static rgw_zone_id validate_zone_id(rgw::sal::Driver* driver, const rgw_zone_id& zone_id)
{
  return resolve_zone_id(driver, zone_id.id);
}

static int sync_info(const DoutPrefixProvider* dpp,
                     rgw::sal::Driver* driver,
                     std::optional<rgw_zone_id> opt_target_zone, std::optional<rgw_bucket> opt_bucket, ceph::Formatter *formatter)
{
  rgw_zone_id zone_id = opt_target_zone.value_or(driver->get_zone()->get_id());

  auto zone_policy_handler = driver->get_zone()->get_sync_policy_handler();

  RGWBucketSyncPolicyHandlerRef bucket_handler;

  std::optional<rgw_bucket> eff_bucket = opt_bucket;

  auto handler = zone_policy_handler;

  if (eff_bucket) {
    std::unique_ptr<rgw::sal::Bucket> bucket;

    int ret = rgw_admin_init_bucket(dpp, driver, *eff_bucket, &bucket);
    if (ret < 0 && ret != -ENOENT) {
      cerr << "ERROR: init_bucket failed: " << cpp_strerror(-ret) << std::endl;
      return ret;
    }

    if (ret >= 0) {
      rgw::sal::Attrs attrs = bucket->get_attrs();
      bucket_handler.reset(handler->alloc_child(bucket->get_info(), std::move(attrs)));
    } else {
      cerr << "WARNING: bucket not found, simulating result" << std::endl;
      bucket_handler.reset(handler->alloc_child(*eff_bucket, nullopt));
    }

    ret = bucket_handler->init(dpp, null_yield);
    if (ret < 0) {
      cerr << "ERROR: failed to init bucket sync policy handler: " << cpp_strerror(-ret) << " (ret=" << ret << ")" << std::endl;
      return ret;
    }

    handler = bucket_handler;
  }

  std::set<rgw_sync_bucket_pipe> sources;
  std::set<rgw_sync_bucket_pipe> dests;

  handler->get_pipes(&sources, &dests, std::nullopt);

  auto source_hints_vec = convert_bucket_set_to_str_vec(handler->get_source_hints());
  auto target_hints_vec = convert_bucket_set_to_str_vec(handler->get_target_hints());

  std::set<rgw_sync_bucket_pipe> resolved_sources;
  std::set<rgw_sync_bucket_pipe> resolved_dests;

  rgw_sync_bucket_entity self_entity(zone_id, opt_bucket);

  set<rgw_zone_id> source_zones;
  set<rgw_zone_id> target_zones;

  zone_policy_handler->reflect(dpp, nullptr, nullptr,
                               nullptr, nullptr,
                               &source_zones,
                               &target_zones,
                               false); /* relaxed: also get all zones that we allow to sync to/from */

  std::set<rgw_sync_bucket_entity> hint_entities;

  get_hint_entities(dpp, driver, source_zones, handler->get_source_hints(), &hint_entities);
  get_hint_entities(dpp, driver, target_zones, handler->get_target_hints(), &hint_entities);

  for (auto& hint_entity : hint_entities) {
    if (!hint_entity.zone ||
	!hint_entity.bucket) {
      continue; /* shouldn't really happen */
    }

    auto zid = validate_zone_id(driver, *hint_entity.zone);
    auto& hint_bucket = *hint_entity.bucket;

    RGWBucketSyncPolicyHandlerRef hint_bucket_handler;
    int r = driver->get_sync_policy_handler(dpp, zid, hint_bucket, &hint_bucket_handler, null_yield);
    if (r < 0) {
      ldpp_dout(dpp, 20) << "could not get bucket sync policy handler for hint bucket=" << hint_bucket << " ... skipping" << dendl;
      continue;
    }

    hint_bucket_handler->get_pipes(&resolved_dests,
                                   &resolved_sources,
                                   self_entity); /* flipping resolved dests and sources as these are
                                                    relative to the remote entity */
  }

  {
    Formatter::ObjectSection os(*formatter, "result");
    encode_json("sources", sources, formatter);
    encode_json("dests", dests, formatter);
    {
      Formatter::ObjectSection hints_section(*formatter, "hints");
      encode_json("sources", source_hints_vec, formatter);
      encode_json("dests", target_hints_vec, formatter);
    }
    {
      Formatter::ObjectSection resolved_hints_section(*formatter, "resolved-hints-1");
      encode_json("sources", resolved_sources, formatter);
      encode_json("dests", resolved_dests, formatter);
    }
    {
      Formatter::ObjectSection resolved_hints_section(*formatter, "resolved-hints");
      encode_json("sources", handler->get_resolved_source_hints(), formatter);
      encode_json("dests", handler->get_resolved_dest_hints(), formatter);
    }
  }

  formatter->flush(cout);

  return 0;
}

#ifdef WITH_RADOSGW_RADOS
static int trim_sync_error_log(const DoutPrefixProvider* dpp,
                               rgw::sal::Driver* driver,
                               int shard_id, const string& marker, int delay_ms)
{
  auto oid = RGWSyncErrorLogger::get_shard_oid(RGW_SYNC_ERROR_LOG_SHARD_PREFIX,
                                               shard_id);
  // call cls_log_trim() until it returns -ENODATA
  for (;;) {
    int ret = static_cast<rgw::sal::RadosStore*>(driver)->svc()->cls->timelog.trim(dpp, oid, {}, {}, {}, marker, nullptr,
					      null_yield);
    if (ret == -ENODATA) {
      return 0;
    }
    if (ret < 0) {
      return ret;
    }
    if (delay_ms) {
      std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
    }
  }
  // unreachable
}
#endif

static bool symmetrical_flow_opt(const string& opt)
{
  return (opt == "symmetrical" || opt == "symmetric");
}

static bool directional_flow_opt(const string& opt)
{
  return (opt == "directional" || opt == "direction");
}

template <class T>
static bool require_opt(std::optional<T> opt, bool extra_check = true)
{
  if (!opt || !extra_check) {
    return false;
  }
  return true;
}

template <class T>
static bool require_non_empty_opt(std::optional<T> opt, bool extra_check = true)
{
  if (!opt || opt->empty() || !extra_check) {
    return false;
  }
  return true;
}

template <class T>
static void show_result(T& obj,
                        ceph::Formatter *formatter,
                        ostream& os)
{
  encode_json("obj", obj, formatter);

  formatter->flush(cout);
}

class SyncPolicyContext
{
  const DoutPrefixProvider* dpp;
  rgw::sal::Driver* driver;
  rgw::sal::ConfigStore* cfgstore;
  RGWZoneGroup zonegroup;
  std::unique_ptr<rgw::sal::ZoneGroupWriter> zonegroup_writer;

  std::optional<rgw_bucket> b;
  std::unique_ptr<rgw::sal::Bucket> bucket;

  rgw_sync_policy_info *policy{nullptr};

public:
  SyncPolicyContext(const DoutPrefixProvider* _dpp,
                    rgw::sal::Driver* _driver,
                    rgw::sal::ConfigStore* _cfgstore,
                    std::optional<rgw_bucket> _bucket)
      : dpp(_dpp), driver(_driver), cfgstore(_cfgstore), b(std::move(_bucket)) {}

  int init(const string& zonegroup_id, const string& zonegroup_name) {
    int ret = rgw::read_zonegroup(dpp, null_yield, cfgstore,
                                  zonegroup_id, zonegroup_name,
                                  zonegroup, &zonegroup_writer);
    if (ret < 0) {
      cerr << "failed to init zonegroup: " << cpp_strerror(-ret) << std::endl;
      return ret;
    }

    if (!b) {
      policy = &zonegroup.sync_policy;
      return 0;
    }

    ret = rgw_admin_init_bucket(dpp, driver, *b, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return ret;
    }

    if (!bucket->get_info().sync_policy) {
      rgw_sync_policy_info new_policy;
      bucket->get_info().set_sync_policy(std::move(new_policy));
    }

    policy = &(*bucket->get_info().sync_policy);

    return 0;
  }

  int write_policy() {
    if (!b) {
      int ret = zonegroup_writer->write(dpp, null_yield, zonegroup);
      if (ret < 0) {
        cerr << "failed to update zonegroup: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      return 0;
    }

    int ret = bucket->put_info(dpp, false, real_time(), null_yield);
    if (ret < 0) {
      cerr << "failed to driver bucket info: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    return 0;
  }

  rgw_sync_policy_info& get_policy() {
    return *policy;
  }
};


} // anonymous namespace

void init_optional_bucket(std::optional<rgw_bucket>& opt_bucket,
                          std::optional<string>& opt_tenant,
                          std::optional<string>& opt_bucket_name,
                          std::optional<string>& opt_bucket_id)
{
  if (opt_tenant || opt_bucket_name || opt_bucket_id) {
    opt_bucket.emplace();
    if (opt_tenant) {
      opt_bucket->tenant = *opt_tenant;
    }
    if (opt_bucket_name) {
      opt_bucket->name = *opt_bucket_name;
    }
    if (opt_bucket_id) {
      opt_bucket->bucket_id = *opt_bucket_id;
    }
  }
}

int rgw_admin_sync(const DoutPrefixProvider* dpp,
                   rgw::sal::Driver* driver,
                   rgw::sal::ConfigStore* cfgstore,
                   rgw::SiteConfig& site,
                   ceph::Formatter* formatter,
                   ceph::Formatter* zone_formatter,
                   rgw_admin_sync_options& opts)
{
  int shard_id = opts.shard_id;
  int trim_delay_ms = opts.trim_delay_ms;
  bool specified_shard_id = opts.specified_shard_id;
  int ret = 0;

  if (opts.command == OPT::MDLOG_LIST) {
    if (!opts.start_date.empty()) {
      std::cerr << "start-date not allowed." << std::endl;
      return -EINVAL;
    }
    if (!opts.end_date.empty()) {
      std::cerr << "end-date not allowed." << std::endl;
      return -EINVAL;
    }
    if (!opts.end_marker.empty()) {
      std::cerr << "end-marker not allowed." << std::endl;
      return -EINVAL;
    }
    if (!opts.start_marker.empty()) {
      if (opts.marker.empty()) {
	opts.marker = opts.start_marker;
      } else {
	std::cerr << "start-marker and marker not both allowed." << std::endl;
	return -EINVAL;
      }
    }

    int i = (specified_shard_id ? shard_id : 0);

    if (opts.period_id.empty()) {
      // use realm's current period
      RGWRealm realm;
      int ret = rgw::read_realm(dpp, null_yield, cfgstore,
                                opts.realm_id, opts.realm_name, realm);
      if (ret < 0 ) {
        cerr << "failed to load realm: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      opts.period_id = realm.current_period;
      std::cerr << "No --period given, using current period="
          << opts.period_id << std::endl;
    }
    RGWMetadataLog *meta_log = static_cast<rgw::sal::RadosStore*>(driver)->svc()->mdlog->get_log(opts.period_id);

    formatter->open_array_section("entries");
    for (; i < g_ceph_context->_conf->rgw_md_log_max_shards; i++) {
      void *handle;
      vector<cls::log::entry> entries;

      meta_log->init_list_entries(i, {}, {}, opts.marker, &handle);
      bool truncated;
      do {
	int ret = meta_log->list_entries(dpp, handle, 1000, entries, NULL, &truncated, null_yield);
        if (ret < 0) {
          cerr << "ERROR: meta_log->list_entries(): " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        for (auto iter = entries.begin(); iter != entries.end(); ++iter) {
          cls::log::entry& entry = *iter;
          static_cast<rgw::sal::RadosStore*>(driver)->ctl()->meta.mgr->dump_log_entry(entry, formatter);
        }
        formatter->flush(cout);
      } while (truncated);

      meta_log->complete_list_entries(handle);

      if (specified_shard_id)
        break;
    }


    formatter->close_section();
    formatter->flush(cout);
  }

  if (opts.command == OPT::MDLOG_STATUS) {
    int i = (specified_shard_id ? shard_id : 0);

    if (opts.period_id.empty()) {
      // use realm's current period
      RGWRealm realm;
      int ret = rgw::read_realm(dpp, null_yield, cfgstore,
                                opts.realm_id, opts.realm_name, realm);
      if (ret < 0 ) {
        cerr << "failed to load realm: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      opts.period_id = realm.current_period;
      std::cerr << "No --period given, using current period="
          << opts.period_id << std::endl;
    }
    RGWMetadataLog *meta_log = static_cast<rgw::sal::RadosStore*>(driver)->svc()->mdlog->get_log(opts.period_id);

    formatter->open_array_section("entries");

    for (; i < g_ceph_context->_conf->rgw_md_log_max_shards; i++) {
      RGWMetadataLogInfo info;
      meta_log->get_info(dpp, i, &info, null_yield);

      ::encode_json("info", info, formatter);

      if (specified_shard_id)
        break;
    }


    formatter->close_section();
    formatter->flush(cout);
  }

  if (opts.command == OPT::MDLOG_AUTOTRIM) {
    // need a full history for purging old mdlog periods
    static_cast<rgw::sal::RadosStore*>(driver)->svc()->mdlog->init_oldest_log_period(null_yield, dpp, cfgstore);

    RGWCoroutinesManager crs(driver->ctx(), driver->get_cr_registry());
    RGWHTTPManager http(driver->ctx(), crs.get_completion_mgr());
    int ret = http.start();
    if (ret < 0) {
      cerr << "failed to initialize http client with " << cpp_strerror(ret) << std::endl;
      return -ret;
    }

    auto num_shards = g_conf()->rgw_md_log_max_shards;
    auto mltcr = create_admin_meta_log_trim_cr(
      dpp, static_cast<rgw::sal::RadosStore*>(driver), &http, num_shards);
    if (!mltcr) {
      cerr << "Cluster misconfigured! Unable to trim." << std::endl;
      return -EIO;
    }
    ret = crs.run(dpp, mltcr);
    if (ret < 0) {
      cerr << "automated mdlog trim failed with " << cpp_strerror(ret) << std::endl;
      return -ret;
    }
  }

  if (opts.command == OPT::MDLOG_TRIM) {
    if (!opts.start_date.empty()) {
      std::cerr << "start-date not allowed." << std::endl;
      return -EINVAL;
    }
    if (!opts.end_date.empty()) {
      std::cerr << "end-date not allowed." << std::endl;
      return -EINVAL;
    }
    if (!opts.start_marker.empty()) {
      std::cerr << "start-marker not allowed." << std::endl;
      return -EINVAL;
    }
    if (!opts.end_marker.empty()) {
      if (opts.marker.empty()) {
	opts.marker = opts.end_marker;
      } else {
	std::cerr << "end-marker and marker not both allowed." << std::endl;
	return -EINVAL;
      }
    }

    if (!specified_shard_id) {
      cerr << "ERROR: shard-id must be specified for trim operation" << std::endl;
      return EINVAL;
    }

    if (opts.marker.empty()) {
      cerr << "ERROR: marker must be specified for trim operation" << std::endl;
      return EINVAL;
    }

    if (opts.period_id.empty()) {
      std::cerr << "missing --period argument" << std::endl;
      return EINVAL;
    }
    RGWMetadataLog *meta_log = static_cast<rgw::sal::RadosStore*>(driver)->svc()->mdlog->get_log(opts.period_id);

    // trim until -ENODATA
    do {
      ret = meta_log->trim(dpp, shard_id, {}, {}, {}, opts.marker, null_yield);
    } while (ret == 0);
    if (ret < 0 && ret != -ENODATA) {
      cerr << "ERROR: meta_log->trim(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }
  if (opts.command == OPT::SYNC_INFO) {
    ret = sync_info(dpp, driver, opts.opt_effective_zone_id, opts.opt_bucket, zone_formatter);
  }
  if (opts.command == OPT::SYNC_STATUS) {
    if (opts.opt_bucket || opts.opt_bucket_name) {
       cerr << "ERROR: 'sync status' command does not support --bucket option." << std::endl;
       cerr << "Use 'radosgw-admin bucket sync status --bucket=<bucketname>' instead." << std::endl;
       return EINVAL;
    }
    sync_status(dpp, driver, formatter);
  }

  if (opts.command == OPT::METADATA_SYNC_STATUS) {
    if (opts.opt_bucket || opts.opt_bucket_name) {
      cerr << "ERROR: 'metadata sync status' command does not support --bucket option." << std::endl;
      return EINVAL;
    }
    RGWMetaSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(driver), static_cast<rgw::sal::RadosStore*>(driver)->svc()->async_processor);

    int ret = sync.init(dpp);
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }

    rgw_meta_sync_status sync_status;
    ret = sync.read_sync_status(dpp, &sync_status);
    if (ret < 0) {
      cerr << "ERROR: sync.read_sync_status() returned ret=" << ret << std::endl;
      return -ret;
    }

    formatter->open_object_section("summary");
    encode_json("sync_status", sync_status, formatter);

    uint64_t full_total = 0;
    uint64_t full_complete = 0;

    for (auto marker_iter : sync_status.sync_markers) {
      full_total += marker_iter.second.total_entries;
      if (marker_iter.second.state == rgw_meta_sync_marker::SyncState::FullSync) {
        full_complete += marker_iter.second.pos;
      } else {
        full_complete += marker_iter.second.total_entries;
      }
    }

    formatter->open_object_section("full_sync");
    encode_json("total", full_total, formatter);
    encode_json("complete", full_complete, formatter);
    formatter->close_section();
    formatter->dump_string("current_time",
			   to_iso_8601(ceph::real_clock::now(),
				       iso_8601_format::YMDhms));
    formatter->close_section();

    formatter->flush(cout);

  }

  if (opts.command == OPT::METADATA_SYNC_INIT) {
    RGWMetaSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(driver), static_cast<rgw::sal::RadosStore*>(driver)->svc()->async_processor);

    int ret = sync.init(dpp);
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }
    ret = sync.init_sync_status(dpp);
    if (ret < 0) {
      cerr << "ERROR: sync.init_sync_status() returned ret=" << ret << std::endl;
      return -ret;
    }
  }


  if (opts.command == OPT::METADATA_SYNC_RUN) {
    RGWMetaSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(driver), static_cast<rgw::sal::RadosStore*>(driver)->svc()->async_processor);

    int ret = sync.init(dpp);
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }

    ret = sync.run(dpp, null_yield, cfgstore);
    if (ret < 0) {
      cerr << "ERROR: sync.run() returned ret=" << ret << std::endl;
      return -ret;
    }
  }

  if (opts.command == OPT::DATA_SYNC_STATUS) {
    if (opts.source_zone.empty()) {
      cerr << "ERROR: source zone not specified" << std::endl;
      return EINVAL;
    }
    RGWDataSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(driver), static_cast<rgw::sal::RadosStore*>(driver)->svc()->async_processor, opts.source_zone, nullptr);

    int ret = sync.init(dpp);
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }

    rgw_data_sync_status sync_status;
    if (specified_shard_id) {
      set<string> pending_buckets;
      set<string> recovering_buckets;
      rgw_data_sync_marker sync_marker;
      ret = sync.read_shard_status(dpp, shard_id, pending_buckets, recovering_buckets, &sync_marker, 
                                   opts.max_entries.value_or(20));
      if (ret < 0 && ret != -ENOENT) {
        cerr << "ERROR: sync.read_shard_status() returned ret=" << ret << std::endl;
        return -ret;
      }
      formatter->open_object_section("summary");
      encode_json("shard_id", shard_id, formatter);
      encode_json("marker", sync_marker, formatter);
      encode_json("pending_buckets", pending_buckets, formatter);
      encode_json("recovering_buckets", recovering_buckets, formatter);
      formatter->dump_string("current_time",
			     to_iso_8601(ceph::real_clock::now(),
					 iso_8601_format::YMDhms));
      formatter->close_section();
      formatter->flush(cout);
    } else {
      ret = sync.read_sync_status(dpp, &sync_status);
      if (ret < 0 && ret != -ENOENT) {
        cerr << "ERROR: sync.read_sync_status() returned ret=" << ret << std::endl;
        return -ret;
      }

      formatter->open_object_section("summary");
      encode_json("sync_status", sync_status, formatter);

      uint64_t full_total = 0;
      uint64_t full_complete = 0;

      for (auto marker_iter : sync_status.sync_markers) {
        full_total += marker_iter.second.total_entries;
        if (marker_iter.second.state == rgw_meta_sync_marker::SyncState::FullSync) {
          full_complete += marker_iter.second.pos;
        } else {
          full_complete += marker_iter.second.total_entries;
        }
      }

      formatter->open_object_section("full_sync");
      encode_json("total", full_total, formatter);
      encode_json("complete", full_complete, formatter);
      formatter->close_section();
      formatter->dump_string("current_time",
			     to_iso_8601(ceph::real_clock::now(),
					 iso_8601_format::YMDhms));
      formatter->close_section();

      formatter->flush(cout);
    }
  }

  if (opts.command == OPT::DATA_SYNC_INIT) {
    if (opts.source_zone.empty()) {
      cerr << "ERROR: source zone not specified" << std::endl;
      return EINVAL;
    }

    RGWDataSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(driver), static_cast<rgw::sal::RadosStore*>(driver)->svc()->async_processor, opts.source_zone, nullptr);

    int ret = sync.init(dpp);
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }

    ret = sync.init_sync_status(dpp);
    if (ret < 0) {
      cerr << "ERROR: sync.init_sync_status() returned ret=" << ret << std::endl;
      return -ret;
    }
  }

  if (opts.command == OPT::DATA_SYNC_RUN) {
    if (opts.source_zone.empty()) {
      cerr << "ERROR: source zone not specified" << std::endl;
      return EINVAL;
    }

    RGWSyncModuleInstanceRef sync_module;
    int ret = static_cast<rgw::sal::RadosStore*>(driver)->svc()->sync_modules->get_manager()->create_instance(dpp, g_ceph_context, static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->get_zone().tier_type,
        static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->get_zone_params().tier_config, &sync_module);
    if (ret < 0) {
      ldpp_dout(dpp, -1) << "ERROR: failed to init sync module instance, ret=" << ret << dendl;
      return ret;
    }

    RGWDataSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(driver), static_cast<rgw::sal::RadosStore*>(driver)->svc()->async_processor, opts.source_zone, nullptr, sync_module);

    ret = sync.init(dpp);
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }

    ret = sync.run(dpp, cfgstore);
    if (ret < 0) {
      cerr << "ERROR: sync.run() returned ret=" << ret << std::endl;
      return -ret;
    }
  }



  if (opts.command == OPT::SYNC_ERROR_LIST) {
    int max_entries = opts.max_entries.value_or(1000);
    if (!opts.start_date.empty()) {
      std::cerr << "start-date not allowed." << std::endl;
      return -EINVAL;
    }
    if (!opts.end_date.empty()) {
      std::cerr << "end-date not allowed." << std::endl;
      return -EINVAL;
    }
    if (!opts.end_marker.empty()) {
      std::cerr << "end-marker not allowed." << std::endl;
      return -EINVAL;
    }
    if (!opts.start_marker.empty()) {
      if (opts.marker.empty()) {
	opts.marker = opts.start_marker;
      } else {
	std::cerr << "start-marker and marker not both allowed." << std::endl;
	return -EINVAL;
      }
    }

    bool truncated;

    if (shard_id < 0) {
      shard_id = 0;
    }

    formatter->open_array_section("entries");

    for (; shard_id < ERROR_LOGGER_SHARDS; ++shard_id) {
      formatter->open_object_section("shard");
      encode_json("shard_id", shard_id, formatter);
      formatter->open_array_section("entries");

      int count = 0;
      string oid = RGWSyncErrorLogger::get_shard_oid(RGW_SYNC_ERROR_LOG_SHARD_PREFIX, shard_id);

      do {
        vector<cls::log::entry> entries;
        ret = static_cast<rgw::sal::RadosStore*>(driver)->svc()->cls->timelog.list(dpp, oid, {}, {}, max_entries - count, entries, opts.marker, &opts.marker, &truncated,
					      null_yield);
	if (ret == -ENOENT) {
	  break;
        }
        if (ret < 0) {
          cerr << "ERROR: svc.cls->timelog.list(): " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        count += entries.size();

        for (auto& cls_entry : entries) {
          rgw_sync_error_info log_entry;

          auto iter = cls_entry.data.cbegin();
          try {
            decode(log_entry, iter);
          } catch (buffer::error& err) {
            cerr << "ERROR: failed to decode log entry" << std::endl;
            continue;
          }
          formatter->open_object_section("entry");
          encode_json("id", cls_entry.id, formatter);
          encode_json("section", cls_entry.section, formatter);
          encode_json("name", cls_entry.name, formatter);
          encode_json("timestamp", cls_entry.timestamp, formatter);
          encode_json("info", log_entry, formatter);
          formatter->close_section();
          formatter->flush(cout);
        }
      } while (truncated && count < max_entries);

      formatter->close_section();
      formatter->close_section();

      if (specified_shard_id) {
        break;
      }
    }

    formatter->close_section();
    formatter->flush(cout);
  }

  if (opts.command == OPT::SYNC_ERROR_TRIM) {
    if (!opts.start_date.empty()) {
      std::cerr << "start-date not allowed." << std::endl;
      return -EINVAL;
    }
    if (!opts.end_date.empty()) {
      std::cerr << "end-date not allowed." << std::endl;
      return -EINVAL;
    }
    if (!opts.start_marker.empty()) {
      std::cerr << "start-marker not allowed." << std::endl;
      return -EINVAL;
    }
    if (!opts.end_marker.empty()) {
      std::cerr << "end_marker not allowed." << std::endl;
      return -EINVAL;
    }
    if (opts.marker.empty()) {
      opts.marker = "9"; // trims everything
    }

    if (shard_id < 0) {
      shard_id = 0;
    }

    for (; shard_id < ERROR_LOGGER_SHARDS; ++shard_id) {
      ret = trim_sync_error_log(dpp, driver, shard_id, opts.marker, trim_delay_ms);
      if (ret < 0) {
        cerr << "ERROR: sync error trim: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      if (specified_shard_id) {
        break;
      }
    }
  }
  if (opts.command == OPT::SYNC_GROUP_CREATE ||
      opts.command == OPT::SYNC_GROUP_MODIFY) {
    CHECK_TRUE(require_non_empty_opt(opts.opt_group_id), "ERROR: --group-id not specified", EINVAL);
    CHECK_TRUE(require_opt(opts.opt_status), "ERROR: --status is not specified (options: forbidden, allowed, enabled)", EINVAL);

    SyncPolicyContext sync_policy_ctx(dpp, driver, cfgstore, opts.opt_bucket);
    ret = sync_policy_ctx.init(opts.zonegroup_id, opts.zonegroup_name);
    if (ret < 0) {
      return -ret;
    }
    auto& sync_policy = sync_policy_ctx.get_policy();

    if (opts.command == OPT::SYNC_GROUP_MODIFY) {
      auto iter = sync_policy.groups.find(*opts.opt_group_id);
      if (iter == sync_policy.groups.end()) {
        cerr << "ERROR: could not find group '" << *opts.opt_group_id << "'" << std::endl;
        return ENOENT;
      }
    }

    auto& group = sync_policy.groups[*opts.opt_group_id];
    group.id = *opts.opt_group_id;

    if (opts.opt_status) {
      if (!group.set_status(*opts.opt_status)) {
        cerr << "ERROR: unrecognized status (options: forbidden, allowed, enabled)" << std::endl;
        return EINVAL;
      }
    }

    ret = sync_policy_ctx.write_policy();
    if (ret < 0) {
      return -ret;
    }

    show_result(sync_policy, zone_formatter, cout);
  }

  if (opts.command == OPT::SYNC_GROUP_GET) {
    SyncPolicyContext sync_policy_ctx(dpp, driver, cfgstore, opts.opt_bucket);
    ret = sync_policy_ctx.init(opts.zonegroup_id, opts.zonegroup_name);
    if (ret < 0) {
      return -ret;
    }
    auto& sync_policy = sync_policy_ctx.get_policy();

    auto& groups = sync_policy.groups;

    if (!opts.opt_group_id) {
      show_result(groups, zone_formatter, cout);
    } else {
      auto iter = sync_policy.groups.find(*opts.opt_group_id);
      if (iter == sync_policy.groups.end()) {
        cerr << "ERROR: could not find group '" << *opts.opt_group_id << "'" << std::endl;
        return ENOENT;
      }

      show_result(iter->second, zone_formatter, cout);
    }
  }

  if (opts.command == OPT::SYNC_GROUP_REMOVE) {
    CHECK_TRUE(require_non_empty_opt(opts.opt_group_id), "ERROR: --group-id not specified", EINVAL);

    SyncPolicyContext sync_policy_ctx(dpp, driver, cfgstore, opts.opt_bucket);
    ret = sync_policy_ctx.init(opts.zonegroup_id, opts.zonegroup_name);
    if (ret < 0) {
      return -ret;
    }
    auto& sync_policy = sync_policy_ctx.get_policy();

    sync_policy.groups.erase(*opts.opt_group_id);

    ret = sync_policy_ctx.write_policy();
    if (ret < 0) {
      return -ret;
    }

    {
      Formatter::ObjectSection os(*zone_formatter, "result");
      encode_json("sync_policy", sync_policy, zone_formatter);
    }

    zone_formatter->flush(cout);
  }

  if (opts.command == OPT::SYNC_GROUP_FLOW_CREATE) {
    CHECK_TRUE(require_non_empty_opt(opts.opt_group_id), "ERROR: --group-id not specified", EINVAL);
    CHECK_TRUE(require_non_empty_opt(opts.opt_flow_id), "ERROR: --flow-id not specified", EINVAL);
    CHECK_TRUE(require_opt(opts.opt_flow_type),
                           "ERROR: --flow-type not specified (options: symmetrical, directional)", EINVAL);
    CHECK_TRUE((symmetrical_flow_opt(*opts.opt_flow_type) ||
                            directional_flow_opt(*opts.opt_flow_type)),
                           "ERROR: --flow-type invalid (options: symmetrical, directional)", EINVAL);

    SyncPolicyContext sync_policy_ctx(dpp, driver, cfgstore, opts.opt_bucket);
    ret = sync_policy_ctx.init(opts.zonegroup_id, opts.zonegroup_name);
    if (ret < 0) {
      return -ret;
    }
    auto& sync_policy = sync_policy_ctx.get_policy();

    auto iter = sync_policy.groups.find(*opts.opt_group_id);
    if (iter == sync_policy.groups.end()) {
      cerr << "ERROR: could not find group '" << *opts.opt_group_id << "'" << std::endl;
      return ENOENT;
    }

    auto& group = iter->second;

    if (symmetrical_flow_opt(*opts.opt_flow_type)) {
      CHECK_TRUE(require_non_empty_opt(opts.opt_zone_ids), "ERROR: --zones not provided for symmetrical flow, or is empty", EINVAL);

      rgw_sync_symmetric_group *flow_group;

      group.data_flow.find_or_create_symmetrical(*opts.opt_flow_id, &flow_group);

      for (auto& z : *opts.opt_zone_ids) {
        flow_group->zones.insert(z);
      }
    } else { /* directional */
      CHECK_TRUE(require_non_empty_opt(opts.opt_source_zone_id), "ERROR: --source-zone not provided for directional flow rule, or is empty", EINVAL);
      CHECK_TRUE(require_non_empty_opt(opts.opt_dest_zone_id), "ERROR: --dest-zone not provided for directional flow rule, or is empty", EINVAL);

      rgw_sync_directional_rule *flow_rule;

      group.data_flow.find_or_create_directional(*opts.opt_source_zone_id, *opts.opt_dest_zone_id, &flow_rule);
    }

    ret = sync_policy_ctx.write_policy();
    if (ret < 0) {
      return -ret;
    }

    show_result(sync_policy, zone_formatter, cout);
  }

  if (opts.command == OPT::SYNC_GROUP_FLOW_REMOVE) {
    CHECK_TRUE(require_non_empty_opt(opts.opt_group_id), "ERROR: --group-id not specified", EINVAL);
    CHECK_TRUE(require_non_empty_opt(opts.opt_flow_id), "ERROR: --flow-id not specified", EINVAL);
    CHECK_TRUE(require_opt(opts.opt_flow_type),
                           "ERROR: --flow-type not specified (options: symmetrical, directional)", EINVAL);
    CHECK_TRUE((symmetrical_flow_opt(*opts.opt_flow_type) ||
                            directional_flow_opt(*opts.opt_flow_type)),
                           "ERROR: --flow-type invalid (options: symmetrical, directional)", EINVAL);

    SyncPolicyContext sync_policy_ctx(dpp, driver, cfgstore, opts.opt_bucket);
    ret = sync_policy_ctx.init(opts.zonegroup_id, opts.zonegroup_name);
    if (ret < 0) {
      return -ret;
    }
    auto& sync_policy = sync_policy_ctx.get_policy();

    auto iter = sync_policy.groups.find(*opts.opt_group_id);
    if (iter == sync_policy.groups.end()) {
      cerr << "ERROR: could not find group '" << *opts.opt_group_id << "'" << std::endl;
      return ENOENT;
    }

    auto& group = iter->second;

    if (symmetrical_flow_opt(*opts.opt_flow_type)) {
      group.data_flow.remove_symmetrical(*opts.opt_flow_id, opts.opt_zone_ids);
    } else { /* directional */
      CHECK_TRUE(require_non_empty_opt(opts.opt_source_zone_id), "ERROR: --source-zone not provided for directional flow rule, or is empty", EINVAL);
      CHECK_TRUE(require_non_empty_opt(opts.opt_dest_zone_id), "ERROR: --dest-zone not provided for directional flow rule, or is empty", EINVAL);

      group.data_flow.remove_directional(*opts.opt_source_zone_id, *opts.opt_dest_zone_id);
    }
    
    ret = sync_policy_ctx.write_policy();
    if (ret < 0) {
      return -ret;
    }

    show_result(sync_policy, zone_formatter, cout);
  }

  if (opts.command == OPT::SYNC_GROUP_PIPE_CREATE ||
      opts.command == OPT::SYNC_GROUP_PIPE_MODIFY) {
    CHECK_TRUE(require_non_empty_opt(opts.opt_group_id), "ERROR: --group-id not specified", EINVAL);
    CHECK_TRUE(require_non_empty_opt(opts.opt_pipe_id), "ERROR: --pipe-id not specified", EINVAL);
    if (opts.command == OPT::SYNC_GROUP_PIPE_CREATE) {
      CHECK_TRUE(require_non_empty_opt(opts.opt_source_zone_ids), "ERROR: --source-zones not provided or is empty; should be list of zones or '*'", EINVAL);
      CHECK_TRUE(require_non_empty_opt(opts.opt_dest_zone_ids), "ERROR: --dest-zones not provided or is empty; should be list of zones or '*'", EINVAL);
    }

    SyncPolicyContext sync_policy_ctx(dpp, driver, cfgstore, opts.opt_bucket);
    ret = sync_policy_ctx.init(opts.zonegroup_id, opts.zonegroup_name);
    if (ret < 0) {
      return -ret;
    }
    auto& sync_policy = sync_policy_ctx.get_policy();

    auto iter = sync_policy.groups.find(*opts.opt_group_id);
    if (iter == sync_policy.groups.end()) {
      cerr << "ERROR: could not find group '" << *opts.opt_group_id << "'" << std::endl;
      return ENOENT;
    }

    auto& group = iter->second;

    rgw_sync_bucket_pipes *pipe;

    if (opts.command == OPT::SYNC_GROUP_PIPE_CREATE) {
      group.find_pipe(*opts.opt_pipe_id, true, &pipe);
    } else {
      if (!group.find_pipe(*opts.opt_pipe_id, false, &pipe)) {
        cerr << "ERROR: could not find pipe '" << *opts.opt_pipe_id << "'" << std::endl;
        return ENOENT;
      }
    }

    if (opts.opt_source_zone_ids) {
      pipe->source.add_zones(*opts.opt_source_zone_ids);
    }
    pipe->source.set_bucket(opts.opt_source_tenant,
                            opts.opt_source_bucket_name,
                            opts.opt_source_bucket_id);
    if (opts.opt_dest_zone_ids) {
      pipe->dest.add_zones(*opts.opt_dest_zone_ids);
    }
    pipe->dest.set_bucket(opts.opt_dest_tenant,
                            opts.opt_dest_bucket_name,
                            opts.opt_dest_bucket_id);

    pipe->params.source.filter.set_prefix(opts.opt_prefix, !!opts.opt_prefix_rm);
    pipe->params.source.filter.set_tags(opts.tags_add, opts.tags_rm);
    if (opts.opt_dest_owner) {
      pipe->params.dest.set_owner(*opts.opt_dest_owner);
    }
    if (opts.opt_storage_class) {
      pipe->params.dest.set_storage_class(*opts.opt_storage_class);
    }
    if (opts.opt_priority) {
      pipe->params.priority = *opts.opt_priority;
    }
    if (opts.opt_mode) {
      if (*opts.opt_mode == "system") {
        pipe->params.mode = rgw_sync_pipe_params::MODE_SYSTEM;
      } else if (*opts.opt_mode == "user") {
        pipe->params.mode = rgw_sync_pipe_params::MODE_USER;
      } else {
        cerr << "ERROR: bad mode value: should be one of the following: system, user" << std::endl;
        return EINVAL;
      }
    }

    if (!rgw::sal::User::empty(opts.user)) {
      pipe->params.user = opts.user->get_id();
    } else if (pipe->params.mode == rgw_sync_pipe_params::MODE_USER &&
               !pipe->params.user.has_value()) {
      cerr << "ERROR: missing --uid for --mode=user" << std::endl;
      return EINVAL;
    }

    ret = sync_policy_ctx.write_policy();
    if (ret < 0) {
      return -ret;
    }

    show_result(sync_policy, zone_formatter, cout);
  }

  if (opts.command == OPT::SYNC_GROUP_PIPE_REMOVE) {
    CHECK_TRUE(require_non_empty_opt(opts.opt_group_id), "ERROR: --group-id not specified", EINVAL);
    CHECK_TRUE(require_non_empty_opt(opts.opt_pipe_id), "ERROR: --pipe-id not specified", EINVAL);

    SyncPolicyContext sync_policy_ctx(dpp, driver, cfgstore, opts.opt_bucket);
    ret = sync_policy_ctx.init(opts.zonegroup_id, opts.zonegroup_name);
    if (ret < 0) {
      return -ret;
    }
    auto& sync_policy = sync_policy_ctx.get_policy();

    auto iter = sync_policy.groups.find(*opts.opt_group_id);
    if (iter == sync_policy.groups.end()) {
      cerr << "ERROR: could not find group '" << *opts.opt_group_id << "'" << std::endl;
      return ENOENT;
    }

    auto& group = iter->second;

    rgw_sync_bucket_pipes *pipe;

    if (!group.find_pipe(*opts.opt_pipe_id, false, &pipe)) {
      cerr << "ERROR: could not find pipe '" << *opts.opt_pipe_id << "'" << std::endl;
      return ENOENT;
    }

    if (opts.opt_source_zone_ids) {
      pipe->source.remove_zones(*opts.opt_source_zone_ids);
    }

    pipe->source.remove_bucket(opts.opt_source_tenant,
                               opts.opt_source_bucket_name,
                               opts.opt_source_bucket_id);
    if (opts.opt_dest_zone_ids) {
      pipe->dest.remove_zones(*opts.opt_dest_zone_ids);
    }
    pipe->dest.remove_bucket(opts.opt_dest_tenant,
                             opts.opt_dest_bucket_name,
                             opts.opt_dest_bucket_id);

    if (!(opts.opt_source_zone_ids ||
          opts.opt_source_tenant ||
          opts.opt_source_bucket ||
          opts.opt_source_bucket_id ||
          opts.opt_dest_zone_ids ||
          opts.opt_dest_tenant ||
          opts.opt_dest_bucket ||
          opts.opt_dest_bucket_id)) {
      group.remove_pipe(*opts.opt_pipe_id);
    }

    ret = sync_policy_ctx.write_policy();
    if (ret < 0) {
      return -ret;
    }

    show_result(sync_policy, zone_formatter, cout);
  }

  if (opts.command == OPT::SYNC_POLICY_GET) {
    SyncPolicyContext sync_policy_ctx(dpp, driver, cfgstore, opts.opt_bucket);
    ret = sync_policy_ctx.init(opts.zonegroup_id, opts.zonegroup_name);
    if (ret < 0) {
      return -ret;
    }
    auto& sync_policy = sync_policy_ctx.get_policy();

    show_result(sync_policy, zone_formatter, cout);
  }

  return 0;
}
