// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <experimental/iterator>
#include <locale>
#include <sstream>

#include "mon/OSDMonitor.h"
#include "mon/Monitor.h"
#include "mon/MDSMonitor.h"
#include "mon/MgrStatMonitor.h"
#include "mon/AuthMonitor.h"
#include "mon/KVMonitor.h"

#include "mon/MonitorDBStore.h"
#include "mon/Session.h"

#include "crush/CrushWrapper.h"
#include "crush/CrushTester.h"
#include "crush/CrushTreeDumper.h"

#include "messages/MOSDBeacon.h"
#include "messages/MOSDFailure.h"
#include "messages/MOSDMarkMeDown.h"
#include "messages/MOSDMarkMeDead.h"
#include "messages/MOSDFull.h"
#include "messages/MOSDMap.h"
#include "messages/MMonGetOSDMap.h"
#include "messages/MOSDBoot.h"
#include "messages/MOSDAlive.h"
#include "messages/MPoolOp.h"
#include "messages/MPoolOpReply.h"
#include "messages/MOSDPGCreate2.h"
#include "messages/MOSDPGCreated.h"
#include "messages/MOSDPGTemp.h"
#include "messages/MOSDPGReadyToMerge.h"
#include "messages/MMonCommand.h"
#include "messages/MRemoveSnaps.h"
#include "messages/MRoute.h"
#include "messages/MMonGetPurgedSnaps.h"
#include "messages/MMonGetPurgedSnapsReply.h"

#include "common/TextTable.h"
#include "common/Timer.h"
#include "common/ceph_argparse.h"
#include "common/perf_counters.h"
#include "common/PriorityCache.h"
#include "common/strtol.h"
#include "common/numa.h"

#include "common/config.h"
#include "common/errno.h"

#include "erasure-code/ErasureCodePlugin.h"
#include "compressor/Compressor.h"
#include "common/Checksummer.h"

#include "include/compat.h"
#include "include/ceph_assert.h"
#include "include/stringify.h"
#include "include/util.h"
#include "common/cmdparse.h"
#include "include/str_list.h"
#include "include/str_map.h"
#include "include/scope_guard.h"
#include "perfglue/heap_profiler.h"

#include "auth/cephx/CephxKeyServer.h"
#include "osd/OSDCap.h"

#include "json_spirit/json_spirit_reader.h"

#include <boost/algorithm/string/predicate.hpp>

using std::dec;
using std::hex;
using std::list;
using std::map;
using std::make_pair;
using std::ostringstream;
using std::pair;
using std::set;
using std::string;
using std::stringstream;
using std::to_string;
using std::vector;

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;
using ceph::ErasureCodeInterfaceRef;
using ceph::ErasureCodePluginRegistry;
using ceph::ErasureCodeProfile;
using ceph::Formatter;
using ceph::JSONFormatter;
using ceph::make_message;

#define dout_subsys ceph_subsys_mon
static const string OSD_PG_CREATING_PREFIX("osd_pg_creating");
static const string OSD_METADATA_PREFIX("osd_metadata");
static const string OSD_SNAP_PREFIX("osd_snap");

/*

  OSD snapshot metadata
  ---------------------

  -- starting with mimic, removed in octopus --

  "removed_epoch_%llu_%08lx" % (pool, epoch)
   -> interval_set<snapid_t>

  "removed_snap_%llu_%016llx" % (pool, last_snap)
   -> { first_snap, end_snap, epoch }   (last_snap = end_snap - 1)


  -- starting with mimic --

  "purged_snap_%llu_%016llx" % (pool, last_snap)
   -> { first_snap, end_snap, epoch }   (last_snap = end_snap - 1)

  - note that the {removed,purged}_snap put the last snap in they key so
    that we can use forward iteration only to search for an epoch in an
    interval.  e.g., to test if epoch N is removed/purged, we'll find a key
    >= N that either does or doesn't contain the given snap.


  -- starting with octopus --

  "purged_epoch_%08lx" % epoch
  -> map<int64_t,interval_set<snapid_t>>

  */
using namespace TOPNSPC::common;
namespace {

struct OSDMemCache : public PriorityCache::PriCache {
  OSDMonitor *osdmon;
  int64_t cache_bytes[PriorityCache::Priority::LAST+1] = {0};
  int64_t committed_bytes = 0;
  double cache_ratio = 0;

  OSDMemCache(OSDMonitor *m) : osdmon(m) {};

  virtual uint64_t _get_used_bytes() const = 0;

  virtual int64_t request_cache_bytes(
      PriorityCache::Priority pri, uint64_t total_cache) const {
    int64_t assigned = get_cache_bytes(pri);

    switch (pri) {
    // All cache items are currently set to have PRI1 priority
    case PriorityCache::Priority::PRI1:
      {
        int64_t request = _get_used_bytes();
        return (request > assigned) ? request - assigned : 0;
      }
    default:
      break;
    }
    return -EOPNOTSUPP;
  }

  virtual int64_t get_cache_bytes(PriorityCache::Priority pri) const {
      return cache_bytes[pri];
  }

  virtual int64_t get_cache_bytes() const {
    int64_t total = 0;

    for (int i = 0; i < PriorityCache::Priority::LAST + 1; i++) {
      PriorityCache::Priority pri = static_cast<PriorityCache::Priority>(i);
      total += get_cache_bytes(pri);
    }
    return total;
  }

  virtual void set_cache_bytes(PriorityCache::Priority pri, int64_t bytes) {
    cache_bytes[pri] = bytes;
  }
  virtual void add_cache_bytes(PriorityCache::Priority pri, int64_t bytes) {
    cache_bytes[pri] += bytes;
  }
  virtual int64_t commit_cache_size(uint64_t total_cache) {
    committed_bytes = PriorityCache::get_chunk(
        get_cache_bytes(), total_cache);
    return committed_bytes;
  }
  virtual int64_t get_committed_size() const {
    return committed_bytes;
  }
  virtual double get_cache_ratio() const {
    return cache_ratio;
  }
  virtual void set_cache_ratio(double ratio) {
    cache_ratio = ratio;
  }
  virtual void shift_bins() {
  }
  virtual void import_bins(const std::vector<uint64_t> &bins) {
  }
  virtual void set_bins(PriorityCache::Priority pri, uint64_t end_bin) {
  }
  virtual uint64_t get_bins(PriorityCache::Priority pri) const {
    return 0;
  }

  virtual string get_cache_name() const = 0;
};

struct IncCache : public OSDMemCache {
  IncCache(OSDMonitor *m) : OSDMemCache(m) {};

  virtual uint64_t _get_used_bytes() const {
    return osdmon->inc_osd_cache.get_bytes();
  }

  virtual string get_cache_name() const {
    return "OSDMap Inc Cache";
  }

  uint64_t _get_num_osdmaps() const {
    return osdmon->inc_osd_cache.get_size();
  }
};

struct FullCache : public OSDMemCache {
  FullCache(OSDMonitor *m) : OSDMemCache(m) {};

  virtual uint64_t _get_used_bytes() const {
    return osdmon->full_osd_cache.get_bytes();
  }

  virtual string get_cache_name() const {
    return "OSDMap Full Cache";
  }

  uint64_t _get_num_osdmaps() const {
    return osdmon->full_osd_cache.get_size();
  }
};

std::shared_ptr<IncCache> inc_cache;
std::shared_ptr<FullCache> full_cache;

const uint32_t MAX_POOL_APPLICATIONS = 4;
const uint32_t MAX_POOL_APPLICATION_KEYS = 64;
const uint32_t MAX_POOL_APPLICATION_LENGTH = 128;

bool is_osd_writable(const OSDCapGrant& grant, const std::string* pool_name) {
  // Note: this doesn't include support for the application tag match
  if ((grant.spec.allow & OSD_CAP_W) != 0) {
    auto& match = grant.match;
    if (match.is_match_all()) {
      return true;
    } else if (pool_name != nullptr &&
               !match.pool_namespace.pool_name.empty() &&
               match.pool_namespace.pool_name == *pool_name) {
      return true;
    }
  }
  return false;
}

bool is_unmanaged_snap_op_permitted(CephContext* cct,
                                    const KeyServer& key_server,
                                    const EntityName& entity_name,
                                    const MonCap& mon_caps,
				    const entity_addr_t& peer_socket_addr,
                                    const std::string* pool_name)
{
  typedef std::map<std::string, std::string> CommandArgs;

  if (mon_caps.is_capable(
	cct, entity_name, "osd",
	"osd pool op unmanaged-snap",
	(pool_name == nullptr ?
	 CommandArgs{} /* pool DNE, require unrestricted cap */ :
	 CommandArgs{{"poolname", *pool_name}}),
	false, true, false,
	peer_socket_addr)) {
    return true;
  }

  AuthCapsInfo caps_info;
  if (!key_server.get_service_caps(entity_name, CEPH_ENTITY_TYPE_OSD,
                                   caps_info)) {
    dout(10) << "unable to locate OSD cap data for " << entity_name
             << " in auth db" << dendl;
    return false;
  }

  string caps_str;
  if (caps_info.caps.length() > 0) {
    auto p = caps_info.caps.cbegin();
    try {
      decode(caps_str, p);
    } catch (const ceph::buffer::error &err) {
      derr << "corrupt OSD cap data for " << entity_name << " in auth db"
           << dendl;
      return false;
    }
  }

  OSDCap osd_cap;
  if (!osd_cap.parse(caps_str, nullptr)) {
    dout(10) << "unable to parse OSD cap data for " << entity_name
             << " in auth db" << dendl;
    return false;
  }

  // if the entity has write permissions in one or all pools, permit
  // usage of unmanaged-snapshots
  if (osd_cap.allow_all()) {
    return true;
  }

  for (auto& grant : osd_cap.grants) {
    if (grant.profile.is_valid()) {
      for (auto& profile_grant : grant.profile_grants) {
        if (is_osd_writable(profile_grant, pool_name)) {
          return true;
        }
      }
    } else if (is_osd_writable(grant, pool_name)) {
      return true;
    }
  }

  return false;
}

} // anonymous namespace

void LastEpochClean::Lec::report(unsigned pg_num, ps_t ps,
				 epoch_t last_epoch_clean)
{
  if (ps >= pg_num) {
    // removed PG
    return;
  }
  epoch_by_pg.resize(pg_num, 0);
  const auto old_lec = epoch_by_pg[ps];
  if (old_lec >= last_epoch_clean) {
    // stale lec
    return;
  }
  epoch_by_pg[ps] = last_epoch_clean;
  if (last_epoch_clean < floor) {
    floor = last_epoch_clean;
  } else if (last_epoch_clean > floor) {
    if (old_lec == floor) {
      // probably should increase floor?
      auto new_floor = std::min_element(std::begin(epoch_by_pg),
					std::end(epoch_by_pg));
      floor = *new_floor;
    }
  }
  if (ps != next_missing) {
    return;
  }
  for (; next_missing < epoch_by_pg.size(); next_missing++) {
    if (epoch_by_pg[next_missing] == 0) {
      break;
    }
  }
}

void LastEpochClean::remove_pool(uint64_t pool)
{
  report_by_pool.erase(pool);
}

void LastEpochClean::report(unsigned pg_num, const pg_t& pg,
			    epoch_t last_epoch_clean)
{
  auto& lec = report_by_pool[pg.pool()];
  return lec.report(pg_num, pg.ps(), last_epoch_clean);
}

epoch_t LastEpochClean::get_lower_bound_by_pool(const OSDMap& latest) const
{
  auto floor = latest.get_epoch();
  for (auto& pool : latest.get_pools()) {
    auto reported = report_by_pool.find(pool.first);
    if (reported == report_by_pool.end()) {
      return 0;
    }
    if (reported->second.next_missing < pool.second.get_pg_num()) {
      return 0;
    }
    if (reported->second.floor < floor) {
      floor = reported->second.floor;
    }
  }
  return floor;
}

void LastEpochClean::dump(Formatter *f) const
{
  f->open_array_section("per_pool");

  for (auto& [pool, lec] : report_by_pool) {
    f->open_object_section("pool");
    f->dump_unsigned("poolid", pool);
    f->dump_unsigned("floor", lec.floor);
    f->close_section();
  }

  f->close_section();
}

class C_UpdateCreatingPGs : public Context {
public:
  OSDMonitor *osdmon;
  utime_t start;
  epoch_t epoch;
  C_UpdateCreatingPGs(OSDMonitor *osdmon, epoch_t e) :
    osdmon(osdmon), start(ceph_clock_now()), epoch(e) {}
  void finish(int r) override {
    if (r >= 0) {
      utime_t end = ceph_clock_now();
      dout(10) << "osdmap epoch " << epoch << " mapping took "
	       << (end - start) << " seconds" << dendl;
      osdmon->update_creating_pgs();
      osdmon->check_pg_creates_subs();
    }
  }
};

#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, osdmap)
static ostream& _prefix(std::ostream *_dout, Monitor &mon, const OSDMap& osdmap) {
  return *_dout << "mon." << mon.name << "@" << mon.rank
		<< "(" << mon.get_state_name()
		<< ").osd e" << osdmap.get_epoch() << " ";
}

OSDMonitor::OSDMonitor(
  CephContext *cct,
  Monitor &mn,
  Paxos &p,
  const string& service_name)
 : PaxosService(mn, p, service_name),
   cct(cct),
   inc_osd_cache(g_conf()->mon_osd_cache_size),
   full_osd_cache(g_conf()->mon_osd_cache_size),
   has_osdmap_manifest(false),
   mapper(mn.cct, &mn.cpu_tp)
{
  inc_cache = std::make_shared<IncCache>(this);
  full_cache = std::make_shared<FullCache>(this);
  cct->_conf.add_observer(this);
  int r = _set_cache_sizes();
  if (r < 0) {
    derr << __func__ << " using default osd cache size - mon_osd_cache_size ("
         << g_conf()->mon_osd_cache_size
         << ") without priority cache management"
         << dendl;
  }
}

const char **OSDMonitor::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    "mon_memory_target",
    "mon_memory_autotune",
    "rocksdb_cache_size",
    NULL
  };
  return KEYS;
}

void OSDMonitor::handle_conf_change(const ConfigProxy& conf,
                                    const std::set<std::string> &changed)
{
  dout(10) << __func__ << " " << changed << dendl;

  if (changed.count("mon_memory_autotune")) {
    _set_cache_autotuning();
  }
  if (changed.count("mon_memory_target") ||
      changed.count("rocksdb_cache_size")) {
    int r = _update_mon_cache_settings();
    if (r < 0) {
      derr << __func__ << " mon_memory_target:"
           << g_conf()->mon_memory_target
           << " rocksdb_cache_size:"
           << g_conf()->rocksdb_cache_size
           << ". Unable to update cache size."
           << dendl;
    }
  }
}

void OSDMonitor::_set_cache_autotuning()
{
  if (!g_conf()->mon_memory_autotune && pcm != nullptr) {
    // Disable cache autotuning
    std::lock_guard l(balancer_lock);
    pcm = nullptr;
  }

  if (g_conf()->mon_memory_autotune && pcm == nullptr) {
    int r = register_cache_with_pcm();
    if (r < 0) {
      dout(10) << __func__
               << " Error while registering osdmon caches with pcm."
               << " Cache auto tuning not enabled."
               << dendl;
      mon_memory_autotune = false;
    } else {
      mon_memory_autotune = true;
    }
  }
}

int OSDMonitor::_update_mon_cache_settings()
{
  if (g_conf()->mon_memory_target <= 0 ||
      g_conf()->mon_memory_target < mon_memory_min ||
      g_conf()->rocksdb_cache_size <= 0) {
    return -EINVAL;
  }

  if (pcm == nullptr && rocksdb_binned_kv_cache == nullptr) {
    derr << __func__ << " not using pcm and rocksdb" << dendl;
    return -EINVAL;
  }

  uint64_t old_mon_memory_target = mon_memory_target;
  uint64_t old_rocksdb_cache_size = rocksdb_cache_size;

  // Set the new pcm memory cache sizes
  mon_memory_target = g_conf()->mon_memory_target;
  rocksdb_cache_size = g_conf()->rocksdb_cache_size;

  uint64_t base = mon_memory_base;
  double fragmentation = mon_memory_fragmentation;
  uint64_t target = mon_memory_target;
  uint64_t min = mon_memory_min;
  uint64_t max = min;

  uint64_t ltarget = (1.0 - fragmentation) * target;
  if (ltarget > base + min) {
    max = ltarget - base;
  }

  int r = _set_cache_ratios();
  if (r < 0) {
    derr << __func__ << " Cache ratios for pcm could not be set."
         << " Review the kv (rocksdb) and mon_memory_target sizes."
         << dendl;
    mon_memory_target = old_mon_memory_target;
    rocksdb_cache_size = old_rocksdb_cache_size;
    return -EINVAL;
  }

  if (mon_memory_autotune && pcm != nullptr) {
    std::lock_guard l(balancer_lock);
    // set pcm cache levels
    pcm->set_target_memory(target);
    pcm->set_min_memory(min);
    pcm->set_max_memory(max);
    // tune memory based on new values
    pcm->tune_memory();
    pcm->balance();
    _set_new_cache_sizes();
    dout(1) << __func__ << " Updated mon cache setting."
             << " target: " << target
             << " min: " << min
             << " max: " << max
             << dendl;
  }
  return 0;
}

int OSDMonitor::_set_cache_sizes()
{
  if (g_conf()->mon_memory_autotune) {
    // set the new osdmon cache targets to be managed by pcm
    mon_osd_cache_size = g_conf()->mon_osd_cache_size;
    rocksdb_cache_size = g_conf()->rocksdb_cache_size;
    mon_memory_base = cct->_conf.get_val<Option::size_t>("osd_memory_base");
    mon_memory_fragmentation = cct->_conf.get_val<double>("osd_memory_expected_fragmentation");
    mon_memory_target = g_conf()->mon_memory_target;
    mon_memory_min = g_conf()->mon_osd_cache_size_min;
    if (mon_memory_target <= 0 || mon_memory_min <= 0) {
      derr << __func__ << " mon_memory_target:" << mon_memory_target
           << " mon_memory_min:" << mon_memory_min
           << ". Invalid size option(s) provided."
           << dendl;
      return -EINVAL;
    }
    // Set the initial inc and full LRU cache sizes
    inc_osd_cache.set_bytes(mon_memory_min);
    full_osd_cache.set_bytes(mon_memory_min);
    mon_memory_autotune = g_conf()->mon_memory_autotune;
  }
  return 0;
}

bool OSDMonitor::_have_pending_crush()
{
  return pending_inc.crush.length() > 0;
}

CrushWrapper &OSDMonitor::_get_stable_crush()
{
  return *osdmap.crush;
}

CrushWrapper OSDMonitor::_get_pending_crush()
{
  bufferlist bl;
  if (pending_inc.crush.length())
    bl = pending_inc.crush;
  else
    osdmap.crush->encode(bl, CEPH_FEATURES_SUPPORTED_DEFAULT);

  auto p = bl.cbegin();
  CrushWrapper crush;
  crush.decode(p);
  return crush;
}

void OSDMonitor::create_initial()
{
  dout(10) << "create_initial for " << mon.monmap->fsid << dendl;

  OSDMap newmap;

  bufferlist bl;
  mon.store->get("mkfs", "osdmap", bl);

  if (bl.length()) {
    newmap.decode(bl);
    newmap.set_fsid(mon.monmap->fsid);
  } else {
    newmap.build_simple(cct, 0, mon.monmap->fsid, 0);
  }
  newmap.set_epoch(1);
  newmap.created = newmap.modified = ceph_clock_now();

  // new clusters should sort bitwise by default.
  newmap.set_flag(CEPH_OSDMAP_SORTBITWISE);

  newmap.flags |=
    CEPH_OSDMAP_RECOVERY_DELETES |
    CEPH_OSDMAP_PURGED_SNAPDIRS |
    CEPH_OSDMAP_PGLOG_HARDLIMIT;
  newmap.full_ratio = g_conf()->mon_osd_full_ratio;
  if (newmap.full_ratio > 1.0) newmap.full_ratio /= 100;
  newmap.backfillfull_ratio = g_conf()->mon_osd_backfillfull_ratio;
  if (newmap.backfillfull_ratio > 1.0) newmap.backfillfull_ratio /= 100;
  newmap.nearfull_ratio = g_conf()->mon_osd_nearfull_ratio;
  if (newmap.nearfull_ratio > 1.0) newmap.nearfull_ratio /= 100;

  // new cluster should require latest by default
  if (g_conf().get_val<bool>("mon_debug_no_require_reef")) {
    if (g_conf().get_val<bool>("mon_debug_no_require_quincy")) {
      derr << __func__ << " mon_debug_no_require_reef and quincy=true" << dendl;
      newmap.require_osd_release = ceph_release_t::pacific;
    } else {
      derr << __func__ << " mon_debug_no_require_reef=true" << dendl;
      newmap.require_osd_release = ceph_release_t::quincy;
    }
  } else {
    newmap.require_osd_release = ceph_release_t::reef;
  }

  ceph_release_t r = ceph_release_from_name(g_conf()->mon_osd_initial_require_min_compat_client);
  if (!r) {
    ceph_abort_msg("mon_osd_initial_require_min_compat_client is not valid");
  }
  newmap.require_min_compat_client = r;

  // encode into pending incremental
  uint64_t features = newmap.get_encoding_features();
  newmap.encode(pending_inc.fullmap,
                features | CEPH_FEATURE_RESERVED);
  pending_inc.full_crc = newmap.get_crc();
  dout(20) << " full crc " << pending_inc.full_crc << dendl;
}

void OSDMonitor::get_store_prefixes(std::set<string>& s) const
{
  s.insert(service_name);
  s.insert(OSD_PG_CREATING_PREFIX);
  s.insert(OSD_METADATA_PREFIX);
  s.insert(OSD_SNAP_PREFIX);
}

void OSDMonitor::update_from_paxos(bool *need_bootstrap)
{
  // we really don't care if the version has been updated, because we may
  // have trimmed without having increased the last committed; yet, we may
  // need to update the in-memory manifest.
  load_osdmap_manifest();

  version_t version = get_last_committed();
  if (version == osdmap.epoch)
    return;
  ceph_assert(version > osdmap.epoch);

  dout(15) << "update_from_paxos paxos e " << version
	   << ", my e " << osdmap.epoch << dendl;

  int prev_num_up_osd = osdmap.num_up_osd;

  if (mapping_job) {
    if (!mapping_job->is_done()) {
      dout(1) << __func__ << " mapping job "
	      << mapping_job.get() << " did not complete, "
	      << mapping_job->shards << " left, canceling" << dendl;
      mapping_job->abort();
    }
    mapping_job.reset();
  }

  load_health();

  /*
   * We will possibly have a stashed latest that *we* wrote, and we will
   * always be sure to have the oldest full map in the first..last range
   * due to encode_trim_extra(), which includes the oldest full map in the trim
   * transaction.
   *
   * encode_trim_extra() does not however write the full map's
   * version to 'full_latest'.  This is only done when we are building the
   * full maps from the incremental versions.  But don't panic!  We make sure
   * that the following conditions find whichever full map version is newer.
   */
  version_t latest_full = get_version_latest_full();
  if (latest_full == 0 && get_first_committed() > 1)
    latest_full = get_first_committed();

  if (get_first_committed() > 1 &&
      latest_full < get_first_committed()) {
    // the monitor could be just sync'ed with its peer, and the latest_full key
    // is not encoded in the paxos commits in encode_pending(), so we need to
    // make sure we get it pointing to a proper version.
    version_t lc = get_last_committed();
    version_t fc = get_first_committed();

    dout(10) << __func__ << " looking for valid full map in interval"
	     << " [" << fc << ", " << lc << "]" << dendl;

    latest_full = 0;
    for (version_t v = lc; v >= fc; v--) {
      string full_key = "full_" + stringify(v);
      if (mon.store->exists(get_service_name(), full_key)) {
        dout(10) << __func__ << " found latest full map v " << v << dendl;
        latest_full = v;
        break;
      }
    }

    ceph_assert(latest_full > 0);
    auto t(std::make_shared<MonitorDBStore::Transaction>());
    put_version_latest_full(t, latest_full);
    mon.store->apply_transaction(t);
    dout(10) << __func__ << " updated the on-disk full map version to "
             << latest_full << dendl;
  }

  if ((latest_full > 0) && (latest_full > osdmap.epoch)) {
    bufferlist latest_bl;
    get_version_full(latest_full, latest_bl);
    ceph_assert(latest_bl.length() != 0);
    dout(7) << __func__ << " loading latest full map e" << latest_full << dendl;
    osdmap = OSDMap();
    osdmap.decode(latest_bl);
  }

  bufferlist bl;
  if (!mon.store->get(OSD_PG_CREATING_PREFIX, "creating", bl)) {
    auto p = bl.cbegin();
    std::lock_guard<std::mutex> l(creating_pgs_lock);
    creating_pgs.decode(p);
    dout(7) << __func__ << " loading creating_pgs last_scan_epoch "
	    << creating_pgs.last_scan_epoch
	    << " with " << creating_pgs.pgs.size() << " pgs" << dendl;
  } else {
    dout(1) << __func__ << " missing creating pgs; upgrade from post-kraken?"
	    << dendl;
  }

  // walk through incrementals
  MonitorDBStore::TransactionRef t;
  size_t tx_size = 0;
  while (version > osdmap.epoch) {
    bufferlist inc_bl;
    int err = get_version(osdmap.epoch+1, inc_bl);
    ceph_assert(err == 0);
    ceph_assert(inc_bl.length());
    // set priority cache manager levels if the osdmap is
    // being populated for the first time.
    if (mon_memory_autotune && pcm == nullptr) {
      int r = register_cache_with_pcm();
      if (r < 0) {
        dout(10) << __func__
                 << " Error while registering osdmon caches with pcm."
                 << " Proceeding without cache auto tuning."
                 << dendl;
      }
    }

    dout(7) << "update_from_paxos  applying incremental " << osdmap.epoch+1
	    << dendl;
    OSDMap::Incremental inc(inc_bl);
    err = osdmap.apply_incremental(inc);
    ceph_assert(err == 0);

    if (!t)
      t.reset(new MonitorDBStore::Transaction);

    // Write out the full map for all past epochs.  Encode the full
    // map with the same features as the incremental.  If we don't
    // know, use the quorum features.  If we don't know those either,
    // encode with all features.
    uint64_t f = inc.encode_features;
    if (!f)
      f = mon.get_quorum_con_features();
    if (!f)
      f = -1;
    bufferlist full_bl;
    osdmap.encode(full_bl, f | CEPH_FEATURE_RESERVED);
    tx_size += full_bl.length();

    bufferlist orig_full_bl;
    get_version_full(osdmap.epoch, orig_full_bl);
    if (orig_full_bl.length()) {
      // the primary provided the full map
      ceph_assert(inc.have_crc);
      if (inc.full_crc != osdmap.crc) {
	// This will happen if the mons were running mixed versions in
	// the past or some other circumstance made the full encoded
	// maps divergent.  Reloading here will bring us back into
	// sync with the primary for this and all future maps.  OSDs
	// will also be brought back into sync when they discover the
	// crc mismatch and request a full map from a mon.
	derr << __func__ << " full map CRC mismatch, resetting to canonical"
	     << dendl;

	dout(20) << __func__ << " my (bad) full osdmap:\n";
	JSONFormatter jf(true);
	jf.dump_object("osdmap", osdmap);
	jf.flush(*_dout);
	*_dout << "\nhexdump:\n";
	full_bl.hexdump(*_dout);
	*_dout << dendl;

	osdmap = OSDMap();
	osdmap.decode(orig_full_bl);

	dout(20) << __func__ << " canonical full osdmap:\n";
	JSONFormatter jf(true);
	jf.dump_object("osdmap", osdmap);
	jf.flush(*_dout);
	*_dout << "\nhexdump:\n";
	orig_full_bl.hexdump(*_dout);
	*_dout << dendl;
      }
    } else {
      ceph_assert(!inc.have_crc);
      put_version_full(t, osdmap.epoch, full_bl);
    }
    put_version_latest_full(t, osdmap.epoch);

    // share
    dout(1) << osdmap << dendl;

    if (osdmap.epoch == 1) {
      t->erase("mkfs", "osdmap");
    }

    if (tx_size > g_conf()->mon_sync_max_payload_size*2) {
      mon.store->apply_transaction(t);
      t = MonitorDBStore::TransactionRef();
      tx_size = 0;
    }
    for (auto [osd, state] : inc.new_state) {
      if (state & CEPH_OSD_UP) {
	// could be marked up *or* down, but we're too lazy to check which
	last_osd_report.erase(osd);
      }
    }
    for (auto [osd, weight] : inc.new_weight) {
      if (weight == CEPH_OSD_OUT) {
        // manually marked out, so drop it
        osd_epochs.erase(osd);
      }
    }
  }

  if (t) {
    mon.store->apply_transaction(t);
  }

  bool marked_osd_down = false;
  for (int o = 0; o < osdmap.get_max_osd(); o++) {
    if (osdmap.is_out(o))
      continue;
    auto found = down_pending_out.find(o);
    if (osdmap.is_down(o)) {
      // populate down -> out map
      if (found == down_pending_out.end()) {
        dout(10) << " adding osd." << o << " to down_pending_out map" << dendl;
        down_pending_out[o] = ceph_clock_now();
	marked_osd_down = true;
      }
    } else {
      if (found != down_pending_out.end()) {
        dout(10) << " removing osd." << o << " from down_pending_out map" << dendl;
        down_pending_out.erase(found);
      }
    }
  }
  // XXX: need to trim MonSession connected with a osd whose id > max_osd?

  check_osdmap_subs();
  check_pg_creates_subs();

  share_map_with_random_osd();
  update_logger();
  process_failures();

  // make sure our feature bits reflect the latest map
  update_msgr_features();

  if (!mon.is_leader()) {
    // will be called by on_active() on the leader, avoid doing so twice
    start_mapping();
  }
  if (osdmap.stretch_mode_enabled) {
    dout(20) << "Stretch mode enabled in this map" << dendl;
    mon.try_engage_stretch_mode();
    if (osdmap.degraded_stretch_mode) {
      dout(20) << "Degraded stretch mode set in this map" << dendl;
      if (!osdmap.recovering_stretch_mode) {
	mon.set_degraded_stretch_mode();
  dout(20) << "prev_num_up_osd: " << prev_num_up_osd << dendl;
  dout(20) << "osdmap.num_up_osd: " << osdmap.num_up_osd << dendl;
  dout(20) << "osdmap.num_osd: " << osdmap.num_osd << dendl;
  dout(20) << "mon_stretch_cluster_recovery_ratio: " << cct->_conf.get_val<double>("mon_stretch_cluster_recovery_ratio") << dendl;
	if (prev_num_up_osd < osdmap.num_up_osd &&
	    (osdmap.num_up_osd / (double)osdmap.num_osd) >
	    cct->_conf.get_val<double>("mon_stretch_cluster_recovery_ratio") &&
      mon.dead_mon_buckets.size() == 0) {
	  // TODO: This works for 2-site clusters when the OSD maps are appropriately
	  // trimmed and everything is "normal" but not if you have a lot of out OSDs
	  // you're ignoring or in some really degenerate failure cases

	  dout(10) << "Enabling recovery stretch mode in this map" << dendl;
	  mon.go_recovery_stretch_mode();
	}
      } else {
	mon.set_recovery_stretch_mode();
      }
    } else {
      mon.set_healthy_stretch_mode();
    }
    if (marked_osd_down &&
	(!osdmap.degraded_stretch_mode || osdmap.recovering_stretch_mode)) {
      dout(20) << "Checking degraded stretch mode due to osd changes" << dendl;
      mon.maybe_go_degraded_stretch_mode();
    }
  }
}

int OSDMonitor::register_cache_with_pcm()
{
  if (mon_memory_target <= 0 || mon_memory_min <= 0) {
    derr << __func__ << " Invalid memory size specified for mon caches."
         << " Caches will not be auto-tuned."
         << dendl;
    return -EINVAL;
  }
  uint64_t base = mon_memory_base;
  double fragmentation = mon_memory_fragmentation;
  // For calculating total target memory, consider rocksdb cache size.
  uint64_t target = mon_memory_target;
  uint64_t min = mon_memory_min;
  uint64_t max = min;

  // Apply the same logic as in bluestore to set the max amount
  // of memory to use for cache. Assume base memory for OSDMaps
  // and then add in some overhead for fragmentation.
  uint64_t ltarget = (1.0 - fragmentation) * target;
  if (ltarget > base + min) {
    max = ltarget - base;
  }

  rocksdb_binned_kv_cache = mon.store->get_priority_cache();
  if (!rocksdb_binned_kv_cache) {
    derr << __func__ << " not using rocksdb" << dendl;
    return -EINVAL;
  }

  int r = _set_cache_ratios();
  if (r < 0) {
    derr << __func__ << " Cache ratios for pcm could not be set."
         << " Review the kv (rocksdb) and mon_memory_target sizes."
         << dendl;
    return -EINVAL;
  }

  pcm = std::make_shared<PriorityCache::Manager>(
      cct, min, max, target, true);
  pcm->insert("kv", rocksdb_binned_kv_cache, true);
  pcm->insert("inc", inc_cache, true);
  pcm->insert("full", full_cache, true);
  dout(1) << __func__ << " pcm target: " << target
           << " pcm max: " << max
           << " pcm min: " << min
           << " inc_osd_cache size: " << inc_osd_cache.get_size()
           << dendl;
  return 0;
}

int OSDMonitor::_set_cache_ratios()
{
  double old_cache_kv_ratio = cache_kv_ratio;

  // Set the cache ratios for kv(rocksdb), inc and full caches
  cache_kv_ratio = (double)rocksdb_cache_size / (double)mon_memory_target;
  if (cache_kv_ratio >= 1.0) {
    derr << __func__ << " Cache kv ratio (" << cache_kv_ratio
         << ") must be in range [0,<1.0]."
         << dendl;
    cache_kv_ratio = old_cache_kv_ratio;
    return -EINVAL;
  }
  rocksdb_binned_kv_cache->set_cache_ratio(cache_kv_ratio);
  cache_inc_ratio = cache_full_ratio = (1.0 - cache_kv_ratio) / 2;
  inc_cache->set_cache_ratio(cache_inc_ratio);
  full_cache->set_cache_ratio(cache_full_ratio);

  dout(1) << __func__ << " kv ratio " << cache_kv_ratio
           << " inc ratio " << cache_inc_ratio
           << " full ratio " << cache_full_ratio
           << dendl;
  return 0;
}

void OSDMonitor::start_mapping()
{
  // initiate mapping job
  if (mapping_job) {
    dout(10) << __func__ << " canceling previous mapping_job " << mapping_job.get()
	     << dendl;
    mapping_job->abort();
  }
  if (!osdmap.get_pools().empty()) {
    auto fin = new C_UpdateCreatingPGs(this, osdmap.get_epoch());
    mapping_job = mapping.start_update(osdmap, mapper,
				       g_conf()->mon_osd_mapping_pgs_per_chunk);
    dout(10) << __func__ << " started mapping job " << mapping_job.get()
	     << " at " << fin->start << dendl;
    mapping_job->set_finish_event(fin);
  } else {
    dout(10) << __func__ << " no pools, no mapping job" << dendl;
    mapping_job = nullptr;
  }
}

void OSDMonitor::update_msgr_features()
{
  const int types[] = {
    entity_name_t::TYPE_OSD,
    entity_name_t::TYPE_CLIENT,
    entity_name_t::TYPE_MDS,
    entity_name_t::TYPE_MON
  };
  for (int type : types) {
    uint64_t mask;
    uint64_t features = osdmap.get_features(type, &mask);
    if ((mon.messenger->get_policy(type).features_required & mask) != features) {
      dout(0) << "crush map has features " << features << ", adjusting msgr requires" << dendl;
      ceph::net::Policy p = mon.messenger->get_policy(type);
      p.features_required = (p.features_required & ~mask) | features;
      mon.messenger->set_policy(type, p);
    }
  }
}

void OSDMonitor::on_active()
{
  update_logger();

  if (mon.is_leader()) {
    mon.clog->debug() << "osdmap " << osdmap;
    if (!priority_convert) {
      // Only do this once at start-up
      convert_pool_priorities();
      priority_convert = true;
    }
  } else {
    list<MonOpRequestRef> ls;
    take_all_failures(ls);
    while (!ls.empty()) {
      MonOpRequestRef op = ls.front();
      op->mark_osdmon_event(__func__);
      dispatch(op);
      ls.pop_front();
    }
  }
  start_mapping();
}

void OSDMonitor::on_restart()
{
  last_osd_report.clear();
}

void OSDMonitor::on_shutdown()
{
  dout(10) << __func__ << dendl;
  if (mapping_job) {
    dout(10) << __func__ << " canceling previous mapping_job " << mapping_job.get()
	     << dendl;
    mapping_job->abort();
  }

  // discard failure info, waiters
  list<MonOpRequestRef> ls;
  take_all_failures(ls);
  ls.clear();
}

void OSDMonitor::update_logger()
{
  dout(10) << "update_logger" << dendl;

  mon.cluster_logger->set(l_cluster_num_osd, osdmap.get_num_osds());
  mon.cluster_logger->set(l_cluster_num_osd_up, osdmap.get_num_up_osds());
  mon.cluster_logger->set(l_cluster_num_osd_in, osdmap.get_num_in_osds());
  mon.cluster_logger->set(l_cluster_osd_epoch, osdmap.get_epoch());
}

void OSDMonitor::create_pending()
{
  pending_inc = OSDMap::Incremental(osdmap.epoch+1);
  pending_inc.fsid = mon.monmap->fsid;
  pending_metadata.clear();
  pending_metadata_rm.clear();
  pending_pseudo_purged_snaps.clear();

  dout(10) << "create_pending e " << pending_inc.epoch << dendl;

  // safety checks (this shouldn't really happen)
  {
    if (osdmap.backfillfull_ratio <= 0) {
      pending_inc.new_backfillfull_ratio = g_conf()->mon_osd_backfillfull_ratio;
      if (pending_inc.new_backfillfull_ratio > 1.0)
	pending_inc.new_backfillfull_ratio /= 100;
      dout(1) << __func__ << " setting backfillfull_ratio = "
	      << pending_inc.new_backfillfull_ratio << dendl;
    }
    if (osdmap.full_ratio <= 0) {
      pending_inc.new_full_ratio = g_conf()->mon_osd_full_ratio;
      if (pending_inc.new_full_ratio > 1.0)
        pending_inc.new_full_ratio /= 100;
      dout(1) << __func__ << " setting full_ratio = "
	      << pending_inc.new_full_ratio << dendl;
    }
    if (osdmap.nearfull_ratio <= 0) {
      pending_inc.new_nearfull_ratio = g_conf()->mon_osd_nearfull_ratio;
      if (pending_inc.new_nearfull_ratio > 1.0)
        pending_inc.new_nearfull_ratio /= 100;
      dout(1) << __func__ << " setting nearfull_ratio = "
	      << pending_inc.new_nearfull_ratio << dendl;
    }
  }
}

creating_pgs_t
OSDMonitor::update_pending_pgs(const OSDMap::Incremental& inc,
			       const OSDMap& nextmap)
{
  dout(10) << __func__ << dendl;
  creating_pgs_t pending_creatings;
  {
    std::lock_guard<std::mutex> l(creating_pgs_lock);
    pending_creatings = creating_pgs;
  }
  // check for new or old pools
  if (pending_creatings.last_scan_epoch < inc.epoch) {
    unsigned queued = 0;
    queued += scan_for_creating_pgs(osdmap.get_pools(),
				    inc.old_pools,
				    inc.modified,
				    &pending_creatings);
    queued += scan_for_creating_pgs(inc.new_pools,
				    inc.old_pools,
				    inc.modified,
				    &pending_creatings);
    dout(10) << __func__ << " " << queued << " pools queued" << dendl;
    for (auto deleted_pool : inc.old_pools) {
      auto removed = pending_creatings.remove_pool(deleted_pool);
      dout(10) << __func__ << " " << removed
               << " pg removed because containing pool deleted: "
               << deleted_pool << dendl;
      last_epoch_clean.remove_pool(deleted_pool);
    }
    // pgmon updates its creating_pgs in check_osd_map() which is called by
    // on_active() and check_osd_map() could be delayed if lease expires, so its
    // creating_pgs could be stale in comparison with the one of osdmon. let's
    // trim them here. otherwise, they will be added back after being erased.
    unsigned removed = 0;
    for (auto& pg : pending_created_pgs) {
      dout(20) << __func__ << " noting created pg " << pg << dendl;
      pending_creatings.created_pools.insert(pg.pool());
      removed += pending_creatings.pgs.erase(pg);
    }
    pending_created_pgs.clear();
    dout(10) << __func__ << " " << removed
	     << " pgs removed because they're created" << dendl;
    pending_creatings.last_scan_epoch = osdmap.get_epoch();
  }

  // filter out any pgs that shouldn't exist.
  {
    auto i = pending_creatings.pgs.begin();
    while (i != pending_creatings.pgs.end()) {
      if (!nextmap.pg_exists(i->first)) {
	dout(10) << __func__ << " removing pg " << i->first
		 << " which should not exist" << dendl;
	i = pending_creatings.pgs.erase(i);
      } else {
	++i;
      }
    }
  }

  // process queue
  unsigned max = std::max<int64_t>(1, g_conf()->mon_osd_max_creating_pgs);
  const auto total = pending_creatings.pgs.size();
  while (pending_creatings.pgs.size() < max &&
	 !pending_creatings.queue.empty()) {
    auto p = pending_creatings.queue.begin();
    int64_t poolid = p->first;
    dout(10) << __func__ << " pool " << poolid
	     << " created " << p->second.created
	     << " modified " << p->second.modified
	     << " [" << p->second.start << "-" << p->second.end << ")"
	     << dendl;
    int64_t n = std::min<int64_t>(max - pending_creatings.pgs.size(),
				  p->second.end - p->second.start);
    ps_t first = p->second.start;
    ps_t end = first + n;
    for (ps_t ps = first; ps < end; ++ps) {
      const pg_t pgid{ps, static_cast<uint64_t>(poolid)};
      // NOTE: use the *current* epoch as the PG creation epoch so that the
      // OSD does not have to generate a long set of PastIntervals.
      pending_creatings.pgs.emplace(
	pgid,
	creating_pgs_t::pg_create_info(inc.epoch,
				       p->second.modified));
      dout(10) << __func__ << " adding " << pgid << dendl;
    }
    p->second.start = end;
    if (p->second.done()) {
      dout(10) << __func__ << " done with queue for " << poolid << dendl;
      pending_creatings.queue.erase(p);
    } else {
      dout(10) << __func__ << " pool " << poolid
	       << " now [" << p->second.start << "-" << p->second.end << ")"
	       << dendl;
    }
  }
  dout(10) << __func__ << " queue remaining: " << pending_creatings.queue.size()
	   << " pools" << dendl;

  if (mon.monmap->min_mon_release >= ceph_release_t::octopus) {
    // walk creating pgs' history and past_intervals forward
    for (auto& i : pending_creatings.pgs) {
      // this mirrors PG::start_peering_interval()
      pg_t pgid = i.first;

      // this is a bit imprecise, but sufficient?
      struct min_size_predicate_t : public IsPGRecoverablePredicate {
	const pg_pool_t *pi;
	bool operator()(const set<pg_shard_t> &have) const {
	  return have.size() >= pi->min_size;
	}
	explicit min_size_predicate_t(const pg_pool_t *i) : pi(i) {}
      } min_size_predicate(nextmap.get_pg_pool(pgid.pool()));

      vector<int> up, acting;
      int up_primary, acting_primary;
      nextmap.pg_to_up_acting_osds(
	pgid, &up, &up_primary, &acting, &acting_primary);
      if (i.second.history.epoch_created == 0) {
	// new pg entry, set it up
	i.second.up = up;
	i.second.acting = acting;
	i.second.up_primary = up_primary;
	i.second.acting_primary = acting_primary;
	i.second.history = pg_history_t(i.second.create_epoch,
					i.second.create_stamp);
	dout(10) << __func__ << "  pg " << pgid << " just added, "
		 << " up " << i.second.up
		 << " p " << i.second.up_primary
		 << " acting " << i.second.acting
		 << " p " << i.second.acting_primary
		 << " history " << i.second.history
		 << " past_intervals " << i.second.past_intervals
		 << dendl;
     } else {
	std::stringstream debug;
	if (PastIntervals::check_new_interval(
	      i.second.acting_primary, acting_primary,
	      i.second.acting, acting,
	      i.second.up_primary, up_primary,
	      i.second.up, up,
	      i.second.history.same_interval_since,
	      i.second.history.last_epoch_clean,
	      &nextmap,
	      &osdmap,
	      pgid,
	      min_size_predicate,
	      &i.second.past_intervals,
	      &debug)) {
	  epoch_t e = inc.epoch;
	  i.second.history.same_interval_since = e;
	  if (i.second.up != up) {
	    i.second.history.same_up_since = e;
	  }
	  if (i.second.acting_primary != acting_primary) {
	    i.second.history.same_primary_since = e;
	  }
	  if (pgid.is_split(
		osdmap.get_pg_num(pgid.pool()),
		nextmap.get_pg_num(pgid.pool()),
		nullptr)) {
	    i.second.history.last_epoch_split = e;
	  }
	  dout(10) << __func__ << "  pg " << pgid << " new interval,"
		   << " up " << i.second.up << " -> " << up
		   << " p " << i.second.up_primary << " -> " << up_primary
		   << " acting " << i.second.acting << " -> " << acting
		   << " p " << i.second.acting_primary << " -> "
		   << acting_primary
		   << " history " << i.second.history
		   << " past_intervals " << i.second.past_intervals
		   << dendl;
	  dout(20) << "  debug: " << debug.str() << dendl;
	  i.second.up = up;
	  i.second.acting = acting;
	  i.second.up_primary = up_primary;
	  i.second.acting_primary = acting_primary;
	}
      }
    }
  }
  dout(10) << __func__
	   << " " << (pending_creatings.pgs.size() - total)
	   << "/" << pending_creatings.pgs.size()
	   << " pgs added from queued pools" << dendl;
  return pending_creatings;
}

void OSDMonitor::maybe_prime_pg_temp()
{
  bool all = false;
  if (pending_inc.crush.length()) {
    dout(10) << __func__ << " new crush map, all" << dendl;
    all = true;
  }

  if (!pending_inc.new_up_client.empty()) {
    dout(10) << __func__ << " new up osds, all" << dendl;
    all = true;
  }

  // check for interesting OSDs
  set<int> osds;
  for (auto p = pending_inc.new_state.begin();
       !all && p != pending_inc.new_state.end();
       ++p) {
    if ((p->second & CEPH_OSD_UP) &&
	osdmap.is_up(p->first)) {
      osds.insert(p->first);
    }
  }
  for (auto p = pending_inc.new_weight.begin();
       !all && p != pending_inc.new_weight.end();
       ++p) {
    if (osdmap.exists(p->first) && p->second < osdmap.get_weight(p->first)) {
      // weight reduction
      osds.insert(p->first);
    } else {
      dout(10) << __func__ << " osd." << p->first << " weight increase, all"
	       << dendl;
      all = true;
    }
  }

  if (!all && osds.empty())
    return;

  if (!all) {
    unsigned estimate =
      mapping.get_osd_acting_pgs(*osds.begin()).size() * osds.size();
    if (estimate > mapping.get_num_pgs() *
	g_conf()->mon_osd_prime_pg_temp_max_estimate) {
      dout(10) << __func__ << " estimate " << estimate << " pgs on "
	       << osds.size() << " osds >= "
	       << g_conf()->mon_osd_prime_pg_temp_max_estimate << " of total "
	       << mapping.get_num_pgs() << " pgs, all"
	       << dendl;
      all = true;
    } else {
      dout(10) << __func__ << " estimate " << estimate << " pgs on "
	       << osds.size() << " osds" << dendl;
    }
  }

  OSDMap next;
  next.deepish_copy_from(osdmap);
  next.apply_incremental(pending_inc);

  if (next.get_pools().empty()) {
    dout(10) << __func__ << " no pools, no pg_temp priming" << dendl;
  } else if (all) {
    PrimeTempJob job(next, this);
    mapper.queue(&job, g_conf()->mon_osd_mapping_pgs_per_chunk, {});
    if (job.wait_for(g_conf()->mon_osd_prime_pg_temp_max_time)) {
      dout(10) << __func__ << " done in " << job.get_duration() << dendl;
    } else {
      dout(10) << __func__ << " did not finish in "
	       << g_conf()->mon_osd_prime_pg_temp_max_time
	       << ", stopping" << dendl;
      job.abort();
    }
  } else {
    dout(10) << __func__ << " " << osds.size() << " interesting osds" << dendl;
    utime_t stop = ceph_clock_now();
    stop += g_conf()->mon_osd_prime_pg_temp_max_time;
    const int chunk = 1000;
    int n = chunk;
    std::unordered_set<pg_t> did_pgs;
    for (auto osd : osds) {
      auto& pgs = mapping.get_osd_acting_pgs(osd);
      dout(20) << __func__ << " osd." << osd << " " << pgs << dendl;
      for (auto pgid : pgs) {
	if (!did_pgs.insert(pgid).second) {
	  continue;
	}
	prime_pg_temp(next, pgid);
	if (--n <= 0) {
	  n = chunk;
	  if (ceph_clock_now() > stop) {
	    dout(10) << __func__ << " consumed more than "
		     << g_conf()->mon_osd_prime_pg_temp_max_time
		     << " seconds, stopping"
		     << dendl;
	    return;
	  }
	}
      }
    }
  }
}

void OSDMonitor::prime_pg_temp(
  const OSDMap& next,
  pg_t pgid)
{
  // TODO: remove this creating_pgs direct access?
  if (creating_pgs.pgs.count(pgid)) {
    return;
  }
  if (!osdmap.pg_exists(pgid)) {
    return;
  }

  vector<int> up, acting;
  mapping.get(pgid, &up, nullptr, &acting, nullptr);

  vector<int> next_up, next_acting;
  int next_up_primary, next_acting_primary;
  next.pg_to_up_acting_osds(pgid, &next_up, &next_up_primary,
			    &next_acting, &next_acting_primary);
  if (acting == next_acting &&
      !(up != acting && next_up == next_acting))
    return;  // no change since last epoch

  if (acting.empty())
    return;  // if previously empty now we can be no worse off
  const pg_pool_t *pool = next.get_pg_pool(pgid.pool());
  if (pool && acting.size() < pool->min_size)
    return;  // can be no worse off than before

  if (next_up == next_acting) {
    acting.clear();
    dout(20) << __func__ << " next_up == next_acting now, clear pg_temp"
	     << dendl;
  }

  dout(20) << __func__ << " " << pgid << " " << up << "/" << acting
	   << " -> " << next_up << "/" << next_acting
	   << ", priming " << acting
	   << dendl;
  {
    std::lock_guard l(prime_pg_temp_lock);
    // do not touch a mapping if a change is pending
    pending_inc.new_pg_temp.emplace(
      pgid,
      mempool::osdmap::vector<int>(acting.begin(), acting.end()));
  }
}

/**
 * @note receiving a transaction in this function gives a fair amount of
 * freedom to the service implementation if it does need it. It shouldn't.
 */
void OSDMonitor::encode_pending(MonitorDBStore::TransactionRef t)
{
  dout(10) << "encode_pending e " << pending_inc.epoch
	   << dendl;

  if (do_prune(t)) {
    dout(1) << __func__ << " osdmap full prune encoded e"
            << pending_inc.epoch << dendl;
  }

  // finalize up pending_inc
  pending_inc.modified = ceph_clock_now();

  int r = pending_inc.propagate_base_properties_to_tiers(cct, osdmap);
  ceph_assert(r == 0);

  if (mapping_job) {
    if (!mapping_job->is_done()) {
      dout(1) << __func__ << " skipping prime_pg_temp; mapping job "
	      << mapping_job.get() << " did not complete, "
	      << mapping_job->shards << " left" << dendl;
      mapping_job->abort();
    } else if (mapping.get_epoch() < osdmap.get_epoch()) {
      dout(1) << __func__ << " skipping prime_pg_temp; mapping job "
	      << mapping_job.get() << " is prior epoch "
	      << mapping.get_epoch() << dendl;
    } else {
      if (g_conf()->mon_osd_prime_pg_temp) {
	maybe_prime_pg_temp();
      }
    } 
  } else if (g_conf()->mon_osd_prime_pg_temp) {
    dout(1) << __func__ << " skipping prime_pg_temp; mapping job did not start"
	    << dendl;
  }
  mapping_job.reset();

  // ensure we don't have blank new_state updates.  these are interrpeted as
  // CEPH_OSD_UP (and almost certainly not what we want!).
  auto p = pending_inc.new_state.begin();
  while (p != pending_inc.new_state.end()) {
    if (p->second == 0) {
      dout(10) << "new_state for osd." << p->first << " is 0, removing" << dendl;
      p = pending_inc.new_state.erase(p);
    } else {
      if (p->second & CEPH_OSD_UP) {
	pending_inc.new_last_up_change = pending_inc.modified;
      }
      ++p;
    }
  }
  if (!pending_inc.new_up_client.empty()) {
    pending_inc.new_last_up_change = pending_inc.modified;
  }
  for (auto& i : pending_inc.new_weight) {
    if (i.first >= osdmap.max_osd) {
      if (i.second) {
	// new osd is already marked in
	pending_inc.new_last_in_change = pending_inc.modified;
        break;
      }
    } else if (!!i.second != !!osdmap.osd_weight[i.first]) {
      // existing osd marked in or out
      pending_inc.new_last_in_change = pending_inc.modified;
      break;
    }
  }

  {
    OSDMap tmp;
    tmp.deepish_copy_from(osdmap);
    tmp.apply_incremental(pending_inc);

    // clean pg_temp mappings
    OSDMap::clean_temps(cct, osdmap, tmp, &pending_inc);

    // clean inappropriate pg_upmap/pg_upmap_items (if any)
    {
      // check every upmapped pg for now
      // until we could reliably identify certain cases to ignore,
      // which is obviously the hard part TBD..
      vector<pg_t> pgs_to_check;
      tmp.get_upmap_pgs(&pgs_to_check);
      if (pgs_to_check.size() <
	  static_cast<uint64_t>(g_conf()->mon_clean_pg_upmaps_per_chunk * 2)) {
        // not enough pgs, do it inline
        tmp.clean_pg_upmaps(cct, &pending_inc);
      } else {
        CleanUpmapJob job(cct, tmp, pending_inc);
        mapper.queue(&job, g_conf()->mon_clean_pg_upmaps_per_chunk, pgs_to_check);
        job.wait();
      }
    }

    // update creating pgs first so that we can remove the created pgid and
    // process the pool flag removal below in the same osdmap epoch.
    auto pending_creatings = update_pending_pgs(pending_inc, tmp);
    bufferlist creatings_bl;
    uint64_t features = CEPH_FEATURES_ALL;
    if (mon.monmap->min_mon_release < ceph_release_t::octopus) {
      dout(20) << __func__ << " encoding pending pgs without octopus features"
	       << dendl;
      features &= ~CEPH_FEATURE_SERVER_OCTOPUS;
    }
    encode(pending_creatings, creatings_bl, features);
    t->put(OSD_PG_CREATING_PREFIX, "creating", creatings_bl);

    // remove any old (or incompat) POOL_CREATING flags
    for (auto& i : tmp.get_pools()) {
      if (tmp.require_osd_release < ceph_release_t::nautilus) {
	// pre-nautilus OSDMaps shouldn't get this flag.
	if (pending_inc.new_pools.count(i.first)) {
	  pending_inc.new_pools[i.first].flags &= ~pg_pool_t::FLAG_CREATING;
	}
      }
      if (i.second.has_flag(pg_pool_t::FLAG_CREATING) &&
	  !pending_creatings.still_creating_pool(i.first)) {
	dout(10) << __func__ << " done creating pool " << i.first
		 << ", clearing CREATING flag" << dendl;
	if (pending_inc.new_pools.count(i.first) == 0) {
	  pending_inc.new_pools[i.first] = i.second;
	}
	pending_inc.new_pools[i.first].flags &= ~pg_pool_t::FLAG_CREATING;
      }
    }

    // collect which pools are currently affected by
    // the near/backfill/full osd(s),
    // and set per-pool near/backfill/full flag instead
    set<int64_t> full_pool_ids;
    set<int64_t> backfillfull_pool_ids;
    set<int64_t> nearfull_pool_ids;
    tmp.get_full_pools(cct,
		       &full_pool_ids,
		       &backfillfull_pool_ids,
                         &nearfull_pool_ids);
    if (full_pool_ids.empty() ||
	backfillfull_pool_ids.empty() ||
	nearfull_pool_ids.empty()) {
      // normal case - no nearfull, backfillfull or full osds
        // try cancel any improper nearfull/backfillfull/full pool
        // flags first
      for (auto &pool: tmp.get_pools()) {
	auto p = pool.first;
	if (tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_NEARFULL) &&
	    nearfull_pool_ids.empty()) {
	  dout(10) << __func__ << " clearing pool '" << tmp.pool_name[p]
		   << "'s nearfull flag" << dendl;
	  if (pending_inc.new_pools.count(p) == 0) {
	    // load original pool info first!
	    pending_inc.new_pools[p] = pool.second;
	  }
	  pending_inc.new_pools[p].flags &= ~pg_pool_t::FLAG_NEARFULL;
	}
	if (tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_BACKFILLFULL) &&
	    backfillfull_pool_ids.empty()) {
	  dout(10) << __func__ << " clearing pool '" << tmp.pool_name[p]
		   << "'s backfillfull flag" << dendl;
	  if (pending_inc.new_pools.count(p) == 0) {
	    pending_inc.new_pools[p] = pool.second;
	  }
	  pending_inc.new_pools[p].flags &= ~pg_pool_t::FLAG_BACKFILLFULL;
	}
	if (tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_FULL) &&
	    full_pool_ids.empty()) {
	  if (tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_FULL_QUOTA)) {
	    // set by EQUOTA, skipping
	    continue;
	  }
	  dout(10) << __func__ << " clearing pool '" << tmp.pool_name[p]
		   << "'s full flag" << dendl;
	  if (pending_inc.new_pools.count(p) == 0) {
	    pending_inc.new_pools[p] = pool.second;
	  }
	  pending_inc.new_pools[p].flags &= ~pg_pool_t::FLAG_FULL;
	}
      }
    }
    if (!full_pool_ids.empty()) {
      dout(10) << __func__ << " marking pool(s) " << full_pool_ids
	       << " as full" << dendl;
      for (auto &p: full_pool_ids) {
	if (tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_FULL)) {
	  continue;
	}
	if (pending_inc.new_pools.count(p) == 0) {
	  pending_inc.new_pools[p] = tmp.pools[p];
	}
	pending_inc.new_pools[p].flags |= pg_pool_t::FLAG_FULL;
	pending_inc.new_pools[p].flags &= ~pg_pool_t::FLAG_BACKFILLFULL;
	pending_inc.new_pools[p].flags &= ~pg_pool_t::FLAG_NEARFULL;
      }
      // cancel FLAG_FULL for pools which are no longer full too
      for (auto &pool: tmp.get_pools()) {
	auto p = pool.first;
	if (full_pool_ids.count(p)) {
	  // skip pools we have just marked as full above
	  continue;
	}
	if (!tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_FULL) ||
	    tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_FULL_QUOTA)) {
	  // don't touch if currently is not full
	  // or is running out of quota (and hence considered as full)
	  continue;
	}
	dout(10) << __func__ << " clearing pool '" << tmp.pool_name[p]
		 << "'s full flag" << dendl;
	if (pending_inc.new_pools.count(p) == 0) {
	  pending_inc.new_pools[p] = pool.second;
	}
	pending_inc.new_pools[p].flags &= ~pg_pool_t::FLAG_FULL;
      }
    }
    if (!backfillfull_pool_ids.empty()) {
      for (auto &p: backfillfull_pool_ids) {
	if (full_pool_ids.count(p)) {
	  // skip pools we have already considered as full above
	  continue;
	}
	if (tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_FULL_QUOTA)) {
	  // make sure FLAG_FULL is truly set, so we are safe not
	  // to set a extra (redundant) FLAG_BACKFILLFULL flag
	  ceph_assert(tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_FULL));
	  continue;
	}
	if (tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_BACKFILLFULL)) {
	  // don't bother if pool is already marked as backfillfull
	  continue;
	}
	dout(10) << __func__ << " marking pool '" << tmp.pool_name[p]
		 << "'s as backfillfull" << dendl;
	if (pending_inc.new_pools.count(p) == 0) {
	  pending_inc.new_pools[p] = tmp.pools[p];
	}
	pending_inc.new_pools[p].flags |= pg_pool_t::FLAG_BACKFILLFULL;
	pending_inc.new_pools[p].flags &= ~pg_pool_t::FLAG_NEARFULL;
      }
      // cancel FLAG_BACKFILLFULL for pools
      // which are no longer backfillfull too
      for (auto &pool: tmp.get_pools()) {
	auto p = pool.first;
	if (full_pool_ids.count(p) || backfillfull_pool_ids.count(p)) {
	  // skip pools we have just marked as backfillfull/full above
	  continue;
	}
	if (!tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_BACKFILLFULL)) {
	  // and don't touch if currently is not backfillfull
	  continue;
	}
	dout(10) << __func__ << " clearing pool '" << tmp.pool_name[p]
		 << "'s backfillfull flag" << dendl;
	if (pending_inc.new_pools.count(p) == 0) {
	  pending_inc.new_pools[p] = pool.second;
	}
	pending_inc.new_pools[p].flags &= ~pg_pool_t::FLAG_BACKFILLFULL;
      }
    }
    if (!nearfull_pool_ids.empty()) {
      for (auto &p: nearfull_pool_ids) {
	if (full_pool_ids.count(p) || backfillfull_pool_ids.count(p)) {
	  continue;
	}
	if (tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_FULL_QUOTA)) {
	  // make sure FLAG_FULL is truly set, so we are safe not
	  // to set a extra (redundant) FLAG_NEARFULL flag
	  ceph_assert(tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_FULL));
	  continue;
	}
	if (tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_NEARFULL)) {
	  // don't bother if pool is already marked as nearfull
	  continue;
	}
	dout(10) << __func__ << " marking pool '" << tmp.pool_name[p]
		 << "'s as nearfull" << dendl;
	if (pending_inc.new_pools.count(p) == 0) {
	  pending_inc.new_pools[p] = tmp.pools[p];
	}
	pending_inc.new_pools[p].flags |= pg_pool_t::FLAG_NEARFULL;
      }
      // cancel FLAG_NEARFULL for pools
      // which are no longer nearfull too
      for (auto &pool: tmp.get_pools()) {
	auto p = pool.first;
	if (full_pool_ids.count(p) ||
	    backfillfull_pool_ids.count(p) ||
	    nearfull_pool_ids.count(p)) {
	  // skip pools we have just marked as
	  // nearfull/backfillfull/full above
	  continue;
	}
	if (!tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_NEARFULL)) {
	  // and don't touch if currently is not nearfull
	  continue;
	}
	dout(10) << __func__ << " clearing pool '" << tmp.pool_name[p]
		 << "'s nearfull flag" << dendl;
	if (pending_inc.new_pools.count(p) == 0) {
	  pending_inc.new_pools[p] = pool.second;
	}
	pending_inc.new_pools[p].flags &= ~pg_pool_t::FLAG_NEARFULL;
      }
    }

    // min_compat_client?
    if (!tmp.require_min_compat_client) {
      auto mv = tmp.get_min_compat_client();
      dout(1) << __func__ << " setting require_min_compat_client to currently "
	      << "required " << mv << dendl;
      mon.clog->info() << "setting require_min_compat_client to currently "
			<< "required " << mv;
      pending_inc.new_require_min_compat_client = mv;
    }

    if (osdmap.require_osd_release < ceph_release_t::nautilus &&
	tmp.require_osd_release >= ceph_release_t::nautilus) {
      dout(10) << __func__ << " first nautilus+ epoch" << dendl;
      // add creating flags?
      for (auto& i : tmp.get_pools()) {
	if (pending_creatings.still_creating_pool(i.first)) {
	  dout(10) << __func__ << " adding CREATING flag to pool " << i.first
		   << dendl;
	  if (pending_inc.new_pools.count(i.first) == 0) {
	    pending_inc.new_pools[i.first] = i.second;
	  }
	  pending_inc.new_pools[i.first].flags |= pg_pool_t::FLAG_CREATING;
	}
      }
      // adjust blocklist items to all be TYPE_ANY
      for (auto& i : tmp.blocklist) {
	auto a = i.first;
	a.set_type(entity_addr_t::TYPE_ANY);
	pending_inc.new_blocklist[a] = i.second;
	pending_inc.old_blocklist.push_back(i.first);
      }
    }

    if (osdmap.require_osd_release < ceph_release_t::octopus &&
	tmp.require_osd_release >= ceph_release_t::octopus) {
      dout(10) << __func__ << " first octopus+ epoch" << dendl;

      // adjust obsoleted cache modes
      for (auto& [poolid, pi] : tmp.pools) {
	if (pi.cache_mode == pg_pool_t::CACHEMODE_FORWARD) {
	  if (pending_inc.new_pools.count(poolid) == 0) {
	    pending_inc.new_pools[poolid] = pi;
	  }
	  dout(10) << __func__ << " switching pool " << poolid
		   << " cachemode from forward -> proxy" << dendl;
	  pending_inc.new_pools[poolid].cache_mode = pg_pool_t::CACHEMODE_PROXY;
	}
	if (pi.cache_mode == pg_pool_t::CACHEMODE_READFORWARD) {
	  if (pending_inc.new_pools.count(poolid) == 0) {
	    pending_inc.new_pools[poolid] = pi;
	  }
	  dout(10) << __func__ << " switching pool " << poolid
		   << " cachemode from readforward -> readproxy" << dendl;
	  pending_inc.new_pools[poolid].cache_mode =
	    pg_pool_t::CACHEMODE_READPROXY;
	}
      }

      // clear removed_snaps for every pool
      for (auto& [poolid, pi] : tmp.pools) {
	if (pi.removed_snaps.empty()) {
	  continue;
	}
	if (pending_inc.new_pools.count(poolid) == 0) {
	  pending_inc.new_pools[poolid] = pi;
	}
	dout(10) << __func__ << " clearing pool " << poolid << " removed_snaps"
		 << dendl;
	pending_inc.new_pools[poolid].removed_snaps.clear();
      }

      // create a combined purged snap epoch key for all purged snaps
      // prior to this epoch, and store it in the current epoch (i.e.,
      // the last pre-octopus epoch, just prior to the one we're
      // encoding now).
      auto it = mon.store->get_iterator(OSD_SNAP_PREFIX);
      it->lower_bound("purged_snap_");
      map<int64_t,snap_interval_set_t> combined;
      while (it->valid()) {
	if (it->key().find("purged_snap_") != 0) {
	  break;
	}
	string k = it->key();
	long long unsigned pool;
	int n = sscanf(k.c_str(), "purged_snap_%llu_", &pool);
	if (n != 1) {
	  derr << __func__ << " invalid purged_snaps key '" << k << "'" << dendl;
	} else {
	  bufferlist v = it->value();
	  auto p = v.cbegin();
	  snapid_t begin, end;
	  ceph::decode(begin, p);
	  ceph::decode(end, p);
	  combined[pool].insert(begin, end - begin);
	}
	it->next();
      }
      if (!combined.empty()) {
	string k = make_purged_snap_epoch_key(pending_inc.epoch - 1);
	bufferlist v;
	ceph::encode(combined, v);
	t->put(OSD_SNAP_PREFIX, k, v);
	dout(10) << __func__ << " recording pre-octopus purged_snaps in epoch "
		 << (pending_inc.epoch - 1) << ", " << v.length() << " bytes"
		 << dendl;
      } else {
	dout(10) << __func__ << " there were no pre-octopus purged snaps"
		 << dendl;
      }

      // clean out the old removed_snap_ and removed_epoch keys
      // ('`' is ASCII '_' + 1)
      t->erase_range(OSD_SNAP_PREFIX, "removed_snap_", "removed_snap`");
      t->erase_range(OSD_SNAP_PREFIX, "removed_epoch_", "removed_epoch`");
    }
  }

  // tell me about it
  for (auto i = pending_inc.new_state.begin();
       i != pending_inc.new_state.end();
       ++i) {
    int s = i->second ? i->second : CEPH_OSD_UP;
    if (s & CEPH_OSD_UP) {
      dout(2) << " osd." << i->first << " DOWN" << dendl;
      // Reset laggy parameters if failure interval exceeds a threshold.
      const osd_xinfo_t& xi = osdmap.get_xinfo(i->first);
      if ((xi.laggy_probability || xi.laggy_interval) && xi.down_stamp.sec()) {
        int last_failure_interval = pending_inc.modified.sec() - xi.down_stamp.sec();
        if (grace_interval_threshold_exceeded(last_failure_interval)) {
          set_default_laggy_params(i->first);
        }
      }
    }
    if (s & CEPH_OSD_EXISTS)
      dout(2) << " osd." << i->first << " DNE" << dendl;
  }
  for (auto i = pending_inc.new_up_client.begin();
       i != pending_inc.new_up_client.end();
       ++i) {
    //FIXME: insert cluster addresses too
    dout(2) << " osd." << i->first << " UP " << i->second << dendl;
  }
  for (map<int32_t,uint32_t>::iterator i = pending_inc.new_weight.begin();
       i != pending_inc.new_weight.end();
       ++i) {
    if (i->second == CEPH_OSD_OUT) {
      dout(2) << " osd." << i->first << " OUT" << dendl;
    } else if (i->second == CEPH_OSD_IN) {
      dout(2) << " osd." << i->first << " IN" << dendl;
    } else {
      dout(2) << " osd." << i->first << " WEIGHT " << hex << i->second << dec << dendl;
    }
  }

  // features for osdmap and its incremental
  uint64_t features;

  // encode full map and determine its crc
  OSDMap tmp;
  {
    tmp.deepish_copy_from(osdmap);
    tmp.apply_incremental(pending_inc);

    // determine appropriate features
    features = tmp.get_encoding_features();
    dout(10) << __func__ << " encoding full map with "
	     << tmp.require_osd_release
	     << " features " << features << dendl;

    // the features should be a subset of the mon quorum's features!
    ceph_assert((features & ~mon.get_quorum_con_features()) == 0);

    bufferlist fullbl;
    encode(tmp, fullbl, features | CEPH_FEATURE_RESERVED);
    pending_inc.full_crc = tmp.get_crc();

    // include full map in the txn.  note that old monitors will
    // overwrite this.  new ones will now skip the local full map
    // encode and reload from this.
    put_version_full(t, pending_inc.epoch, fullbl);
  }

  // encode
  ceph_assert(get_last_committed() + 1 == pending_inc.epoch);
  bufferlist bl;
  encode(pending_inc, bl, features | CEPH_FEATURE_RESERVED);

  dout(20) << " full_crc " << tmp.get_crc()
	   << " inc_crc " << pending_inc.inc_crc << dendl;

  /* put everything in the transaction */
  put_version(t, pending_inc.epoch, bl);
  put_last_committed(t, pending_inc.epoch);

  // metadata, too!
  for (map<int,bufferlist>::iterator p = pending_metadata.begin();
       p != pending_metadata.end();
       ++p) {
    Metadata m;
    auto mp = p->second.cbegin();
    decode(m, mp);
    auto it = m.find("osd_objectstore");
    if (it != m.end()) {
      if (it->second == "filestore") {
        filestore_osds.insert(p->first);
      } else {
        filestore_osds.erase(p->first);
      }
    }
    t->put(OSD_METADATA_PREFIX, stringify(p->first), p->second);
  }
  for (set<int>::iterator p = pending_metadata_rm.begin();
       p != pending_metadata_rm.end();
       ++p) {
    filestore_osds.erase(*p);
    t->erase(OSD_METADATA_PREFIX, stringify(*p));
  }
  pending_metadata.clear();
  pending_metadata_rm.clear();

  // purged_snaps
  if (tmp.require_osd_release >= ceph_release_t::octopus &&
      !pending_inc.new_purged_snaps.empty()) {
    // all snaps purged this epoch (across all pools)
    string k = make_purged_snap_epoch_key(pending_inc.epoch);
    bufferlist v;
    encode(pending_inc.new_purged_snaps, v);
    t->put(OSD_SNAP_PREFIX, k, v);
  }
  for (auto& i : pending_inc.new_purged_snaps) {
    for (auto q = i.second.begin();
	 q != i.second.end();
	 ++q) {
      insert_purged_snap_update(i.first, q.get_start(), q.get_end(),
				pending_inc.epoch,
				t);
    }
  }
  for (auto& [pool, snaps] : pending_pseudo_purged_snaps) {
    for (auto snap : snaps) {
      insert_purged_snap_update(pool, snap, snap + 1,
				pending_inc.epoch,
				t);
    }
  }

  // health
  health_check_map_t next;
  tmp.check_health(cct, &next);
  // OSD_FILESTORE
  check_for_filestore_osds(&next);
  encode_health(next, t);
}

int OSDMonitor::load_metadata(int osd, map<string, string>& m, ostream *err)
{
  bufferlist bl;
  int r = mon.store->get(OSD_METADATA_PREFIX, stringify(osd), bl);
  if (r < 0)
    return r;
  try {
    auto p = bl.cbegin();
    decode(m, p);
  }
  catch (ceph::buffer::error& e) {
    if (err)
      *err << "osd." << osd << " metadata is corrupt";
    return -EIO;
  }
  return 0;
}

void OSDMonitor::count_metadata(const string& field, map<string,int> *out)
{
  for (int osd = 0; osd < osdmap.get_max_osd(); ++osd) {
    if (osdmap.is_up(osd)) {
      map<string,string> meta;
      load_metadata(osd, meta, nullptr);
      auto p = meta.find(field);
      if (p == meta.end()) {
	(*out)["unknown"]++;
      } else {
	(*out)[p->second]++;
      }
    }
  }
}

void OSDMonitor::count_metadata(const string& field, Formatter *f)
{
  map<string,int> by_val;
  count_metadata(field, &by_val);
  f->open_object_section(field.c_str());
  for (auto& p : by_val) {
    f->dump_int(p.first.c_str(), p.second);
  }
  f->close_section();
}

void OSDMonitor::get_versions(std::map<string, list<string>> &versions)
{
  for (int osd = 0; osd < osdmap.get_max_osd(); ++osd) {
    if (osdmap.is_up(osd)) {
      map<string,string> meta;
      load_metadata(osd, meta, nullptr);
      auto p = meta.find("ceph_version_short");
      if (p == meta.end()) continue;
      versions[p->second].push_back(string("osd.") + stringify(osd));
    }
  }
}

int OSDMonitor::get_osd_objectstore_type(int osd, string *type)
{
  map<string, string> metadata;
  int r = load_metadata(osd, metadata, nullptr);
  if (r < 0)
    return r;

  auto it = metadata.find("osd_objectstore");
  if (it == metadata.end())
    return -ENOENT;
  *type = it->second;
  return 0;
}

void OSDMonitor::get_filestore_osd_list()
{
  for (unsigned osd = 0; osd < osdmap.get_num_osds(); ++osd) {
    string objectstore_type;
    int r = get_osd_objectstore_type(osd, &objectstore_type);
    if (r == 0 && objectstore_type == "filestore") {
      filestore_osds.insert(osd);
    }
  }
}

void OSDMonitor::check_for_filestore_osds(health_check_map_t *checks)
{
  if (g_conf()->mon_warn_on_filestore_osds &&
      filestore_osds.size() > 0) {
    ostringstream ss, deprecated_tip;
    list<string> detail;
    ss << filestore_osds.size()
       << " osd(s) "
       << (filestore_osds.size() == 1 ? "is" : "are")
       << " running Filestore";
    deprecated_tip << ss.str();
    ss << " [Deprecated]";
    auto& d = checks->add("OSD_FILESTORE", HEALTH_WARN, ss.str(),
                          filestore_osds.size());
    deprecated_tip << ", which has been deprecated and"
                   << " not been optimized for QoS"
                   << " (Filestore OSDs will use 'osd_op_queue = wpq' strictly)";
    detail.push_back(deprecated_tip.str());
    d.detail.swap(detail);
  }
}

bool OSDMonitor::is_pool_currently_all_bluestore(int64_t pool_id,
						 const pg_pool_t &pool,
						 ostream *err)
{
  // just check a few pgs for efficiency - this can't give a guarantee anyway,
  // since filestore osds could always join the pool later
  set<int> checked_osds;
  for (unsigned ps = 0; ps < std::min(8u, pool.get_pg_num()); ++ps) {
    vector<int> up, acting;
    pg_t pgid(ps, pool_id);
    osdmap.pg_to_up_acting_osds(pgid, up, acting);
    for (int osd : up) {
      if (checked_osds.find(osd) != checked_osds.end())
	continue;
      string objectstore_type;
      int r = get_osd_objectstore_type(osd, &objectstore_type);
      // allow with missing metadata, e.g. due to an osd never booting yet
      if (r < 0 || objectstore_type == "bluestore") {
	checked_osds.insert(osd);
	continue;
      }
      *err << "osd." << osd << " uses " << objectstore_type;
      return false;
    }
  }
  return true;
}

int OSDMonitor::dump_osd_metadata(int osd, Formatter *f, ostream *err)
{
  map<string,string> m;
  if (int r = load_metadata(osd, m, err))
    return r;
  for (map<string,string>::iterator p = m.begin(); p != m.end(); ++p)
    f->dump_string(p->first.c_str(), p->second);
  return 0;
}

void OSDMonitor::print_nodes(Formatter *f)
{
  // group OSDs by their hosts
  map<string, list<int> > osds; // hostname => osd
  for (int osd = 0; osd < osdmap.get_max_osd(); osd++) {
    map<string, string> m;
    if (load_metadata(osd, m, NULL)) {
      continue;
    }
    map<string, string>::iterator hostname = m.find("hostname");
    if (hostname == m.end()) {
      // not likely though
      continue;
    }
    osds[hostname->second].push_back(osd);
  }

  dump_services(f, osds, "osd");
}

void OSDMonitor::share_map_with_random_osd()
{
  if (osdmap.get_num_up_osds() == 0) {
    dout(10) << __func__ << " no up osds, don't share with anyone" << dendl;
    return;
  }

  MonSession *s = mon.session_map.get_random_osd_session(&osdmap);
  if (!s) {
    dout(10) << __func__ << " no up osd on our session map" << dendl;
    return;
  }

  dout(10) << "committed, telling random " << s->name
	   << " all about it" << dendl;

  // get feature of the peer
  // use quorum_con_features, if it's an anonymous connection.
  uint64_t features = s->con_features ? s->con_features :
                                        mon.get_quorum_con_features();
  // whatev, they'll request more if they need it
  MOSDMap *m = build_incremental(osdmap.get_epoch() - 1, osdmap.get_epoch(), features);
  s->con->send_message(m);
  // NOTE: do *not* record osd has up to this epoch (as we do
  // elsewhere) as they may still need to request older values.
}

version_t OSDMonitor::get_trim_to() const
{
  if (mon.get_quorum().empty()) {
    dout(10) << __func__ << " quorum not formed, trim_to = 0" << dendl;
    return 0;
  }

  {
    std::lock_guard<std::mutex> l(creating_pgs_lock);
    if (!creating_pgs.pgs.empty()) {
      dout(10) << __func__ << " pgs creating, trim_to = 0" << dendl;
      return 0;
    }
  }

  if (g_conf().get_val<bool>("mon_debug_block_osdmap_trim")) {
    dout(0) << __func__
            << " blocking osdmap trim"
            << " ('mon_debug_block_osdmap_trim' set to 'true')"
            << " trim_to = 0" << dendl;
    return 0;
  }

  {
    epoch_t floor = get_min_last_epoch_clean();
    dout(10) << " min_last_epoch_clean " << floor << dendl;
    if (g_conf()->mon_osd_force_trim_to > 0 &&
	g_conf()->mon_osd_force_trim_to < (int)get_last_committed()) {
      floor = g_conf()->mon_osd_force_trim_to;
      dout(10) << __func__
               << " explicit mon_osd_force_trim_to = " << floor << dendl;
    }
    unsigned min = g_conf()->mon_min_osdmap_epochs;
    if (floor + min > get_last_committed()) {
      if (min < get_last_committed())
	floor = get_last_committed() - min;
      else
	floor = 0;
    }
    if (floor > get_first_committed()) {
      dout(10) << __func__ << " trim_to = " << floor << dendl;
      return floor;
    }
  }
  dout(10) << __func__ << " trim_to = 0" << dendl;
  return 0;
}

epoch_t OSDMonitor::get_min_last_epoch_clean() const
{
  auto floor = last_epoch_clean.get_lower_bound_by_pool(osdmap);
  // also scan osd epochs
  // don't trim past the oldest reported osd epoch
  for (auto [osd, epoch] : osd_epochs) {
    if (epoch < floor) {
      floor = epoch;
    }
  }
  return floor;
}

void OSDMonitor::encode_trim_extra(MonitorDBStore::TransactionRef tx,
				   version_t first)
{
  dout(10) << __func__ << " including full map for e " << first << dendl;
  bufferlist bl;
  get_version_full(first, bl);
  put_version_full(tx, first, bl);

  if (has_osdmap_manifest &&
      first > osdmap_manifest.get_first_pinned()) {
    _prune_update_trimmed(tx, first);
  }
}


/* full osdmap prune
 *
 * for more information, please refer to doc/dev/mon-osdmap-prune.rst
 */

void OSDMonitor::load_osdmap_manifest()
{
  bool store_has_manifest =
    mon.store->exists(get_service_name(), "osdmap_manifest");

  if (!store_has_manifest) {
    if (!has_osdmap_manifest) {
      return;
    }

    dout(20) << __func__
             << " dropping osdmap manifest from memory." << dendl;
    osdmap_manifest = osdmap_manifest_t();
    has_osdmap_manifest = false;
    return;
  }

  dout(20) << __func__
           << " osdmap manifest detected in store; reload." << dendl;

  bufferlist manifest_bl;
  int r = get_value("osdmap_manifest", manifest_bl);
  if (r < 0) {
    derr << __func__ << " unable to read osdmap version manifest" << dendl;
    ceph_abort_msg("error reading manifest");
  }
  osdmap_manifest.decode(manifest_bl);
  has_osdmap_manifest = true;

  dout(10) << __func__ << " store osdmap manifest pinned ("
           << osdmap_manifest.get_first_pinned()
           << " .. "
           << osdmap_manifest.get_last_pinned()
           << ")"
           << dendl;
}

bool OSDMonitor::should_prune() const
{
  version_t first = get_first_committed();
  version_t last = get_last_committed();
  version_t min_osdmap_epochs =
    g_conf().get_val<int64_t>("mon_min_osdmap_epochs");
  version_t prune_min =
    g_conf().get_val<uint64_t>("mon_osdmap_full_prune_min");
  version_t prune_interval =
    g_conf().get_val<uint64_t>("mon_osdmap_full_prune_interval");
  version_t last_pinned = osdmap_manifest.get_last_pinned();
  version_t last_to_pin = last - min_osdmap_epochs;

  // Make it or break it constraints.
  //
  // If any of these conditions fails, we will not prune, regardless of
  // whether we have an on-disk manifest with an on-going pruning state.
  //
  if ((last - first) <= min_osdmap_epochs) {
    // between the first and last committed epochs, we don't have
    // enough epochs to trim, much less to prune.
    dout(10) << __func__
             << " currently holding only " << (last - first)
             << " epochs (min osdmap epochs: " << min_osdmap_epochs
             << "); do not prune."
             << dendl;
    return false;

  } else if ((last_to_pin - first) < prune_min) {
    // between the first committed epoch and the last epoch we would prune,
    // we simply don't have enough versions over the minimum to prune maps.
    dout(10) << __func__
             << " could only prune " << (last_to_pin - first)
             << " epochs (" << first << ".." << last_to_pin << "), which"
                " is less than the required minimum (" << prune_min << ")"
             << dendl;
    return false;

  } else if (has_osdmap_manifest && last_pinned >= last_to_pin) {
    dout(10) << __func__
             << " we have pruned as far as we can; do not prune."
             << dendl;
    return false;

  } else if (last_pinned + prune_interval > last_to_pin) {
    dout(10) << __func__
             << " not enough epochs to form an interval (last pinned: "
             << last_pinned << ", last to pin: "
             << last_to_pin << ", interval: " << prune_interval << ")"
             << dendl;
    return false;
  }

  dout(15) << __func__
           << " should prune (" << last_pinned << ".." << last_to_pin << ")"
           << " lc (" << first << ".." << last << ")"
           << dendl;
  return true;
}

void OSDMonitor::_prune_update_trimmed(
    MonitorDBStore::TransactionRef tx,
    version_t first)
{
  dout(10) << __func__
           << " first " << first
           << " last_pinned " << osdmap_manifest.get_last_pinned()
           << dendl;

  osdmap_manifest_t manifest = osdmap_manifest;

  if (!manifest.is_pinned(first)) {
    manifest.pin(first);
  }

  set<version_t>::iterator p_end = manifest.pinned.find(first);
  set<version_t>::iterator p = manifest.pinned.begin();
  manifest.pinned.erase(p, p_end);
  ceph_assert(manifest.get_first_pinned() == first);

  if (manifest.get_last_pinned() == first+1 ||
      manifest.pinned.size() == 1) {
    // we reached the end of the line, as pinned maps go; clean up our
    // manifest, and let `should_prune()` decide whether we should prune
    // again.
    tx->erase(get_service_name(), "osdmap_manifest");
    return;
  }

  bufferlist bl;
  manifest.encode(bl);
  tx->put(get_service_name(), "osdmap_manifest", bl);
}

void OSDMonitor::prune_init(osdmap_manifest_t& manifest)
{
  dout(1) << __func__ << dendl;

  version_t pin_first;

  // verify constrainsts on stable in-memory state
  if (!has_osdmap_manifest) {
    // we must have never pruned, OR if we pruned the state must no longer
    // be relevant (i.e., the state must have been removed alongside with
    // the trim that *must* have removed past the last pinned map in a
    // previous prune).
    ceph_assert(osdmap_manifest.pinned.empty());
    ceph_assert(!mon.store->exists(get_service_name(), "osdmap_manifest"));
    pin_first = get_first_committed();

  } else {
    // we must have pruned in the past AND its state is still relevant
    // (i.e., even if we trimmed, we still hold pinned maps in the manifest,
    // and thus we still hold a manifest in the store).
    ceph_assert(!osdmap_manifest.pinned.empty());
    ceph_assert(osdmap_manifest.get_first_pinned() == get_first_committed());
    ceph_assert(osdmap_manifest.get_last_pinned() < get_last_committed());

    dout(10) << __func__
             << " first_pinned " << osdmap_manifest.get_first_pinned()
             << " last_pinned " << osdmap_manifest.get_last_pinned()
             << dendl;

    pin_first = osdmap_manifest.get_last_pinned();
  }

  manifest.pin(pin_first);
}

bool OSDMonitor::_prune_sanitize_options() const
{
  uint64_t prune_interval =
    g_conf().get_val<uint64_t>("mon_osdmap_full_prune_interval");
  uint64_t prune_min =
    g_conf().get_val<uint64_t>("mon_osdmap_full_prune_min");
  uint64_t txsize =
    g_conf().get_val<uint64_t>("mon_osdmap_full_prune_txsize");

  bool r = true;

  if (prune_interval == 0) {
    derr << __func__
         << " prune is enabled BUT prune interval is zero; abort."
         << dendl;
    r = false;
  } else if (prune_interval == 1) {
    derr << __func__
         << " prune interval is equal to one, which essentially means"
            " no pruning; abort."
         << dendl;
    r = false;
  }
  if (prune_min == 0) {
    derr << __func__
         << " prune is enabled BUT prune min is zero; abort."
         << dendl;
    r = false;
  }
  if (prune_interval > prune_min) {
    derr << __func__
         << " impossible to ascertain proper prune interval because"
         << " it is greater than the minimum prune epochs"
         << " (min: " << prune_min << ", interval: " << prune_interval << ")"
         << dendl;
    r = false;
  }

  if (txsize < prune_interval - 1) {
    derr << __func__
         << " 'mon_osdmap_full_prune_txsize' (" << txsize
         << ") < 'mon_osdmap_full_prune_interval-1' (" << prune_interval - 1
         << "); abort." << dendl;
    r = false;
  }
  return r;
}

bool OSDMonitor::is_prune_enabled() const {
  return g_conf().get_val<bool>("mon_osdmap_full_prune_enabled");
}

bool OSDMonitor::is_prune_supported() const {
  return mon.get_required_mon_features().contains_any(
      ceph::features::mon::FEATURE_OSDMAP_PRUNE);
}

/** do_prune
 *
 * @returns true if has side-effects; false otherwise.
 */
bool OSDMonitor::do_prune(MonitorDBStore::TransactionRef tx)
{
  bool enabled = is_prune_enabled();

  dout(1) << __func__ << " osdmap full prune "
          << ( enabled ? "enabled" : "disabled")
          << dendl;

  if (!enabled || !_prune_sanitize_options() || !should_prune()) {
    return false;
  }

  // we are beyond the minimum prune versions, we need to remove maps because
  // otherwise the store will grow unbounded and we may end up having issues
  // with available disk space or store hangs.

  // we will not pin all versions. We will leave a buffer number of versions.
  // this allows us the monitor to trim maps without caring too much about
  // pinned maps, and then allow us to use another ceph-mon without these
  // capabilities, without having to repair the store.

  osdmap_manifest_t manifest = osdmap_manifest;

  version_t first = get_first_committed();
  version_t last = get_last_committed();

  version_t last_to_pin = last - g_conf()->mon_min_osdmap_epochs;
  version_t last_pinned = manifest.get_last_pinned();
  uint64_t prune_interval =
    g_conf().get_val<uint64_t>("mon_osdmap_full_prune_interval");
  uint64_t txsize =
    g_conf().get_val<uint64_t>("mon_osdmap_full_prune_txsize");

  prune_init(manifest);

  // we need to get rid of some osdmaps

  dout(5) << __func__
          << " lc (" << first << " .. " << last << ")"
          << " last_pinned " << last_pinned
          << " interval " << prune_interval
          << " last_to_pin " << last_to_pin
          << dendl;

  // We will be erasing maps as we go.
  //
  // We will erase all maps between `last_pinned` and the `next_to_pin`.
  //
  // If `next_to_pin` happens to be greater than `last_to_pin`, then
  // we stop pruning. We could prune the maps between `next_to_pin` and
  // `last_to_pin`, but by not doing it we end up with neater pruned
  // intervals, aligned with `prune_interval`. Besides, this should not be a
  // problem as long as `prune_interval` is set to a sane value, instead of
  // hundreds or thousands of maps.

  auto map_exists = [this](version_t v) {
    string k = mon.store->combine_strings("full", v);
    return mon.store->exists(get_service_name(), k);
  };

  // 'interval' represents the number of maps from the last pinned
  // i.e., if we pinned version 1 and have an interval of 10, we're pinning
  // version 11 next; all intermediate versions will be removed.
  //
  // 'txsize' represents the maximum number of versions we'll be removing in
  // this iteration. If 'txsize' is large enough to perform multiple passes
  // pinning and removing maps, we will do so; if not, we'll do at least one
  // pass. We are quite relaxed about honouring 'txsize', but we'll always
  // ensure that we never go *over* the maximum.

  // e.g., if we pin 1 and 11, we're removing versions [2..10]; i.e., 9 maps.
  uint64_t removal_interval = prune_interval - 1;

  if (txsize < removal_interval) {
    dout(5) << __func__
	    << " setting txsize to removal interval size ("
	    << removal_interval << " versions"
	    << dendl;
    txsize = removal_interval;
  }
  ceph_assert(removal_interval > 0);

  uint64_t num_pruned = 0;
  while (num_pruned + removal_interval <= txsize) { 
    last_pinned = manifest.get_last_pinned();

    if (last_pinned + prune_interval > last_to_pin) {
      break;
    }
    ceph_assert(last_pinned < last_to_pin);

    version_t next_pinned = last_pinned + prune_interval;
    ceph_assert(next_pinned <= last_to_pin);
    manifest.pin(next_pinned);

    dout(20) << __func__
	     << " last_pinned " << last_pinned
	     << " next_pinned " << next_pinned
	     << " num_pruned " << num_pruned
	     << " removal interval (" << (last_pinned+1)
	     << ".." << (next_pinned-1) << ")"
	     << " txsize " << txsize << dendl;

    ceph_assert(map_exists(last_pinned));
    ceph_assert(map_exists(next_pinned));

    for (version_t v = last_pinned+1; v < next_pinned; ++v) {
      ceph_assert(!manifest.is_pinned(v));

      dout(20) << __func__ << "   pruning full osdmap e" << v << dendl;
      string full_key = mon.store->combine_strings("full", v);
      tx->erase(get_service_name(), full_key);
      ++num_pruned;
    }
  }

  ceph_assert(num_pruned > 0);

  bufferlist bl;
  manifest.encode(bl);
  tx->put(get_service_name(), "osdmap_manifest", bl);

  return true;
}


// -------------

bool OSDMonitor::preprocess_query(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  Message *m = op->get_req();
  dout(10) << "preprocess_query " << *m << " from " << m->get_orig_source_inst() << dendl;

  switch (m->get_type()) {
    // READs
  case MSG_MON_COMMAND:
    try {
      return preprocess_command(op);
    } catch (const bad_cmd_get& e) {
      bufferlist bl;
      mon.reply_command(op, -EINVAL, e.what(), bl, get_last_committed());
      return true;
    }
  case CEPH_MSG_MON_GET_OSDMAP:
    return preprocess_get_osdmap(op);

    // damp updates
  case MSG_OSD_MARK_ME_DOWN:
    return preprocess_mark_me_down(op);
  case MSG_OSD_MARK_ME_DEAD:
    return preprocess_mark_me_dead(op);
  case MSG_OSD_FULL:
    return preprocess_full(op);
  case MSG_OSD_FAILURE:
    return preprocess_failure(op);
  case MSG_OSD_BOOT:
    return preprocess_boot(op);
  case MSG_OSD_ALIVE:
    return preprocess_alive(op);
  case MSG_OSD_PG_CREATED:
    return preprocess_pg_created(op);
  case MSG_OSD_PG_READY_TO_MERGE:
    return preprocess_pg_ready_to_merge(op);
  case MSG_OSD_PGTEMP:
    return preprocess_pgtemp(op);
  case MSG_OSD_BEACON:
    return preprocess_beacon(op);

  case CEPH_MSG_POOLOP:
    return preprocess_pool_op(op);

  case MSG_REMOVE_SNAPS:
    return preprocess_remove_snaps(op);

  case MSG_MON_GET_PURGED_SNAPS:
    return preprocess_get_purged_snaps(op);

  default:
    ceph_abort();
    return false;
  }
}

bool OSDMonitor::prepare_update(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  Message *m = op->get_req();
  dout(7) << "prepare_update " << *m << " from " << m->get_orig_source_inst() << dendl;

  switch (m->get_type()) {
    // damp updates
  case MSG_OSD_MARK_ME_DOWN:
    return prepare_mark_me_down(op);
  case MSG_OSD_MARK_ME_DEAD:
    return prepare_mark_me_dead(op);
  case MSG_OSD_FULL:
    return prepare_full(op);
  case MSG_OSD_FAILURE:
    return prepare_failure(op);
  case MSG_OSD_BOOT:
    return prepare_boot(op);
  case MSG_OSD_ALIVE:
    return prepare_alive(op);
  case MSG_OSD_PG_CREATED:
    return prepare_pg_created(op);
  case MSG_OSD_PGTEMP:
    return prepare_pgtemp(op);
  case MSG_OSD_PG_READY_TO_MERGE:
    return prepare_pg_ready_to_merge(op);
  case MSG_OSD_BEACON:
    return prepare_beacon(op);

  case MSG_MON_COMMAND:
    try {
      return prepare_command(op);
    } catch (const bad_cmd_get& e) {
      bufferlist bl;
      mon.reply_command(op, -EINVAL, e.what(), bl, get_last_committed());
      return false; /* nothing to propose */
    }

  case CEPH_MSG_POOLOP:
    return prepare_pool_op(op);

  case MSG_REMOVE_SNAPS:
    return prepare_remove_snaps(op);


  default:
    ceph_abort();
  }

  return false;
}

bool OSDMonitor::should_propose(double& delay)
{
  dout(10) << "should_propose" << dendl;

  // if full map, propose immediately!  any subsequent changes will be clobbered.
  if (pending_inc.fullmap.length())
    return true;

  // adjust osd weights?
  if (!osd_weight.empty() &&
      osd_weight.size() == (unsigned)osdmap.get_max_osd()) {
    dout(0) << " adjusting osd weights based on " << osd_weight << dendl;
    osdmap.adjust_osd_weights(osd_weight, pending_inc);
    delay = 0.0;
    osd_weight.clear();
    return true;
  }

  return PaxosService::should_propose(delay);
}



// ---------------------------
// READs

bool OSDMonitor::preprocess_get_osdmap(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MMonGetOSDMap>();

  uint64_t features = mon.get_quorum_con_features();
  if (op->get_session() && op->get_session()->con_features)
    features = op->get_session()->con_features;

  dout(10) << __func__ << " " << *m << dendl;
  MOSDMap *reply = new MOSDMap(mon.monmap->fsid, features);
  epoch_t first = get_first_committed();
  epoch_t last = osdmap.get_epoch();
  int max = g_conf()->osd_map_message_max;
  ssize_t max_bytes = g_conf()->osd_map_message_max_bytes;
  for (epoch_t e = std::max(first, m->get_full_first());
       e <= std::min(last, m->get_full_last()) && max > 0 && max_bytes > 0;
       ++e, --max) {
    bufferlist& bl = reply->maps[e];
    int r = get_version_full(e, features, bl);
    ceph_assert(r >= 0);
    max_bytes -= bl.length();
  }
  for (epoch_t e = std::max(first, m->get_inc_first());
       e <= std::min(last, m->get_inc_last()) && max > 0 && max_bytes > 0;
       ++e, --max) {
    bufferlist& bl = reply->incremental_maps[e];
    int r = get_version(e, features, bl);
    ceph_assert(r >= 0);
    max_bytes -= bl.length();
  }
  reply->cluster_osdmap_trim_lower_bound = first;
  reply->newest_map = last;
  mon.send_reply(op, reply);
  return true;
}


// ---------------------------
// UPDATEs

// failure --

bool OSDMonitor::check_source(MonOpRequestRef op, uuid_d fsid) {
  // check permissions
  MonSession *session = op->get_session();
  if (!session)
    return true;
  if (!session->is_capable("osd", MON_CAP_X)) {
    dout(0) << "got MOSDFailure from entity with insufficient caps "
	    << session->caps << dendl;
    return true;
  }
  if (fsid != mon.monmap->fsid) {
    dout(0) << "check_source: on fsid " << fsid
	    << " != " << mon.monmap->fsid << dendl;
    return true;
  }
  return false;
}


bool OSDMonitor::preprocess_failure(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MOSDFailure>();
  // who is target_osd
  int badboy = m->get_target_osd();

  // check permissions
  if (check_source(op, m->fsid))
    goto didit;

  // first, verify the reporting host is valid
  if (m->get_orig_source().is_osd()) {
    int from = m->get_orig_source().num();
    if (!osdmap.exists(from) ||
	!osdmap.get_addrs(from).legacy_equals(m->get_orig_source_addrs()) ||
	(osdmap.is_down(from) && m->if_osd_failed())) {
      dout(5) << "preprocess_failure from dead osd." << from
	      << ", ignoring" << dendl;
      send_incremental(op, m->get_epoch()+1);
      goto didit;
    }
  }


  // weird?
  if (osdmap.is_down(badboy)) {
    dout(5) << "preprocess_failure dne(/dup?): osd." << m->get_target_osd()
	    << " " << m->get_target_addrs()
	    << ", from " << m->get_orig_source() << dendl;
    if (m->get_epoch() < osdmap.get_epoch())
      send_incremental(op, m->get_epoch()+1);
    goto didit;
  }
  if (osdmap.get_addrs(badboy) != m->get_target_addrs()) {
    dout(5) << "preprocess_failure wrong osd: report osd." << m->get_target_osd()
	    << " " << m->get_target_addrs()
	    << " != map's " << osdmap.get_addrs(badboy)
	    << ", from " << m->get_orig_source() << dendl;
    if (m->get_epoch() < osdmap.get_epoch())
      send_incremental(op, m->get_epoch()+1);
    goto didit;
  }

  // already reported?
  if (osdmap.is_down(badboy) ||
      osdmap.get_up_from(badboy) > m->get_epoch()) {
    dout(5) << "preprocess_failure dup/old: osd." << m->get_target_osd()
	    << " " << m->get_target_addrs()
	    << ", from " << m->get_orig_source() << dendl;
    if (m->get_epoch() < osdmap.get_epoch())
      send_incremental(op, m->get_epoch()+1);
    goto didit;
  }

  if (!can_mark_down(badboy)) {
    dout(5) << "preprocess_failure ignoring report of osd."
	    << m->get_target_osd() << " " << m->get_target_addrs()
	    << " from " << m->get_orig_source() << dendl;
    goto didit;
  }

  dout(10) << "preprocess_failure new: osd." << m->get_target_osd()
	   << " " << m->get_target_addrs()
	   << ", from " << m->get_orig_source() << dendl;
  return false;

 didit:
  mon.no_reply(op);
  return true;
}

class C_AckMarkedDown : public C_MonOp {
  OSDMonitor *osdmon;
public:
  C_AckMarkedDown(
    OSDMonitor *osdmon,
    MonOpRequestRef op)
    : C_MonOp(op), osdmon(osdmon) {}

  void _finish(int r) override {
    if (r == 0) {
      auto m = op->get_req<MOSDMarkMeDown>();
      osdmon->mon.send_reply(
        op,
        new MOSDMarkMeDown(
          m->fsid,
          m->target_osd,
          m->target_addrs,
          m->get_epoch(),
          false));   // ACK itself does not request an ack
    } else if (r == -EAGAIN) {
        osdmon->dispatch(op);
    } else {
        ceph_abort_msgf("C_AckMarkedDown: unknown result %d", r);
    }
  }
  ~C_AckMarkedDown() override {
  }
};

bool OSDMonitor::preprocess_mark_me_down(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MOSDMarkMeDown>();
  int from = m->target_osd;

  // check permissions
  if (check_source(op, m->fsid))
    goto reply;

  // first, verify the reporting host is valid
  if (!m->get_orig_source().is_osd())
    goto reply;

  if (!osdmap.exists(from) ||
      osdmap.is_down(from) ||
      osdmap.get_addrs(from) != m->target_addrs) {
    dout(5) << "preprocess_mark_me_down from dead osd."
	    << from << ", ignoring" << dendl;
    send_incremental(op, m->get_epoch()+1);
    goto reply;
  }

  // no down might be set
  if (!can_mark_down(from))
    goto reply;

  dout(10) << "MOSDMarkMeDown for: " << m->get_orig_source()
	   << " " << m->target_addrs << dendl;
  return false;

 reply:
  if (m->request_ack) {
    Context *c(new C_AckMarkedDown(this, op));
    c->complete(0);
  }
  return true;
}

bool OSDMonitor::prepare_mark_me_down(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MOSDMarkMeDown>();
  int target_osd = m->target_osd;

  ceph_assert(osdmap.is_up(target_osd));
  ceph_assert(osdmap.get_addrs(target_osd) == m->target_addrs);

  mon.clog->info() << "osd." << target_osd << " marked itself " << ((m->down_and_dead) ? "down and dead" : "down");
  pending_inc.new_state[target_osd] = CEPH_OSD_UP;
  if (m->down_and_dead) {
    if (!pending_inc.new_xinfo.count(target_osd)) {
      pending_inc.new_xinfo[target_osd] = osdmap.osd_xinfo[target_osd];
    }
    pending_inc.new_xinfo[target_osd].dead_epoch = m->get_epoch();
  }
  if (m->request_ack)
    wait_for_finished_proposal(op, new C_AckMarkedDown(this, op));
  return true;
}

bool OSDMonitor::preprocess_mark_me_dead(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MOSDMarkMeDead>();
  int from = m->target_osd;

  // check permissions
  if (check_source(op, m->fsid)) {
    mon.no_reply(op);
    return true;
  }

  // first, verify the reporting host is valid
  if (!m->get_orig_source().is_osd()) {
    mon.no_reply(op);
    return true;
  }

  if (!osdmap.exists(from) ||
      !osdmap.is_down(from)) {
    dout(5) << __func__ << " from nonexistent or up osd." << from
	    << ", ignoring" << dendl;
    send_incremental(op, m->get_epoch()+1);
    mon.no_reply(op);
    return true;
  }

  return false;
}

bool OSDMonitor::prepare_mark_me_dead(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MOSDMarkMeDead>();
  int target_osd = m->target_osd;

  ceph_assert(osdmap.is_down(target_osd));

  mon.clog->info() << "osd." << target_osd << " marked itself dead as of e"
		    << m->get_epoch();
  if (!pending_inc.new_xinfo.count(target_osd)) {
    pending_inc.new_xinfo[target_osd] = osdmap.osd_xinfo[target_osd];
  }
  pending_inc.new_xinfo[target_osd].dead_epoch = m->get_epoch();
  wait_for_finished_proposal(
    op,
    new LambdaContext(
      [op, this] (int r) {
	if (r >= 0) {
	  mon.no_reply(op);	  // ignore on success
	}
      }
      ));
  return true;
}

bool OSDMonitor::can_mark_down(int i)
{
  if (osdmap.is_nodown(i)) {
    dout(5) << __func__ << " osd." << i << " is marked as nodown, "
            << "will not mark it down" << dendl;
    return false;
  }

  int num_osds = osdmap.get_num_osds();
  if (num_osds == 0) {
    dout(5) << __func__ << " no osds" << dendl;
    return false;
  }
  int up = osdmap.get_num_up_osds() - pending_inc.get_net_marked_down(&osdmap);
  float up_ratio = (float)up / (float)num_osds;
  if (up_ratio < g_conf()->mon_osd_min_up_ratio) {
    dout(2) << __func__ << " current up_ratio " << up_ratio << " < min "
	    << g_conf()->mon_osd_min_up_ratio
	    << ", will not mark osd." << i << " down" << dendl;
    return false;
  }
  return true;
}

bool OSDMonitor::can_mark_up(int i)
{
  if (osdmap.is_noup(i)) {
    dout(5) << __func__ << " osd." << i << " is marked as noup, "
            << "will not mark it up" << dendl;
    return false;
  }

  return true;
}

/**
 * @note the parameter @p i apparently only exists here so we can output the
 *	 osd's id on messages.
 */
bool OSDMonitor::can_mark_out(int i)
{
  if (osdmap.is_noout(i)) {
    dout(5) << __func__ << " osd." << i << " is marked as noout, "
            << "will not mark it out" << dendl;
    return false;
  }

  int num_osds = osdmap.get_num_osds();
  if (num_osds == 0) {
    dout(5) << __func__ << " no osds" << dendl;
    return false;
  }
  int in = osdmap.get_num_in_osds() - pending_inc.get_net_marked_out(&osdmap);
  float in_ratio = (float)in / (float)num_osds;
  if (in_ratio < g_conf()->mon_osd_min_in_ratio) {
    if (i >= 0)
      dout(5) << __func__ << " current in_ratio " << in_ratio << " < min "
	      << g_conf()->mon_osd_min_in_ratio
	      << ", will not mark osd." << i << " out" << dendl;
    else
      dout(5) << __func__ << " current in_ratio " << in_ratio << " < min "
	      << g_conf()->mon_osd_min_in_ratio
	      << ", will not mark osds out" << dendl;
    return false;
  }

  return true;
}

bool OSDMonitor::can_mark_in(int i)
{
  if (osdmap.is_noin(i)) {
    dout(5) << __func__ << " osd." << i << " is marked as noin, "
            << "will not mark it in" << dendl;
    return false;
  }

  return true;
}

bool OSDMonitor::check_failures(utime_t now)
{
  bool found_failure = false;
  auto p = failure_info.begin();
  while (p != failure_info.end()) {
    auto& [target_osd, fi] = *p;
    if (can_mark_down(target_osd) &&
	check_failure(now, target_osd, fi)) {
      found_failure = true;
      ++p;
    } else if (is_failure_stale(now, fi)) {
      dout(10) << " dropping stale failure_info for osd." << target_osd
	       << " from " << fi.reporters.size() << " reporters"
	       << dendl;
      p = failure_info.erase(p);
    } else {
      ++p;
    }
  }
  return found_failure;
}

utime_t OSDMonitor::get_grace_time(utime_t now,
				   int target_osd,
				   failure_info_t& fi) const
{
  utime_t orig_grace(g_conf()->osd_heartbeat_grace, 0);
  if (!g_conf()->mon_osd_adjust_heartbeat_grace) {
    return orig_grace;
  }
  utime_t grace = orig_grace;
  double halflife = (double)g_conf()->mon_osd_laggy_halflife;
  double decay_k = ::log(.5) / halflife;

  // scale grace period based on historical probability of 'lagginess'
  // (false positive failures due to slowness).
  const osd_xinfo_t& xi = osdmap.get_xinfo(target_osd);
  const utime_t failed_for = now - fi.get_failed_since();
  double decay = exp((double)failed_for * decay_k);
  dout(20) << " halflife " << halflife << " decay_k " << decay_k
	   << " failed_for " << failed_for << " decay " << decay << dendl;
  double my_grace = decay * (double)xi.laggy_interval * xi.laggy_probability;
  grace += my_grace;

  // consider the peers reporting a failure a proxy for a potential
  // 'subcluster' over the overall cluster that is similarly
  // laggy.  this is clearly not true in all cases, but will sometimes
  // help us localize the grace correction to a subset of the system
  // (say, a rack with a bad switch) that is unhappy.
  double peer_grace = 0;
  for (auto& [reporter, report] : fi.reporters) {
    if (osdmap.exists(reporter)) {
      const osd_xinfo_t& xi = osdmap.get_xinfo(reporter);
      utime_t elapsed = now - xi.down_stamp;
      double decay = exp((double)elapsed * decay_k);
      peer_grace += decay * (double)xi.laggy_interval * xi.laggy_probability;
    }
  }
  peer_grace /= (double)fi.reporters.size();
  grace += peer_grace;
  dout(10) << " osd." << target_osd << " has "
	   << fi.reporters.size() << " reporters, "
	   << grace << " grace (" << orig_grace << " + " << my_grace
	   << " + " << peer_grace << "), max_failed_since " << fi.get_failed_since()
	   << dendl;

  return grace;
}

bool OSDMonitor::check_failure(utime_t now, int target_osd, failure_info_t& fi)
{
  // already pending failure?
  if (pending_inc.new_state.count(target_osd) &&
      pending_inc.new_state[target_osd] & CEPH_OSD_UP) {
    dout(10) << " already pending failure" << dendl;
    return true;
  }

  set<string> reporters_by_subtree;
  auto reporter_subtree_level = g_conf().get_val<string>("mon_osd_reporter_subtree_level");
  ceph_assert(fi.reporters.size());
  for (auto p = fi.reporters.begin(); p != fi.reporters.end();) {
    // get the parent bucket whose type matches with "reporter_subtree_level".
    // fall back to OSD if the level doesn't exist.
    if (osdmap.exists(p->first)) {
      auto reporter_loc = osdmap.crush->get_full_location(p->first);
      if (auto iter = reporter_loc.find(reporter_subtree_level);
          iter == reporter_loc.end()) {
        reporters_by_subtree.insert("osd." + to_string(p->first));
      } else {
        reporters_by_subtree.insert(iter->second);
      }
      ++p;
    } else {
      fi.cancel_report(p->first);;
      p = fi.reporters.erase(p);
    }
  }
  if (reporters_by_subtree.size() < g_conf().get_val<uint64_t>("mon_osd_min_down_reporters")) {
    return false;
  }
  const utime_t failed_for = now - fi.get_failed_since();
  const utime_t grace = get_grace_time(now, target_osd, fi);
  if (failed_for >= grace) {
    dout(1) << " we have enough reporters to mark osd." << target_osd
	    << " down" << dendl;
    pending_inc.new_state[target_osd] = CEPH_OSD_UP;

    mon.clog->info() << "osd." << target_osd << " failed ("
		      << osdmap.crush->get_full_location_ordered_string(
			target_osd)
		      << ") ("
		      << (int)reporters_by_subtree.size()
		      << " reporters from different "
		      << reporter_subtree_level << " after "
		      << failed_for << " >= grace " << grace << ")";
    return true;
  }
  return false;
}

bool OSDMonitor::is_failure_stale(utime_t now, failure_info_t& fi) const
{
  // if it takes too long to either cancel the report to mark the osd down,
  // some reporters must have failed to cancel their reports. let's just
  // forget these reports.
  const utime_t failed_for = now - fi.get_failed_since();
  auto heartbeat_grace = cct->_conf.get_val<int64_t>("osd_heartbeat_grace");
  auto heartbeat_stale = cct->_conf.get_val<int64_t>("osd_heartbeat_stale");
  return failed_for >= (heartbeat_grace + heartbeat_stale);
}

void OSDMonitor::force_failure(int target_osd, int by)
{
  // already pending failure?
  if (pending_inc.new_state.count(target_osd) &&
      pending_inc.new_state[target_osd] & CEPH_OSD_UP) {
    dout(10) << " already pending failure" << dendl;
    return;
  }

  dout(1) << " we're forcing failure of osd." << target_osd << dendl;
  pending_inc.new_state[target_osd] = CEPH_OSD_UP;
  if (!pending_inc.new_xinfo.count(target_osd)) {
    pending_inc.new_xinfo[target_osd] = osdmap.osd_xinfo[target_osd];
  }
  pending_inc.new_xinfo[target_osd].dead_epoch = pending_inc.epoch;

  mon.clog->info() << "osd." << target_osd << " failed ("
		    << osdmap.crush->get_full_location_ordered_string(target_osd)
		    << ") (connection refused reported by osd." << by << ")";
  return;
}

bool OSDMonitor::prepare_failure(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MOSDFailure>();
  dout(1) << "prepare_failure osd." << m->get_target_osd()
	  << " " << m->get_target_addrs()
	  << " from " << m->get_orig_source()
          << " is reporting failure:" << m->if_osd_failed() << dendl;

  int target_osd = m->get_target_osd();
  int reporter = m->get_orig_source().num();
  ceph_assert(osdmap.is_up(target_osd));
  ceph_assert(osdmap.get_addrs(target_osd) == m->get_target_addrs());

  mon.no_reply(op);

  if (m->if_osd_failed()) {
    // calculate failure time
    utime_t now = ceph_clock_now();
    utime_t failed_since =
      m->get_recv_stamp() - utime_t(m->failed_for, 0);

    // add a report
    if (m->is_immediate()) {
      mon.clog->debug() << "osd." << m->get_target_osd()
			 << " reported immediately failed by "
			 << m->get_orig_source();
      force_failure(target_osd, reporter);
      return true;
    }
    mon.clog->debug() << "osd." << m->get_target_osd() << " reported failed by "
		      << m->get_orig_source();

    failure_info_t& fi = failure_info[target_osd];
    fi.add_report(reporter, failed_since, op);
    return check_failure(now, target_osd, fi);
  } else {
    // remove the report
    mon.clog->debug() << "osd." << m->get_target_osd()
		       << " failure report canceled by "
		       << m->get_orig_source();
    if (failure_info.count(target_osd)) {
      failure_info_t& fi = failure_info[target_osd];
      fi.cancel_report(reporter);
      if (fi.reporters.empty()) {
	dout(10) << " removing last failure_info for osd." << target_osd
		 << dendl;
	failure_info.erase(target_osd);
      } else {
	dout(10) << " failure_info for osd." << target_osd << " now "
		 << fi.reporters.size() << " reporters" << dendl;
      }
    } else {
      dout(10) << " no failure_info for osd." << target_osd << dendl;
    }
  }

  return false;
}

void OSDMonitor::process_failures()
{
  map<int,failure_info_t>::iterator p = failure_info.begin();
  while (p != failure_info.end()) {
    if (osdmap.is_up(p->first)) {
      ++p;
    } else {
      dout(10) << "process_failures osd." << p->first << dendl;
      list<MonOpRequestRef> ls;
      p->second.take_report_messages(ls);
      failure_info.erase(p++);

      while (!ls.empty()) {
        MonOpRequestRef o = ls.front();
        if (o) {
          o->mark_event(__func__);
          MOSDFailure *m = o->get_req<MOSDFailure>();
          send_latest(o, m->get_epoch());
	  mon.no_reply(o);
        }
	ls.pop_front();
      }
    }
  }
}

void OSDMonitor::take_all_failures(list<MonOpRequestRef>& ls)
{
  dout(10) << __func__ << " on " << failure_info.size() << " osds" << dendl;

  for (map<int,failure_info_t>::iterator p = failure_info.begin();
       p != failure_info.end();
       ++p) {
    p->second.take_report_messages(ls);
  }
  failure_info.clear();
}

int OSDMonitor::get_grace_interval_threshold()
{
  int halflife = g_conf()->mon_osd_laggy_halflife;
  // Scale the halflife period (default: 1_hr) by
  // a factor (48) to calculate the threshold.
  int grace_threshold_factor = 48;
  return halflife * grace_threshold_factor;
}

bool OSDMonitor::grace_interval_threshold_exceeded(int last_failed_interval)
{
  int grace_interval_threshold_secs = get_grace_interval_threshold();
  if (last_failed_interval > grace_interval_threshold_secs) {
    dout(1) << " last_failed_interval " << last_failed_interval
            << " > grace_interval_threshold_secs " << grace_interval_threshold_secs
            << dendl;
    return true;
  }
  return false;
}

void OSDMonitor::set_default_laggy_params(int target_osd)
{
  if (pending_inc.new_xinfo.count(target_osd) == 0) {
    pending_inc.new_xinfo[target_osd] = osdmap.osd_xinfo[target_osd];
  }
  osd_xinfo_t& xi = pending_inc.new_xinfo[target_osd];
  xi.down_stamp = pending_inc.modified;
  xi.laggy_probability = 0.0;
  xi.laggy_interval = 0;
  dout(20) << __func__ << " reset laggy, now xi " << xi << dendl;
}


// boot --

bool OSDMonitor::preprocess_boot(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MOSDBoot>();
  int from = m->get_orig_source_inst().name.num();

  // check permissions, ignore if failed (no response expected)
  MonSession *session = op->get_session();
  if (!session)
    goto ignore;
  if (!session->is_capable("osd", MON_CAP_X)) {
    dout(0) << "got preprocess_boot message from entity with insufficient caps"
	    << session->caps << dendl;
    goto ignore;
  }

  if (m->sb.cluster_fsid != mon.monmap->fsid) {
    dout(0) << "preprocess_boot on fsid " << m->sb.cluster_fsid
	    << " != " << mon.monmap->fsid << dendl;
    goto ignore;
  }

  if (m->get_orig_source_inst().addr.is_blank_ip()) {
    dout(0) << "preprocess_boot got blank addr for " << m->get_orig_source_inst() << dendl;
    goto ignore;
  }

  ceph_assert(m->get_orig_source_inst().name.is_osd());

  // lower bound of N-2
  if (!HAVE_FEATURE(m->osd_features, SERVER_PACIFIC)) {
    mon.clog->info() << "disallowing boot of OSD "
		     << m->get_orig_source_inst()
		     << " because the osd lacks CEPH_FEATURE_SERVER_PACIFIC";
    goto ignore;
  }

  // make sure osd versions do not span more than 3 releases
  if (HAVE_FEATURE(m->osd_features, SERVER_QUINCY) &&
      osdmap.require_osd_release < ceph_release_t::octopus) {
    mon.clog->info() << "disallowing boot of quincy+ OSD "
		      << m->get_orig_source_inst()
		      << " because require_osd_release < octopus";
    goto ignore;
  }
  if (HAVE_FEATURE(m->osd_features, SERVER_REEF) &&
      osdmap.require_osd_release < ceph_release_t::pacific) {
    mon.clog->info() << "disallowing boot of reef+ OSD "
		      << m->get_orig_source_inst()
		      << " because require_osd_release < pacific";
    goto ignore;
  }

  // See crimson/osd/osd.cc: OSD::_send_boot
  if (auto type_iter = m->metadata.find("osd_type");
      type_iter != m->metadata.end()) {
    const auto &otype = type_iter->second;
    // m->metadata["osd_type"] must be "crimson", classic doesn't send osd_type
    if (otype == "crimson") {
      if (!osdmap.get_allow_crimson()) {
	mon.clog->info()
	  << "Disallowing boot of crimson-osd without allow_crimson "
	  << "OSDMap flag.  Run ceph osd set_allow_crimson to set "
	  << "allow_crimson flag.  Note that crimson-osd is "
	  << "considered unstable and may result in crashes or "
	  << "data loss.  Its usage should be restricted to "
	  << "testing and development.";
	goto ignore;
      }
    } else {
      derr << __func__ << ": osd " << m->get_orig_source_inst()
	   << " sent non-crimson osd_type field in MOSDBoot: "
	   << otype
	   << " -- booting anyway"
	   << dendl;
    }
  }

  if (osdmap.stretch_mode_enabled &&
      !(m->osd_features & CEPH_FEATUREMASK_STRETCH_MODE)) {
    mon.clog->info() << "disallowing boot of OSD "
		      << m->get_orig_source_inst()
		      << " because stretch mode is on and OSD lacks support";
    goto ignore;
  }

  // already booted?
  if (osdmap.is_up(from) &&
      osdmap.get_addrs(from).legacy_equals(m->get_orig_source_addrs()) &&
      osdmap.get_cluster_addrs(from).legacy_equals(m->cluster_addrs)) {
    // yup.
    dout(7) << "preprocess_boot dup from " << m->get_orig_source()
	    << " " << m->get_orig_source_addrs()
	    << " =~ " << osdmap.get_addrs(from) << dendl;
    _booted(op, false);
    return true;
  }

  if (osdmap.exists(from) &&
      !osdmap.get_uuid(from).is_zero() &&
      osdmap.get_uuid(from) != m->sb.osd_fsid) {
    dout(7) << __func__ << " from " << m->get_orig_source_inst()
            << " clashes with existing osd: different fsid"
            << " (ours: " << osdmap.get_uuid(from)
            << " ; theirs: " << m->sb.osd_fsid << ")" << dendl;
    goto ignore;
  }

  if (osdmap.exists(from) &&
      osdmap.get_info(from).up_from > m->version &&
      osdmap.get_most_recent_addrs(from).legacy_equals(
	m->get_orig_source_addrs())) {
    dout(7) << "prepare_boot msg from before last up_from, ignoring" << dendl;
    send_latest(op, m->sb.current_epoch+1);
    return true;
  }

  // noup?
  if (!can_mark_up(from)) {
    dout(7) << "preprocess_boot ignoring boot from " << m->get_orig_source_inst() << dendl;
    send_latest(op, m->sb.current_epoch+1);
    return true;
  }

  dout(10) << "preprocess_boot from " << m->get_orig_source_inst() << dendl;
  return false;

 ignore:
  return true;
}

bool OSDMonitor::prepare_boot(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MOSDBoot>();
  dout(7) << __func__ << " from " << m->get_source()
	  << " sb " << m->sb
	  << " client_addrs" << m->get_connection()->get_peer_addrs()
	  << " cluster_addrs " << m->cluster_addrs
	  << " hb_back_addrs " << m->hb_back_addrs
	  << " hb_front_addrs " << m->hb_front_addrs
	  << dendl;

  ceph_assert(m->get_orig_source().is_osd());
  int from = m->get_orig_source().num();

  // does this osd exist?
  if (from >= osdmap.get_max_osd()) {
    dout(1) << "boot from osd." << from << " >= max_osd "
	    << osdmap.get_max_osd() << dendl;
    return false;
  }

  int oldstate = osdmap.exists(from) ? osdmap.get_state(from) : CEPH_OSD_NEW;
  if (pending_inc.new_state.count(from))
    oldstate ^= pending_inc.new_state[from];

  // already up?  mark down first?
  if (osdmap.is_up(from)) {
    dout(7) << __func__ << " was up, first marking down osd." << from << " "
	    << osdmap.get_addrs(from) << dendl;
    // preprocess should have caught these;  if not, assert.
    ceph_assert(!osdmap.get_addrs(from).legacy_equals(
		  m->get_orig_source_addrs()) ||
		!osdmap.get_cluster_addrs(from).legacy_equals(m->cluster_addrs));
    ceph_assert(osdmap.get_uuid(from) == m->sb.osd_fsid);

    if (pending_inc.new_state.count(from) == 0 ||
	(pending_inc.new_state[from] & CEPH_OSD_UP) == 0) {
      // mark previous guy down
      pending_inc.new_state[from] = CEPH_OSD_UP;
    }
    wait_for_finished_proposal(op, new C_RetryMessage(this, op));
  } else if (pending_inc.new_up_client.count(from)) {
    // already prepared, just wait
    dout(7) << __func__ << " already prepared, waiting on "
	    << m->get_orig_source_addr() << dendl;
    wait_for_finished_proposal(op, new C_RetryMessage(this, op));
  } else {
    // mark new guy up.
    pending_inc.new_up_client[from] = m->get_orig_source_addrs();
    pending_inc.new_up_cluster[from] = m->cluster_addrs;
    pending_inc.new_hb_back_up[from] = m->hb_back_addrs;
    pending_inc.new_hb_front_up[from] = m->hb_front_addrs;

    down_pending_out.erase(from);  // if any

    if (m->sb.weight)
      osd_weight[from] = m->sb.weight;

    // set uuid?
    dout(10) << " setting osd." << from << " uuid to " << m->sb.osd_fsid
	     << dendl;
    if (!osdmap.exists(from) || osdmap.get_uuid(from) != m->sb.osd_fsid) {
      // preprocess should have caught this;  if not, assert.
      ceph_assert(!osdmap.exists(from) || osdmap.get_uuid(from).is_zero());
      pending_inc.new_uuid[from] = m->sb.osd_fsid;
    }

    // fresh osd?
    if (m->sb.newest_map == 0 && osdmap.exists(from)) {
      const osd_info_t& i = osdmap.get_info(from);
      if (i.up_from > i.lost_at) {
	dout(10) << " fresh osd; marking lost_at too" << dendl;
	pending_inc.new_lost[from] = osdmap.get_epoch();
      }
    }

    // metadata
    bufferlist osd_metadata;
    encode(m->metadata, osd_metadata);
    pending_metadata[from] = osd_metadata;
    pending_metadata_rm.erase(from);

    // adjust last clean unmount epoch?
    const osd_info_t& info = osdmap.get_info(from);
    dout(10) << " old osd_info: " << info << dendl;
    if (m->sb.mounted > info.last_clean_begin ||
	(m->sb.mounted == info.last_clean_begin &&
	 m->sb.clean_thru > info.last_clean_end)) {
      epoch_t begin = m->sb.mounted;
      epoch_t end = m->sb.clean_thru;

      dout(10) << __func__ << " osd." << from << " last_clean_interval "
	       << "[" << info.last_clean_begin << "," << info.last_clean_end
	       << ") -> [" << begin << "-" << end << ")"
	       << dendl;
      pending_inc.new_last_clean_interval[from] =
	pair<epoch_t,epoch_t>(begin, end);
    }

    if (pending_inc.new_xinfo.count(from) == 0)
      pending_inc.new_xinfo[from] = osdmap.osd_xinfo[from];
    osd_xinfo_t& xi = pending_inc.new_xinfo[from];
    if (m->boot_epoch == 0) {
      xi.laggy_probability *= (1.0 - g_conf()->mon_osd_laggy_weight);
      xi.laggy_interval *= (1.0 - g_conf()->mon_osd_laggy_weight);
      dout(10) << " not laggy, new xi " << xi << dendl;
    } else {
      if (xi.down_stamp.sec()) {
        int interval = ceph_clock_now().sec() -
	  xi.down_stamp.sec();
        if (g_conf()->mon_osd_laggy_max_interval &&
	    (interval > g_conf()->mon_osd_laggy_max_interval)) {
          interval =  g_conf()->mon_osd_laggy_max_interval;
        }
        xi.laggy_interval =
	  interval * g_conf()->mon_osd_laggy_weight +
	  xi.laggy_interval * (1.0 - g_conf()->mon_osd_laggy_weight);
      }
      xi.laggy_probability =
	g_conf()->mon_osd_laggy_weight +
	xi.laggy_probability * (1.0 - g_conf()->mon_osd_laggy_weight);
      dout(10) << " laggy, now xi " << xi << dendl;
    }

    // set features shared by the osd
    if (m->osd_features)
      xi.features = m->osd_features;
    else
      xi.features = m->get_connection()->get_features();

    // mark in?
    if ((g_conf()->mon_osd_auto_mark_auto_out_in &&
	 (oldstate & CEPH_OSD_AUTOOUT)) ||
	(g_conf()->mon_osd_auto_mark_new_in && (oldstate & CEPH_OSD_NEW)) ||
	(g_conf()->mon_osd_auto_mark_in)) {
      if (can_mark_in(from)) {
	if (xi.old_weight > 0) {
	  pending_inc.new_weight[from] = xi.old_weight;
	  xi.old_weight = 0;
	} else {
	  pending_inc.new_weight[from] = CEPH_OSD_IN;
	}
      } else {
	dout(7) << __func__ << " NOIN set, will not mark in "
		<< m->get_orig_source_addr() << dendl;
      }
    }

    // wait
    wait_for_finished_proposal(op, new C_Booted(this, op));
  }
  return true;
}

void OSDMonitor::_booted(MonOpRequestRef op, bool logit)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MOSDBoot>();
  dout(7) << "_booted " << m->get_orig_source_inst() 
	  << " w " << m->sb.weight << " from " << m->sb.current_epoch << dendl;

  if (logit) {
    mon.clog->info() << m->get_source() << " " << m->get_orig_source_addrs()
		      << " boot";
  }

  send_latest(op, m->sb.current_epoch+1);
}


// -------------
// full

bool OSDMonitor::preprocess_full(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MOSDFull>();
  int from = m->get_orig_source().num();
  set<string> state;
  unsigned mask = CEPH_OSD_NEARFULL | CEPH_OSD_BACKFILLFULL | CEPH_OSD_FULL;

  // check permissions, ignore if failed
  MonSession *session = op->get_session();
  if (!session)
    goto ignore;
  if (!session->is_capable("osd", MON_CAP_X)) {
    dout(0) << "MOSDFull from entity with insufficient privileges:"
	    << session->caps << dendl;
    goto ignore;
  }

  // ignore a full message from the osd instance that already went down
  if (!osdmap.exists(from)) {
    dout(7) << __func__ << " ignoring full message from nonexistent "
	    << m->get_orig_source_inst() << dendl;
    goto ignore;
  }
  if ((!osdmap.is_up(from) &&
       osdmap.get_most_recent_addrs(from).legacy_equals(
	 m->get_orig_source_addrs())) ||
      (osdmap.is_up(from) &&
       !osdmap.get_addrs(from).legacy_equals(m->get_orig_source_addrs()))) {
    dout(7) << __func__ << " ignoring full message from down "
	    << m->get_orig_source_inst() << dendl;
    goto ignore;
  }

  OSDMap::calc_state_set(osdmap.get_state(from), state);

  if ((osdmap.get_state(from) & mask) == m->state) {
    dout(7) << __func__ << " state already " << state << " for osd." << from
	    << " " << m->get_orig_source_inst() << dendl;
    _reply_map(op, m->version);
    goto ignore;
  }

  dout(10) << __func__ << " want state " << state << " for osd." << from
	   << " " << m->get_orig_source_inst() << dendl;
  return false;

 ignore:
  return true;
}

bool OSDMonitor::prepare_full(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MOSDFull>();
  const int from = m->get_orig_source().num();

  const unsigned mask = CEPH_OSD_NEARFULL | CEPH_OSD_BACKFILLFULL | CEPH_OSD_FULL;
  const unsigned want_state = m->state & mask;  // safety first

  unsigned cur_state = osdmap.get_state(from);
  auto p = pending_inc.new_state.find(from);
  if (p != pending_inc.new_state.end()) {
    cur_state ^= p->second;
  }
  cur_state &= mask;

  set<string> want_state_set, cur_state_set;
  OSDMap::calc_state_set(want_state, want_state_set);
  OSDMap::calc_state_set(cur_state, cur_state_set);

  if (cur_state != want_state) {
    if (p != pending_inc.new_state.end()) {
      p->second &= ~mask;
    } else {
      pending_inc.new_state[from] = 0;
    }
    pending_inc.new_state[from] |= (osdmap.get_state(from) & mask) ^ want_state;
    dout(7) << __func__ << " osd." << from << " " << cur_state_set
	    << " -> " << want_state_set << dendl;
  } else {
    dout(7) << __func__ << " osd." << from << " " << cur_state_set
	    << " = wanted " << want_state_set << ", just waiting" << dendl;
  }

  wait_for_finished_proposal(op, new C_ReplyMap(this, op, m->version));
  return true;
}

// -------------
// alive

bool OSDMonitor::preprocess_alive(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MOSDAlive>();
  int from = m->get_orig_source().num();

  // check permissions, ignore if failed
  MonSession *session = op->get_session();
  if (!session)
    goto ignore;
  if (!session->is_capable("osd", MON_CAP_X)) {
    dout(0) << "attempt to send MOSDAlive from entity with insufficient privileges:"
	    << session->caps << dendl;
    goto ignore;
  }

  if (!osdmap.is_up(from) ||
      !osdmap.get_addrs(from).legacy_equals(m->get_orig_source_addrs())) {
    dout(7) << "preprocess_alive ignoring alive message from down "
	    << m->get_orig_source() << " " << m->get_orig_source_addrs()
	    << dendl;
    goto ignore;
  }

  if (osdmap.get_up_thru(from) >= m->want) {
    // yup.
    dout(7) << "preprocess_alive want up_thru " << m->want << " dup from " << m->get_orig_source_inst() << dendl;
    _reply_map(op, m->version);
    return true;
  }

  dout(10) << "preprocess_alive want up_thru " << m->want
	   << " from " << m->get_orig_source_inst() << dendl;
  return false;

 ignore:
  return true;
}

bool OSDMonitor::prepare_alive(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MOSDAlive>();
  int from = m->get_orig_source().num();

  if (0) {  // we probably don't care much about these
    mon.clog->debug() << m->get_orig_source_inst() << " alive";
  }

  dout(7) << "prepare_alive want up_thru " << m->want << " have " << m->version
	  << " from " << m->get_orig_source_inst() << dendl;

  update_up_thru(from, m->version); // set to the latest map the OSD has
  wait_for_finished_proposal(op, new C_ReplyMap(this, op, m->version));
  return true;
}

void OSDMonitor::_reply_map(MonOpRequestRef op, epoch_t e)
{
  op->mark_osdmon_event(__func__);
  dout(7) << "_reply_map " << e
	  << " from " << op->get_req()->get_orig_source_inst()
	  << dendl;
  send_latest(op, e);
}

// pg_created
bool OSDMonitor::preprocess_pg_created(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m  = op->get_req<MOSDPGCreated>();
  dout(10) << __func__ << " " << *m << dendl;
  auto session = op->get_session();
  mon.no_reply(op);
  if (!session) {
    dout(10) << __func__ << ": no monitor session!" << dendl;
    return true;
  }
  if (!session->is_capable("osd", MON_CAP_X)) {
    derr << __func__ << " received from entity "
         << "with insufficient privileges " << session->caps << dendl;
    return true;
  }
  // always forward the "created!" to the leader
  return false;
}

bool OSDMonitor::prepare_pg_created(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MOSDPGCreated>();
  dout(10) << __func__ << " " << *m << dendl;
  auto src = m->get_orig_source();
  auto from = src.num();
  if (!src.is_osd() ||
      !mon.osdmon()->osdmap.is_up(from) ||
      !mon.osdmon()->osdmap.get_addrs(from).legacy_equals(
	m->get_orig_source_addrs())) {
    dout(1) << __func__ << " ignoring stats from non-active osd." << dendl;
    return false;
  }
  pending_created_pgs.push_back(m->pgid);
  return true;
}

bool OSDMonitor::preprocess_pg_ready_to_merge(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MOSDPGReadyToMerge>();
  dout(10) << __func__ << " " << *m << dendl;
  const pg_pool_t *pi;
  auto session = op->get_session();
  if (!session) {
    dout(10) << __func__ << ": no monitor session!" << dendl;
    goto ignore;
  }
  if (!session->is_capable("osd", MON_CAP_X)) {
    derr << __func__ << " received from entity "
         << "with insufficient privileges " << session->caps << dendl;
    goto ignore;
  }
  pi = osdmap.get_pg_pool(m->pgid.pool());
  if (!pi) {
    derr << __func__ << " pool for " << m->pgid << " dne" << dendl;
    goto ignore;
  }
  if (pi->get_pg_num() <= m->pgid.ps()) {
    dout(20) << " pg_num " << pi->get_pg_num() << " already < " << m->pgid << dendl;
    goto ignore;
  }
  if (pi->get_pg_num() != m->pgid.ps() + 1) {
    derr << " OSD trying to merge wrong pgid " << m->pgid << dendl;
    goto ignore;
  }
  if (pi->get_pg_num_pending() > m->pgid.ps()) {
    dout(20) << " pg_num_pending " << pi->get_pg_num_pending() << " > " << m->pgid << dendl;
    goto ignore;
  }
  return false;

 ignore:
  mon.no_reply(op);
  return true;
}

bool OSDMonitor::prepare_pg_ready_to_merge(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m  = op->get_req<MOSDPGReadyToMerge>();
  dout(10) << __func__ << " " << *m << dendl;
  pg_pool_t p;
  if (pending_inc.new_pools.count(m->pgid.pool()))
    p = pending_inc.new_pools[m->pgid.pool()];
  else
    p = *osdmap.get_pg_pool(m->pgid.pool());
  if (p.get_pg_num() != m->pgid.ps() + 1 ||
      p.get_pg_num_pending() > m->pgid.ps()) {
    dout(10) << __func__
	     << " race with concurrent pg_num[_pending] update, will retry"
	     << dendl;
    wait_for_finished_proposal(op, new C_RetryMessage(this, op));
    return false; /* nothing to propose, yet */
  }

  if (m->ready) {
    p.dec_pg_num(m->pgid,
		 pending_inc.epoch,
		 m->source_version,
		 m->target_version,
		 m->last_epoch_started,
		 m->last_epoch_clean);
    p.last_change = pending_inc.epoch;
  } else {
    // back off the merge attempt!
    p.set_pg_num_pending(p.get_pg_num());
  }

  // force pre-nautilus clients to resend their ops, since they
  // don't understand pg_num_pending changes form a new interval
  p.last_force_op_resend_prenautilus = pending_inc.epoch;

  pending_inc.new_pools[m->pgid.pool()] = p;

  auto prob = g_conf().get_val<double>("mon_inject_pg_merge_bounce_probability");
  if (m->ready &&
      prob > 0 &&
      prob > (double)(rand() % 1000)/1000.0) {
    derr << __func__ << " injecting pg merge pg_num bounce" << dendl;
    auto n = new MMonCommand(mon.monmap->get_fsid());
    n->set_connection(m->get_connection());
    n->cmd = { "{\"prefix\":\"osd pool set\", \"pool\": \"" +
	       osdmap.get_pool_name(m->pgid.pool()) +
	       "\", \"var\": \"pg_num_actual\", \"val\": \"" +
	       stringify(m->pgid.ps() + 1) + "\"}" };
    MonOpRequestRef nop = mon.op_tracker.create_request<MonOpRequest>(n);
    nop->set_type_service();
    wait_for_finished_proposal(op, new C_RetryMessage(this, nop));
  } else {
    wait_for_finished_proposal(op, new C_ReplyMap(this, op, m->version));
  }
  return true;
}


// -------------
// pg_temp changes

bool OSDMonitor::preprocess_pgtemp(MonOpRequestRef op)
{
  auto m = op->get_req<MOSDPGTemp>();
  dout(10) << "preprocess_pgtemp " << *m << dendl;
  mempool::osdmap::vector<int> empty;
  int from = m->get_orig_source().num();
  size_t ignore_cnt = 0;

  // check caps
  MonSession *session = op->get_session();
  if (!session)
    goto ignore;
  if (!session->is_capable("osd", MON_CAP_X)) {
    dout(0) << "attempt to send MOSDPGTemp from entity with insufficient caps "
	    << session->caps << dendl;
    goto ignore;
  }

  if (!osdmap.is_up(from) ||
      !osdmap.get_addrs(from).legacy_equals(m->get_orig_source_addrs())) {
    dout(7) << "ignoring pgtemp message from down "
	    << m->get_orig_source() << " " << m->get_orig_source_addrs()
	    << dendl;
    goto ignore;
  }

  if (m->forced) {
    return false;
  }

  for (auto p = m->pg_temp.begin(); p != m->pg_temp.end(); ++p) {
    dout(20) << " " << p->first
	     << (osdmap.pg_temp->count(p->first) ? osdmap.pg_temp->get(p->first) : empty)
             << " -> " << p->second << dendl;

    // does the pool exist?
    if (!osdmap.have_pg_pool(p->first.pool())) {
      /*
       * 1. If the osdmap does not have the pool, it means the pool has been
       *    removed in-between the osd sending this message and us handling it.
       * 2. If osdmap doesn't have the pool, it is safe to assume the pool does
       *    not exist in the pending either, as the osds would not send a
       *    message about a pool they know nothing about (yet).
       * 3. However, if the pool does exist in the pending, then it must be a
       *    new pool, and not relevant to this message (see 1).
       */
      dout(10) << __func__ << " ignore " << p->first << " -> " << p->second
               << ": pool has been removed" << dendl;
      ignore_cnt++;
      continue;
    }

    int acting_primary = -1;
    osdmap.pg_to_up_acting_osds(
      p->first, nullptr, nullptr, nullptr, &acting_primary);
    if (acting_primary != from) {
      /* If the source isn't the primary based on the current osdmap, we know
       * that the interval changed and that we can discard this message.
       * Indeed, we must do so to avoid 16127 since we can't otherwise determine
       * which of two pg temp mappings on the same pg is more recent.
       */
      dout(10) << __func__ << " ignore " << p->first << " -> " << p->second
	       << ": primary has changed" << dendl;
      ignore_cnt++;
      continue;
    }

    // removal?
    if (p->second.empty() && (osdmap.pg_temp->count(p->first) ||
			      osdmap.primary_temp->count(p->first)))
      return false;
    // change?
    //  NOTE: we assume that this will clear pg_primary, so consider
    //        an existing pg_primary field to imply a change
    if (p->second.size() &&
	(osdmap.pg_temp->count(p->first) == 0 ||
	 osdmap.pg_temp->get(p->first) != p->second ||
	 osdmap.primary_temp->count(p->first)))
      return false;
  }

  // should we ignore all the pgs?
  if (ignore_cnt == m->pg_temp.size())
    goto ignore;

  dout(7) << "preprocess_pgtemp e" << m->map_epoch << " no changes from " << m->get_orig_source_inst() << dendl;
  _reply_map(op, m->map_epoch);
  return true;

 ignore:
  mon.no_reply(op);
  return true;
}

void OSDMonitor::update_up_thru(int from, epoch_t up_thru)
{
  epoch_t old_up_thru = osdmap.get_up_thru(from);
  auto ut = pending_inc.new_up_thru.find(from);
  if (ut != pending_inc.new_up_thru.end()) {
    old_up_thru = ut->second;
  }
  if (up_thru > old_up_thru) {
    // set up_thru too, so the osd doesn't have to ask again
    pending_inc.new_up_thru[from] = up_thru;
  }
}

bool OSDMonitor::prepare_pgtemp(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MOSDPGTemp>();
  int from = m->get_orig_source().num();
  dout(7) << "prepare_pgtemp e" << m->map_epoch << " from " << m->get_orig_source_inst() << dendl;
  for (map<pg_t,vector<int32_t> >::iterator p = m->pg_temp.begin(); p != m->pg_temp.end(); ++p) {
    uint64_t pool = p->first.pool();
    if (pending_inc.old_pools.count(pool)) {
      dout(10) << __func__ << " ignore " << p->first << " -> " << p->second
               << ": pool pending removal" << dendl;
      continue;
    }
    if (!osdmap.have_pg_pool(pool)) {
      dout(10) << __func__ << " ignore " << p->first << " -> " << p->second
               << ": pool has been removed" << dendl;
      continue;
    }
    pending_inc.new_pg_temp[p->first] =
      mempool::osdmap::vector<int>(p->second.begin(), p->second.end());

    // unconditionally clear pg_primary (until this message can encode
    // a change for that, too.. at which point we need to also fix
    // preprocess_pg_temp)
    if (osdmap.primary_temp->count(p->first) ||
	pending_inc.new_primary_temp.count(p->first))
      pending_inc.new_primary_temp[p->first] = -1;
  }

  // set up_thru too, so the osd doesn't have to ask again
  update_up_thru(from, m->map_epoch);

  wait_for_finished_proposal(op, new C_ReplyMap(this, op, m->map_epoch));
  return true;
}


// ---

bool OSDMonitor::preprocess_remove_snaps(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MRemoveSnaps>();
  dout(7) << "preprocess_remove_snaps " << *m << dendl;

  // check privilege, ignore if failed
  MonSession *session = op->get_session();
  mon.no_reply(op);
  if (!session)
    goto ignore;
  if (!session->caps.is_capable(
	cct,
	session->entity_name,
        "osd", "osd pool rmsnap", {}, true, true, false,
	session->get_peer_socket_addr())) {
    dout(0) << "got preprocess_remove_snaps from entity with insufficient caps "
	    << session->caps << dendl;
    goto ignore;
  }

  for (map<int, vector<snapid_t> >::iterator q = m->snaps.begin();
       q != m->snaps.end();
       ++q) {
    if (!osdmap.have_pg_pool(q->first)) {
      dout(10) << " ignoring removed_snaps " << q->second
	       << " on non-existent pool " << q->first << dendl;
      continue;
    }
    const pg_pool_t *pi = osdmap.get_pg_pool(q->first);
    for (vector<snapid_t>::iterator p = q->second.begin();
	 p != q->second.end();
	 ++p) {
      if (*p > pi->get_snap_seq() ||
	  !_is_removed_snap(q->first, *p)) {
	return false;
      }
    }
  }

  if (HAVE_FEATURE(m->get_connection()->get_features(), SERVER_OCTOPUS)) {
    auto reply = make_message<MRemoveSnaps>();
    reply->snaps = m->snaps;
    mon.send_reply(op, reply.detach());
  }

 ignore:
  return true;
}

bool OSDMonitor::prepare_remove_snaps(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MRemoveSnaps>();
  dout(7) << "prepare_remove_snaps " << *m << dendl;

  for (auto& [pool, snaps] : m->snaps) {
    if (!osdmap.have_pg_pool(pool)) {
      dout(10) << " ignoring removed_snaps " << snaps
	       << " on non-existent pool " << pool << dendl;
      continue;
    }

    pg_pool_t& pi = osdmap.pools[pool];
    for (auto s : snaps) {
      if (!_is_removed_snap(pool, s) &&
	  (!pending_inc.new_pools.count(pool) ||
	   !pending_inc.new_pools[pool].removed_snaps.contains(s)) &&
	  (!pending_inc.new_removed_snaps.count(pool) ||
	   !pending_inc.new_removed_snaps[pool].contains(s))) {
	pg_pool_t *newpi = pending_inc.get_new_pool(pool, &pi);
	if (osdmap.require_osd_release < ceph_release_t::octopus) {
	  newpi->removed_snaps.insert(s);
	  dout(10) << " pool " << pool << " removed_snaps added " << s
		   << " (now " << newpi->removed_snaps << ")" << dendl;
	}
	newpi->flags |= pg_pool_t::FLAG_SELFMANAGED_SNAPS;
	if (s > newpi->get_snap_seq()) {
	  dout(10) << " pool " << pool << " snap_seq "
		   << newpi->get_snap_seq() << " -> " << s << dendl;
	  newpi->set_snap_seq(s);
	}
	newpi->set_snap_epoch(pending_inc.epoch);
	dout(10) << " added pool " << pool << " snap " << s
		 << " to removed_snaps queue" << dendl;
	pending_inc.new_removed_snaps[pool].insert(s);
      }
    }
  }

  if (HAVE_FEATURE(m->get_connection()->get_features(), SERVER_OCTOPUS)) {
    auto reply = make_message<MRemoveSnaps>();
    reply->snaps = m->snaps;
    wait_for_finished_proposal(op, new C_ReplyOp(this, op, reply));
  }

  return true;
}

bool OSDMonitor::preprocess_get_purged_snaps(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MMonGetPurgedSnaps>();
  dout(7) << __func__ << " " << *m << dendl;

  map<epoch_t,mempool::osdmap::map<int64_t,snap_interval_set_t>> r;

  string k = make_purged_snap_epoch_key(m->start);
  auto it = mon.store->get_iterator(OSD_SNAP_PREFIX);
  it->upper_bound(k);
  unsigned long epoch = m->last;
  while (it->valid()) {
    if (it->key().find("purged_epoch_") != 0) {
      break;
    }
    string k = it->key();
    int n = sscanf(k.c_str(), "purged_epoch_%lx", &epoch);
    if (n != 1) {
      derr << __func__ << " unable to parse key '" << it->key() << "'" << dendl;
    } else if (epoch > m->last) {
      break;
    } else {
      bufferlist bl = it->value();
      auto p = bl.cbegin();
      auto &v = r[epoch];
      try {
	ceph::decode(v, p);
      } catch (ceph::buffer::error& e) {
	derr << __func__ << " unable to parse value for key '" << it->key()
	     << "': \n";
	bl.hexdump(*_dout);
	*_dout << dendl;
      }
      n += 4 + v.size() * 16;
    }
    if (n > 1048576) {
      // impose a semi-arbitrary limit to message size
      break;
    }
    it->next();
  }

  auto reply = make_message<MMonGetPurgedSnapsReply>(m->start, epoch);
  reply->purged_snaps.swap(r);
  mon.send_reply(op, reply.detach());

  return true;
}

// osd beacon
bool OSDMonitor::preprocess_beacon(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  // check caps
  auto session = op->get_session();
  mon.no_reply(op);
  if (!session) {
    dout(10) << __func__ << " no monitor session!" << dendl;
    return true;
  }
  if (!session->is_capable("osd", MON_CAP_X)) {
    derr << __func__ << " received from entity "
         << "with insufficient privileges " << session->caps << dendl;
    return true;
  }
  // Always forward the beacon to the leader, even if they are the same as
  // the old one. The leader will mark as down osds that haven't sent
  // beacon for a few minutes.
  return false;
}

bool OSDMonitor::prepare_beacon(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  const auto beacon = op->get_req<MOSDBeacon>();
  const auto src = beacon->get_orig_source();
  dout(10) << __func__ << " " << *beacon
	   << " from " << src << dendl;
  int from = src.num();

  if (!src.is_osd() ||
      !osdmap.is_up(from) ||
      !osdmap.get_addrs(from).legacy_equals(beacon->get_orig_source_addrs())) {
    if (src.is_osd() && !osdmap.is_up(from)) {
      // share some new maps with this guy in case it may not be
      // aware of its own deadness...
      send_latest(op, beacon->version+1);
    }
    dout(1) << " ignoring beacon from non-active osd." << from << dendl;
    return false; /* nothing to propose */
  }

  last_osd_report[from].first = ceph_clock_now();
  last_osd_report[from].second = beacon->osd_beacon_report_interval;
  if (osdmap.is_in(from)) {
    osd_epochs[from] = beacon->version;
  }
  for (const auto& pg : beacon->pgs) {
    if (auto* pool = osdmap.get_pg_pool(pg.pool()); pool != nullptr) {
      unsigned pg_num = pool->get_pg_num();
      last_epoch_clean.report(pg_num, pg, beacon->min_last_epoch_clean);
    }
  }

  if (osdmap.osd_xinfo[from].last_purged_snaps_scrub <
      beacon->last_purged_snaps_scrub) {
    if (pending_inc.new_xinfo.count(from) == 0) {
      pending_inc.new_xinfo[from] = osdmap.osd_xinfo[from];
    }
    pending_inc.new_xinfo[from].last_purged_snaps_scrub =
      beacon->last_purged_snaps_scrub;
    return true;
  } else {
    return false; /* nothing to propose */
  }
}

// ---------------
// map helpers

void OSDMonitor::send_latest(MonOpRequestRef op, epoch_t start)
{
  op->mark_osdmon_event(__func__);
  dout(5) << "send_latest to " << op->get_req()->get_orig_source_inst()
	  << " start " << start << dendl;
  if (start == 0)
    send_full(op);
  else
    send_incremental(op, start);
}


MOSDMap *OSDMonitor::build_latest_full(uint64_t features)
{
  MOSDMap *r = new MOSDMap(mon.monmap->fsid, features);
  get_version_full(osdmap.get_epoch(), features, r->maps[osdmap.get_epoch()]);
  r->cluster_osdmap_trim_lower_bound = get_first_committed();
  r->newest_map = osdmap.get_epoch();
  return r;
}

MOSDMap *OSDMonitor::build_incremental(epoch_t from, epoch_t to, uint64_t features)
{
  dout(10) << "build_incremental [" << from << ".." << to << "] with features "
	   << std::hex << features << std::dec << dendl;
  MOSDMap *m = new MOSDMap(mon.monmap->fsid, features);
  m->cluster_osdmap_trim_lower_bound = get_first_committed();
  m->newest_map = osdmap.get_epoch();

  for (epoch_t e = to; e >= from && e > 0; e--) {
    bufferlist bl;
    int err = get_version(e, features, bl);
    if (err == 0) {
      ceph_assert(bl.length());
      // if (get_version(e, bl) > 0) {
      dout(20) << "build_incremental    inc " << e << " "
	       << bl.length() << " bytes" << dendl;
      m->incremental_maps[e] = bl;
    } else {
      ceph_assert(err == -ENOENT);
      ceph_assert(!bl.length());
      get_version_full(e, features, bl);
      if (bl.length() > 0) {
      //else if (get_version("full", e, bl) > 0) {
      dout(20) << "build_incremental   full " << e << " "
	       << bl.length() << " bytes" << dendl;
      m->maps[e] = bl;
      } else {
	ceph_abort();  // we should have all maps.
      }
    }
  }
  return m;
}

void OSDMonitor::send_full(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  dout(5) << "send_full to " << op->get_req()->get_orig_source_inst() << dendl;
  mon.send_reply(op, build_latest_full(op->get_session()->con_features));
}

void OSDMonitor::send_incremental(MonOpRequestRef op, epoch_t first)
{
  op->mark_osdmon_event(__func__);

  MonSession *s = op->get_session();
  ceph_assert(s);

  if (s->proxy_con) {
    // oh, we can tell the other mon to do it
    dout(10) << __func__ << " asking proxying mon to send_incremental from "
	     << first << dendl;
    MRoute *r = new MRoute(s->proxy_tid, NULL);
    r->send_osdmap_first = first;
    s->proxy_con->send_message(r);
    op->mark_event("reply: send routed send_osdmap_first reply");
  } else {
    // do it ourselves
    send_incremental(first, s, false, op);
  }
}

void OSDMonitor::send_incremental(epoch_t first,
				  MonSession *session,
				  bool onetime,
				  MonOpRequestRef req)
{
  dout(5) << "send_incremental [" << first << ".." << osdmap.get_epoch() << "]"
	  << " to " << session->name << dendl;

  // get feature of the peer
  // use quorum_con_features, if it's an anonymous connection.
  uint64_t features = session->con_features ? session->con_features :
    mon.get_quorum_con_features();

  if (first <= session->osd_epoch) {
    dout(10) << __func__ << " " << session->name << " should already have epoch "
	     << session->osd_epoch << dendl;
    first = session->osd_epoch + 1;
  }

  if (first < get_first_committed()) {
    MOSDMap *m = new MOSDMap(osdmap.get_fsid(), features);
    m->cluster_osdmap_trim_lower_bound = get_first_committed();
    m->newest_map = osdmap.get_epoch();

    first = get_first_committed();
    bufferlist bl;
    int err = get_version_full(first, features, bl);
    ceph_assert(err == 0);
    ceph_assert(bl.length());
    dout(20) << "send_incremental starting with base full "
	     << first << " " << bl.length() << " bytes" << dendl;
    m->maps[first] = bl;

    if (req) {
      mon.send_reply(req, m);
      session->osd_epoch = first;
      return;
    } else {
      session->con->send_message(m);
      session->osd_epoch = first;
    }
    first++;
  }

  while (first <= osdmap.get_epoch()) {
    epoch_t last = std::min<epoch_t>(first + g_conf()->osd_map_message_max - 1,
				     osdmap.get_epoch());
    MOSDMap *m = build_incremental(first, last, features);

    if (req) {
      // send some maps.  it may not be all of them, but it will get them
      // started.
      mon.send_reply(req, m);
    } else {
      session->con->send_message(m);
      first = last + 1;
    }
    session->osd_epoch = last;
    if (onetime || req)
      break;
  }
}

int OSDMonitor::get_version(version_t ver, bufferlist& bl)
{
  return get_version(ver, mon.get_quorum_con_features(), bl);
}

void OSDMonitor::reencode_incremental_map(bufferlist& bl, uint64_t features)
{
  OSDMap::Incremental inc;
  auto q = bl.cbegin();
  inc.decode(q);
  // always encode with subset of osdmap's canonical features
  uint64_t f = features & inc.encode_features;
  dout(20) << __func__ << " " << inc.epoch << " with features " << f
	   << dendl;
  bl.clear();
  if (inc.fullmap.length()) {
    // embedded full map?
    OSDMap m;
    m.decode(inc.fullmap);
    inc.fullmap.clear();
    m.encode(inc.fullmap, f | CEPH_FEATURE_RESERVED);
  }
  if (inc.crush.length()) {
    // embedded crush map
    CrushWrapper c;
    auto p = inc.crush.cbegin();
    c.decode(p);
    inc.crush.clear();
    c.encode(inc.crush, f);
  }
  inc.encode(bl, f | CEPH_FEATURE_RESERVED);
}

void OSDMonitor::reencode_full_map(bufferlist& bl, uint64_t features)
{
  OSDMap m;
  auto q = bl.cbegin();
  m.decode(q);
  // always encode with subset of osdmap's canonical features
  uint64_t f = features & m.get_encoding_features();
  dout(20) << __func__ << " " << m.get_epoch() << " with features " << f
	   << dendl;
  bl.clear();
  m.encode(bl, f | CEPH_FEATURE_RESERVED);
}

int OSDMonitor::get_version(version_t ver, uint64_t features, bufferlist& bl)
{
  uint64_t significant_features = OSDMap::get_significant_features(features);
  if (inc_osd_cache.lookup({ver, significant_features}, &bl)) {
    return 0;
  }
  int ret = PaxosService::get_version(ver, bl);
  if (ret < 0) {
    return ret;
  }
  // NOTE: this check is imprecise; the OSDMap encoding features may
  // be a subset of the latest mon quorum features, but worst case we
  // reencode once and then cache the (identical) result under both
  // feature masks.
  if (significant_features !=
      OSDMap::get_significant_features(mon.get_quorum_con_features())) {
    reencode_incremental_map(bl, features);
  }
  inc_osd_cache.add_bytes({ver, significant_features}, bl);
  return 0;
}

int OSDMonitor::get_inc(version_t ver, OSDMap::Incremental& inc)
{
  bufferlist inc_bl;
  int err = get_version(ver, inc_bl);
  ceph_assert(err == 0);
  ceph_assert(inc_bl.length());

  auto p = inc_bl.cbegin();
  inc.decode(p);
  dout(10) << __func__ << "     "
           << " epoch " << inc.epoch
           << " inc_crc " << inc.inc_crc
           << " full_crc " << inc.full_crc
           << " encode_features " << inc.encode_features << dendl;
  return 0;
}

int OSDMonitor::get_full_from_pinned_map(version_t ver, bufferlist& bl)
{
  dout(10) << __func__ << " ver " << ver << dendl;

  version_t closest_pinned = osdmap_manifest.get_lower_closest_pinned(ver);
  if (closest_pinned == 0) {
    return -ENOENT;
  }
  if (closest_pinned > ver) {
    dout(0) << __func__ << " pinned: " << osdmap_manifest.pinned << dendl;
  }
  ceph_assert(closest_pinned <= ver);

  dout(10) << __func__ << " closest pinned ver " << closest_pinned << dendl;

  // get osdmap incremental maps and apply on top of this one.
  bufferlist osdm_bl;
  bool has_cached_osdmap = false;
  for (version_t v = ver-1; v >= closest_pinned; --v) {
    if (full_osd_cache.lookup({v, mon.get_quorum_con_features()},
                                &osdm_bl)) {
      dout(10) << __func__ << " found map in cache ver " << v << dendl;
      closest_pinned = v;
      has_cached_osdmap = true;
      break;
    }
  }

  if (!has_cached_osdmap) {
    int err = PaxosService::get_version_full(closest_pinned, osdm_bl);
    if (err != 0) {
      derr << __func__ << " closest pinned map ver " << closest_pinned
           << " not available! error: " << cpp_strerror(err) << dendl;
    }
    ceph_assert(err == 0);
  }

  ceph_assert(osdm_bl.length());

  OSDMap osdm;
  osdm.decode(osdm_bl);

  dout(10) << __func__ << " loaded osdmap epoch " << closest_pinned
           << " e" << osdm.epoch
           << " crc " << osdm.get_crc()
           << " -- applying incremental maps." << dendl;

  uint64_t encode_features = 0;
  for (version_t v = closest_pinned + 1; v <= ver; ++v) {
    dout(20) << __func__ << "    applying inc epoch " << v << dendl;

    OSDMap::Incremental inc;
    int err = get_inc(v, inc);
    ceph_assert(err == 0);

    encode_features = inc.encode_features;

    err = osdm.apply_incremental(inc);
    ceph_assert(err == 0);

    // this block performs paranoid checks on map retrieval
    if (g_conf().get_val<bool>("mon_debug_extra_checks") &&
        inc.full_crc != 0) {

      uint64_t f = encode_features;
      if (!f) {
        f = (mon.quorum_con_features ? mon.quorum_con_features : -1);
      }

      // encode osdmap to force calculating crcs
      bufferlist tbl;
      osdm.encode(tbl, f | CEPH_FEATURE_RESERVED);
      // decode osdmap to compare crcs with what's expected by incremental
      OSDMap tosdm;
      tosdm.decode(tbl);

      if (tosdm.get_crc() != inc.full_crc) {
        derr << __func__
             << "    osdmap crc mismatch! (osdmap crc " << tosdm.get_crc()
             << ", expected " << inc.full_crc << ")" << dendl;
        ceph_abort_msg("osdmap crc mismatch");
      }
    }

    // note: we cannot add the recently computed map to the cache, as is,
    // because we have not encoded the map into a bl.
  }

  if (!encode_features) {
    dout(10) << __func__
             << " last incremental map didn't have features;"
             << " defaulting to quorum's or all" << dendl;
    encode_features =
      (mon.quorum_con_features ? mon.quorum_con_features : -1);
  }
  osdm.encode(bl, encode_features | CEPH_FEATURE_RESERVED);

  return 0;
}

int OSDMonitor::get_version_full(version_t ver, bufferlist& bl)
{
  return get_version_full(ver, mon.get_quorum_con_features(), bl);
}

int OSDMonitor::get_version_full(version_t ver, uint64_t features,
				 bufferlist& bl)
{
  uint64_t significant_features = OSDMap::get_significant_features(features);
  if (full_osd_cache.lookup({ver, significant_features}, &bl)) {
    return 0;
  }
  int ret = PaxosService::get_version_full(ver, bl);
  if (ret == -ENOENT) {
    // build map?
    ret = get_full_from_pinned_map(ver, bl);
  }
  if (ret < 0) {
    return ret;
  }
  // NOTE: this check is imprecise; the OSDMap encoding features may
  // be a subset of the latest mon quorum features, but worst case we
  // reencode once and then cache the (identical) result under both
  // feature masks.
  if (significant_features !=
      OSDMap::get_significant_features(mon.get_quorum_con_features())) {
    reencode_full_map(bl, features);
  }
  full_osd_cache.add_bytes({ver, significant_features}, bl);
  return 0;
}

epoch_t OSDMonitor::blocklist(const entity_addrvec_t& av, utime_t until)
{
  dout(10) << "blocklist " << av << " until " << until << dendl;
  for (auto a : av.v) {
    if (osdmap.require_osd_release >= ceph_release_t::nautilus) {
      a.set_type(entity_addr_t::TYPE_ANY);
    } else {
      a.set_type(entity_addr_t::TYPE_LEGACY);
    }
    pending_inc.new_blocklist[a] = until;
  }
  return pending_inc.epoch;
}

epoch_t OSDMonitor::blocklist(entity_addr_t a, utime_t until)
{
  if (osdmap.require_osd_release >= ceph_release_t::nautilus) {
    a.set_type(entity_addr_t::TYPE_ANY);
  } else {
    a.set_type(entity_addr_t::TYPE_LEGACY);
  }
  dout(10) << "blocklist " << a << " until " << until << dendl;
  pending_inc.new_blocklist[a] = until;
  return pending_inc.epoch;
}


void OSDMonitor::check_osdmap_subs()
{
  dout(10) << __func__ << dendl;
  if (!osdmap.get_epoch()) {
    return;
  }
  auto osdmap_subs = mon.session_map.subs.find("osdmap");
  if (osdmap_subs == mon.session_map.subs.end()) {
    return;
  }
  auto p = osdmap_subs->second->begin();
  while (!p.end()) {
    auto sub = *p;
    ++p;
    check_osdmap_sub(sub);
  }
}

void OSDMonitor::check_osdmap_sub(Subscription *sub)
{
  dout(10) << __func__ << " " << sub << " next " << sub->next
	   << (sub->onetime ? " (onetime)":" (ongoing)") << dendl;
  if (sub->next <= osdmap.get_epoch()) {
    if (sub->next >= 1)
      send_incremental(sub->next, sub->session, sub->incremental_onetime);
    else
      sub->session->con->send_message(build_latest_full(sub->session->con_features));
    if (sub->onetime)
      mon.session_map.remove_sub(sub);
    else
      sub->next = osdmap.get_epoch() + 1;
  }
}

void OSDMonitor::check_pg_creates_subs()
{
  if (!osdmap.get_num_up_osds()) {
    return;
  }
  ceph_assert(osdmap.get_up_osd_features() & CEPH_FEATURE_MON_STATEFUL_SUB);
  mon.with_session_map([this](const MonSessionMap& session_map) {
      auto pg_creates_subs = session_map.subs.find("osd_pg_creates");
      if (pg_creates_subs == session_map.subs.end()) {
	return;
      }
      for (auto sub : *pg_creates_subs->second) {
	check_pg_creates_sub(sub);
      }
    });
}

void OSDMonitor::check_pg_creates_sub(Subscription *sub)
{
  dout(20) << __func__ << " .. " << sub->session->name << dendl;
  ceph_assert(sub->type == "osd_pg_creates");
  // only send these if the OSD is up.  we will check_subs() when they do
  // come up so they will get the creates then.
  if (sub->session->name.is_osd() &&
      mon.osdmon()->osdmap.is_up(sub->session->name.num())) {
    sub->next = send_pg_creates(sub->session->name.num(),
				sub->session->con.get(),
				sub->next);
  }
}

void OSDMonitor::do_application_enable(int64_t pool_id,
                                       const std::string &app_name,
				       const std::string &app_key,
				       const std::string &app_value,
				       bool force)
{
  ceph_assert(paxos.is_plugged() && is_writeable());

  dout(20) << __func__ << ": pool_id=" << pool_id << ", app_name=" << app_name
           << dendl;

  ceph_assert(osdmap.require_osd_release >= ceph_release_t::luminous);

  auto pp = osdmap.get_pg_pool(pool_id);
  ceph_assert(pp != nullptr);

  pg_pool_t p = *pp;
  if (pending_inc.new_pools.count(pool_id)) {
    p = pending_inc.new_pools[pool_id];
  }

  if (app_key.empty()) {
    p.application_metadata.insert({app_name, {}});
  } else {
    if (force) {
      p.application_metadata[app_name][app_key] = app_value;
    } else {
      p.application_metadata.insert({app_name, {{app_key, app_value}}});
    }
  }
  p.last_change = pending_inc.epoch;
  pending_inc.new_pools[pool_id] = p;
}

void OSDMonitor::do_set_pool_opt(int64_t pool_id,
				 pool_opts_t::key_t opt,
				 pool_opts_t::value_t val)
{
  dout(10) << __func__ << " pool: " << pool_id << " option: " << opt
	   << " val: " << val << dendl;
  auto p = pending_inc.new_pools.try_emplace(
    pool_id, *osdmap.get_pg_pool(pool_id));
  p.first->second.opts.set(opt, val);
}

unsigned OSDMonitor::scan_for_creating_pgs(
  const mempool::osdmap::map<int64_t,pg_pool_t>& pools,
  const mempool::osdmap::set<int64_t>& removed_pools,
  utime_t modified,
  creating_pgs_t* creating_pgs) const
{
  unsigned queued = 0;
  for (auto& p : pools) {
    int64_t poolid = p.first;
    if (creating_pgs->created_pools.count(poolid)) {
      dout(10) << __func__ << " already created " << poolid << dendl;
      continue;
    }
    const pg_pool_t& pool = p.second;
    int ruleno = pool.get_crush_rule();
    if (ruleno < 0 || !osdmap.crush->rule_exists(ruleno))
      continue;

    const auto last_scan_epoch = creating_pgs->last_scan_epoch;
    const auto created = pool.get_last_change();
    if (last_scan_epoch && created <= last_scan_epoch) {
      dout(10) << __func__ << " no change in pool " << poolid
	       << " " << pool << dendl;
      continue;
    }
    if (removed_pools.count(poolid)) {
      dout(10) << __func__ << " pool is being removed: " << poolid
	       << " " << pool << dendl;
      continue;
    }
    dout(10) << __func__ << " queueing pool create for " << poolid
	     << " " << pool << dendl;
    creating_pgs->create_pool(poolid, pool.get_pg_num(),
			      created, modified);
    queued++;
  }
  return queued;
}

void OSDMonitor::update_creating_pgs()
{
  dout(10) << __func__ << " " << creating_pgs.pgs.size() << " pgs creating, "
	   << creating_pgs.queue.size() << " pools in queue" << dendl;
  decltype(creating_pgs_by_osd_epoch) new_pgs_by_osd_epoch;
  std::lock_guard<std::mutex> l(creating_pgs_lock);
  for (const auto& pg : creating_pgs.pgs) {
    int acting_primary = -1;
    auto pgid = pg.first;
    if (!osdmap.pg_exists(pgid)) {
      dout(20) << __func__ << " ignoring " << pgid << " which should not exist"
	       << dendl;
      continue;
    }
    auto mapped = pg.second.create_epoch;
    dout(20) << __func__ << " looking up " << pgid << "@" << mapped << dendl;
    spg_t spgid(pgid);
    mapping.get_primary_and_shard(pgid, &acting_primary, &spgid);
    // check the previous creating_pgs, look for the target to whom the pg was
    // previously mapped
    for (const auto& pgs_by_epoch : creating_pgs_by_osd_epoch) {
      const auto last_acting_primary = pgs_by_epoch.first;
      for (auto& pgs: pgs_by_epoch.second) {
	if (pgs.second.count(spgid)) {
	  if (last_acting_primary == acting_primary) {
	    mapped = pgs.first;
	  } else {
	    dout(20) << __func__ << " " << pgid << " "
		     << " acting_primary:" << last_acting_primary
		     << " -> " << acting_primary << dendl;
	    // note epoch if the target of the create message changed.
	    mapped = mapping.get_epoch();
          }
          break;
        } else {
	  // newly creating
	  mapped = mapping.get_epoch();
	}
      }
    }
    dout(10) << __func__ << " will instruct osd." << acting_primary
	     << " to create " << pgid << "@" << mapped << dendl;
    new_pgs_by_osd_epoch[acting_primary][mapped].insert(spgid);
  }
  creating_pgs_by_osd_epoch = std::move(new_pgs_by_osd_epoch);
  creating_pgs_epoch = mapping.get_epoch();
}

epoch_t OSDMonitor::send_pg_creates(int osd, Connection *con, epoch_t next) const
{
  dout(30) << __func__ << " osd." << osd << " next=" << next
	   << " " << creating_pgs_by_osd_epoch << dendl;
  std::lock_guard<std::mutex> l(creating_pgs_lock);
  if (creating_pgs_epoch <= creating_pgs.last_scan_epoch) {
    dout(20) << __func__
	     << " not using stale creating_pgs@" << creating_pgs_epoch << dendl;
    // the subscribers will be updated when the mapping is completed anyway
    return next;
  }
  auto creating_pgs_by_epoch = creating_pgs_by_osd_epoch.find(osd);
  if (creating_pgs_by_epoch == creating_pgs_by_osd_epoch.end())
    return next;
  ceph_assert(!creating_pgs_by_epoch->second.empty());

  auto m = make_message<MOSDPGCreate2>(creating_pgs_epoch);

  epoch_t last = 0;
  for (auto epoch_pgs = creating_pgs_by_epoch->second.lower_bound(next);
       epoch_pgs != creating_pgs_by_epoch->second.end(); ++epoch_pgs) {
    auto epoch = epoch_pgs->first;
    auto& pgs = epoch_pgs->second;
    dout(20) << __func__ << " osd." << osd << " from " << next
             << " : epoch " << epoch << " " << pgs.size() << " pgs" << dendl;
    last = epoch;
    for (auto& pg : pgs) {
      // Need the create time from the monitor using its clock to set
      // last_scrub_stamp upon pg creation.
      auto create = creating_pgs.pgs.find(pg.pgid);
      ceph_assert(create != creating_pgs.pgs.end());
      m->pgs.emplace(pg, make_pair(create->second.create_epoch,
                             create->second.create_stamp));
      if (create->second.history.epoch_created) {
        dout(20) << __func__ << "   " << pg << " " << create->second.history
      	   << " " << create->second.past_intervals << dendl;
        m->pg_extra.emplace(pg, make_pair(create->second.history,
                                    create->second.past_intervals));
      }
      dout(20) << __func__ << " will create " << pg
      	       << " at " << create->second.create_epoch << dendl;
    }
  }
  if (!m->pgs.empty()) {
    con->send_message2(std::move(m));
  } else {
    dout(20) << __func__ << " osd." << osd << " from " << next
             << " has nothing to send" << dendl;
    return next;
  }

  // sub is current through last + 1
  return last + 1;
}

// TICK


void OSDMonitor::tick()
{
  if (!is_active()) return;

  dout(10) << osdmap << dendl;

  // always update osdmap manifest, regardless of being the leader.
  load_osdmap_manifest();

  // always tune priority cache manager memory on leader and peons
  if (ceph_using_tcmalloc() && mon_memory_autotune) {
    std::lock_guard l(balancer_lock);
    if (pcm != nullptr) {
      pcm->tune_memory();
      pcm->balance();
      _set_new_cache_sizes();
      dout(10) << "tick balancer "
               << " inc cache_bytes: " << inc_cache->get_cache_bytes()
               << " inc comtd_bytes: " << inc_cache->get_committed_size()
               << " inc used_bytes: " << inc_cache->_get_used_bytes()
               << " inc num_osdmaps: " << inc_cache->_get_num_osdmaps()
               << dendl;
      dout(10) << "tick balancer "
               << " full cache_bytes: " << full_cache->get_cache_bytes()
               << " full comtd_bytes: " << full_cache->get_committed_size()
               << " full used_bytes: " << full_cache->_get_used_bytes()
               << " full num_osdmaps: " << full_cache->_get_num_osdmaps()
               << dendl;
    }
  }

  if (!mon.is_leader()) return;

  bool do_propose = false;
  utime_t now = ceph_clock_now();

  if (handle_osd_timeouts(now, last_osd_report)) {
    do_propose = true;
  }

  // mark osds down?
  if (check_failures(now)) {
    do_propose = true;
  }

  // Force a proposal if we need to prune; pruning is performed on
  // ``encode_pending()``, hence why we need to regularly trigger a proposal
  // even if there's nothing going on.
  if (is_prune_enabled() && should_prune()) {
    do_propose = true;
  }

  // mark down osds out?

  /* can_mark_out() checks if we can mark osds as being out. The -1 has no
   * influence at all. The decision is made based on the ratio of "in" osds,
   * and the function returns false if this ratio is lower that the minimum
   * ratio set by g_conf()->mon_osd_min_in_ratio. So it's not really up to us.
   */
  if (can_mark_out(-1)) {
    string down_out_subtree_limit = g_conf().get_val<string>(
      "mon_osd_down_out_subtree_limit");
    set<int> down_cache;  // quick cache of down subtrees

    map<int,utime_t>::iterator i = down_pending_out.begin();
    while (i != down_pending_out.end()) {
      int o = i->first;
      utime_t down = now;
      down -= i->second;
      ++i;

      if (osdmap.is_down(o) &&
	  osdmap.is_in(o) &&
	  can_mark_out(o)) {
	utime_t orig_grace(g_conf()->mon_osd_down_out_interval, 0);
	utime_t grace = orig_grace;
	double my_grace = 0.0;

	if (g_conf()->mon_osd_adjust_down_out_interval) {
	  // scale grace period the same way we do the heartbeat grace.
	  const osd_xinfo_t& xi = osdmap.get_xinfo(o);
	  double halflife = (double)g_conf()->mon_osd_laggy_halflife;
	  double decay_k = ::log(.5) / halflife;
	  double decay = exp((double)down * decay_k);
	  dout(20) << "osd." << o << " laggy halflife " << halflife << " decay_k " << decay_k
		   << " down for " << down << " decay " << decay << dendl;
	  my_grace = decay * (double)xi.laggy_interval * xi.laggy_probability;
	  grace += my_grace;
	}

	// is this an entire large subtree down?
	if (down_out_subtree_limit.length()) {
	  int type = osdmap.crush->get_type_id(down_out_subtree_limit);
	  if (type > 0) {
	    if (osdmap.containing_subtree_is_down(cct, o, type, &down_cache)) {
	      dout(10) << "tick entire containing " << down_out_subtree_limit
		       << " subtree for osd." << o
		       << " is down; resetting timer" << dendl;
	      // reset timer, too.
	      down_pending_out[o] = now;
	      continue;
	    }
	  }
	}

        bool down_out = !osdmap.is_destroyed(o) &&
          g_conf()->mon_osd_down_out_interval > 0 && down.sec() >= grace;
        bool destroyed_out = osdmap.is_destroyed(o) &&
          g_conf()->mon_osd_destroyed_out_interval > 0 &&
        // this is not precise enough as we did not make a note when this osd
        // was marked as destroyed, but let's not bother with that
        // complexity for now.
          down.sec() >= g_conf()->mon_osd_destroyed_out_interval;
        if (down_out || destroyed_out) {
	  dout(10) << "tick marking osd." << o << " OUT after " << down
		   << " sec (target " << grace << " = " << orig_grace << " + " << my_grace << ")" << dendl;
	  pending_inc.new_weight[o] = CEPH_OSD_OUT;

	  // set the AUTOOUT bit.
	  if (pending_inc.new_state.count(o) == 0)
	    pending_inc.new_state[o] = 0;
	  pending_inc.new_state[o] |= CEPH_OSD_AUTOOUT;

	  // remember previous weight
	  if (pending_inc.new_xinfo.count(o) == 0)
	    pending_inc.new_xinfo[o] = osdmap.osd_xinfo[o];
	  pending_inc.new_xinfo[o].old_weight = osdmap.osd_weight[o];

	  do_propose = true;

	  mon.clog->info() << "Marking osd." << o << " out (has been down for "
                            << int(down.sec()) << " seconds)";
	} else
	  continue;
      }

      down_pending_out.erase(o);
    }
  } else {
    dout(10) << "tick NOOUT flag set, not checking down osds" << dendl;
  }

  // expire blocklisted items?
  for (ceph::unordered_map<entity_addr_t,utime_t>::iterator p = osdmap.blocklist.begin();
       p != osdmap.blocklist.end();
       ++p) {
    if (p->second < now) {
      dout(10) << "expiring blocklist item " << p->first << " expired " << p->second << " < now " << now << dendl;
      pending_inc.old_blocklist.push_back(p->first);
      do_propose = true;
    }
  }
  for (auto p = osdmap.range_blocklist.begin();
       p != osdmap.range_blocklist.end();
       ++p) {
    if (p->second < now) {
      dout(10) << "expiring range_blocklist item " << p->first
	       << " expired " << p->second << " < now " << now << dendl;
      pending_inc.old_range_blocklist.push_back(p->first);
      do_propose = true;
    }
  }

  if (try_prune_purged_snaps()) {
    do_propose = true;
  }

  if (update_pools_status())
    do_propose = true;

  if (do_propose ||
      !pending_inc.new_pg_temp.empty())  // also propose if we adjusted pg_temp
    propose_pending();
}

void OSDMonitor::_set_new_cache_sizes()
{
  uint64_t cache_size = 0;
  int64_t inc_alloc = 0;
  int64_t full_alloc = 0;
  int64_t kv_alloc = 0;

  if (pcm != nullptr && rocksdb_binned_kv_cache != nullptr) {
    cache_size = pcm->get_tuned_mem();
    inc_alloc = inc_cache->get_committed_size();
    full_alloc = full_cache->get_committed_size();
    kv_alloc = rocksdb_binned_kv_cache->get_committed_size();
  }

  inc_osd_cache.set_bytes(inc_alloc);
  full_osd_cache.set_bytes(full_alloc);

  dout(1) << __func__ << " cache_size:" << cache_size
           << " inc_alloc: " << inc_alloc
           << " full_alloc: " << full_alloc
           << " kv_alloc: " << kv_alloc
           << dendl;
}

bool OSDMonitor::handle_osd_timeouts(const utime_t &now,
				     std::map<int, std::pair<utime_t, int>> &last_osd_report)
{
  utime_t timeo(g_conf()->mon_osd_report_timeout, 0);
  if (now - mon.get_leader_since() < timeo) {
    // We haven't been the leader for long enough to consider OSD timeouts
    return false;
  }

  int max_osd = osdmap.get_max_osd();
  bool new_down = false;

  for (int i=0; i < max_osd; ++i) {
    dout(30) << __func__ << ": checking up on osd " << i << dendl;
    if (!osdmap.exists(i)) {
      last_osd_report.erase(i); // if any
      continue;
    }
    if (!osdmap.is_up(i))
      continue;
    const std::map<int, std::pair<utime_t, int>>::const_iterator t = last_osd_report.find(i);
    if (t == last_osd_report.end()) {
      // it wasn't in the map; start the timer.
      last_osd_report[i].first = now;
      last_osd_report[i].second = 0;
    } else if (can_mark_down(i)) {
      utime_t diff = now - t->second.first;
      // we use the max(mon_osd_report_timeout, 2*osd_beacon_report_interval) as timeout
      // to allow for the osd to miss a beacon.
      int mon_osd_report_timeout = g_conf()->mon_osd_report_timeout;
      utime_t max_timeout(std::max(mon_osd_report_timeout,  2 * t->second.second), 0);
      if (diff > max_timeout) {
        mon.clog->info() << "osd." << i << " marked down after no beacon for "
                          << diff << " seconds";
        derr << "no beacon from osd." << i << " since " << t->second.first
             << ", " << diff << " seconds ago.  marking down" << dendl;
        pending_inc.new_state[i] = CEPH_OSD_UP;
        new_down = true;
      }
    }
  }
  return new_down;
}

static void dump_cpu_list(Formatter *f, const char *name,
			  const string& strlist)
{
  cpu_set_t cpu_set;
  size_t cpu_set_size;
  if (parse_cpu_set_list(strlist.c_str(), &cpu_set_size, &cpu_set) < 0) {
    return;
  }
  set<int> cpus = cpu_set_to_set(cpu_set_size, &cpu_set);
  f->open_array_section(name);
  for (auto cpu : cpus) {
    f->dump_int("cpu", cpu);
  }
  f->close_section();
}

void OSDMonitor::dump_info(Formatter *f)
{
  f->open_object_section("osdmap");
  osdmap.dump(f, cct);
  f->close_section();

  f->open_array_section("osd_metadata");
  for (int i=0; i<osdmap.get_max_osd(); ++i) {
    if (osdmap.exists(i)) {
      f->open_object_section("osd");
      f->dump_unsigned("id", i);
      dump_osd_metadata(i, f, NULL);
      f->close_section();
    }
  }
  f->close_section();

  f->open_object_section("osdmap_clean_epochs");
  f->dump_unsigned("min_last_epoch_clean", get_min_last_epoch_clean());

  f->open_object_section("last_epoch_clean");
  last_epoch_clean.dump(f);
  f->close_section();

  f->open_array_section("osd_epochs");
  for (auto& osd_epoch : osd_epochs) {
    f->open_object_section("osd");
    f->dump_unsigned("id", osd_epoch.first);
    f->dump_unsigned("epoch", osd_epoch.second);
    f->close_section();
  }
  f->close_section(); // osd_epochs

  f->close_section(); // osd_clean_epochs

  f->dump_unsigned("osdmap_first_committed", get_first_committed());
  f->dump_unsigned("osdmap_last_committed", get_last_committed());

  f->open_object_section("crushmap");
  osdmap.crush->dump(f);
  f->close_section();

  if (has_osdmap_manifest) {
    f->open_object_section("osdmap_manifest");
    osdmap_manifest.dump(f);
    f->close_section();
  }
}

namespace {
  enum osd_pool_get_choices {
    SIZE, MIN_SIZE,
    PG_NUM, PGP_NUM, CRUSH_RULE, HASHPSPOOL, EC_OVERWRITES,
    NODELETE, NOPGCHANGE, NOSIZECHANGE,
    WRITE_FADVISE_DONTNEED, NOSCRUB, NODEEP_SCRUB,
    HIT_SET_TYPE, HIT_SET_PERIOD, HIT_SET_COUNT, HIT_SET_FPP,
    USE_GMT_HITSET, TARGET_MAX_OBJECTS, TARGET_MAX_BYTES,
    CACHE_TARGET_DIRTY_RATIO, CACHE_TARGET_DIRTY_HIGH_RATIO,
    CACHE_TARGET_FULL_RATIO,
    CACHE_MIN_FLUSH_AGE, CACHE_MIN_EVICT_AGE,
    ERASURE_CODE_PROFILE, MIN_READ_RECENCY_FOR_PROMOTE,
    MIN_WRITE_RECENCY_FOR_PROMOTE, FAST_READ,
    HIT_SET_GRADE_DECAY_RATE, HIT_SET_SEARCH_LAST_N,
    SCRUB_MIN_INTERVAL, SCRUB_MAX_INTERVAL, DEEP_SCRUB_INTERVAL,
    RECOVERY_PRIORITY, RECOVERY_OP_PRIORITY, SCRUB_PRIORITY,
    COMPRESSION_MODE, COMPRESSION_ALGORITHM, COMPRESSION_REQUIRED_RATIO,
    COMPRESSION_MAX_BLOB_SIZE, COMPRESSION_MIN_BLOB_SIZE,
    CSUM_TYPE, CSUM_MAX_BLOCK, CSUM_MIN_BLOCK, FINGERPRINT_ALGORITHM,
    PG_AUTOSCALE_MODE, PG_NUM_MIN, TARGET_SIZE_BYTES, TARGET_SIZE_RATIO,
    PG_AUTOSCALE_BIAS, DEDUP_TIER, DEDUP_CHUNK_ALGORITHM, 
    DEDUP_CDC_CHUNK_SIZE, POOL_EIO, BULK, PG_NUM_MAX };

  std::set<osd_pool_get_choices>
    subtract_second_from_first(const std::set<osd_pool_get_choices>& first,
				const std::set<osd_pool_get_choices>& second)
    {
      std::set<osd_pool_get_choices> result;
      std::set_difference(first.begin(), first.end(),
			  second.begin(), second.end(),
			  std::inserter(result, result.end()));
      return result;
    }
}


bool OSDMonitor::preprocess_command(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MMonCommand>();
  int r = 0;
  bufferlist rdata;
  stringstream ss, ds;

  cmdmap_t cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon.reply_command(op, -EINVAL, rs, get_last_committed());
    return true;
  }

  MonSession *session = op->get_session();
  if (!session) {
    derr << __func__ << " no session" << dendl;
    mon.reply_command(op, -EACCES, "access denied", get_last_committed());
    return true;
  }

  string prefix;
  cmd_getval(cmdmap, "prefix", prefix);

  string format = cmd_getval_or<string>(cmdmap, "format", "plain");
  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  if (prefix == "osd stat") {
    if (f) {
      f->open_object_section("osdmap");
      osdmap.print_summary(f.get(), ds, "", true);
      f->close_section();
      f->flush(rdata);
    } else {
      osdmap.print_summary(nullptr, ds, "", true);
      rdata.append(ds);
    }
  }
  else if (prefix == "osd dump" ||
	   prefix == "osd tree" ||
	   prefix == "osd tree-from" ||
	   prefix == "osd ls" ||
	   prefix == "osd getmap" ||
	   prefix == "osd getcrushmap" ||
	   prefix == "osd ls-tree" ||
	   prefix == "osd info") {

    epoch_t epoch = cmd_getval_or<int64_t>(cmdmap, "epoch", osdmap.get_epoch());
    bufferlist osdmap_bl;
    int err = get_version_full(epoch, osdmap_bl);
    if (err == -ENOENT) {
      r = -ENOENT;
      ss << "there is no map for epoch " << epoch;
      goto reply;
    }
    ceph_assert(err == 0);
    ceph_assert(osdmap_bl.length());

    OSDMap *p;
    if (epoch == osdmap.get_epoch()) {
      p = &osdmap;
    } else {
      p = new OSDMap;
      p->decode(osdmap_bl);
    }

    auto sg = make_scope_guard([&] {
      if (p != &osdmap) {
        delete p;
      }
    });

    if (prefix == "osd dump") {
      stringstream ds;
      if (f) {
	f->open_object_section("osdmap");
	p->dump(f.get(), cct);
	f->close_section();
	f->flush(ds);
      } else {
	p->print(cct, ds);
      }
      rdata.append(ds);
      if (!f)
	ds << " ";
    } else if (prefix == "osd ls") {
      if (f) {
	f->open_array_section("osds");
	for (int i = 0; i < osdmap.get_max_osd(); i++) {
	  if (osdmap.exists(i)) {
	    f->dump_int("osd", i);
	  }
	}
	f->close_section();
	f->flush(ds);
      } else {
	bool first = true;
	for (int i = 0; i < osdmap.get_max_osd(); i++) {
	  if (osdmap.exists(i)) {
	    if (!first)
	      ds << "\n";
	    first = false;
	    ds << i;
	  }
	}
      }
      rdata.append(ds);
    } else if (prefix == "osd info") {
      int64_t osd_id;
      bool do_single_osd = true;
      if (!cmd_getval(cmdmap, "id", osd_id)) {
	do_single_osd = false;
      }

      if (do_single_osd && !osdmap.exists(osd_id)) {
	ss << "osd." << osd_id << " does not exist";
	r = -EINVAL;
	goto reply;
      }

      if (f) {
	if (do_single_osd) {
	  osdmap.dump_osd(osd_id, f.get());
	} else {
	  osdmap.dump_osds(f.get());
	}
	f->flush(ds);
      } else {
	if (do_single_osd) {
	  osdmap.print_osd(osd_id, ds);
	} else {
	  osdmap.print_osds(ds);
	}
      }
      rdata.append(ds);
    } else if (prefix == "osd tree" || prefix == "osd tree-from") {
      string bucket;
      if (prefix == "osd tree-from") {
        cmd_getval(cmdmap, "bucket", bucket);
        if (!osdmap.crush->name_exists(bucket)) {
          ss << "bucket '" << bucket << "' does not exist";
          r = -ENOENT;
          goto reply;
        }
        int id = osdmap.crush->get_item_id(bucket);
        if (id >= 0) {
          ss << "\"" << bucket << "\" is not a bucket";
          r = -EINVAL;
          goto reply;
        }
      }

      vector<string> states;
      cmd_getval(cmdmap, "states", states);
      unsigned filter = 0;
      for (auto& s : states) {
	if (s == "up") {
	  filter |= OSDMap::DUMP_UP;
	} else if (s == "down") {
	  filter |= OSDMap::DUMP_DOWN;
	} else if (s == "in") {
	  filter |= OSDMap::DUMP_IN;
	} else if (s == "out") {
	  filter |= OSDMap::DUMP_OUT;
	} else if (s == "destroyed") {
	  filter |= OSDMap::DUMP_DESTROYED;
	} else {
	  ss << "unrecognized state '" << s << "'";
	  r = -EINVAL;
	  goto reply;
	}
      }
      if ((filter & (OSDMap::DUMP_IN|OSDMap::DUMP_OUT)) ==
	  (OSDMap::DUMP_IN|OSDMap::DUMP_OUT)) {
        ss << "cannot specify both 'in' and 'out'";
        r = -EINVAL;
        goto reply;
      }
      if (((filter & (OSDMap::DUMP_UP|OSDMap::DUMP_DOWN)) ==
	   (OSDMap::DUMP_UP|OSDMap::DUMP_DOWN)) ||
           ((filter & (OSDMap::DUMP_UP|OSDMap::DUMP_DESTROYED)) ==
           (OSDMap::DUMP_UP|OSDMap::DUMP_DESTROYED)) ||
           ((filter & (OSDMap::DUMP_DOWN|OSDMap::DUMP_DESTROYED)) ==
           (OSDMap::DUMP_DOWN|OSDMap::DUMP_DESTROYED))) {
	ss << "can specify only one of 'up', 'down' and 'destroyed'";
	r = -EINVAL;
	goto reply;
      }
      if (f) {
	f->open_object_section("tree");
	p->print_tree(f.get(), NULL, filter, bucket);
	f->close_section();
	f->flush(ds);
      } else {
	p->print_tree(NULL, &ds, filter, bucket);
      }
      rdata.append(ds);
    } else if (prefix == "osd getmap") {
      rdata.append(osdmap_bl);
      ss << "got osdmap epoch " << p->get_epoch();
    } else if (prefix == "osd getcrushmap") {
      p->crush->encode(rdata, mon.get_quorum_con_features());
      ss << p->get_crush_version();
    } else if (prefix == "osd ls-tree") {
      string bucket_name;
      cmd_getval(cmdmap, "name", bucket_name);
      set<int> osds;
      r = p->get_osds_by_bucket_name(bucket_name, &osds);
      if (r == -ENOENT) {
        ss << "\"" << bucket_name << "\" does not exist";
        goto reply;
      } else if (r < 0) {
        ss << "can not parse bucket name:\"" << bucket_name << "\"";
        goto reply;
      }

      if (f) {
        f->open_array_section("osds");
        for (auto &i : osds) {
          if (osdmap.exists(i)) {
            f->dump_int("osd", i);
          }
        }
        f->close_section();
        f->flush(ds);
      } else {
        bool first = true;
        for (auto &i : osds) {
          if (osdmap.exists(i)) {
            if (!first)
              ds << "\n";
            first = false;
            ds << i;
          }
        }
      }

      rdata.append(ds);
    }
  } else if (prefix == "osd getmaxosd") {
    if (f) {
      f->open_object_section("getmaxosd");
      f->dump_unsigned("epoch", osdmap.get_epoch());
      f->dump_int("max_osd", osdmap.get_max_osd());
      f->close_section();
      f->flush(rdata);
    } else {
      ds << "max_osd = " << osdmap.get_max_osd() << " in epoch " << osdmap.get_epoch();
      rdata.append(ds);
    }
  } else if (prefix == "osd utilization") {
    string out;
    osdmap.summarize_mapping_stats(NULL, NULL, &out, f.get());
    if (f)
      f->flush(rdata);
    else
      rdata.append(out);
    r = 0;
    goto reply;
  } else if (prefix  == "osd find") {
    int64_t osd;
    if (!cmd_getval(cmdmap, "id", osd)) {
      ss << "unable to parse osd id value '"
         << cmd_vartype_stringify(cmdmap["id"]) << "'";
      r = -EINVAL;
      goto reply;
    }
    if (!osdmap.exists(osd)) {
      ss << "osd." << osd << " does not exist";
      r = -ENOENT;
      goto reply;
    }
    string format;
    cmd_getval(cmdmap, "format", format);
    boost::scoped_ptr<Formatter> f(Formatter::create(format, "json-pretty", "json-pretty"));
    f->open_object_section("osd_location");
    f->dump_int("osd", osd);
    f->dump_object("addrs", osdmap.get_addrs(osd));
    f->dump_stream("osd_fsid") << osdmap.get_uuid(osd);

    // try to identify host, pod/container name, etc.
    map<string,string> m;
    load_metadata(osd, m, nullptr);
    if (auto p = m.find("hostname"); p != m.end()) {
      f->dump_string("host", p->second);
    }
    for (auto& k : {
	"pod_name", "pod_namespace", // set by rook
	"container_name"             // set by cephadm, ceph-ansible
	}) {
      if (auto p = m.find(k); p != m.end()) {
	f->dump_string(k, p->second);
      }
    }

    // crush is helpful too
    f->open_object_section("crush_location");
    map<string,string> loc = osdmap.crush->get_full_location(osd);
    for (map<string,string>::iterator p = loc.begin(); p != loc.end(); ++p)
      f->dump_string(p->first.c_str(), p->second);
    f->close_section();
    f->close_section();
    f->flush(rdata);
  } else if (prefix == "osd metadata") {
    int64_t osd = -1;
    if (cmd_vartype_stringify(cmdmap["id"]).size() &&
        !cmd_getval(cmdmap, "id", osd)) {
      ss << "unable to parse osd id value '"
         << cmd_vartype_stringify(cmdmap["id"]) << "'";
      r = -EINVAL;
      goto reply;
    }
    if (osd >= 0 && !osdmap.exists(osd)) {
      ss << "osd." << osd << " does not exist";
      r = -ENOENT;
      goto reply;
    }
    string format;
    cmd_getval(cmdmap, "format", format);
    boost::scoped_ptr<Formatter> f(Formatter::create(format, "json-pretty", "json-pretty"));
    if (osd >= 0) {
      f->open_object_section("osd_metadata");
      f->dump_unsigned("id", osd);
      r = dump_osd_metadata(osd, f.get(), &ss);
      if (r < 0)
        goto reply;
      f->close_section();
    } else {
      r = 0;
      f->open_array_section("osd_metadata");
      for (int i=0; i<osdmap.get_max_osd(); ++i) {
        if (osdmap.exists(i)) {
          f->open_object_section("osd");
          f->dump_unsigned("id", i);
          r = dump_osd_metadata(i, f.get(), NULL);
          if (r == -EINVAL || r == -ENOENT) {
            // Drop error, continue to get other daemons' metadata
            dout(4) << "No metadata for osd." << i << dendl;
            r = 0;
          } else if (r < 0) {
            // Unexpected error
            goto reply;
          }
          f->close_section();
        }
      }
      f->close_section();
    }
    f->flush(rdata);
  } else if (prefix == "osd versions") {
    if (!f)
      f.reset(Formatter::create("json-pretty"));
    count_metadata("ceph_version", f.get());
    f->flush(rdata);
    r = 0;
  } else if (prefix == "osd count-metadata") {
    if (!f)
      f.reset(Formatter::create("json-pretty"));
    string field;
    cmd_getval(cmdmap, "property", field);
    count_metadata(field, f.get());
    f->flush(rdata);
    r = 0;
  } else if (prefix == "osd numa-status") {
    TextTable tbl;
    if (f) {
      f->open_array_section("osds");
    } else {
      tbl.define_column("OSD", TextTable::LEFT, TextTable::RIGHT);
      tbl.define_column("HOST", TextTable::LEFT, TextTable::LEFT);
      tbl.define_column("NETWORK", TextTable::RIGHT, TextTable::RIGHT);
      tbl.define_column("STORAGE", TextTable::RIGHT, TextTable::RIGHT);
      tbl.define_column("AFFINITY", TextTable::RIGHT, TextTable::RIGHT);
      tbl.define_column("CPUS", TextTable::LEFT, TextTable::LEFT);
    }
    for (int i=0; i<osdmap.get_max_osd(); ++i) {
      if (osdmap.exists(i)) {
	map<string,string> m;
	ostringstream err;
	if (load_metadata(i, m, &err) < 0) {
	  continue;
	}
	string host;
	auto p = m.find("hostname");
	if (p != m.end()) {
	  host = p->second;
	}
	if (f) {
	  f->open_object_section("osd");
	  f->dump_int("osd", i);
	  f->dump_string("host", host);
	  for (auto n : { "network_numa_node", "objectstore_numa_node",
		"numa_node" }) {
	    p = m.find(n);
	    if (p != m.end()) {
	      f->dump_int(n, atoi(p->second.c_str()));
	    }
	  }
	  for (auto n : { "network_numa_nodes", "objectstore_numa_nodes" }) {
	    p = m.find(n);
	    if (p != m.end()) {
	      list<string> ls = get_str_list(p->second, ",");
	      f->open_array_section(n);
	      for (auto node : ls) {
		f->dump_int("node", atoi(node.c_str()));
	      }
	      f->close_section();
	    }
	  }
	  for (auto n : { "numa_node_cpus" }) {
	    p = m.find(n);
	    if (p != m.end()) {
	      dump_cpu_list(f.get(), n, p->second);
	    }
	  }
	  f->close_section();
	} else {
	  tbl << i;
	  tbl << host;
	  p = m.find("network_numa_nodes");
	  if (p != m.end()) {
	    tbl << p->second;
	  } else {
	    tbl << "-";
	  }
	  p = m.find("objectstore_numa_nodes");
	  if (p != m.end()) {
	    tbl << p->second;
	  } else {
	    tbl << "-";
	  }
	  p = m.find("numa_node");
	  auto q = m.find("numa_node_cpus");
	  if (p != m.end() && q != m.end()) {
	    tbl << p->second;
	    tbl << q->second;
	  } else {
	    tbl << "-";
	    tbl << "-";
	  }
	  tbl << TextTable::endrow;
	}
      }
    }
    if (f) {
      f->close_section();
      f->flush(rdata);
    } else {
      rdata.append(stringify(tbl));
    }
  } else if (prefix == "osd map") {
    string poolstr, objstr, namespacestr;
    cmd_getval(cmdmap, "pool", poolstr);
    cmd_getval(cmdmap, "object", objstr);
    cmd_getval(cmdmap, "nspace", namespacestr);

    int64_t pool = osdmap.lookup_pg_pool_name(poolstr.c_str());
    if (pool < 0) {
      ss << "pool " << poolstr << " does not exist";
      r = -ENOENT;
      goto reply;
    }
    object_locator_t oloc(pool, namespacestr);
    object_t oid(objstr);
    pg_t pgid = osdmap.object_locator_to_pg(oid, oloc);
    pg_t mpgid = osdmap.raw_pg_to_pg(pgid);
    vector<int> up, acting;
    int up_p, acting_p;
    osdmap.pg_to_up_acting_osds(mpgid, &up, &up_p, &acting, &acting_p);

    string fullobjname;
    if (!namespacestr.empty())
      fullobjname = namespacestr + string("/") + oid.name;
    else
      fullobjname = oid.name;
    if (f) {
      f->open_object_section("osd_map");
      f->dump_unsigned("epoch", osdmap.get_epoch());
      f->dump_string("pool", poolstr);
      f->dump_int("pool_id", pool);
      f->dump_stream("objname") << fullobjname;
      f->dump_stream("raw_pgid") << pgid;
      f->dump_stream("pgid") << mpgid;
      f->open_array_section("up");
      for (vector<int>::iterator p = up.begin(); p != up.end(); ++p)
        f->dump_int("osd", *p);
      f->close_section();
      f->dump_int("up_primary", up_p);
      f->open_array_section("acting");
      for (vector<int>::iterator p = acting.begin(); p != acting.end(); ++p)
        f->dump_int("osd", *p);
      f->close_section();
      f->dump_int("acting_primary", acting_p);
      f->close_section(); // osd_map
      f->flush(rdata);
    } else {
      ds << "osdmap e" << osdmap.get_epoch()
        << " pool '" << poolstr << "' (" << pool << ")"
        << " object '" << fullobjname << "' ->"
        << " pg " << pgid << " (" << mpgid << ")"
        << " -> up (" << pg_vector_string(up) << ", p" << up_p << ") acting ("
        << pg_vector_string(acting) << ", p" << acting_p << ")";
      rdata.append(ds);
    }

  } else if (prefix == "pg map") {
    pg_t pgid;
    vector<int> up, acting;
    r = parse_pgid(cmdmap, ss, pgid);
    if (r < 0)
      goto reply;
    pg_t mpgid = osdmap.raw_pg_to_pg(pgid);
    osdmap.pg_to_up_acting_osds(pgid, up, acting);
    if (f) {
      f->open_object_section("pg_map");
      f->dump_unsigned("epoch", osdmap.get_epoch());
      f->dump_stream("raw_pgid") << pgid;
      f->dump_stream("pgid") << mpgid;
      f->open_array_section("up");
      for (auto osd : up) {
	f->dump_int("up_osd", osd);
      }
      f->close_section();
      f->open_array_section("acting");
      for (auto osd : acting) {
	f->dump_int("acting_osd", osd);
      }
      f->close_section();
      f->close_section();
      f->flush(rdata);
    } else {
      ds << "osdmap e" << osdmap.get_epoch()
         << " pg " << pgid << " (" << mpgid << ")"
         << " -> up " << up << " acting " << acting;
      rdata.append(ds);
    }
    goto reply;

  } else if (prefix == "osd lspools") {
    if (f)
      f->open_array_section("pools");
    for (map<int64_t, pg_pool_t>::iterator p = osdmap.pools.begin();
	 p != osdmap.pools.end();
	 ++p) {
      if (f) {
	f->open_object_section("pool");
	f->dump_int("poolnum", p->first);
	f->dump_string("poolname", osdmap.pool_name[p->first]);
	f->close_section();
      } else {
	ds << p->first << ' ' << osdmap.pool_name[p->first];
	if (next(p) != osdmap.pools.end()) {
	  ds << '\n';
	}
      }
    }
    if (f) {
      f->close_section();
      f->flush(ds);
    }
    rdata.append(ds);
  } else if (prefix == "osd blocklist ls" ||
	     prefix == "osd blacklist ls") {
    if (f)
      f->open_array_section("blocklist");

    for (ceph::unordered_map<entity_addr_t,utime_t>::iterator p = osdmap.blocklist.begin();
	 p != osdmap.blocklist.end();
	 ++p) {
      if (f) {
	f->open_object_section("entry");
	f->dump_string("addr", p->first.get_legacy_str());
	f->dump_stream("until") << p->second;
	f->close_section();
      } else {
	stringstream ss;
	string s;
	ss << p->first << " " << p->second;
	getline(ss, s);
	s += "\n";
	rdata.append(s);
      }
    }
    if (f) {
      f->close_section();
      f->flush(rdata);
    }
    if (f)
      f->open_array_section("range_blocklist");

    for (auto p = osdmap.range_blocklist.begin();
	 p != osdmap.range_blocklist.end();
	 ++p) {
      if (f) {
	f->open_object_section("entry");
	f->dump_string("range", p->first.get_legacy_str());
	f->dump_stream("until") << p->second;
	f->close_section();
      } else {
	stringstream ss;
	string s;
	ss << p->first << " " << p->second;
	getline(ss, s);
	s += "\n";
	rdata.append(s);
      }
    }
    if (f) {
      f->close_section();
      f->flush(rdata);
    }
    ss << "listed " << osdmap.blocklist.size() + osdmap.range_blocklist.size() << " entries";

  } else if (prefix == "osd pool ls") {
    string detail;
    cmd_getval(cmdmap, "detail", detail);
    if (!f && detail == "detail") {
      ostringstream ss;
      osdmap.print_pools(cct, ss);
      rdata.append(ss.str());
    } else {
      if (f)
	f->open_array_section("pools");
      for (auto &[pid, pdata] : osdmap.get_pools()) {
	if (f) {
	  if (detail == "detail") {
	    f->open_object_section("pool");
	    f->dump_int("pool_id", pid);
	    f->dump_string("pool_name", osdmap.get_pool_name(pid));
	    pdata.dump(f.get());
	    osdmap.dump_read_balance_score(cct, pid, pdata, f.get());
	    f->close_section();
	  } else {
	    f->dump_string("pool_name", osdmap.get_pool_name(pid));
	  }
	} else {
	  rdata.append(osdmap.get_pool_name(pid) + "\n");
	}
      }
      if (f) {
	f->close_section();
	f->flush(rdata);
      }
    }

  } else if (prefix == "osd crush get-tunable") {
    string tunable;
    cmd_getval(cmdmap, "tunable", tunable);
    ostringstream rss;
    if (f)
      f->open_object_section("tunable");
    if (tunable == "straw_calc_version") {
      if (f)
	f->dump_int(tunable.c_str(), osdmap.crush->get_straw_calc_version());
      else
	rss << osdmap.crush->get_straw_calc_version() << "\n";
    } else {
      r = -EINVAL;
      goto reply;
    }
    if (f) {
      f->close_section();
      f->flush(rdata);
    } else {
      rdata.append(rss.str());
    }
    r = 0;

  } else if (prefix == "osd pool get") {
    string poolstr;
    cmd_getval(cmdmap, "pool", poolstr);
    int64_t pool = osdmap.lookup_pg_pool_name(poolstr.c_str());
    if (pool < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      r = -ENOENT;
      goto reply;
    }

    const pg_pool_t *p = osdmap.get_pg_pool(pool);
    string var;
    cmd_getval(cmdmap, "var", var);

    typedef std::map<std::string, osd_pool_get_choices> choices_map_t;
    const choices_map_t ALL_CHOICES = {
      {"size", SIZE},
      {"min_size", MIN_SIZE},
      {"pg_num", PG_NUM}, {"pgp_num", PGP_NUM},
      {"crush_rule", CRUSH_RULE},
      {"hashpspool", HASHPSPOOL},
      {"eio", POOL_EIO},
      {"allow_ec_overwrites", EC_OVERWRITES}, {"nodelete", NODELETE},
      {"nopgchange", NOPGCHANGE}, {"nosizechange", NOSIZECHANGE},
      {"noscrub", NOSCRUB}, {"nodeep-scrub", NODEEP_SCRUB},
      {"write_fadvise_dontneed", WRITE_FADVISE_DONTNEED},
      {"hit_set_type", HIT_SET_TYPE}, {"hit_set_period", HIT_SET_PERIOD},
      {"hit_set_count", HIT_SET_COUNT}, {"hit_set_fpp", HIT_SET_FPP},
      {"use_gmt_hitset", USE_GMT_HITSET},
      {"target_max_objects", TARGET_MAX_OBJECTS},
      {"target_max_bytes", TARGET_MAX_BYTES},
      {"cache_target_dirty_ratio", CACHE_TARGET_DIRTY_RATIO},
      {"cache_target_dirty_high_ratio", CACHE_TARGET_DIRTY_HIGH_RATIO},
      {"cache_target_full_ratio", CACHE_TARGET_FULL_RATIO},
      {"cache_min_flush_age", CACHE_MIN_FLUSH_AGE},
      {"cache_min_evict_age", CACHE_MIN_EVICT_AGE},
      {"erasure_code_profile", ERASURE_CODE_PROFILE},
      {"min_read_recency_for_promote", MIN_READ_RECENCY_FOR_PROMOTE},
      {"min_write_recency_for_promote", MIN_WRITE_RECENCY_FOR_PROMOTE},
      {"fast_read", FAST_READ},
      {"hit_set_grade_decay_rate", HIT_SET_GRADE_DECAY_RATE},
      {"hit_set_search_last_n", HIT_SET_SEARCH_LAST_N},
      {"scrub_min_interval", SCRUB_MIN_INTERVAL},
      {"scrub_max_interval", SCRUB_MAX_INTERVAL},
      {"deep_scrub_interval", DEEP_SCRUB_INTERVAL},
      {"recovery_priority", RECOVERY_PRIORITY},
      {"recovery_op_priority", RECOVERY_OP_PRIORITY},
      {"scrub_priority", SCRUB_PRIORITY},
      {"compression_mode", COMPRESSION_MODE},
      {"compression_algorithm", COMPRESSION_ALGORITHM},
      {"compression_required_ratio", COMPRESSION_REQUIRED_RATIO},
      {"compression_max_blob_size", COMPRESSION_MAX_BLOB_SIZE},
      {"compression_min_blob_size", COMPRESSION_MIN_BLOB_SIZE},
      {"csum_type", CSUM_TYPE},
      {"csum_max_block", CSUM_MAX_BLOCK},
      {"csum_min_block", CSUM_MIN_BLOCK},
      {"fingerprint_algorithm", FINGERPRINT_ALGORITHM},
      {"pg_autoscale_mode", PG_AUTOSCALE_MODE},
      {"pg_num_min", PG_NUM_MIN},
      {"pg_num_max", PG_NUM_MAX},
      {"target_size_bytes", TARGET_SIZE_BYTES},
      {"target_size_ratio", TARGET_SIZE_RATIO},
      {"pg_autoscale_bias", PG_AUTOSCALE_BIAS},
      {"dedup_tier", DEDUP_TIER},
      {"dedup_chunk_algorithm", DEDUP_CHUNK_ALGORITHM},
      {"dedup_cdc_chunk_size", DEDUP_CDC_CHUNK_SIZE},
      {"bulk", BULK}
    };

    typedef std::set<osd_pool_get_choices> choices_set_t;

    const choices_set_t ONLY_TIER_CHOICES = {
      HIT_SET_TYPE, HIT_SET_PERIOD, HIT_SET_COUNT, HIT_SET_FPP,
      TARGET_MAX_OBJECTS, TARGET_MAX_BYTES, CACHE_TARGET_FULL_RATIO,
      CACHE_TARGET_DIRTY_RATIO, CACHE_TARGET_DIRTY_HIGH_RATIO,
      CACHE_MIN_FLUSH_AGE, CACHE_MIN_EVICT_AGE,
      MIN_READ_RECENCY_FOR_PROMOTE,
      MIN_WRITE_RECENCY_FOR_PROMOTE,
      HIT_SET_GRADE_DECAY_RATE, HIT_SET_SEARCH_LAST_N
    };
    const choices_set_t ONLY_ERASURE_CHOICES = {
      EC_OVERWRITES, ERASURE_CODE_PROFILE
    };

    choices_set_t selected_choices;
    if (var == "all") {
      for(choices_map_t::const_iterator it = ALL_CHOICES.begin();
	  it != ALL_CHOICES.end(); ++it) {
	selected_choices.insert(it->second);
      }

      if(!p->is_tier()) {
	selected_choices = subtract_second_from_first(selected_choices,
						      ONLY_TIER_CHOICES);
      }

      if(!p->is_erasure()) {
	selected_choices = subtract_second_from_first(selected_choices,
						      ONLY_ERASURE_CHOICES);
      }
    } else /* var != "all" */  {
      choices_map_t::const_iterator found = ALL_CHOICES.find(var);
      if (found == ALL_CHOICES.end()) {
        ss << "pool '" << poolstr
	       << "': invalid variable: '" << var << "'";
        r = -EINVAL;
        goto reply;
      }

      osd_pool_get_choices selected = found->second;

      if (!p->is_tier() &&
	  ONLY_TIER_CHOICES.find(selected) != ONLY_TIER_CHOICES.end()) {
	ss << "pool '" << poolstr
	   << "' is not a tier pool: variable not applicable";
	r = -EACCES;
	goto reply;
      }

      if (!p->is_erasure() &&
	  ONLY_ERASURE_CHOICES.find(selected)
	  != ONLY_ERASURE_CHOICES.end()) {
	ss << "pool '" << poolstr
	   << "' is not a erasure pool: variable not applicable";
	r = -EACCES;
	goto reply;
      }

      if (pool_opts_t::is_opt_name(var) &&
	  !p->opts.is_set(pool_opts_t::get_opt_desc(var).key)) {
	ss << "option '" << var << "' is not set on pool '" << poolstr << "'";
	r = -ENOENT;
	goto reply;
      }

      selected_choices.insert(selected);
    }

    if (f) {
      f->open_object_section("pool");
      f->dump_string("pool", poolstr);
      f->dump_int("pool_id", pool);
      for(choices_set_t::const_iterator it = selected_choices.begin();
	  it != selected_choices.end(); ++it) {
	choices_map_t::const_iterator i;
        for (i = ALL_CHOICES.begin(); i != ALL_CHOICES.end(); ++i) {
          if (i->second == *it) {
            break;
          }
        }
        ceph_assert(i != ALL_CHOICES.end());
	switch(*it) {
	  case PG_NUM:
	    f->dump_int("pg_num", p->get_pg_num());
	    break;
	  case PGP_NUM:
	    f->dump_int("pgp_num", p->get_pgp_num());
	    break;
	  case SIZE:
	    f->dump_int("size", p->get_size());
	    break;
	  case MIN_SIZE:
	    f->dump_int("min_size", p->get_min_size());
	    break;
	  case CRUSH_RULE:
	    if (osdmap.crush->rule_exists(p->get_crush_rule())) {
	      f->dump_string("crush_rule", osdmap.crush->get_rule_name(
			       p->get_crush_rule()));
	    } else {
	      f->dump_string("crush_rule", stringify(p->get_crush_rule()));
	    }
	    break;
	  case EC_OVERWRITES:
	    f->dump_bool("allow_ec_overwrites",
                         p->has_flag(pg_pool_t::FLAG_EC_OVERWRITES));
	    break;
	  case PG_AUTOSCALE_MODE:
	    f->dump_string("pg_autoscale_mode",
			   pg_pool_t::get_pg_autoscale_mode_name(
			     p->pg_autoscale_mode));
	    break;
	  case HASHPSPOOL:
	  case POOL_EIO:
	  case NODELETE:
	  case BULK:
	  case NOPGCHANGE:
	  case NOSIZECHANGE:
	  case WRITE_FADVISE_DONTNEED:
	  case NOSCRUB:
	  case NODEEP_SCRUB:
	    f->dump_bool(i->first.c_str(),
			   p->has_flag(pg_pool_t::get_flag_by_name(i->first)));
	    break;
	  case HIT_SET_PERIOD:
	    f->dump_int("hit_set_period", p->hit_set_period);
	    break;
	  case HIT_SET_COUNT:
	    f->dump_int("hit_set_count", p->hit_set_count);
	    break;
	  case HIT_SET_TYPE:
	    f->dump_string("hit_set_type",
			   HitSet::get_type_name(p->hit_set_params.get_type()));
	    break;
	  case HIT_SET_FPP:
	    {
	      if (p->hit_set_params.get_type() == HitSet::TYPE_BLOOM) {
		BloomHitSet::Params *bloomp =
		  static_cast<BloomHitSet::Params*>(p->hit_set_params.impl.get());
		f->dump_float("hit_set_fpp", bloomp->get_fpp());
	      } else if(var != "all") {
		f->close_section();
		ss << "hit set is not of type Bloom; " <<
		  "invalid to get a false positive rate!";
		r = -EINVAL;
		goto reply;
	      }
	    }
	    break;
	  case USE_GMT_HITSET:
	    f->dump_bool("use_gmt_hitset", p->use_gmt_hitset);
	    break;
	  case TARGET_MAX_OBJECTS:
	    f->dump_unsigned("target_max_objects", p->target_max_objects);
	    break;
	  case TARGET_MAX_BYTES:
	    f->dump_unsigned("target_max_bytes", p->target_max_bytes);
	    break;
	  case CACHE_TARGET_DIRTY_RATIO:
	    f->dump_unsigned("cache_target_dirty_ratio_micro",
			     p->cache_target_dirty_ratio_micro);
	    f->dump_float("cache_target_dirty_ratio",
			  ((float)p->cache_target_dirty_ratio_micro/1000000));
	    break;
	  case CACHE_TARGET_DIRTY_HIGH_RATIO:
	    f->dump_unsigned("cache_target_dirty_high_ratio_micro",
			     p->cache_target_dirty_high_ratio_micro);
	    f->dump_float("cache_target_dirty_high_ratio",
			  ((float)p->cache_target_dirty_high_ratio_micro/1000000));
	    break;
	  case CACHE_TARGET_FULL_RATIO:
	    f->dump_unsigned("cache_target_full_ratio_micro",
			     p->cache_target_full_ratio_micro);
	    f->dump_float("cache_target_full_ratio",
			  ((float)p->cache_target_full_ratio_micro/1000000));
	    break;
	  case CACHE_MIN_FLUSH_AGE:
	    f->dump_unsigned("cache_min_flush_age", p->cache_min_flush_age);
	    break;
	  case CACHE_MIN_EVICT_AGE:
	    f->dump_unsigned("cache_min_evict_age", p->cache_min_evict_age);
	    break;
	  case ERASURE_CODE_PROFILE:
	    f->dump_string("erasure_code_profile", p->erasure_code_profile);
	    break;
	  case MIN_READ_RECENCY_FOR_PROMOTE:
	    f->dump_int("min_read_recency_for_promote",
			p->min_read_recency_for_promote);
	    break;
	  case MIN_WRITE_RECENCY_FOR_PROMOTE:
	    f->dump_int("min_write_recency_for_promote",
			p->min_write_recency_for_promote);
	    break;
          case FAST_READ:
            f->dump_int("fast_read", p->fast_read);
            break;
	  case HIT_SET_GRADE_DECAY_RATE:
	    f->dump_int("hit_set_grade_decay_rate",
			p->hit_set_grade_decay_rate);
	    break;
	  case HIT_SET_SEARCH_LAST_N:
	    f->dump_int("hit_set_search_last_n",
			p->hit_set_search_last_n);
	    break;
	  case SCRUB_MIN_INTERVAL:
	  case SCRUB_MAX_INTERVAL:
	  case DEEP_SCRUB_INTERVAL:
          case RECOVERY_PRIORITY:
          case RECOVERY_OP_PRIORITY:
          case SCRUB_PRIORITY:
	  case COMPRESSION_MODE:
	  case COMPRESSION_ALGORITHM:
	  case COMPRESSION_REQUIRED_RATIO:
	  case COMPRESSION_MAX_BLOB_SIZE:
	  case COMPRESSION_MIN_BLOB_SIZE:
	  case CSUM_TYPE:
	  case CSUM_MAX_BLOCK:
	  case CSUM_MIN_BLOCK:
	  case FINGERPRINT_ALGORITHM:
	  case PG_NUM_MIN:
	  case PG_NUM_MAX:
	  case TARGET_SIZE_BYTES:
	  case TARGET_SIZE_RATIO:
	  case PG_AUTOSCALE_BIAS:
	  case DEDUP_TIER:
	  case DEDUP_CHUNK_ALGORITHM:
	  case DEDUP_CDC_CHUNK_SIZE:
            pool_opts_t::key_t key = pool_opts_t::get_opt_desc(i->first).key;
            if (p->opts.is_set(key)) {
              if(*it == CSUM_TYPE) {
                int64_t val;
                p->opts.get(pool_opts_t::CSUM_TYPE, &val);
                f->dump_string(i->first.c_str(), Checksummer::get_csum_type_string(val));
              } else {
                p->opts.dump(i->first, f.get());
              }
	    }
            break;
	}
      }
      f->close_section();
      f->flush(rdata);
    } else /* !f */ {
      for(choices_set_t::const_iterator it = selected_choices.begin();
	  it != selected_choices.end(); ++it) {
	choices_map_t::const_iterator i;
	switch(*it) {
	  case PG_NUM:
	    ss << "pg_num: " << p->get_pg_num() << "\n";
	    break;
	  case PGP_NUM:
	    ss << "pgp_num: " << p->get_pgp_num() << "\n";
	    break;
	  case SIZE:
	    ss << "size: " << p->get_size() << "\n";
	    break;
	  case MIN_SIZE:
	    ss << "min_size: " << p->get_min_size() << "\n";
	    break;
	  case CRUSH_RULE:
	    if (osdmap.crush->rule_exists(p->get_crush_rule())) {
	      ss << "crush_rule: " << osdmap.crush->get_rule_name(
		p->get_crush_rule()) << "\n";
	    } else {
	      ss << "crush_rule: " << p->get_crush_rule() << "\n";
	    }
	    break;
	  case PG_AUTOSCALE_MODE:
	    ss << "pg_autoscale_mode: " << pg_pool_t::get_pg_autoscale_mode_name(
	      p->pg_autoscale_mode) <<"\n";
	    break;
	  case HIT_SET_PERIOD:
	    ss << "hit_set_period: " << p->hit_set_period << "\n";
	    break;
	  case HIT_SET_COUNT:
	    ss << "hit_set_count: " << p->hit_set_count << "\n";
	    break;
	  case HIT_SET_TYPE:
	    ss << "hit_set_type: " <<
	      HitSet::get_type_name(p->hit_set_params.get_type()) << "\n";
	    break;
	  case HIT_SET_FPP:
	    {
	      if (p->hit_set_params.get_type() == HitSet::TYPE_BLOOM) {
		BloomHitSet::Params *bloomp =
		  static_cast<BloomHitSet::Params*>(p->hit_set_params.impl.get());
		ss << "hit_set_fpp: " << bloomp->get_fpp() << "\n";
	      } else if(var != "all") {
		ss << "hit set is not of type Bloom; " <<
		  "invalid to get a false positive rate!";
		r = -EINVAL;
		goto reply;
	      }
	    }
	    break;
	  case USE_GMT_HITSET:
	    ss << "use_gmt_hitset: " << p->use_gmt_hitset << "\n";
	    break;
	  case TARGET_MAX_OBJECTS:
	    ss << "target_max_objects: " << p->target_max_objects << "\n";
	    break;
	  case TARGET_MAX_BYTES:
	    ss << "target_max_bytes: " << p->target_max_bytes << "\n";
	    break;
	  case CACHE_TARGET_DIRTY_RATIO:
	    ss << "cache_target_dirty_ratio: "
	       << ((float)p->cache_target_dirty_ratio_micro/1000000) << "\n";
	    break;
	  case CACHE_TARGET_DIRTY_HIGH_RATIO:
	    ss << "cache_target_dirty_high_ratio: "
	       << ((float)p->cache_target_dirty_high_ratio_micro/1000000) << "\n";
	    break;
	  case CACHE_TARGET_FULL_RATIO:
	    ss << "cache_target_full_ratio: "
	       << ((float)p->cache_target_full_ratio_micro/1000000) << "\n";
	    break;
	  case CACHE_MIN_FLUSH_AGE:
	    ss << "cache_min_flush_age: " << p->cache_min_flush_age << "\n";
	    break;
	  case CACHE_MIN_EVICT_AGE:
	    ss << "cache_min_evict_age: " << p->cache_min_evict_age << "\n";
	    break;
	  case ERASURE_CODE_PROFILE:
	    ss << "erasure_code_profile: " << p->erasure_code_profile << "\n";
	    break;
	  case MIN_READ_RECENCY_FOR_PROMOTE:
	    ss << "min_read_recency_for_promote: " <<
	      p->min_read_recency_for_promote << "\n";
	    break;
	  case HIT_SET_GRADE_DECAY_RATE:
	    ss << "hit_set_grade_decay_rate: " <<
	      p->hit_set_grade_decay_rate << "\n";
	    break;
	  case HIT_SET_SEARCH_LAST_N:
	    ss << "hit_set_search_last_n: " <<
	      p->hit_set_search_last_n << "\n";
	    break;
	  case EC_OVERWRITES:
	    ss << "allow_ec_overwrites: " <<
	      (p->has_flag(pg_pool_t::FLAG_EC_OVERWRITES) ? "true" : "false") <<
	      "\n";
	    break;
	  case HASHPSPOOL:
	  case POOL_EIO:
	  case NODELETE:
	  case BULK:
	  case NOPGCHANGE:
	  case NOSIZECHANGE:
	  case WRITE_FADVISE_DONTNEED:
	  case NOSCRUB:
	  case NODEEP_SCRUB:
	    for (i = ALL_CHOICES.begin(); i != ALL_CHOICES.end(); ++i) {
	      if (i->second == *it)
		break;
	    }
	    ceph_assert(i != ALL_CHOICES.end());
	    ss << i->first << ": " <<
	      (p->has_flag(pg_pool_t::get_flag_by_name(i->first)) ?
	       "true" : "false") << "\n";
	    break;
	  case MIN_WRITE_RECENCY_FOR_PROMOTE:
	    ss << "min_write_recency_for_promote: " <<
	      p->min_write_recency_for_promote << "\n";
	    break;
          case FAST_READ:
            ss << "fast_read: " << p->fast_read << "\n";
            break;
	  case SCRUB_MIN_INTERVAL:
	  case SCRUB_MAX_INTERVAL:
	  case DEEP_SCRUB_INTERVAL:
          case RECOVERY_PRIORITY:
          case RECOVERY_OP_PRIORITY:
          case SCRUB_PRIORITY:
	  case COMPRESSION_MODE:
	  case COMPRESSION_ALGORITHM:
	  case COMPRESSION_REQUIRED_RATIO:
	  case COMPRESSION_MAX_BLOB_SIZE:
	  case COMPRESSION_MIN_BLOB_SIZE:
	  case CSUM_TYPE:
	  case CSUM_MAX_BLOCK:
	  case CSUM_MIN_BLOCK:
	  case FINGERPRINT_ALGORITHM:
	  case PG_NUM_MIN:
	  case PG_NUM_MAX:
	  case TARGET_SIZE_BYTES:
	  case TARGET_SIZE_RATIO:
	  case PG_AUTOSCALE_BIAS:
	  case DEDUP_TIER:
	  case DEDUP_CHUNK_ALGORITHM:
	  case DEDUP_CDC_CHUNK_SIZE:
	    for (i = ALL_CHOICES.begin(); i != ALL_CHOICES.end(); ++i) {
	      if (i->second == *it)
		break;
	    }
	    ceph_assert(i != ALL_CHOICES.end());
	    {
	      pool_opts_t::key_t key = pool_opts_t::get_opt_desc(i->first).key;
	      if (p->opts.is_set(key)) {
                if(key == pool_opts_t::CSUM_TYPE) {
                  int64_t val;
                  p->opts.get(key, &val);
  		  ss << i->first << ": " << Checksummer::get_csum_type_string(val) << "\n";
                } else {
  		  ss << i->first << ": " << p->opts.get(key) << "\n";
                }
	      }
	    }
	    break;
	}
	rdata.append(ss.str());
	ss.str("");
      }
    }
    r = 0;
  } else if (prefix == "osd pool get-quota") {
    string pool_name;
    cmd_getval(cmdmap, "pool", pool_name);

    int64_t poolid = osdmap.lookup_pg_pool_name(pool_name);
    if (poolid < 0) {
      ceph_assert(poolid == -ENOENT);
      ss << "unrecognized pool '" << pool_name << "'";
      r = -ENOENT;
      goto reply;
    }
    const pg_pool_t *p = osdmap.get_pg_pool(poolid);
    const pool_stat_t* pstat = mon.mgrstatmon()->get_pool_stat(poolid);
    if (!pstat) {
      ss << "no stats for pool '" << pool_name << "'";
      r = -ENOENT;
      goto reply;
    }
    const object_stat_sum_t& sum = pstat->stats.sum;
    if (f) {
      f->open_object_section("pool_quotas");
      f->dump_string("pool_name", pool_name);
      f->dump_unsigned("pool_id", poolid);
      f->dump_unsigned("quota_max_objects", p->quota_max_objects);
      f->dump_int("current_num_objects", sum.num_objects);
      f->dump_unsigned("quota_max_bytes", p->quota_max_bytes);
      f->dump_int("current_num_bytes", sum.num_bytes);
      f->close_section();
      f->flush(rdata);
    } else {
      stringstream rs;
      rs << "quotas for pool '" << pool_name << "':\n"
         << "  max objects: ";
      if (p->quota_max_objects == 0)
        rs << "N/A";
      else {
        rs << si_u_t(p->quota_max_objects) << " objects";
        rs << "  (current num objects: " << sum.num_objects << " objects)";
      }
      rs << "\n"
         << "  max bytes  : ";
      if (p->quota_max_bytes == 0)
        rs << "N/A";
      else {
        rs << byte_u_t(p->quota_max_bytes);
        rs << "  (current num bytes: " << sum.num_bytes << " bytes)";
      }
      rdata.append(rs.str());
    }
    rdata.append("\n");
    r = 0;
  } else if (prefix == "osd crush rule list" ||
	     prefix == "osd crush rule ls") {
    if (f) {
      f->open_array_section("rules");
      osdmap.crush->list_rules(f.get());
      f->close_section();
      f->flush(rdata);
    } else {
      ostringstream ss;
      osdmap.crush->list_rules(&ss);
      rdata.append(ss.str());
    }
  } else if (prefix == "osd crush rule ls-by-class") {
    string class_name;
    cmd_getval(cmdmap, "class", class_name);
    if (class_name.empty()) {
      ss << "no class specified";
      r = -EINVAL;
      goto reply;
    }
    set<int> rules;
    r = osdmap.crush->get_rules_by_class(class_name, &rules);
    if (r < 0) {
      ss << "failed to get rules by class '" << class_name << "'";
      goto reply;
    }
    if (f) {
      f->open_array_section("rules");
      for (auto &rule: rules) {
        f->dump_string("name", osdmap.crush->get_rule_name(rule));
      }
      f->close_section();
      f->flush(rdata);
    } else {
      ostringstream rs;
      for (auto &rule: rules) {
        rs << osdmap.crush->get_rule_name(rule) << "\n";
      }
      rdata.append(rs.str());
    }
  } else if (prefix == "osd crush rule dump") {
    string name;
    cmd_getval(cmdmap, "name", name);
    string format;
    cmd_getval(cmdmap, "format", format);
    boost::scoped_ptr<Formatter> f(Formatter::create(format, "json-pretty", "json-pretty"));
    if (name == "") {
      f->open_array_section("rules");
      osdmap.crush->dump_rules(f.get());
      f->close_section();
    } else {
      int ruleno = osdmap.crush->get_rule_id(name);
      if (ruleno < 0) {
	ss << "unknown crush rule '" << name << "'";
	r = ruleno;
	goto reply;
      }
      osdmap.crush->dump_rule(ruleno, f.get());
    }
    ostringstream rs;
    f->flush(rs);
    rs << "\n";
    rdata.append(rs.str());
  } else if (prefix == "osd crush dump") {
    string format;
    cmd_getval(cmdmap, "format", format);
    boost::scoped_ptr<Formatter> f(Formatter::create(format, "json-pretty", "json-pretty"));
    f->open_object_section("crush_map");
    osdmap.crush->dump(f.get());
    f->close_section();
    ostringstream rs;
    f->flush(rs);
    rs << "\n";
    rdata.append(rs.str());
  } else if (prefix == "osd crush show-tunables") {
    string format;
    cmd_getval(cmdmap, "format", format);
    boost::scoped_ptr<Formatter> f(Formatter::create(format, "json-pretty", "json-pretty"));
    f->open_object_section("crush_map_tunables");
    osdmap.crush->dump_tunables(f.get());
    f->close_section();
    ostringstream rs;
    f->flush(rs);
    rs << "\n";
    rdata.append(rs.str());
  } else if (prefix == "osd crush tree") {
    bool show_shadow = false;
    if (!cmd_getval_compat_cephbool(cmdmap, "show_shadow", show_shadow)) {
      std::string shadow;
      if (cmd_getval(cmdmap, "shadow", shadow) &&
	  shadow == "--show-shadow") {
	show_shadow = true;
      }
    }
    boost::scoped_ptr<Formatter> f(Formatter::create(format));
    if (f) {
      f->open_object_section("crush_tree");
      osdmap.crush->dump_tree(nullptr,
                              f.get(),
                              osdmap.get_pool_names(),
                              show_shadow);
      f->close_section();
      f->flush(rdata);
    } else {
      ostringstream ss;
      osdmap.crush->dump_tree(&ss,
                              nullptr,
                              osdmap.get_pool_names(),
                              show_shadow);
      rdata.append(ss.str());
    }
  } else if (prefix == "osd crush ls") {
    string name;
    if (!cmd_getval(cmdmap, "node", name)) {
      ss << "no node specified";
      r = -EINVAL;
      goto reply;
    }
    if (!osdmap.crush->name_exists(name)) {
      ss << "node '" << name << "' does not exist";
      r = -ENOENT;
      goto reply;
    }
    int id = osdmap.crush->get_item_id(name);
    list<int> result;
    if (id >= 0) {
      result.push_back(id);
    } else {
      int num = osdmap.crush->get_bucket_size(id);
      for (int i = 0; i < num; ++i) {
	result.push_back(osdmap.crush->get_bucket_item(id, i));
      }
    }
    if (f) {
      f->open_array_section("items");
      for (auto i : result) {
	f->dump_string("item", osdmap.crush->get_item_name(i));
      }
      f->close_section();
      f->flush(rdata);
    } else {
      ostringstream ss;
      for (auto i : result) {
	ss << osdmap.crush->get_item_name(i) << "\n";
      }
      rdata.append(ss.str());
    }
    r = 0;
  } else if (prefix == "osd crush class ls") {
    boost::scoped_ptr<Formatter> f(Formatter::create(format, "json-pretty", "json-pretty"));
    f->open_array_section("crush_classes");
    for (auto i : osdmap.crush->class_name)
      f->dump_string("class", i.second);
    f->close_section();
    f->flush(rdata);
  } else if (prefix == "osd crush class ls-osd") {
    string name;
    cmd_getval(cmdmap, "class", name);
    set<int> osds;
    osdmap.crush->get_devices_by_class(name, &osds);
    if (f) {
      f->open_array_section("osds");
      for (auto &osd: osds)
        f->dump_int("osd", osd);
      f->close_section();
      f->flush(rdata);
    } else {
      bool first = true;
      for (auto &osd : osds) {
        if (!first)
          ds << "\n";
        first = false;
        ds << osd;
      }
      rdata.append(ds);
    }
  } else if (prefix == "osd crush get-device-class") {
    vector<string> idvec;
    cmd_getval(cmdmap, "ids", idvec);
    map<int, string> class_by_osd;
    for (auto& id : idvec) {
      ostringstream ts;
      long osd = parse_osd_id(id.c_str(), &ts);
      if (osd < 0) {
        ss << "unable to parse osd id:'" << id << "'";
        r = -EINVAL;
        goto reply;
      }
      auto device_class = osdmap.crush->get_item_class(osd);
      if (device_class)
        class_by_osd[osd] = device_class;
      else
        class_by_osd[osd] = ""; // no class
    }
    if (f) {
      f->open_array_section("osd_device_classes");
      for (auto& i : class_by_osd) {
        f->open_object_section("osd_device_class");
        f->dump_int("osd", i.first);
        f->dump_string("device_class", i.second);
        f->close_section();
      }
      f->close_section();
      f->flush(rdata);
    } else {
      if (class_by_osd.size() == 1) {
        // for single input, make a clean output
        ds << class_by_osd.begin()->second;
      } else {
        // note that we do not group osds by class here
        for (auto it = class_by_osd.begin();
             it != class_by_osd.end();
             it++) {
          ds << "osd." << it->first << ' ' << it->second;
          if (next(it) != class_by_osd.end())
            ds << '\n';
        }
      }
      rdata.append(ds);
    }
  } else if (prefix == "osd erasure-code-profile ls") {
    const auto &profiles = osdmap.get_erasure_code_profiles();
    if (f)
      f->open_array_section("erasure-code-profiles");
    for (auto i = profiles.begin(); i != profiles.end(); ++i) {
      if (f)
        f->dump_string("profile", i->first.c_str());
      else
	rdata.append(i->first + "\n");
    }
    if (f) {
      f->close_section();
      ostringstream rs;
      f->flush(rs);
      rs << "\n";
      rdata.append(rs.str());
    }
  } else if (prefix == "osd crush weight-set ls") {
    boost::scoped_ptr<Formatter> f(Formatter::create(format));
    if (f) {
      f->open_array_section("weight_sets");
      if (osdmap.crush->have_choose_args(CrushWrapper::DEFAULT_CHOOSE_ARGS)) {
	f->dump_string("pool", "(compat)");
      }
      for (auto& i : osdmap.crush->choose_args) {
	if (i.first >= 0) {
	  f->dump_string("pool", osdmap.get_pool_name(i.first));
	}
      }
      f->close_section();
      f->flush(rdata);
    } else {
      ostringstream rs;
      if (osdmap.crush->have_choose_args(CrushWrapper::DEFAULT_CHOOSE_ARGS)) {
	rs << "(compat)\n";
      }
      for (auto& i : osdmap.crush->choose_args) {
	if (i.first >= 0) {
	  rs << osdmap.get_pool_name(i.first) << "\n";
	}
      }
      rdata.append(rs.str());
    }
  } else if (prefix == "osd crush weight-set dump") {
    boost::scoped_ptr<Formatter> f(Formatter::create(format, "json-pretty",
						     "json-pretty"));
    osdmap.crush->dump_choose_args(f.get());
    f->flush(rdata);
  } else if (prefix == "osd erasure-code-profile get") {
    string name;
    cmd_getval(cmdmap, "name", name);
    if (!osdmap.has_erasure_code_profile(name)) {
      ss << "unknown erasure code profile '" << name << "'";
      r = -ENOENT;
      goto reply;
    }
    const map<string,string> &profile = osdmap.get_erasure_code_profile(name);
    if (f)
      f->open_object_section("profile");
    for (map<string,string>::const_iterator i = profile.begin();
	 i != profile.end();
	 ++i) {
      if (f)
        f->dump_string(i->first.c_str(), i->second.c_str());
      else
	rdata.append(i->first + "=" + i->second + "\n");
    }
    if (f) {
      f->close_section();
      ostringstream rs;
      f->flush(rs);
      rs << "\n";
      rdata.append(rs.str());
    }
  } else if (prefix == "osd pool application get") {
    boost::scoped_ptr<Formatter> f(Formatter::create(format, "json-pretty",
                                                     "json-pretty"));
    string pool_name;
    cmd_getval(cmdmap, "pool", pool_name);
    string app;
    cmd_getval(cmdmap, "app", app);
    string key;
    cmd_getval(cmdmap, "key", key);

    if (pool_name.empty()) {
      // all
      f->open_object_section("pools");
      for (const auto &pool : osdmap.pools) {
        std::string name("<unknown>");
        const auto &pni = osdmap.pool_name.find(pool.first);
        if (pni != osdmap.pool_name.end())
          name = pni->second;
        f->open_object_section(name.c_str());
        for (auto &app_pair : pool.second.application_metadata) {
          f->open_object_section(app_pair.first.c_str());
          for (auto &kv_pair : app_pair.second) {
            f->dump_string(kv_pair.first.c_str(), kv_pair.second);
          }
          f->close_section();
        }
        f->close_section(); // name
      }
      f->close_section(); // pools
      f->flush(rdata);
    } else {
      int64_t pool = osdmap.lookup_pg_pool_name(pool_name.c_str());
      if (pool < 0) {
        ss << "unrecognized pool '" << pool_name << "'";
        r = -ENOENT;
        goto reply;
      }
      auto p = osdmap.get_pg_pool(pool);
      // filter by pool
      if (app.empty()) {
        f->open_object_section(pool_name.c_str());
        for (auto &app_pair : p->application_metadata) {
          f->open_object_section(app_pair.first.c_str());
          for (auto &kv_pair : app_pair.second) {
            f->dump_string(kv_pair.first.c_str(), kv_pair.second);
          }
          f->close_section(); // application
        }
        f->close_section(); // pool_name
        f->flush(rdata);
        goto reply;
      }

      auto app_it = p->application_metadata.find(app);
      if (app_it == p->application_metadata.end()) {
        ss << "pool '" << pool_name << "' has no application '" << app << "'";
        r = -ENOENT;
        goto reply;
      }
      // filter by pool + app
      if (key.empty()) {
        f->open_object_section(app_it->first.c_str());
        for (auto &kv_pair : app_it->second) {
          f->dump_string(kv_pair.first.c_str(), kv_pair.second);
        }
        f->close_section(); // application
        f->flush(rdata);
        goto reply;
      }
      // filter by pool + app + key
      auto key_it = app_it->second.find(key);
      if (key_it == app_it->second.end()) {
        ss << "application '" << app << "' on pool '" << pool_name
           << "' does not have key '" << key << "'";
        r = -ENOENT;
        goto reply;
      }
      ss << key_it->second << "\n";
      rdata.append(ss.str());
      ss.str("");
    }
  } else if (prefix == "osd get-require-min-compat-client") {
    ss << osdmap.require_min_compat_client << std::endl;
    rdata.append(ss.str());
    ss.str("");
    goto reply;
  } else if (prefix == "osd pool application enable" ||
             prefix == "osd pool application disable" ||
             prefix == "osd pool application set" ||
             prefix == "osd pool application rm") {
    bool changed = false;
    r = preprocess_command_pool_application(prefix, cmdmap, ss, &changed);
    if (r != 0) {
      // Error, reply.
      goto reply;
    } else if (changed) {
      // Valid mutation, proceed to prepare phase
      return false;
    } else {
      // Idempotent case, reply
      goto reply;
    }
  } else {
    // try prepare update
    return false;
  }

 reply:
  string rs;
  getline(ss, rs);
  mon.reply_command(op, r, rs, rdata, get_last_committed());
  return true;
}

void OSDMonitor::set_pool_flags(int64_t pool_id, uint64_t flags)
{
  pg_pool_t *pool = pending_inc.get_new_pool(pool_id,
    osdmap.get_pg_pool(pool_id));
  ceph_assert(pool);
  pool->set_flag(flags);
}

void OSDMonitor::clear_pool_flags(int64_t pool_id, uint64_t flags)
{
  pg_pool_t *pool = pending_inc.get_new_pool(pool_id,
    osdmap.get_pg_pool(pool_id));
  ceph_assert(pool);
  pool->unset_flag(flags);
}

string OSDMonitor::make_purged_snap_epoch_key(epoch_t epoch)
{
  char k[80];
  snprintf(k, sizeof(k), "purged_epoch_%08lx", (unsigned long)epoch);
  return k;
}

string OSDMonitor::make_purged_snap_key(int64_t pool, snapid_t snap)
{
  char k[80];
  snprintf(k, sizeof(k), "purged_snap_%llu_%016llx",
	   (unsigned long long)pool, (unsigned long long)snap);
  return k;
}

string OSDMonitor::make_purged_snap_key_value(
  int64_t pool, snapid_t snap, snapid_t num,
  epoch_t epoch, bufferlist *v)
{
  // encode the *last* epoch in the key so that we can use forward
  // iteration only to search for an epoch in an interval.
  encode(snap, *v);
  encode(snap + num, *v);
  encode(epoch, *v);
  return make_purged_snap_key(pool, snap + num - 1);
}


int OSDMonitor::lookup_purged_snap(
  int64_t pool, snapid_t snap,
  snapid_t *begin, snapid_t *end)
{
  string k = make_purged_snap_key(pool, snap);
  auto it = mon.store->get_iterator(OSD_SNAP_PREFIX);
  it->lower_bound(k);
  if (!it->valid()) {
    dout(20) << __func__
	     << " pool " << pool << " snap " << snap
	     << " - key '" << k << "' not found" << dendl;
    return -ENOENT;
  }
  if (it->key().find("purged_snap_") != 0) {
    dout(20) << __func__
	     << " pool " << pool << " snap " << snap
	     << " - key '" << k << "' got '" << it->key()
	     << "', wrong prefix" << dendl;
    return -ENOENT;
  }
  string gotk = it->key();
  const char *format = "purged_snap_%llu_";
  long long int keypool;
  int n = sscanf(gotk.c_str(), format, &keypool);
  if (n != 1) {
    derr << __func__ << " invalid k '" << gotk << "'" << dendl;
    return -ENOENT;
  }
  if (pool != keypool) {
    dout(20) << __func__
	     << " pool " << pool << " snap " << snap
	     << " - key '" << k << "' got '" << gotk
	     << "', wrong pool " << keypool
	     << dendl;
    return -ENOENT;
  }
  bufferlist v = it->value();
  auto p = v.cbegin();
  decode(*begin, p);
  decode(*end, p);
  if (snap < *begin || snap >= *end) {
    dout(20) << __func__
	     << " pool " << pool << " snap " << snap
	     << " - found [" << *begin << "," << *end << "), no overlap"
	     << dendl;
    return -ENOENT;
  }
  return 0;
}

void OSDMonitor::insert_purged_snap_update(
  int64_t pool,
  snapid_t start, snapid_t end,
  epoch_t epoch,
  MonitorDBStore::TransactionRef t)
{
  snapid_t before_begin, before_end;
  snapid_t after_begin, after_end;
  int b = lookup_purged_snap(pool, start - 1,
			     &before_begin, &before_end);
  int a = lookup_purged_snap(pool, end,
			     &after_begin, &after_end);
  if (!b && !a) {
    dout(10) << __func__
	     << " [" << start << "," << end << ") - joins ["
	     << before_begin << "," << before_end << ") and ["
	     << after_begin << "," << after_end << ")" << dendl;
    // erase only the begin record; we'll overwrite the end one.
    t->erase(OSD_SNAP_PREFIX, make_purged_snap_key(pool, before_end - 1));
    bufferlist v;
    string k = make_purged_snap_key_value(pool,
					  before_begin, after_end - before_begin,
					  pending_inc.epoch, &v);
    t->put(OSD_SNAP_PREFIX, k, v);
  } else if (!b) {
    dout(10) << __func__
	     << " [" << start << "," << end << ") - join with earlier ["
	     << before_begin << "," << before_end << ")" << dendl;
    t->erase(OSD_SNAP_PREFIX, make_purged_snap_key(pool, before_end - 1));
    bufferlist v;
    string k = make_purged_snap_key_value(pool,
					  before_begin, end - before_begin,
					  pending_inc.epoch, &v);
    t->put(OSD_SNAP_PREFIX, k, v);
  } else if (!a) {
    dout(10) << __func__
	     << " [" << start << "," << end << ") - join with later ["
	     << after_begin << "," << after_end << ")" << dendl;
    // overwrite after record
    bufferlist v;
    string k = make_purged_snap_key_value(pool,
					  start, after_end - start,
					  pending_inc.epoch, &v);
    t->put(OSD_SNAP_PREFIX, k, v);
  } else {
    dout(10) << __func__
	     << " [" << start << "," << end << ") - new"
	     << dendl;
    bufferlist v;
    string k = make_purged_snap_key_value(pool,
					  start, end - start,
					  pending_inc.epoch, &v);
    t->put(OSD_SNAP_PREFIX, k, v);
  }
}

bool OSDMonitor::try_prune_purged_snaps()
{
  if (!mon.mgrstatmon()->is_readable()) {
    return false;
  }
  if (!pending_inc.new_purged_snaps.empty()) {
    return false;  // we already pruned for this epoch
  }

  unsigned max_prune = cct->_conf.get_val<uint64_t>(
    "mon_max_snap_prune_per_epoch");
  if (!max_prune) {
    max_prune = 100000;
  }
  dout(10) << __func__ << " max_prune " << max_prune << dendl;

  unsigned actually_pruned = 0;
  auto& purged_snaps = mon.mgrstatmon()->get_digest().purged_snaps;
  for (auto& p : osdmap.get_pools()) {
    auto q = purged_snaps.find(p.first);
    if (q == purged_snaps.end()) {
      continue;
    }
    auto& purged = q->second;
    if (purged.empty()) {
      dout(20) << __func__ << " " << p.first << " nothing purged" << dendl;
      continue;
    }
    dout(20) << __func__ << " pool " << p.first << " purged " << purged << dendl;
    snap_interval_set_t to_prune;
    unsigned maybe_pruned = actually_pruned;
    for (auto i = purged.begin(); i != purged.end(); ++i) {
      snapid_t begin = i.get_start();
      auto end = i.get_start() + i.get_len();
      snapid_t pbegin = 0, pend = 0;
      int r = lookup_purged_snap(p.first, begin, &pbegin, &pend);
      if (r == 0) {
	// already purged.
	// be a bit aggressive about backing off here, because the mon may
	// do a lot of work going through this set, and if we know the
	// purged set from the OSDs is at least *partly* stale we may as
	// well wait for it to be fresh.
	dout(20) << __func__ << "  we've already purged " << pbegin
		 << "~" << (pend - pbegin) << dendl;
	break;  // next pool
      }
      if (pbegin && pbegin > begin && pbegin < end) {
	// the tail of [begin,end) is purged; shorten the range
	end = pbegin;
      }
      to_prune.insert(begin, end - begin);
      maybe_pruned += end - begin;
      if (maybe_pruned >= max_prune) {
	break;
      }
    }
    if (!to_prune.empty()) {
      // PGs may still be reporting things as purged that we have already
      // pruned from removed_snaps_queue.
      snap_interval_set_t actual;
      auto r = osdmap.removed_snaps_queue.find(p.first);
      if (r != osdmap.removed_snaps_queue.end()) {
	actual.intersection_of(to_prune, r->second);
      }
      actually_pruned += actual.size();
      dout(10) << __func__ << " pool " << p.first << " reports pruned " << to_prune
	       << ", actual pruned " << actual << dendl;
      if (!actual.empty()) {
	pending_inc.new_purged_snaps[p.first].swap(actual);
      }
    }
    if (actually_pruned >= max_prune) {
      break;
    }
  }
  dout(10) << __func__ << " actually pruned " << actually_pruned << dendl;
  return !!actually_pruned;
}

bool OSDMonitor::update_pools_status()
{
  if (!mon.mgrstatmon()->is_readable())
    return false;

  bool ret = false;

  auto& pools = osdmap.get_pools();
  for (auto it = pools.begin(); it != pools.end(); ++it) {
    const pool_stat_t *pstat = mon.mgrstatmon()->get_pool_stat(it->first);
    if (!pstat)
      continue;
    const object_stat_sum_t& sum = pstat->stats.sum;
    const pg_pool_t &pool = it->second;
    const string& pool_name = osdmap.get_pool_name(it->first);

    bool pool_is_full =
      (pool.quota_max_bytes > 0 && (uint64_t)sum.num_bytes >= pool.quota_max_bytes) ||
      (pool.quota_max_objects > 0 && (uint64_t)sum.num_objects >= pool.quota_max_objects);

    if (pool.has_flag(pg_pool_t::FLAG_FULL_QUOTA)) {
      if (pool_is_full)
        continue;

      mon.clog->info() << "pool '" << pool_name
                       << "' no longer out of quota; removing NO_QUOTA flag";
      // below we cancel FLAG_FULL too, we'll set it again in
      // OSDMonitor::encode_pending if it still fails the osd-full checking.
      clear_pool_flags(it->first,
                       pg_pool_t::FLAG_FULL_QUOTA | pg_pool_t::FLAG_FULL);
      ret = true;
    } else {
      if (!pool_is_full)
	continue;

      if (pool.quota_max_bytes > 0 &&
          (uint64_t)sum.num_bytes >= pool.quota_max_bytes) {
        mon.clog->warn() << "pool '" << pool_name << "' is full"
                         << " (reached quota's max_bytes: "
                         << byte_u_t(pool.quota_max_bytes) << ")";
      }
      if (pool.quota_max_objects > 0 &&
		 (uint64_t)sum.num_objects >= pool.quota_max_objects) {
        mon.clog->warn() << "pool '" << pool_name << "' is full"
                         << " (reached quota's max_objects: "
                         << pool.quota_max_objects << ")";
      }
      // set both FLAG_FULL_QUOTA and FLAG_FULL
      // note that below we try to cancel FLAG_BACKFILLFULL/NEARFULL too
      // since FLAG_FULL should always take precedence
      set_pool_flags(it->first,
                     pg_pool_t::FLAG_FULL_QUOTA | pg_pool_t::FLAG_FULL);
      clear_pool_flags(it->first,
                       pg_pool_t::FLAG_NEARFULL |
                       pg_pool_t::FLAG_BACKFILLFULL);
      ret = true;
    }
  }
  return ret;
}

int OSDMonitor::prepare_new_pool(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MPoolOp>();
  dout(10) << "prepare_new_pool from " << m->get_connection() << dendl;
  MonSession *session = op->get_session();
  if (!session)
    return -EPERM;
  string erasure_code_profile;
  stringstream ss;
  string rule_name;
  bool bulk = false;
  int ret = 0;
  ret = prepare_new_pool(m->name, m->crush_rule, rule_name,
			 0, 0, 0, 0, 0, 0, 0.0,
			 erasure_code_profile,
			 pg_pool_t::TYPE_REPLICATED, 0, FAST_READ_OFF, {}, bulk,
			 cct->_conf.get_val<bool>("osd_pool_default_crimson"),
			 &ss);

  if (ret < 0) {
    dout(10) << __func__ << " got " << ret << " " << ss.str() << dendl;
  }
  return ret;
}

int OSDMonitor::crush_rename_bucket(const string& srcname,
				    const string& dstname,
				    ostream *ss)
{
  int ret;
  //
  // Avoid creating a pending crush if it does not already exists and
  // the rename would fail.
  //
  if (!_have_pending_crush()) {
    ret = _get_stable_crush().can_rename_bucket(srcname,
						dstname,
						ss);
    if (ret)
      return ret;
  }

  CrushWrapper newcrush = _get_pending_crush();

  ret = newcrush.rename_bucket(srcname,
			       dstname,
			       ss);
  if (ret)
    return ret;

  pending_inc.crush.clear();
  newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
  *ss << "renamed bucket " << srcname << " into " << dstname;	
  return 0;
}

void OSDMonitor::check_legacy_ec_plugin(const string& plugin, const string& profile) const
{
  string replacement = "";

  if (plugin == "jerasure_generic" || 
      plugin == "jerasure_sse3" ||
      plugin == "jerasure_sse4" ||
      plugin == "jerasure_neon") {
    replacement = "jerasure";
  } else if (plugin == "shec_generic" ||
	     plugin == "shec_sse3" ||
	     plugin == "shec_sse4" ||
             plugin == "shec_neon") {
    replacement = "shec";
  }

  if (replacement != "") {
    dout(0) << "WARNING: erasure coding profile " << profile << " uses plugin "
	    << plugin << " that has been deprecated. Please use " 
	    << replacement << " instead." << dendl;
  }
}

int OSDMonitor::normalize_profile(const string& profilename,
				  ErasureCodeProfile &profile,
				  bool force,
				  ostream *ss)
{
  ErasureCodeInterfaceRef erasure_code;
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  ErasureCodeProfile::const_iterator plugin = profile.find("plugin");
  check_legacy_ec_plugin(plugin->second, profilename);
  int err = instance.factory(plugin->second,
			     g_conf().get_val<std::string>("erasure_code_dir"),
			     profile, &erasure_code, ss);
  if (err) {
    return err;
  }

  err = erasure_code->init(profile, ss);
  if (err) {
    return err;
  }

  auto it = profile.find("stripe_unit");
  if (it != profile.end()) {
    string err_str;
    uint32_t stripe_unit = strict_iecstrtoll(it->second, &err_str);
    if (!err_str.empty()) {
      *ss << "could not parse stripe_unit '" << it->second
	  << "': " << err_str << std::endl;
      return -EINVAL;
    }
    uint32_t data_chunks = erasure_code->get_data_chunk_count();
    uint32_t chunk_size = erasure_code->get_chunk_size(stripe_unit * data_chunks);
    if (chunk_size != stripe_unit) {
      *ss << "stripe_unit " << stripe_unit << " does not match ec profile "
	  << "alignment. Would be padded to " << chunk_size
	  << std::endl;
      return -EINVAL;
    }
    if ((stripe_unit % 4096) != 0 && !force) {
      *ss << "stripe_unit should be a multiple of 4096 bytes for best performance."
	  << "use --force to override this check" << std::endl;
      return -EINVAL;
    }
  }
  return 0;
}

int OSDMonitor::crush_rule_create_erasure(const string &name,
					     const string &profile,
					     int *rule,
					     ostream *ss)
{
  int ruleid = osdmap.crush->get_rule_id(name);
  if (ruleid != -ENOENT) {
    *rule = ruleid;
    return -EEXIST;
  }

  CrushWrapper newcrush = _get_pending_crush();

  ruleid = newcrush.get_rule_id(name);
  if (ruleid != -ENOENT) {
    *rule = ruleid;
    return -EALREADY;
  } else {
    ErasureCodeInterfaceRef erasure_code;
    int err = get_erasure_code(profile, &erasure_code, ss);
    if (err) {
      *ss << "failed to load plugin using profile " << profile << std::endl;
      return err;
    }

    err = erasure_code->create_rule(name, newcrush, ss);
    erasure_code.reset();
    if (err < 0)
      return err;
    *rule = err;
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
    return 0;
  }
}

int OSDMonitor::get_erasure_code(const string &erasure_code_profile,
				 ErasureCodeInterfaceRef *erasure_code,
				 ostream *ss) const
{
  if (pending_inc.has_erasure_code_profile(erasure_code_profile))
    return -EAGAIN;
  ErasureCodeProfile profile =
    osdmap.get_erasure_code_profile(erasure_code_profile);
  ErasureCodeProfile::const_iterator plugin =
    profile.find("plugin");
  if (plugin == profile.end()) {
    *ss << "cannot determine the erasure code plugin"
	<< " because there is no 'plugin' entry in the erasure_code_profile "
	<< profile << std::endl;
    return -EINVAL;
  }
  check_legacy_ec_plugin(plugin->second, erasure_code_profile);
  auto& instance = ErasureCodePluginRegistry::instance();
  return instance.factory(plugin->second,
			  g_conf().get_val<std::string>("erasure_code_dir"),
			  profile, erasure_code, ss);
}

int OSDMonitor::check_cluster_features(uint64_t features,
				       stringstream &ss)
{
  stringstream unsupported_ss;
  int unsupported_count = 0;
  if ((mon.get_quorum_con_features() & features) != features) {
    unsupported_ss << "the monitor cluster";
    ++unsupported_count;
  }

  set<int32_t> up_osds;
  osdmap.get_up_osds(up_osds);
  for (set<int32_t>::iterator it = up_osds.begin();
       it != up_osds.end(); ++it) {
    const osd_xinfo_t &xi = osdmap.get_xinfo(*it);
    if ((xi.features & features) != features) {
      if (unsupported_count > 0)
	unsupported_ss << ", ";
      unsupported_ss << "osd." << *it;
      unsupported_count ++;
    }
  }

  if (unsupported_count > 0) {
    ss << "features " << features << " unsupported by: "
       << unsupported_ss.str();
    return -ENOTSUP;
  }

  // check pending osd state, too!
  for (map<int32_t,osd_xinfo_t>::const_iterator p =
	 pending_inc.new_xinfo.begin();
       p != pending_inc.new_xinfo.end(); ++p) {
    const osd_xinfo_t &xi = p->second;
    if ((xi.features & features) != features) {
      dout(10) << __func__ << " pending osd." << p->first
	       << " features are insufficient; retry" << dendl;
      return -EAGAIN;
    }
  }

  return 0;
}

bool OSDMonitor::validate_crush_against_features(const CrushWrapper *newcrush,
                                                 stringstream& ss)
{
  OSDMap::Incremental new_pending = pending_inc;
  encode(*newcrush, new_pending.crush, mon.get_quorum_con_features());
  OSDMap newmap;
  newmap.deepish_copy_from(osdmap);
  newmap.apply_incremental(new_pending);

  // client compat
  if (newmap.require_min_compat_client != ceph_release_t::unknown) {
    auto mv = newmap.get_min_compat_client();
    if (mv > newmap.require_min_compat_client) {
      ss << "new crush map requires client version " << mv
	 << " but require_min_compat_client is "
	 << newmap.require_min_compat_client;
      return false;
    }
  }

  // osd compat
  uint64_t features =
    newmap.get_features(CEPH_ENTITY_TYPE_MON, NULL) |
    newmap.get_features(CEPH_ENTITY_TYPE_OSD, NULL);
  stringstream features_ss;
  int r = check_cluster_features(features, features_ss);
  if (r) {
    ss << "Could not change CRUSH: " << features_ss.str();
    return false;
  }

  return true;
}

bool OSDMonitor::erasure_code_profile_in_use(
  const mempool::osdmap::map<int64_t, pg_pool_t> &pools,
  const string &profile,
  ostream *ss)
{
  bool found = false;
  for (map<int64_t, pg_pool_t>::const_iterator p = pools.begin();
       p != pools.end();
       ++p) {
    if (p->second.erasure_code_profile == profile && p->second.is_erasure()) {
      *ss << osdmap.pool_name[p->first] << " ";
      found = true;
    }
  }
  if (found) {
    *ss << "pool(s) are using the erasure code profile '" << profile << "'";
  }
  return found;
}

int OSDMonitor::parse_erasure_code_profile(const vector<string> &erasure_code_profile,
					   map<string,string> *erasure_code_profile_map,
					   ostream *ss)
{
  int r = g_conf().with_val<string>("osd_pool_default_erasure_code_profile",
				   get_json_str_map,
				   *ss,
				   erasure_code_profile_map,
				   true);
  if (r)
    return r;
  ceph_assert((*erasure_code_profile_map).count("plugin"));
  string default_plugin = (*erasure_code_profile_map)["plugin"];
  map<string,string> user_map;
  for (vector<string>::const_iterator i = erasure_code_profile.begin();
       i != erasure_code_profile.end();
       ++i) {
    size_t equal = i->find('=');
    if (equal == string::npos) {
      user_map[*i] = string();
      (*erasure_code_profile_map)[*i] = string();
    } else {
      const string key = i->substr(0, equal);
      equal++;
      const string value = i->substr(equal);
      if (key.find("ruleset-") == 0) {
	*ss << "property '" << key << "' is no longer supported; try "
	    << "'crush-" << key.substr(8) << "' instead";
	return -EINVAL;
      }
      user_map[key] = value;
      (*erasure_code_profile_map)[key] = value;
    }
  }

  if (user_map.count("plugin") && user_map["plugin"] != default_plugin)
    (*erasure_code_profile_map) = user_map;

  return 0;
}

int OSDMonitor::prepare_pool_size(const unsigned pool_type,
				  const string &erasure_code_profile,
                                  uint8_t repl_size,
				  unsigned *size, unsigned *min_size,
				  ostream *ss)
{
  int err = 0;
  bool set_min_size = false;
  switch (pool_type) {
  case pg_pool_t::TYPE_REPLICATED:
    if (osdmap.stretch_mode_enabled) {
      if (repl_size == 0)
	repl_size = g_conf().get_val<uint64_t>("mon_stretch_pool_size");
      if (repl_size != g_conf().get_val<uint64_t>("mon_stretch_pool_size")) {
	*ss << "prepare_pool_size: we are in stretch mode but size "
	   << repl_size << " does not match!";
	return -EINVAL;
      }
      *min_size = g_conf().get_val<uint64_t>("mon_stretch_pool_min_size");
      set_min_size = true;
    }
    if (repl_size == 0) {
      repl_size = g_conf().get_val<uint64_t>("osd_pool_default_size");
    }
    *size = repl_size;
    if (!set_min_size)
      *min_size = g_conf().get_osd_pool_default_min_size(repl_size);
    break;
  case pg_pool_t::TYPE_ERASURE:
    {
      if (osdmap.stretch_mode_enabled) {
	*ss << "prepare_pool_size: we are in stretch mode; cannot create EC pools!";
	return -EINVAL;
      }
      ErasureCodeInterfaceRef erasure_code;
      err = get_erasure_code(erasure_code_profile, &erasure_code, ss);
      if (err == 0) {
	*size = erasure_code->get_chunk_count();
	*min_size =
	  erasure_code->get_data_chunk_count() +
	  std::min<int>(1, erasure_code->get_coding_chunk_count() - 1);
	assert(*min_size <= *size);
	assert(*min_size >= erasure_code->get_data_chunk_count());
      }
    }
    break;
  default:
    *ss << "prepare_pool_size: " << pool_type << " is not a known pool type";
    err = -EINVAL;
    break;
  }
  return err;
}

int OSDMonitor::prepare_pool_stripe_width(const unsigned pool_type,
					  const string &erasure_code_profile,
					  uint32_t *stripe_width,
					  ostream *ss)
{
  int err = 0;
  switch (pool_type) {
  case pg_pool_t::TYPE_REPLICATED:
    // ignored
    break;
  case pg_pool_t::TYPE_ERASURE:
    {
      ErasureCodeProfile profile =
	osdmap.get_erasure_code_profile(erasure_code_profile);
      ErasureCodeInterfaceRef erasure_code;
      err = get_erasure_code(erasure_code_profile, &erasure_code, ss);
      if (err)
	break;
      uint32_t data_chunks = erasure_code->get_data_chunk_count();
      uint32_t stripe_unit = g_conf().get_val<Option::size_t>("osd_pool_erasure_code_stripe_unit");
      auto it = profile.find("stripe_unit");
      if (it != profile.end()) {
	string err_str;
	stripe_unit = strict_iecstrtoll(it->second, &err_str);
	ceph_assert(err_str.empty());
      }
      *stripe_width = data_chunks *
	erasure_code->get_chunk_size(stripe_unit * data_chunks);
    }
    break;
  default:
    *ss << "prepare_pool_stripe_width: "
       << pool_type << " is not a known pool type";
    err = -EINVAL;
    break;
  }
  return err;
}

int OSDMonitor::get_replicated_stretch_crush_rule()
{
  /* we don't write down the stretch rule anywhere, so
   * we have to guess it. How? Look at all the pools
   * and count up how many times a given rule is used
   * on stretch pools and then return the one with
   * the most users!
   */
  map<int,int> rule_counts;
  for (const auto& pooli : osdmap.pools) {
    const pg_pool_t& p = pooli.second;
    if (p.is_replicated() && p.is_stretch_pool()) {
      if (!rule_counts.count(p.crush_rule)) {
	rule_counts[p.crush_rule] = 1;
      } else {
	++rule_counts[p.crush_rule];
      }
    }
  }

  if (rule_counts.empty()) {
    return -ENOENT;
  }

  int most_used_count = 0;
  int most_used_rule = -1;
  for (auto i : rule_counts) {
    if (i.second > most_used_count) {
      most_used_rule = i.first;
      most_used_count = i.second;
    }
  }
  ceph_assert(most_used_count > 0);
  ceph_assert(most_used_rule >= 0);
  return most_used_rule;
}

int OSDMonitor::prepare_pool_crush_rule(const unsigned pool_type,
					const string &erasure_code_profile,
					const string &rule_name,
					int *crush_rule,
					ostream *ss)
{

  if (*crush_rule < 0) {
    switch (pool_type) {
    case pg_pool_t::TYPE_REPLICATED:
      {
	if (rule_name == "") {
	  if (osdmap.stretch_mode_enabled) {
	    *crush_rule = get_replicated_stretch_crush_rule();
	  } else {
	    // Use default rule
	    *crush_rule = osdmap.crush->get_osd_pool_default_crush_replicated_rule(cct);
	  }
	  if (*crush_rule < 0) {
	    // Errors may happen e.g. if no valid rule is available
	    *ss << "No suitable CRUSH rule exists, check "
                << "'osd pool default crush *' config options";
	    return -ENOENT;
	  }
	} else {
	  return get_crush_rule(rule_name, crush_rule, ss);
	}
      }
      break;
    case pg_pool_t::TYPE_ERASURE:
      {
	int err = crush_rule_create_erasure(rule_name,
					       erasure_code_profile,
					       crush_rule, ss);
	switch (err) {
	case -EALREADY:
	  dout(20) << "prepare_pool_crush_rule: rule "
		   << rule_name << " try again" << dendl;
	  // fall through
	case 0:
	  // need to wait for the crush rule to be proposed before proceeding
	  err = -EAGAIN;
	  break;
	case -EEXIST:
	  err = 0;
	  break;
 	}
	return err;
      }
      break;
    default:
      *ss << "prepare_pool_crush_rule: " << pool_type
	 << " is not a known pool type";
      return -EINVAL;
    }
  } else {
    if (!osdmap.crush->rule_exists(*crush_rule)) {
      *ss << "CRUSH rule " << *crush_rule << " not found";
      return -ENOENT;
    }
  }

  return 0;
}

int OSDMonitor::get_crush_rule(const string &rule_name,
			       int *crush_rule,
			       ostream *ss)
{
  int ret;
  ret = osdmap.crush->get_rule_id(rule_name);
  if (ret != -ENOENT) {
    // found it, use it
    *crush_rule = ret;
  } else {
    CrushWrapper newcrush = _get_pending_crush();

    ret = newcrush.get_rule_id(rule_name);
    if (ret != -ENOENT) {
      // found it, wait for it to be proposed
      dout(20) << __func__ << ": rule " << rule_name
	       << " try again" << dendl;
      return -EAGAIN;
    } else {
      // Cannot find it , return error
      *ss << "specified rule " << rule_name << " doesn't exist";
      return ret;
    }
  }
  return 0;
}

/*
* Get the number of 'in' osds according to the crush_rule,
*/
uint32_t OSDMonitor::get_osd_num_by_crush(int crush_rule)
{
  set<int> out_osds;
  set<int> crush_in_osds;
  set<int> roots;
  CrushWrapper newcrush = _get_pending_crush();
  newcrush.find_takes_by_rule(crush_rule, &roots);
  for (auto root : roots) {
    const char *rootname = newcrush.get_item_name(root);
    set<int> crush_all_osds;
    newcrush.get_leaves(rootname, &crush_all_osds);
    std::set_difference(crush_all_osds.begin(), crush_all_osds.end(),
                        out_osds.begin(), out_osds.end(),
                        std::inserter(crush_in_osds, crush_in_osds.end()));
  }
  return crush_in_osds.size();
}

int OSDMonitor::check_pg_num(int64_t pool,
                             int pg_num,
                             int size,
                             int crush_rule,
                             ostream *ss)
{
  auto max_pgs_per_osd = g_conf().get_val<uint64_t>("mon_max_pg_per_osd");
  uint64_t projected = 0;
  uint32_t osd_num_by_crush = 0;
  set<int64_t> crush_pool_ids;
  if (pool < 0) {
    // a new pool
    projected += pg_num * size;
  }

  osd_num_by_crush = get_osd_num_by_crush(crush_rule);
  osdmap.get_pool_ids_by_rule(crush_rule, &crush_pool_ids);

  for (const auto& [pool_id, pool_info] : osdmap.get_pools()) {
    // Check only for pools affected by crush rule
    if (crush_pool_ids.contains(pool_id)) {
      if (pool_id == pool) {
        // Specified pool, use given pg_num and size values.
        projected += pg_num * size;
      } else {
        // Use pg_num_target for evaluating the projected pg num
        projected += pool_info.get_pg_num_target() * pool_info.get_size();
      }
    }
  }
  // assume min cluster size 3
  osd_num_by_crush = std::max(osd_num_by_crush, 3u);
  auto projected_pgs_per_osd = projected / osd_num_by_crush;

  if (projected_pgs_per_osd > max_pgs_per_osd) {
    if (pool >= 0) {
      *ss << "pool id " << pool;
    }
    *ss << " pg_num " << pg_num
        << " size " << size
        << " for this pool would result in "
        << projected_pgs_per_osd
        << " cumulative PGs per OSD (" << projected
        << " total PG replicas on " << osd_num_by_crush
        << " 'in' root OSDs by crush rule) "
        << "which exceeds the mon_max_pg_per_osd "
        << "value of " << max_pgs_per_osd;
    return -ERANGE;
  }
  return 0;
}

/**
 * @param name The name of the new pool
 * @param crush_rule The crush rule to use. If <0, will use the system default
 * @param crush_rule_name The crush rule to use, if crush_rulset <0
 * @param pg_num The pg_num to use. If set to 0, will use the system default
 * @param pgp_num The pgp_num to use. If set to 0, will use the system default
 * @param pg_num_min min pg_num
 * @param pg_num_max max pg_num
 * @param repl_size Replication factor, or 0 for default
 * @param erasure_code_profile The profile name in OSDMap to be used for erasure code
 * @param pool_type TYPE_ERASURE, or TYPE_REP
 * @param expected_num_objects expected number of objects on the pool
 * @param fast_read fast read type. 
 * @param pg_autoscale_mode autoscale mode, one of on, off, warn
 * @param bool bulk indicates whether pool should be a bulk pool
 * @param bool crimson indicates whether pool is a crimson pool
 * @param ss human readable error message, if any.
 *
 * @return 0 on success, negative errno on failure.
 */
int OSDMonitor::prepare_new_pool(string& name,
				 int crush_rule,
				 const string &crush_rule_name,
                                 unsigned pg_num, unsigned pgp_num,
				 unsigned pg_num_min,
				 unsigned pg_num_max,
                                 const uint64_t repl_size,
				 const uint64_t target_size_bytes,
				 const float target_size_ratio,
				 const string &erasure_code_profile,
                                 const unsigned pool_type,
                                 const uint64_t expected_num_objects,
                                 FastReadType fast_read,
				 string pg_autoscale_mode,
				 bool bulk,
				 bool crimson,
				 ostream *ss)
{
  if (crimson && pg_autoscale_mode.empty()) {
    // default pg_autoscale_mode to off for crimson, we'll error out below if
    // the user tried to actually set pg_autoscale_mode to something other than
    // "off"
    pg_autoscale_mode = "off";
  }

  if (name.length() == 0)
    return -EINVAL;

  if (pg_num == 0) {
    auto pg_num_from_mode =
      [pg_num=g_conf().get_val<uint64_t>("osd_pool_default_pg_num")]
      (const string& mode) {
      return mode == "on" ? 1 : pg_num;
    };
    pg_num = pg_num_from_mode(
      pg_autoscale_mode.empty() ?
      g_conf().get_val<string>("osd_pool_default_pg_autoscale_mode") :
      pg_autoscale_mode);
  }
  if (pgp_num == 0)
    pgp_num = g_conf().get_val<uint64_t>("osd_pool_default_pgp_num");
  if (!pgp_num)
    pgp_num = pg_num;
  if (pg_num > g_conf().get_val<uint64_t>("mon_max_pool_pg_num")) {
    *ss << "'pg_num' must be greater than 0 and less than or equal to "
        << g_conf().get_val<uint64_t>("mon_max_pool_pg_num")
        << " (you may adjust 'mon max pool pg num' for higher values)";
    return -ERANGE;
  }
  if (pgp_num > pg_num) {
    *ss << "'pgp_num' must be greater than 0 and lower or equal than 'pg_num'"
        << ", which in this case is " << pg_num;
    return -ERANGE;
  }

  if (crimson) {
    /* crimson-osd requires that the pool be replicated and that pg_num/pgp_num
     * be static.  User must also have specified set-allow-crimson */
    const auto *suffix = " (--crimson specified or osd_pool_default_crimson set)";
    if (pool_type != pg_pool_t::TYPE_REPLICATED) {
      *ss << "crimson-osd only supports replicated pools" << suffix;
      return -EINVAL;
    } else if (pg_autoscale_mode != "off") {
      *ss << "crimson-osd does not support changing pg_num or pgp_num, "
	  << "pg_autoscale_mode must be set to 'off'" << suffix;
      return -EINVAL;
    } else if (!osdmap.get_allow_crimson()) {
      *ss << "set-allow-crimson must be set to create a pool with the "
	  << "crimson flag" << suffix;
      return -EINVAL;
    }
  }

  if (pool_type == pg_pool_t::TYPE_REPLICATED && fast_read == FAST_READ_ON) {
    *ss << "'fast_read' can only apply to erasure coding pool";
    return -EINVAL;
  }
  int r;
  r = prepare_pool_crush_rule(pool_type, erasure_code_profile,
				 crush_rule_name, &crush_rule, ss);
  if (r) {
    dout(10) << "prepare_pool_crush_rule returns " << r << dendl;
    return r;
  }
  unsigned size, min_size;
  r = prepare_pool_size(pool_type, erasure_code_profile, repl_size,
                        &size, &min_size, ss);
  if (r) {
    dout(10) << "prepare_pool_size returns " << r << dendl;
    return r;
  }
  if (g_conf()->mon_osd_crush_smoke_test) {
    CrushWrapper newcrush = _get_pending_crush();
    ostringstream err;
    CrushTester tester(newcrush, err);
    tester.set_min_x(0);
    tester.set_max_x(50);
    tester.set_rule(crush_rule);
    tester.set_num_rep(size);
    auto start = ceph::coarse_mono_clock::now();
    r = tester.test_with_fork(cct, g_conf()->mon_lease);
    dout(10) << __func__ << " crush test_with_fork tester created " << dendl;
    auto duration = ceph::coarse_mono_clock::now() - start;
    if (r < 0) {
      dout(10) << "tester.test_with_fork returns " << r
	       << ": " << err.str() << dendl;
      *ss << "crush test failed with " << r << ": " << err.str();
      return r;
    }
    dout(10) << __func__ << " crush smoke test duration: "
             << duration << dendl;
  }
  r = check_pg_num(-1, pg_num, size, crush_rule, ss);
  if (r) {
    dout(10) << "check_pg_num returns " << r << dendl;
    return r;
  }

  if (osdmap.crush->get_rule_type(crush_rule) != (int)pool_type) {
    *ss << "crush rule " << crush_rule << " type does not match pool";
    return -EINVAL;
  }

  uint32_t stripe_width = 0;
  r = prepare_pool_stripe_width(pool_type, erasure_code_profile, &stripe_width, ss);
  if (r) {
    dout(10) << "prepare_pool_stripe_width returns " << r << dendl;
    return r;
  }
  
  bool fread = false;
  if (pool_type == pg_pool_t::TYPE_ERASURE) {
    switch (fast_read) {
      case FAST_READ_OFF:
        fread = false;
        break;
      case FAST_READ_ON:
        fread = true;
        break;
      case FAST_READ_DEFAULT:
        fread = g_conf()->osd_pool_default_ec_fast_read;
        break;
      default:
        *ss << "invalid fast_read setting: " << fast_read;
        return -EINVAL;
    }
  }

  for (map<int64_t,string>::iterator p = pending_inc.new_pool_names.begin();
       p != pending_inc.new_pool_names.end();
       ++p) {
    if (p->second == name)
      return 0;
  }

  if (-1 == pending_inc.new_pool_max)
    pending_inc.new_pool_max = osdmap.pool_max;
  int64_t pool = ++pending_inc.new_pool_max;
  pg_pool_t empty;
  pg_pool_t *pi = pending_inc.get_new_pool(pool, &empty);
  pi->create_time = ceph_clock_now();
  pi->type = pool_type;
  pi->fast_read = fread; 
  pi->flags = g_conf()->osd_pool_default_flags;
  if (bulk) {
    pi->set_flag(pg_pool_t::FLAG_BULK);
  } else if (g_conf()->osd_pool_default_flag_bulk) {
      pi->set_flag(pg_pool_t::FLAG_BULK);
  }
  if (g_conf()->osd_pool_default_flag_hashpspool)
    pi->set_flag(pg_pool_t::FLAG_HASHPSPOOL);
  if (g_conf()->osd_pool_default_flag_nodelete)
    pi->set_flag(pg_pool_t::FLAG_NODELETE);
  if (g_conf()->osd_pool_default_flag_nopgchange)
    pi->set_flag(pg_pool_t::FLAG_NOPGCHANGE);
  if (g_conf()->osd_pool_default_flag_nosizechange)
    pi->set_flag(pg_pool_t::FLAG_NOSIZECHANGE);
  pi->set_flag(pg_pool_t::FLAG_CREATING);
  if (g_conf()->osd_pool_use_gmt_hitset)
    pi->use_gmt_hitset = true;
  else
    pi->use_gmt_hitset = false;
  if (crimson) {
    pi->set_flag(pg_pool_t::FLAG_CRIMSON);
    pi->set_flag(pg_pool_t::FLAG_NOPGCHANGE);
  }

  pi->size = size;
  pi->min_size = min_size;
  pi->crush_rule = crush_rule;
  pi->expected_num_objects = expected_num_objects;
  pi->object_hash = CEPH_STR_HASH_RJENKINS;
  if (osdmap.stretch_mode_enabled) {
    pi->peering_crush_bucket_count = osdmap.stretch_bucket_count;
    pi->peering_crush_bucket_target = osdmap.stretch_bucket_count;
    pi->peering_crush_bucket_barrier = osdmap.stretch_mode_bucket;
    pi->peering_crush_mandatory_member = CRUSH_ITEM_NONE;
    if (osdmap.degraded_stretch_mode) {
      pi->peering_crush_bucket_count = osdmap.degraded_stretch_mode;
      pi->peering_crush_bucket_target = osdmap.degraded_stretch_mode;
      // pi->peering_crush_bucket_mandatory_member = CRUSH_ITEM_NONE;
      // TODO: drat, we don't record this ^ anywhere, though given that it
      // necessarily won't exist elsewhere it likely doesn't matter
      pi->min_size = pi->min_size / 2;
      pi->size = pi->size / 2; // only support 2 zones now
    }
  }

  if (auto m = pg_pool_t::get_pg_autoscale_mode_by_name(
        g_conf().get_val<string>("osd_pool_default_pg_autoscale_mode"));
      m != pg_pool_t::pg_autoscale_mode_t::UNKNOWN) {
    pi->pg_autoscale_mode = m;
  } else {
    pi->pg_autoscale_mode = pg_pool_t::pg_autoscale_mode_t::OFF;
  }
  auto max = g_conf().get_val<int64_t>("mon_osd_max_initial_pgs");
  pi->set_pg_num(
    max > 0 ? std::min<uint64_t>(pg_num, std::max<int64_t>(1, max))
    : pg_num);
  pi->set_pg_num_pending(pi->get_pg_num());
  pi->set_pg_num_target(pg_num);
  pi->set_pgp_num(pi->get_pg_num());
  pi->set_pgp_num_target(pgp_num);
  if (osdmap.require_osd_release >= ceph_release_t::nautilus &&
      pg_num_min) {
    pi->opts.set(pool_opts_t::PG_NUM_MIN, static_cast<int64_t>(pg_num_min));
  }
  if (osdmap.require_osd_release >= ceph_release_t::quincy &&
      pg_num_max) {
    pi->opts.set(pool_opts_t::PG_NUM_MAX, static_cast<int64_t>(pg_num_max));
  }
  if (auto m = pg_pool_t::get_pg_autoscale_mode_by_name(
	pg_autoscale_mode); m != pg_pool_t::pg_autoscale_mode_t::UNKNOWN) {
    pi->pg_autoscale_mode = m;
  }

  pi->last_change = pending_inc.epoch;
  pi->auid = 0;

  if (pool_type == pg_pool_t::TYPE_ERASURE) {
      pi->erasure_code_profile = erasure_code_profile;
  } else {
      pi->erasure_code_profile = "";
  }
  pi->stripe_width = stripe_width;

  if (osdmap.require_osd_release >= ceph_release_t::nautilus &&
      target_size_bytes) {
    // only store for nautilus+ because TARGET_SIZE_BYTES may be
    // larger than int32_t max.
    pi->opts.set(pool_opts_t::TARGET_SIZE_BYTES, static_cast<int64_t>(target_size_bytes));
  }
  if (target_size_ratio > 0.0 &&
      osdmap.require_osd_release >= ceph_release_t::nautilus) {
    // only store for nautilus+, just to be consistent and tidy.
    pi->opts.set(pool_opts_t::TARGET_SIZE_RATIO, target_size_ratio);
  }

  pi->cache_target_dirty_ratio_micro =
    g_conf()->osd_pool_default_cache_target_dirty_ratio * 1000000;
  pi->cache_target_dirty_high_ratio_micro =
    g_conf()->osd_pool_default_cache_target_dirty_high_ratio * 1000000;
  pi->cache_target_full_ratio_micro =
    g_conf()->osd_pool_default_cache_target_full_ratio * 1000000;
  pi->cache_min_flush_age = g_conf()->osd_pool_default_cache_min_flush_age;
  pi->cache_min_evict_age = g_conf()->osd_pool_default_cache_min_evict_age;

  pending_inc.new_pool_names[pool] = name;
  return 0;
}

bool OSDMonitor::prepare_set_flag(MonOpRequestRef op, int flag)
{
  op->mark_osdmon_event(__func__);
  ostringstream ss;
  if (pending_inc.new_flags < 0)
    pending_inc.new_flags = osdmap.get_flags();
  pending_inc.new_flags |= flag;
  ss << OSDMap::get_flag_string(flag) << " is set";
  wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, ss.str(),
						    get_last_committed() + 1));
  return true;
}

bool OSDMonitor::prepare_unset_flag(MonOpRequestRef op, int flag)
{
  op->mark_osdmon_event(__func__);
  ostringstream ss;
  if (pending_inc.new_flags < 0)
    pending_inc.new_flags = osdmap.get_flags();
  pending_inc.new_flags &= ~flag;
  ss << OSDMap::get_flag_string(flag) << " is unset";
  wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, ss.str(),
						    get_last_committed() + 1));
  return true;
}

int OSDMonitor::prepare_command_pool_set(const cmdmap_t& cmdmap,
                                         stringstream& ss)
{
  string poolstr;
  cmd_getval(cmdmap, "pool", poolstr);
  int64_t pool = osdmap.lookup_pg_pool_name(poolstr.c_str());
  if (pool < 0) {
    ss << "unrecognized pool '" << poolstr << "'";
    return -ENOENT;
  }
  string var;
  cmd_getval(cmdmap, "var", var);

  pg_pool_t p = *osdmap.get_pg_pool(pool);
  if (pending_inc.new_pools.count(pool))
    p = pending_inc.new_pools[pool];

  // accept val as a json string in the normal case (current
  // generation monitor).  parse out int or float values from the
  // string as needed.  however, if it is not a string, try to pull
  // out an int, in case an older monitor with an older json schema is
  // forwarding a request.
  string val;
  string interr, floaterr;
  int64_t n = 0;
  double f = 0;
  int64_t uf = 0;  // micro-f
  cmd_getval(cmdmap, "val", val);

  auto si_options = {
    "target_max_objects"
  };
  auto iec_options = {
    "target_max_bytes",
    "target_size_bytes",
    "compression_max_blob_size",
    "compression_min_blob_size",
    "csum_max_block",
    "csum_min_block",
  };
  if (count(begin(si_options), end(si_options), var)) {
    n = strict_si_cast<int64_t>(val, &interr);
  } else if (count(begin(iec_options), end(iec_options), var)) {
    n = strict_iec_cast<int64_t>(val, &interr);
  } else {
    // parse string as both int and float; different fields use different types.
    n = strict_strtoll(val.c_str(), 10, &interr);
    f = strict_strtod(val.c_str(), &floaterr);
    uf = llrintl(f * (double)1000000.0);
  }

  if (!p.is_tier() &&
      (var == "hit_set_type" || var == "hit_set_period" ||
       var == "hit_set_count" || var == "hit_set_fpp" ||
       var == "target_max_objects" || var == "target_max_bytes" ||
       var == "cache_target_full_ratio" || var == "cache_target_dirty_ratio" ||
       var == "cache_target_dirty_high_ratio" || var == "use_gmt_hitset" ||
       var == "cache_min_flush_age" || var == "cache_min_evict_age" ||
       var == "hit_set_grade_decay_rate" || var == "hit_set_search_last_n" ||
       var == "min_read_recency_for_promote" || var == "min_write_recency_for_promote")) {
    return -EACCES;
  }

  if (var == "size") {
    if (p.has_flag(pg_pool_t::FLAG_NOSIZECHANGE)) {
      ss << "pool size change is disabled; you must unset nosizechange flag for the pool first";
      return -EPERM;
    }
    if (p.type == pg_pool_t::TYPE_ERASURE) {
      ss << "can not change the size of an erasure-coded pool";
      return -ENOTSUP;
    }
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    }
    if (n <= 0 || n > 10) {
      ss << "pool size must be between 1 and 10";
      return -EINVAL;
    }
    if (n == 1) {
      if (!g_conf().get_val<bool>("mon_allow_pool_size_one")) {
	ss << "configuring pool size as 1 is disabled by default.";
	return -EPERM;
      }
      bool sure = false;
      cmd_getval(cmdmap, "yes_i_really_mean_it", sure);
      if (!sure) { ss << "WARNING: setting pool size 1 could lead to data loss "
	"without recovery. If you are *ABSOLUTELY CERTAIN* that is what you want, "
	  "pass the flag --yes-i-really-mean-it.";
	return -EPERM;
      }
    }
    if (osdmap.crush->get_rule_type(p.get_crush_rule()) != (int)p.type) {
      ss << "crush rule " << p.get_crush_rule() << " type does not match pool";
      return -EINVAL;
    }
    if (n > p.size) {
      // only when increasing pool size
      int r = check_pg_num(pool, p.get_pg_num(), n, p.get_crush_rule(), &ss);
      if (r < 0) {
        return r;
      }
    }
    p.size = n;
    p.min_size = g_conf().get_osd_pool_default_min_size(p.size);
  } else if (var == "min_size") {
    if (p.has_flag(pg_pool_t::FLAG_NOSIZECHANGE)) {
      ss << "pool min size change is disabled; you must unset nosizechange flag for the pool first";
      return -EPERM;
    }
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    }

    if (p.type != pg_pool_t::TYPE_ERASURE) {
      if (n < 1 || n > p.size) {
	ss << "pool min_size must be between 1 and size, which is set to " << (int)p.size;
	return -EINVAL;
      }
    } else {
       ErasureCodeInterfaceRef erasure_code;
       int k;
       stringstream tmp;
       int err = get_erasure_code(p.erasure_code_profile, &erasure_code, &tmp);
       if (err == 0) {
	 k = erasure_code->get_data_chunk_count();
       } else {
	 ss << __func__ << " get_erasure_code failed: " << tmp.str();
	 return err;
       }

       if (n < k || n > p.size) {
	 ss << "pool min_size must be between " << k << " and size, which is set to " << (int)p.size;
	 return -EINVAL;
       }
    }
    p.min_size = n;
  } else if (var == "pg_num_actual") {
    if (p.has_flag(pg_pool_t::FLAG_NOPGCHANGE)) {
      ss << "pool pg_num change is disabled; you must unset nopgchange flag for the pool first";
      return -EPERM;
    }
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    }
    if (n == (int)p.get_pg_num()) {
      return 0;
    }
    if (static_cast<uint64_t>(n) > g_conf().get_val<uint64_t>("mon_max_pool_pg_num")) {
      ss << "'pg_num' must be greater than 0 and less than or equal to "
         << g_conf().get_val<uint64_t>("mon_max_pool_pg_num")
         << " (you may adjust 'mon max pool pg num' for higher values)";
      return -ERANGE;
    }
    if (p.has_flag(pg_pool_t::FLAG_CREATING)) {
      ss << "cannot adjust pg_num while initial PGs are being created";
      return -EBUSY;
    }
    if (n > (int)p.get_pg_num()) {
      if (p.get_pg_num() != p.get_pg_num_pending()) {
	// force pre-nautilus clients to resend their ops, since they
	// don't understand pg_num_pending changes form a new interval
	p.last_force_op_resend_prenautilus = pending_inc.epoch;
      }
      p.set_pg_num(n);
    } else {
      if (osdmap.require_osd_release < ceph_release_t::nautilus) {
	ss << "nautilus OSDs are required to adjust pg_num_pending";
	return -EPERM;
      }
      if (n < (int)p.get_pgp_num()) {
	ss << "specified pg_num " << n << " < pgp_num " << p.get_pgp_num();
	return -EINVAL;
      }
      if (n < (int)p.get_pg_num() - 1) {
	ss << "specified pg_num " << n << " < pg_num (" << p.get_pg_num()
	   << ") - 1; only single pg decrease is currently supported";
	return -EINVAL;
      }
      p.set_pg_num_pending(n);
      // force pre-nautilus clients to resend their ops, since they
      // don't understand pg_num_pending changes form a new interval
      p.last_force_op_resend_prenautilus = pending_inc.epoch;
    }
    // force pre-luminous clients to resend their ops, since they
    // don't understand that split PGs now form a new interval.
    p.last_force_op_resend_preluminous = pending_inc.epoch;
  } else if (var == "pg_num") {
    if (p.has_flag(pg_pool_t::FLAG_NOPGCHANGE)) {
      ss << "pool pg_num change is disabled; you must unset nopgchange flag for the pool first";
      return -EPERM;
    }
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    }
    if (n == (int)p.get_pg_num_target()) {
      return 0;
    }
    if (n <= 0 || static_cast<uint64_t>(n) >
                  g_conf().get_val<uint64_t>("mon_max_pool_pg_num")) {
      ss << "'pg_num' must be greater than 0 and less than or equal to "
         << g_conf().get_val<uint64_t>("mon_max_pool_pg_num")
         << " (you may adjust 'mon max pool pg num' for higher values)";
      return -ERANGE;
    }
    if (n > (int)p.get_pg_num_target()) {
      int r = check_pg_num(pool, n, p.get_size(), p.get_crush_rule(), &ss);
      if (r) {
	return r;
      }
      bool force = false;
      cmd_getval(cmdmap, "yes_i_really_mean_it", force);
      if (p.cache_mode != pg_pool_t::CACHEMODE_NONE && !force) {
	ss << "splits in cache pools must be followed by scrubs and leave sufficient free space to avoid overfilling.  use --yes-i-really-mean-it to force.";
	return -EPERM;
      }
    } else {
      if (osdmap.require_osd_release < ceph_release_t::nautilus) {
	ss << "nautilus OSDs are required to decrease pg_num";
	return -EPERM;
      }
    }
    int64_t pg_min = 0, pg_max = 0;
    p.opts.get(pool_opts_t::PG_NUM_MIN, &pg_min);
    p.opts.get(pool_opts_t::PG_NUM_MAX, &pg_max);
    if (pg_min && n < pg_min) {
      ss << "specified pg_num " << n
	 << " < pg_num_min " << pg_min;
      return -EINVAL;
    }
    if (pg_max && n > pg_max) {
      ss << "specified pg_num " << n
	 << " < pg_num_max " << pg_max;
      return -EINVAL;
    }
    if (osdmap.require_osd_release < ceph_release_t::nautilus) {
      // pre-nautilus osdmap format; increase pg_num directly
      assert(n > (int)p.get_pg_num());
      // force pre-nautilus clients to resend their ops, since they
      // don't understand pg_num_target changes form a new interval
      p.last_force_op_resend_prenautilus = pending_inc.epoch;
      // force pre-luminous clients to resend their ops, since they
      // don't understand that split PGs now form a new interval.
      p.last_force_op_resend_preluminous = pending_inc.epoch;
      p.set_pg_num(n);
    } else {
      // set targets; mgr will adjust pg_num_actual and pgp_num later.
      // make pgp_num track pg_num if it already matches.  if it is set
      // differently, leave it different and let the user control it
      // manually.
      if (p.get_pg_num_target() == p.get_pgp_num_target()) {
	p.set_pgp_num_target(n);
      }
      p.set_pg_num_target(n);
    }
  } else if (var == "pgp_num_actual") {
    if (p.has_flag(pg_pool_t::FLAG_NOPGCHANGE)) {
      ss << "pool pgp_num change is disabled; you must unset nopgchange flag for the pool first";
      return -EPERM;
    }
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    }
    if (n <= 0) {
      ss << "specified pgp_num must > 0, but you set to " << n;
      return -EINVAL;
    }
    if (n > (int)p.get_pg_num()) {
      ss << "specified pgp_num " << n << " > pg_num " << p.get_pg_num();
      return -EINVAL;
    }
    if (n > (int)p.get_pg_num_pending()) {
      ss << "specified pgp_num " << n
	 << " > pg_num_pending " << p.get_pg_num_pending();
      return -EINVAL;
    }
    p.set_pgp_num(n);
  } else if (var == "pgp_num") {
    if (p.has_flag(pg_pool_t::FLAG_NOPGCHANGE)) {
      ss << "pool pgp_num change is disabled; you must unset nopgchange flag for the pool first";
      return -EPERM;
    }
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    }
    if (n <= 0) {
      ss << "specified pgp_num must > 0, but you set to " << n;
      return -EINVAL;
    }
    if (n > (int)p.get_pg_num_target()) {
      ss << "specified pgp_num " << n << " > pg_num " << p.get_pg_num_target();
      return -EINVAL;
    }
    if (osdmap.require_osd_release < ceph_release_t::nautilus) {
      // pre-nautilus osdmap format; increase pgp_num directly
      p.set_pgp_num(n);
    } else {
      p.set_pgp_num_target(n);
    }
  } else if (var == "pg_autoscale_mode") {
    auto m = pg_pool_t::get_pg_autoscale_mode_by_name(val);
    if (m == pg_pool_t::pg_autoscale_mode_t::UNKNOWN) {
      ss << "specified invalid mode " << val;
      return -EINVAL;
    }
    if (osdmap.require_osd_release < ceph_release_t::nautilus) {
      ss << "must set require_osd_release to nautilus or later before setting pg_autoscale_mode";
      return -EINVAL;
    }
    p.pg_autoscale_mode = m;
  } else if (var == "crush_rule") {
    int id = osdmap.crush->get_rule_id(val);
    if (id == -ENOENT) {
      ss << "crush rule " << val << " does not exist";
      return -ENOENT;
    }
    if (id < 0) {
      ss << cpp_strerror(id);
      return -ENOENT;
    }
    if (osdmap.crush->get_rule_type(id) != (int)p.get_type()) {
      ss << "crush rule " << id << " type does not match pool";
      return -EINVAL;
    }
    p.crush_rule = id;
  } else if (var == "nodelete" || var == "nopgchange" ||
	     var == "nosizechange" || var == "write_fadvise_dontneed" ||
	     var == "noscrub" || var == "nodeep-scrub" || var == "bulk") {
    uint64_t flag = pg_pool_t::get_flag_by_name(var);
    // make sure we only compare against 'n' if we didn't receive a string
    if (val == "true" || (interr.empty() && n == 1)) {
      p.set_flag(flag);
    } else if (val == "false" || (interr.empty() && n == 0)) {
      if (flag == pg_pool_t::FLAG_NOPGCHANGE && p.is_crimson()) {
	ss << "cannot clear FLAG_NOPGCHANGE on a crimson pool";
	return -EINVAL;
      }
      p.unset_flag(flag);
    } else {
      ss << "expecting value 'true', 'false', '0', or '1'";
      return -EINVAL;
    }
  } else if (var == "eio") {
    uint64_t flag = pg_pool_t::get_flag_by_name(var);

    // make sure we only compare against 'n' if we didn't receive a string
    if (val == "true" || (interr.empty() && n == 1)) {
      p.set_flag(flag);
    } else if (val == "false" || (interr.empty() && n == 0)) {
      p.unset_flag(flag);
    } else {
      ss << "expecting value 'true', 'false', '0', or '1'";
      return -EINVAL;
    }
  } else if (var == "hashpspool") {
    uint64_t flag = pg_pool_t::get_flag_by_name(var);
    bool force = false;
    cmd_getval(cmdmap, "yes_i_really_mean_it", force);

    if (!force) {
      ss << "are you SURE?  this will remap all placement groups in this pool,"
	    " this triggers large data movement,"
	    " pass --yes-i-really-mean-it if you really do.";
      return -EPERM;
    }
    // make sure we only compare against 'n' if we didn't receive a string
    if (val == "true" || (interr.empty() && n == 1)) {
      p.set_flag(flag);
    } else if (val == "false" || (interr.empty() && n == 0)) {
      p.unset_flag(flag);
    } else {
      ss << "expecting value 'true', 'false', '0', or '1'";
      return -EINVAL;
    }
  } else if (var == "hit_set_type") {
    if (val == "none")
      p.hit_set_params = HitSet::Params();
    else {
      int err = check_cluster_features(CEPH_FEATURE_OSD_CACHEPOOL, ss);
      if (err)
	return err;
      if (val == "bloom") {
	BloomHitSet::Params *bsp = new BloomHitSet::Params;
	bsp->set_fpp(g_conf().get_val<double>("osd_pool_default_hit_set_bloom_fpp"));
	p.hit_set_params = HitSet::Params(bsp);
      } else if (val == "explicit_hash")
	p.hit_set_params = HitSet::Params(new ExplicitHashHitSet::Params);
      else if (val == "explicit_object")
	p.hit_set_params = HitSet::Params(new ExplicitObjectHitSet::Params);
      else {
	ss << "unrecognized hit_set type '" << val << "'";
	return -EINVAL;
      }
    }
  } else if (var == "hit_set_period") {
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    } else if (n < 0) {
      ss << "hit_set_period should be non-negative";
      return -EINVAL;
    }
    p.hit_set_period = n;
  } else if (var == "hit_set_count") {
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    } else if (n < 0) {
      ss << "hit_set_count should be non-negative";
      return -EINVAL;
    }
    p.hit_set_count = n;
  } else if (var == "hit_set_fpp") {
    if (floaterr.length()) {
      ss << "error parsing floating point value '" << val << "': " << floaterr;
      return -EINVAL;
    } else if (f < 0 || f > 1.0) {
      ss << "hit_set_fpp should be in the range 0..1";
      return -EINVAL;
    }
    if (p.hit_set_params.get_type() != HitSet::TYPE_BLOOM) {
      ss << "hit set is not of type Bloom; invalid to set a false positive rate!";
      return -EINVAL;
    }
    BloomHitSet::Params *bloomp = static_cast<BloomHitSet::Params*>(p.hit_set_params.impl.get());
    bloomp->set_fpp(f);
  } else if (var == "use_gmt_hitset") {
    if (val == "true" || (interr.empty() && n == 1)) {
      p.use_gmt_hitset = true;
    } else {
      ss << "expecting value 'true' or '1'";
      return -EINVAL;
    }
  } else if (var == "allow_ec_overwrites") {
    if (!p.is_erasure()) {
      ss << "ec overwrites can only be enabled for an erasure coded pool";
      return -EINVAL;
    }
    stringstream err;
    if (!g_conf()->mon_debug_no_require_bluestore_for_ec_overwrites &&
	!is_pool_currently_all_bluestore(pool, p, &err)) {
      ss << "pool must only be stored on bluestore for scrubbing to work: " << err.str();
      return -EINVAL;
    }
    if (val == "true" || (interr.empty() && n == 1)) {
	p.flags |= pg_pool_t::FLAG_EC_OVERWRITES;
    } else if (val == "false" || (interr.empty() && n == 0)) {
      ss << "ec overwrites cannot be disabled once enabled";
      return -EINVAL;
    } else {
      ss << "expecting value 'true', 'false', '0', or '1'";
      return -EINVAL;
    }
  } else if (var == "target_max_objects") {
    if (interr.length()) {
      ss << "error parsing int '" << val << "': " << interr;
      return -EINVAL;
    }
    p.target_max_objects = n;
  } else if (var == "target_max_bytes") {
    if (interr.length()) {
      ss << "error parsing int '" << val << "': " << interr;
      return -EINVAL;
    }
    p.target_max_bytes = n;
  } else if (var == "cache_target_dirty_ratio") {
    if (floaterr.length()) {
      ss << "error parsing float '" << val << "': " << floaterr;
      return -EINVAL;
    }
    if (f < 0 || f > 1.0) {
      ss << "value must be in the range 0..1";
      return -ERANGE;
    }
    p.cache_target_dirty_ratio_micro = uf;
  } else if (var == "cache_target_dirty_high_ratio") {
    if (floaterr.length()) {
      ss << "error parsing float '" << val << "': " << floaterr;
      return -EINVAL;
    }
    if (f < 0 || f > 1.0) {
      ss << "value must be in the range 0..1";
      return -ERANGE;
    }
    p.cache_target_dirty_high_ratio_micro = uf;
  } else if (var == "cache_target_full_ratio") {
    if (floaterr.length()) {
      ss << "error parsing float '" << val << "': " << floaterr;
      return -EINVAL;
    }
    if (f < 0 || f > 1.0) {
      ss << "value must be in the range 0..1";
      return -ERANGE;
    }
    p.cache_target_full_ratio_micro = uf;
  } else if (var == "cache_min_flush_age") {
    if (interr.length()) {
      ss << "error parsing int '" << val << "': " << interr;
      return -EINVAL;
    }
    p.cache_min_flush_age = n;
  } else if (var == "cache_min_evict_age") {
    if (interr.length()) {
      ss << "error parsing int '" << val << "': " << interr;
      return -EINVAL;
    }
    p.cache_min_evict_age = n;
  } else if (var == "min_read_recency_for_promote") {
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    }
    p.min_read_recency_for_promote = n;
  } else if (var == "hit_set_grade_decay_rate") {
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    }
    if (n > 100 || n < 0) {
      ss << "value out of range,valid range is 0 - 100";
      return -EINVAL;
    }
    p.hit_set_grade_decay_rate = n;
  } else if (var == "hit_set_search_last_n") {
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    }
    if (n > p.hit_set_count || n < 0) {
      ss << "value out of range,valid range is 0 - hit_set_count";
      return -EINVAL;
    }
    p.hit_set_search_last_n = n;
  } else if (var == "min_write_recency_for_promote") {
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    }
    p.min_write_recency_for_promote = n;
  } else if (var == "fast_read") {
    if (p.is_replicated()) {
        ss << "fast read is not supported in replication pool";
        return -EINVAL;
    }
    if (val == "true" || (interr.empty() && n == 1)) {
      p.fast_read = true;
    } else if (val == "false" || (interr.empty() && n == 0)) {
      p.fast_read = false;
    } else {
      ss << "expecting value 'true', 'false', '0', or '1'";
      return -EINVAL;
    }
  } else if (pool_opts_t::is_opt_name(var)) {
    bool unset = val == "unset";
    if (var == "compression_mode") {
      if (!unset) {
        auto cmode = Compressor::get_comp_mode_type(val);
        if (!cmode) {
	  ss << "unrecognized compression mode '" << val << "'";
	  return -EINVAL;
        }
      }
    } else if (var == "compression_algorithm") {
      if (!unset) {
        auto alg = Compressor::get_comp_alg_type(val);
        if (!alg) {
          ss << "unrecognized compression_algorithm '" << val << "'";
	  return -EINVAL;
        }
      }
    } else if (var == "compression_required_ratio") {
      if (floaterr.length()) {
        ss << "error parsing float value '" << val << "': " << floaterr;
        return -EINVAL;
      }
      if (f < 0 || f > 1) {
        ss << "compression_required_ratio is out of range (0-1): '" << val << "'";
	return -EINVAL;
      }
    } else if (var == "csum_type") {
      auto t = unset ? 0 : Checksummer::get_csum_string_type(val);
      if (t < 0 ) {
        ss << "unrecognized csum_type '" << val << "'";
	return -EINVAL;
      }
      //preserve csum_type numeric value
      n = t;
      interr.clear(); 
    } else if (var == "compression_max_blob_size" ||
               var == "compression_min_blob_size" ||
               var == "csum_max_block" ||
               var == "csum_min_block") {
      if (interr.length()) {
        ss << "error parsing int value '" << val << "': " << interr;
        return -EINVAL;
      }
    } else if (var == "fingerprint_algorithm") {
      if (!unset) {
        auto alg = pg_pool_t::get_fingerprint_from_str(val);
        if (!alg) {
          ss << "unrecognized fingerprint_algorithm '" << val << "'";
	  return -EINVAL;
        }
      }
    } else if (var == "target_size_bytes") {
      if (interr.length()) {
        ss << "error parsing unit value '" << val << "': " << interr;
        return -EINVAL;
      }
      if (osdmap.require_osd_release < ceph_release_t::nautilus) {
        ss << "must set require_osd_release to nautilus or "
           << "later before setting target_size_bytes";
        return -EINVAL;
      }
    } else if (var == "target_size_ratio") {
      if (f < 0.0) {
	ss << "target_size_ratio cannot be negative";
	return -EINVAL;
      }
    } else if (var == "pg_num_min") {
      if (interr.length()) {
        ss << "error parsing int value '" << val << "': " << interr;
        return -EINVAL;
      }
      if (n > (int)p.get_pg_num_target()) {
	ss << "specified pg_num_min " << n
	   << " > pg_num " << p.get_pg_num_target();
	return -EINVAL;
      }
    } else if (var == "pg_num_max") {
      if (interr.length()) {
        ss << "error parsing int value '" << val << "': " << interr;
        return -EINVAL;
      }
      if (n && n < (int)p.get_pg_num_target()) {
	ss << "specified pg_num_max " << n
	   << " < pg_num " << p.get_pg_num_target();
	return -EINVAL;
      }
    } else if (var == "recovery_priority") {
      if (interr.length()) {
        ss << "error parsing int value '" << val << "': " << interr;
        return -EINVAL;
      }
      if (!g_conf()->debug_allow_any_pool_priority) {
        if (n > OSD_POOL_PRIORITY_MAX || n < OSD_POOL_PRIORITY_MIN) {
          ss << "pool recovery_priority must be between " << OSD_POOL_PRIORITY_MIN
	     << " and " << OSD_POOL_PRIORITY_MAX;
          return -EINVAL;
        }
      }
    } else if (var == "pg_autoscale_bias") {
      if (f < 0.0 || f > 1000.0) {
	ss << "pg_autoscale_bias must be between 0 and 1000";
	return -EINVAL;
      }
    } else if (var == "dedup_tier") {
      if (interr.empty()) {
	ss << "expecting value 'pool name'";
	return -EINVAL;
      }
      // Current base tier in dedup does not support ec pool 
      if (p.is_erasure()) {
	ss << "pool '" << poolstr
	   << "' is an ec pool, which cannot be a base tier";
	return -ENOTSUP;
      }
      int64_t lowtierpool_id = osdmap.lookup_pg_pool_name(val);
      if (lowtierpool_id < 0) {
	ss << "unrecognized pool '" << val << "'";
	return -ENOENT;
      }
      const pg_pool_t *tp = osdmap.get_pg_pool(lowtierpool_id);
      ceph_assert(tp);
      n = lowtierpool_id;
      // The original input is string (pool name), but we convert it to int64_t.
      // So, clear interr
      interr.clear();
    } else if (var == "dedup_chunk_algorithm") {
      if (!unset) {
        auto alg = pg_pool_t::get_dedup_chunk_algorithm_from_str(val);
        if (!alg) {
          ss << "unrecognized fingerprint_algorithm '" << val << "'";
	  return -EINVAL;
        }
      }
    } else if (var == "dedup_cdc_chunk_size") {
      if (interr.length()) {
        ss << "error parsing int value '" << val << "': " << interr;
        return -EINVAL;
      }
    }

    pool_opts_t::opt_desc_t desc = pool_opts_t::get_opt_desc(var);
    switch (desc.type) {
    case pool_opts_t::STR:
      if (unset) {
	p.opts.unset(desc.key);
      } else {
	p.opts.set(desc.key, static_cast<std::string>(val));
      }
      break;
    case pool_opts_t::INT:
      if (interr.length()) {
	ss << "error parsing integer value '" << val << "': " << interr;
	return -EINVAL;
      }
      if (n == 0) {
	p.opts.unset(desc.key);
      } else {
	p.opts.set(desc.key, static_cast<int64_t>(n));
      }
      break;
    case pool_opts_t::DOUBLE:
      if (floaterr.length()) {
	ss << "error parsing floating point value '" << val << "': " << floaterr;
	return -EINVAL;
      }
      if (f == 0) {
	p.opts.unset(desc.key);
      } else {
	p.opts.set(desc.key, static_cast<double>(f));
      }
      break;
    default:
      ceph_assert(!"unknown type");
    }
  } else {
    ss << "unrecognized variable '" << var << "'";
    return -EINVAL;
  }
  if (val != "unset") {
    ss << "set pool " << pool << " " << var << " to " << val;
  } else {
    ss << "unset pool " << pool << " " << var;
  }
  p.last_change = pending_inc.epoch;
  pending_inc.new_pools[pool] = p;
  return 0;
}

int OSDMonitor::prepare_command_pool_application(const string &prefix,
                                                 const cmdmap_t& cmdmap,
                                                 stringstream& ss)
{
  return _command_pool_application(prefix, cmdmap, ss, nullptr, true);
}

int OSDMonitor::preprocess_command_pool_application(const string &prefix,
                                                    const cmdmap_t& cmdmap,
                                                    stringstream& ss,
                                                    bool *modified)
{
  return _command_pool_application(prefix, cmdmap, ss, modified, false);
}


/**
 * Common logic for preprocess and prepare phases of pool application
 * tag commands.  In preprocess mode we're only detecting invalid
 * commands, and determining whether it was a modification or a no-op.
 * In prepare mode we're actually updating the pending state.
 */
int OSDMonitor::_command_pool_application(const string &prefix,
                                          const cmdmap_t& cmdmap,
                                          stringstream& ss,
                                          bool *modified,
                                          bool preparing)
{
  string pool_name;
  cmd_getval(cmdmap, "pool", pool_name);
  int64_t pool = osdmap.lookup_pg_pool_name(pool_name.c_str());
  if (pool < 0) {
    ss << "unrecognized pool '" << pool_name << "'";
    return -ENOENT;
  }

  pg_pool_t p = *osdmap.get_pg_pool(pool);
  if (preparing) {
    if (pending_inc.new_pools.count(pool)) {
      p = pending_inc.new_pools[pool];
    }
  }

  string app;
  cmd_getval(cmdmap, "app", app);
  bool app_exists = (p.application_metadata.count(app) > 0);

  string key;
  cmd_getval(cmdmap, "key", key);
  if (key == "all") {
    ss << "key cannot be 'all'";
    return -EINVAL;
  }

  string value;
  cmd_getval(cmdmap, "value", value);
  if (value == "all") {
    ss << "value cannot be 'all'";
    return -EINVAL;
  }

  if (boost::algorithm::ends_with(prefix, "enable")) {
    if (app.empty()) {
      ss << "application name must be provided";
      return -EINVAL;
    }

    if (p.is_tier()) {
      ss << "application must be enabled on base tier";
      return -EINVAL;
    }

    bool force = false;
    cmd_getval(cmdmap, "yes_i_really_mean_it", force);

    if (!app_exists && !p.application_metadata.empty() && !force) {
      ss << "Are you SURE? Pool '" << pool_name << "' already has an enabled "
         << "application; pass --yes-i-really-mean-it to proceed anyway";
      return -EPERM;
    }

    if (!app_exists && p.application_metadata.size() >= MAX_POOL_APPLICATIONS) {
      ss << "too many enabled applications on pool '" << pool_name << "'; "
         << "max " << MAX_POOL_APPLICATIONS;
      return -EINVAL;
    }

    if (app.length() > MAX_POOL_APPLICATION_LENGTH) {
      ss << "application name '" << app << "' too long; max length "
         << MAX_POOL_APPLICATION_LENGTH;
      return -EINVAL;
    }

    if (!app_exists) {
      p.application_metadata[app] = {};
    }
    ss << "enabled application '" << app << "' on pool '" << pool_name << "'";

  } else if (boost::algorithm::ends_with(prefix, "disable")) {
    bool force = false;
    cmd_getval(cmdmap, "yes_i_really_mean_it", force);

    if (!force) {
      ss << "Are you SURE? Disabling an application within a pool might result "
         << "in loss of application functionality; pass "
         << "--yes-i-really-mean-it to proceed anyway";
      return -EPERM;
    }

    if (!app_exists) {
      ss << "application '" << app << "' is not enabled on pool '" << pool_name
         << "'";
      return 0; // idempotent
    }

    p.application_metadata.erase(app);
    ss << "disable application '" << app << "' on pool '" << pool_name << "'";

  } else if (boost::algorithm::ends_with(prefix, "set")) {
    if (p.is_tier()) {
      ss << "application metadata must be set on base tier";
      return -EINVAL;
    }

    if (!app_exists) {
      ss << "application '" << app << "' is not enabled on pool '" << pool_name
         << "'";
      return -ENOENT;
    }

    string key;
    cmd_getval(cmdmap, "key", key);

    if (key.empty()) {
      ss << "key must be provided";
      return -EINVAL;
    }

    auto &app_keys = p.application_metadata[app];
    if (app_keys.count(key) == 0 &&
        app_keys.size() >= MAX_POOL_APPLICATION_KEYS) {
      ss << "too many keys set for application '" << app << "' on pool '"
         << pool_name << "'; max " << MAX_POOL_APPLICATION_KEYS;
      return -EINVAL;
    }

    if (key.length() > MAX_POOL_APPLICATION_LENGTH) {
      ss << "key '" << app << "' too long; max length "
         << MAX_POOL_APPLICATION_LENGTH;
      return -EINVAL;
    }

    string value;
    cmd_getval(cmdmap, "value", value);
    if (value.length() > MAX_POOL_APPLICATION_LENGTH) {
      ss << "value '" << value << "' too long; max length "
         << MAX_POOL_APPLICATION_LENGTH;
      return -EINVAL;
    }

    p.application_metadata[app][key] = value;
    ss << "set application '" << app << "' key '" << key << "' to '"
       << value << "' on pool '" << pool_name << "'";
  } else if (boost::algorithm::ends_with(prefix, "rm")) {
    if (!app_exists) {
      ss << "application '" << app << "' is not enabled on pool '" << pool_name
         << "'";
      return -ENOENT;
    }

    string key;
    cmd_getval(cmdmap, "key", key);
    auto it = p.application_metadata[app].find(key);
    if (it == p.application_metadata[app].end()) {
      ss << "application '" << app << "' on pool '" << pool_name
         << "' does not have key '" << key << "'";
      return 0; // idempotent
    }

    p.application_metadata[app].erase(it);
    ss << "removed application '" << app << "' key '" << key << "' on pool '"
       << pool_name << "'";
  } else {
    ceph_abort();
  }

  if (preparing) {
    p.last_change = pending_inc.epoch;
    pending_inc.new_pools[pool] = p;
  }

  // Because we fell through this far, we didn't hit no-op cases,
  // so pool was definitely modified
  if (modified != nullptr) {
    *modified = true;
  }

  return 0;
}

int OSDMonitor::_prepare_command_osd_crush_remove(
    CrushWrapper &newcrush,
    int32_t id,
    int32_t ancestor,
    bool has_ancestor,
    bool unlink_only)
{
  int err = 0;

  if (has_ancestor) {
    err = newcrush.remove_item_under(cct, id, ancestor,
        unlink_only);
  } else {
    err = newcrush.remove_item(cct, id, unlink_only);
  }
  return err;
}

void OSDMonitor::do_osd_crush_remove(CrushWrapper& newcrush)
{
  pending_inc.crush.clear();
  newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
}

int OSDMonitor::prepare_command_osd_crush_remove(
    CrushWrapper &newcrush,
    int32_t id,
    int32_t ancestor,
    bool has_ancestor,
    bool unlink_only)
{
  int err = _prepare_command_osd_crush_remove(
      newcrush, id, ancestor,
      has_ancestor, unlink_only);

  if (err < 0)
    return err;

  ceph_assert(err == 0);
  do_osd_crush_remove(newcrush);

  return 0;
}

int OSDMonitor::prepare_command_osd_remove(int32_t id)
{
  if (osdmap.is_up(id)) {
    return -EBUSY;
  }

  pending_inc.new_state[id] = osdmap.get_state(id);
  pending_inc.new_uuid[id] = uuid_d();
  pending_metadata_rm.insert(id);
  pending_metadata.erase(id);

  return 0;
}

int32_t OSDMonitor::_allocate_osd_id(int32_t* existing_id)
{
  ceph_assert(existing_id);
  *existing_id = -1;

  for (int32_t i = 0; i < osdmap.get_max_osd(); ++i) {
    if (!osdmap.exists(i) &&
        pending_inc.new_up_client.count(i) == 0 &&
        (pending_inc.new_state.count(i) == 0 ||
         (pending_inc.new_state[i] & CEPH_OSD_EXISTS) == 0)) {
      *existing_id = i;
      return -1;
    }
  }

  if (pending_inc.new_max_osd < 0) {
    return osdmap.get_max_osd();
  }
  return pending_inc.new_max_osd;
}

void OSDMonitor::do_osd_create(
    const int32_t id,
    const uuid_d& uuid,
    const string& device_class,
    int32_t* new_id)
{
  dout(10) << __func__ << " uuid " << uuid << dendl;
  ceph_assert(new_id);

  // We presume validation has been performed prior to calling this
  // function. We assert with prejudice.

  int32_t allocated_id = -1; // declare here so we can jump
  int32_t existing_id = -1;
  if (!uuid.is_zero()) {
    existing_id = osdmap.identify_osd(uuid);
    if (existing_id >= 0) {
      ceph_assert(id < 0 || id == existing_id);
      *new_id = existing_id;
      goto out;
    } else if (id >= 0) {
      // uuid does not exist, and id has been provided, so just create
      // the new osd.id
      *new_id = id;
      goto out;
    }
  }

  // allocate a new id
  allocated_id = _allocate_osd_id(&existing_id);
  dout(10) << __func__ << " allocated id " << allocated_id
           << " existing id " << existing_id << dendl;
  if (existing_id >= 0) {
    ceph_assert(existing_id < osdmap.get_max_osd());
    ceph_assert(allocated_id < 0);
    *new_id = existing_id;
  } else if (allocated_id >= 0) {
    ceph_assert(existing_id < 0);
    // raise max_osd
    if (pending_inc.new_max_osd < 0) {
      pending_inc.new_max_osd = osdmap.get_max_osd() + 1;
    } else {
      ++pending_inc.new_max_osd;
    }
    *new_id = pending_inc.new_max_osd - 1;
    ceph_assert(*new_id == allocated_id);
  } else {
    ceph_abort_msg("unexpected condition");
  }

out:
  if (device_class.size()) {
    CrushWrapper newcrush = _get_pending_crush();
    if (newcrush.get_max_devices() < *new_id + 1) {
      newcrush.set_max_devices(*new_id + 1);
    }
    string name = string("osd.") + stringify(*new_id);
    if (!newcrush.item_exists(*new_id)) {
      newcrush.set_item_name(*new_id, name);
    }
    ostringstream ss;
    int r = newcrush.update_device_class(*new_id, device_class, name, &ss);
    if (r < 0) {
      derr << __func__ << " failed to set " << name << " device_class "
	   << device_class << ": " << cpp_strerror(r) << " - " << ss.str()
	   << dendl;
      // non-fatal... this might be a replay and we want to be idempotent.
    } else {
      dout(20) << __func__ << " set " << name << " device_class " << device_class
	       << dendl;
      pending_inc.crush.clear();
      newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
    }
  } else {
    dout(20) << __func__ << " no device_class" << dendl;
  }

  dout(10) << __func__ << " using id " << *new_id << dendl;
  if (osdmap.get_max_osd() <= *new_id && pending_inc.new_max_osd <= *new_id) {
    pending_inc.new_max_osd = *new_id + 1;
  }

  pending_inc.new_weight[*new_id] = CEPH_OSD_IN;
  // do not set EXISTS; OSDMap::set_weight, called by apply_incremental, will
  // set it for us.  (ugh.)
  pending_inc.new_state[*new_id] |= CEPH_OSD_NEW;
  if (!uuid.is_zero())
    pending_inc.new_uuid[*new_id] = uuid;
}

int OSDMonitor::validate_osd_create(
    const int32_t id,
    const uuid_d& uuid,
    const bool check_osd_exists,
    int32_t* existing_id,
    stringstream& ss)
{

  dout(10) << __func__ << " id " << id << " uuid " << uuid
           << " check_osd_exists " << check_osd_exists << dendl;

  ceph_assert(existing_id);

  if (id < 0 && uuid.is_zero()) {
    // we have nothing to validate
    *existing_id = -1;
    return 0;
  } else if (uuid.is_zero()) {
    // we have an id but we will ignore it - because that's what
    // `osd create` does.
    return 0;
  }

  /*
   * This function will be used to validate whether we are able to
   * create a new osd when the `uuid` is specified.
   *
   * It will be used by both `osd create` and `osd new`, as the checks
   * are basically the same when it pertains to osd id and uuid validation.
   * However, `osd create` presumes an `uuid` is optional, for legacy
   * reasons, while `osd new` requires the `uuid` to be provided. This
   * means that `osd create` will not be idempotent if an `uuid` is not
   * provided, but we will always guarantee the idempotency of `osd new`.
   */

  ceph_assert(!uuid.is_zero());
  if (pending_inc.identify_osd(uuid) >= 0) {
    // osd is about to exist
    return -EAGAIN;
  }

  int32_t i = osdmap.identify_osd(uuid);
  if (i >= 0) {
    // osd already exists
    if (id >= 0 && i != id) {
      ss << "uuid " << uuid << " already in use for different id " << i;
      return -EEXIST;
    }
    // return a positive errno to distinguish between a blocking error
    // and an error we consider to not be a problem (i.e., this would be
    // an idempotent operation).
    *existing_id = i;
    return EEXIST;
  }
  // i < 0
  if (id >= 0) {
    if (pending_inc.new_state.count(id)) {
      // osd is about to exist
      return -EAGAIN;
    }
    // we may not care if an osd exists if we are recreating a previously
    // destroyed osd.
    if (check_osd_exists && osdmap.exists(id)) {
      ss << "id " << id << " already in use and does not match uuid "
         << uuid;
      return -EINVAL;
    }
  }
  return 0;
}

int OSDMonitor::prepare_command_osd_create(
    const int32_t id,
    const uuid_d& uuid,
    int32_t* existing_id,
    stringstream& ss)
{
  dout(10) << __func__ << " id " << id << " uuid " << uuid << dendl;
  ceph_assert(existing_id);
  if (osdmap.is_destroyed(id)) {
    ss << "ceph osd create has been deprecated. Please use ceph osd new "
          "instead.";
    return -EINVAL;
  }

  if (uuid.is_zero()) {
    dout(10) << __func__ << " no uuid; assuming legacy `osd create`" << dendl;
  }

  return validate_osd_create(id, uuid, true, existing_id, ss);
}

int OSDMonitor::prepare_command_osd_new(
    MonOpRequestRef op,
    const cmdmap_t& cmdmap,
    const map<string,string>& params,
    stringstream &ss,
    Formatter *f)
{
  uuid_d uuid;
  string uuidstr;
  int64_t id = -1;

  ceph_assert(paxos.is_plugged());

  dout(10) << __func__ << " " << op << dendl;

  /* validate command. abort now if something's wrong. */

  /* `osd new` will expect a `uuid` to be supplied; `id` is optional.
   *
   * If `id` is not specified, we will identify any existing osd based
   * on `uuid`. Operation will be idempotent iff secrets match.
   *
   * If `id` is specified, we will identify any existing osd based on
   * `uuid` and match against `id`. If they match, operation will be
   * idempotent iff secrets match.
   *
   * `-i secrets.json` will be optional. If supplied, will be used
   * to check for idempotency when `id` and `uuid` match.
   *
   * If `id` is not specified, and `uuid` does not exist, an id will
   * be found or allocated for the osd.
   *
   * If `id` is specified, and the osd has been previously marked
   * as destroyed, then the `id` will be reused.
   */
  if (!cmd_getval(cmdmap, "uuid", uuidstr)) {
    ss << "requires the OSD's UUID to be specified.";
    return -EINVAL;
  } else if (!uuid.parse(uuidstr.c_str())) {
    ss << "invalid UUID value '" << uuidstr << "'.";
    return -EINVAL;
  }

  if (cmd_getval(cmdmap, "id", id) &&
      (id < 0)) {
    ss << "invalid OSD id; must be greater or equal than zero.";
    return -EINVAL;
  }

  // are we running an `osd create`-like command, or recreating
  // a previously destroyed osd?

  bool is_recreate_destroyed = (id >= 0 && osdmap.is_destroyed(id));

  // we will care about `id` to assess whether osd is `destroyed`, or
  // to create a new osd.
  // we will need an `id` by the time we reach auth.

  int32_t existing_id = -1;
  int err = validate_osd_create(id, uuid, !is_recreate_destroyed,
                                &existing_id, ss);

  bool may_be_idempotent = false;
  if (err == EEXIST) {
    // this is idempotent from the osdmon's point-of-view
    may_be_idempotent = true;
    ceph_assert(existing_id >= 0);
    id = existing_id;
  } else if (err < 0) {
    return err;
  }

  if (!may_be_idempotent) {
    // idempotency is out of the window. We are either creating a new
    // osd or recreating a destroyed osd.
    //
    // We now need to figure out if we have an `id` (and if it's valid),
    // of find an `id` if we don't have one.

    // NOTE: we need to consider the case where the `id` is specified for
    // `osd create`, and we must honor it. So this means checking if
    // the `id` is destroyed, and if so assume the destroy; otherwise,
    // check if it `exists` - in which case we complain about not being
    // `destroyed`. In the end, if nothing fails, we must allow the
    // creation, so that we are compatible with `create`.
    if (id >= 0 && osdmap.exists(id) && !osdmap.is_destroyed(id)) {
      dout(10) << __func__ << " osd." << id << " isn't destroyed" << dendl;
      ss << "OSD " << id << " has not yet been destroyed";
      return -EINVAL;
    } else if (id < 0) {
      // find an `id`
      id = _allocate_osd_id(&existing_id);
      if (id < 0) {
        ceph_assert(existing_id >= 0);
        id = existing_id;
      }
      dout(10) << __func__ << " found id " << id << " to use" << dendl;
    } else if (id >= 0 && osdmap.is_destroyed(id)) {
      dout(10) << __func__ << " recreating osd." << id << dendl;
    } else {
      dout(10) << __func__ << " creating new osd." << id << dendl;
    }
  } else {
    ceph_assert(id >= 0);
    ceph_assert(osdmap.exists(id));
  }

  // we are now able to either create a brand new osd or reuse an existing
  // osd that has been previously destroyed.

  dout(10) << __func__ << " id " << id << " uuid " << uuid << dendl;

  if (may_be_idempotent && params.empty()) {
    // nothing to do, really.
    dout(10) << __func__ << " idempotent and no params -- no op." << dendl;
    ceph_assert(id >= 0);
    if (f) {
      f->open_object_section("created_osd");
      f->dump_int("osdid", id);
      f->close_section();
    } else {
      ss << id;
    }
    return EEXIST;
  }

  string device_class;
  auto p = params.find("crush_device_class");
  if (p != params.end()) {
    device_class = p->second;
    dout(20) << __func__ << " device_class will be " << device_class << dendl;
  }
  string cephx_secret, lockbox_secret, dmcrypt_key;
  bool has_lockbox = false;
  bool has_secrets = params.count("cephx_secret")
    || params.count("cephx_lockbox_secret")
    || params.count("dmcrypt_key");

  KVMonitor *svc = nullptr;
  AuthMonitor::auth_entity_t cephx_entity, lockbox_entity;

  if (has_secrets) {
    if (params.count("cephx_secret") == 0) {
      ss << "requires a cephx secret.";
      return -EINVAL;
    }
    cephx_secret = params.at("cephx_secret");

    bool has_lockbox_secret = (params.count("cephx_lockbox_secret") > 0);
    bool has_dmcrypt_key = (params.count("dmcrypt_key") > 0);

    dout(10) << __func__ << " has lockbox " << has_lockbox_secret
             << " dmcrypt " << has_dmcrypt_key << dendl;

    if (has_lockbox_secret && has_dmcrypt_key) {
      has_lockbox = true;
      lockbox_secret = params.at("cephx_lockbox_secret");
      dmcrypt_key = params.at("dmcrypt_key");
    } else if (!has_lockbox_secret != !has_dmcrypt_key) {
      ss << "requires both a cephx lockbox secret and a dm-crypt key.";
      return -EINVAL;
    }

    dout(10) << __func__ << " validate secrets using osd id " << id << dendl;

    err = mon.authmon()->validate_osd_new(id, uuid,
        cephx_secret,
        lockbox_secret,
        cephx_entity,
        lockbox_entity,
        ss);
    if (err < 0) {
      return err;
    } else if (may_be_idempotent && err != EEXIST) {
      // for this to be idempotent, `id` should already be >= 0; no need
      // to use validate_id.
      ceph_assert(id >= 0);
      ss << "osd." << id << " exists but secrets do not match";
      return -EEXIST;
    }

    if (has_lockbox) {
      svc = mon.kvmon();
      err = svc->validate_osd_new(uuid, dmcrypt_key, ss);
      if (err < 0) {
        return err;
      } else if (may_be_idempotent && err != EEXIST) {
        ceph_assert(id >= 0);
        ss << "osd." << id << " exists but dm-crypt key does not match.";
        return -EEXIST;
      }
    }
  }
  ceph_assert(!has_secrets || !cephx_secret.empty());
  ceph_assert(!has_lockbox || !lockbox_secret.empty());

  if (may_be_idempotent) {
    // we have nothing to do for either the osdmon or the authmon,
    // and we have no lockbox - so the config key service will not be
    // touched. This is therefore an idempotent operation, and we can
    // just return right away.
    dout(10) << __func__ << " idempotent -- no op." << dendl;
    ceph_assert(id >= 0);
    if (f) {
      f->open_object_section("created_osd");
      f->dump_int("osdid", id);
      f->close_section();
    } else {
      ss << id;
    }
    return EEXIST;
  }
  ceph_assert(!may_be_idempotent);

  // perform updates.
  if (has_secrets) {
    ceph_assert(!cephx_secret.empty());
    ceph_assert((lockbox_secret.empty() && dmcrypt_key.empty()) ||
           (!lockbox_secret.empty() && !dmcrypt_key.empty()));

    err = mon.authmon()->do_osd_new(cephx_entity,
        lockbox_entity,
        has_lockbox);
    ceph_assert(0 == err);

    if (has_lockbox) {
      ceph_assert(nullptr != svc);
      svc->do_osd_new(uuid, dmcrypt_key);
    }
  }

  if (is_recreate_destroyed) {
    ceph_assert(id >= 0);
    ceph_assert(osdmap.is_destroyed(id));
    pending_inc.new_state[id] |= CEPH_OSD_DESTROYED;
    if ((osdmap.get_state(id) & CEPH_OSD_NEW) == 0) {
      pending_inc.new_state[id] |= CEPH_OSD_NEW;
    }
    if (osdmap.get_state(id) & CEPH_OSD_UP) {
      // due to http://tracker.ceph.com/issues/20751 some clusters may
      // have UP set for non-existent OSDs; make sure it is cleared
      // for a newly created osd.
      pending_inc.new_state[id] |= CEPH_OSD_UP;
    }
    pending_inc.new_uuid[id] = uuid;
  } else {
    ceph_assert(id >= 0);
    int32_t new_id = -1;
    do_osd_create(id, uuid, device_class, &new_id);
    ceph_assert(new_id >= 0);
    ceph_assert(id == new_id);
  }

  if (f) {
    f->open_object_section("created_osd");
    f->dump_int("osdid", id);
    f->close_section();
  } else {
    ss << id;
  }

  return 0;
}

bool OSDMonitor::prepare_command(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MMonCommand>();
  stringstream ss;
  cmdmap_t cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon.reply_command(op, -EINVAL, rs, get_last_committed());
    return false; /* nothing to propose */
  }

  MonSession *session = op->get_session();
  if (!session) {
    derr << __func__ << " no session" << dendl;
    mon.reply_command(op, -EACCES, "access denied", get_last_committed());
    return false; /* nothing to propose */
  }

  return prepare_command_impl(op, cmdmap);
}

static int parse_reweights(CephContext *cct,
			   const cmdmap_t& cmdmap,
			   const OSDMap& osdmap,
			   map<int32_t, uint32_t>* weights)
{
  string weights_str;
  if (!cmd_getval(cmdmap, "weights", weights_str)) {
    return -EINVAL;
  }
  std::replace(begin(weights_str), end(weights_str), '\'', '"');
  json_spirit::mValue json_value;
  if (!json_spirit::read(weights_str, json_value)) {
    return -EINVAL;
  }
  if (json_value.type() != json_spirit::obj_type) {
    return -EINVAL;
  }
  const auto obj = json_value.get_obj();
  try {
    for (auto& osd_weight : obj) {
      auto osd_id = std::stoi(osd_weight.first);
      if (!osdmap.exists(osd_id)) {
	return -ENOENT;
      }
      if (osd_weight.second.type() != json_spirit::str_type) {
	return -EINVAL;
      }
      auto weight = std::stoul(osd_weight.second.get_str());
      weights->insert({osd_id, weight});
    }
  } catch (const std::logic_error& e) {
    return -EINVAL;
  }
  return 0;
}

int OSDMonitor::prepare_command_osd_destroy(
    int32_t id,
    stringstream& ss)
{
  ceph_assert(paxos.is_plugged());

  // we check if the osd exists for the benefit of `osd purge`, which may
  // have previously removed the osd. If the osd does not exist, return
  // -ENOENT to convey this, and let the caller deal with it.
  //
  // we presume that all auth secrets and config keys were removed prior
  // to this command being called. if they exist by now, we also assume
  // they must have been created by some other command and do not pertain
  // to this non-existent osd.
  if (!osdmap.exists(id)) {
    dout(10) << __func__ << " osd." << id << " does not exist." << dendl;
    return -ENOENT;
  }

  uuid_d uuid = osdmap.get_uuid(id);
  dout(10) << __func__ << " destroying osd." << id
           << " uuid " << uuid << dendl;

  // if it has been destroyed, we assume our work here is done.
  if (osdmap.is_destroyed(id)) {
    ss << "destroyed osd." << id;
    return 0;
  }

  EntityName cephx_entity, lockbox_entity;
  bool idempotent_auth = false, idempotent_cks = false;

  int err = mon.authmon()->validate_osd_destroy(id, uuid,
                                                 cephx_entity,
                                                 lockbox_entity,
                                                 ss);
  if (err < 0) {
    if (err == -ENOENT) {
      idempotent_auth = true;
    } else {
      return err;
    }
  }

  auto svc = mon.kvmon();
  err = svc->validate_osd_destroy(id, uuid);
  if (err < 0) {
    ceph_assert(err == -ENOENT);
    err = 0;
    idempotent_cks = true;
  }

  if (!idempotent_auth) {
    err = mon.authmon()->do_osd_destroy(cephx_entity, lockbox_entity);
    ceph_assert(0 == err);
  }

  if (!idempotent_cks) {
    svc->do_osd_destroy(id, uuid);
  }

  pending_inc.new_state[id] = CEPH_OSD_DESTROYED;
  pending_inc.new_uuid[id] = uuid_d();

  // we can only propose_pending() once per service, otherwise we'll be
  // defying PaxosService and all laws of nature. Therefore, as we may
  // be used during 'osd purge', let's keep the caller responsible for
  // proposing.
  ceph_assert(err == 0);
  return 0;
}

int OSDMonitor::prepare_command_osd_purge(
    int32_t id,
    stringstream& ss)
{
  ceph_assert(paxos.is_plugged());
  dout(10) << __func__ << " purging osd." << id << dendl;

  ceph_assert(!osdmap.is_up(id));

  /*
   * This may look a bit weird, but this is what's going to happen:
   *
   *  1. we make sure that removing from crush works
   *  2. we call `prepare_command_osd_destroy()`. If it returns an
   *     error, then we abort the whole operation, as no updates
   *     have been made. However, we this function will have
   *     side-effects, thus we need to make sure that all operations
   *     performed henceforth will *always* succeed.
   *  3. we call `prepare_command_osd_remove()`. Although this
   *     function can return an error, it currently only checks if the
   *     osd is up - and we have made sure that it is not so, so there
   *     is no conflict, and it is effectively an update.
   *  4. finally, we call `do_osd_crush_remove()`, which will perform
   *     the crush update we delayed from before.
   */

  CrushWrapper newcrush = _get_pending_crush();

  bool may_be_idempotent = false;

  int err = _prepare_command_osd_crush_remove(newcrush, id, 0, false, false);
  if (err == -ENOENT) {
    err = 0;
    may_be_idempotent = true;
  } else if (err < 0) {
    ss << "error removing osd." << id << " from crush";
    return err;
  }

  // no point destroying the osd again if it has already been marked destroyed
  if (!osdmap.is_destroyed(id)) {
    err = prepare_command_osd_destroy(id, ss);
    if (err < 0) {
      if (err == -ENOENT) {
        err = 0;
      } else {
        return err;
      }
    } else {
      may_be_idempotent = false;
    }
  }
  ceph_assert(0 == err);

  if (may_be_idempotent && !osdmap.exists(id)) {
    dout(10) << __func__ << " osd." << id << " does not exist and "
             << "we are idempotent." << dendl;
    return -ENOENT;
  }

  err = prepare_command_osd_remove(id);
  // we should not be busy, as we should have made sure this id is not up.
  ceph_assert(0 == err);

  do_osd_crush_remove(newcrush);
  return 0;
}

int OSDMonitor::parse_pgid(const cmdmap_t& cmdmap, stringstream &ss,
                           /* out */ pg_t &pgid, std::optional<string> pgids) {
  string pgidstr;
  if (!cmd_getval(cmdmap, "pgid", pgidstr)) {
    ss << "unable to parse 'pgid' value '"
       << cmd_vartype_stringify(cmdmap.at("pgid")) << "'";
    return -EINVAL;
  }
  if (!pgid.parse(pgidstr.c_str())) {
    ss << "invalid pgid '" << pgidstr << "'";
    return -EINVAL;
  }
  if (!osdmap.pg_exists(pgid)) {
    ss << "pgid '" << pgid << "' does not exist";
    return -ENOENT;
  }
  if (pgids.has_value())
    pgids.value() = pgidstr;
  return 0;
}

bool OSDMonitor::prepare_command_impl(MonOpRequestRef op,
				      const cmdmap_t& cmdmap)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MMonCommand>();
  stringstream ss;
  string rs;
  bufferlist rdata;
  int err = 0;

  string format = cmd_getval_or<string>(cmdmap, "format", "plain");
  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  string prefix;
  cmd_getval(cmdmap, "prefix", prefix);

  int64_t osdid;
  string osd_name;
  bool osdid_present = false;
  if (prefix != "osd pg-temp" &&
      prefix != "osd pg-upmap" &&
      prefix != "osd pg-upmap-items") {  // avoid commands with non-int id arg
    osdid_present = cmd_getval(cmdmap, "id", osdid);
  }
  if (osdid_present) {
    ostringstream oss;
    oss << "osd." << osdid;
    osd_name = oss.str();
  }

  // Even if there's a pending state with changes that could affect
  // a command, considering that said state isn't yet committed, we
  // just don't care about those changes if the command currently being
  // handled acts as a no-op against the current committed state.
  // In a nutshell, we assume this command  happens *before*.
  //
  // Let me make this clearer:
  //
  //   - If we have only one client, and that client issues some
  //     operation that would conflict with this operation  but is
  //     still on the pending state, then we would be sure that said
  //     operation wouldn't have returned yet, so the client wouldn't
  //     issue this operation (unless the client didn't wait for the
  //     operation to finish, and that would be the client's own fault).
  //
  //   - If we have more than one client, each client will observe
  //     whatever is the state at the moment of the commit.  So, if we
  //     have two clients, one issuing an unlink and another issuing a
  //     link, and if the link happens while the unlink is still on the
  //     pending state, from the link's point-of-view this is a no-op.
  //     If different clients are issuing conflicting operations and
  //     they care about that, then the clients should make sure they
  //     enforce some kind of concurrency mechanism -- from our
  //     perspective that's what Douglas Adams would call an SEP.
  //
  // This should be used as a general guideline for most commands handled
  // in this function.  Adapt as you see fit, but please bear in mind that
  // this is the expected behavior.
   
 
  if (prefix == "osd setcrushmap" ||
      (prefix == "osd crush set" && !osdid_present)) {
    if (pending_inc.crush.length()) {
      dout(10) << __func__ << " waiting for pending crush update " << dendl;
      goto wait;
    }
    dout(10) << "prepare_command setting new crush map" << dendl;
    bufferlist data(m->get_data());
    CrushWrapper crush;
    try {
      auto bl = data.cbegin();
      crush.decode(bl);
    }
    catch (const std::exception &e) {
      err = -EINVAL;
      ss << "Failed to parse crushmap: " << e.what();
      goto reply_no_propose;
    }
  
    int64_t prior_version = 0;
    if (cmd_getval(cmdmap, "prior_version", prior_version)) {
      if (prior_version == osdmap.get_crush_version() - 1) {
	// see if we are a resend of the last update.  this is imperfect
	// (multiple racing updaters may not both get reliable success)
	// but we expect crush updaters (via this interface) to be rare-ish.
	bufferlist current, proposed;
	osdmap.crush->encode(current, mon.get_quorum_con_features());
	crush.encode(proposed, mon.get_quorum_con_features());
	if (current.contents_equal(proposed)) {
	  dout(10) << __func__
		   << " proposed matches current and version equals previous"
		   << dendl;
	  err = 0;
	  ss << osdmap.get_crush_version();
	  goto reply_no_propose;
	}
      }
      if (prior_version != osdmap.get_crush_version()) {
	err = -EPERM;
	ss << "prior_version " << prior_version << " != crush version "
	   << osdmap.get_crush_version();
	goto reply_no_propose;
      }
    }

    if (!validate_crush_against_features(&crush, ss)) {
      err = -EINVAL;
      goto reply_no_propose;
    }

    err = osdmap.validate_crush_rules(&crush, &ss);
    if (err < 0) {
      goto reply_no_propose;
    }

    if (g_conf()->mon_osd_crush_smoke_test) {
      // sanity check: test some inputs to make sure this map isn't
      // totally broken
      dout(10) << " testing map" << dendl;
      stringstream ess;
      CrushTester tester(crush, ess);
      tester.set_min_x(0);
      tester.set_max_x(50);
      tester.set_num_rep(3);  // arbitrary
      auto start = ceph::coarse_mono_clock::now();
      int r = tester.test_with_fork(cct, g_conf()->mon_lease);
      auto duration = ceph::coarse_mono_clock::now() - start;
      if (r < 0) {
	dout(10) << " tester.test_with_fork returns " << r
		 << ": " << ess.str() << dendl;
	ss << "crush smoke test failed with " << r << ": " << ess.str();
	err = r;
	goto reply_no_propose;
      }
      dout(10) << __func__ << " crush somke test duration: "
               << duration << ", result: " << ess.str() << dendl;
    }

    pending_inc.crush = data;
    ss << osdmap.get_crush_version() + 1;
    goto update;

  } else if (prefix == "osd crush set-all-straw-buckets-to-straw2") {
    CrushWrapper newcrush = _get_pending_crush();
    for (int b = 0; b < newcrush.get_max_buckets(); ++b) {
      int bid = -1 - b;
      if (newcrush.bucket_exists(bid) &&
	  newcrush.get_bucket_alg(bid) == CRUSH_BUCKET_STRAW) {
	dout(20) << " bucket " << bid << " is straw, can convert" << dendl;
	newcrush.bucket_set_alg(bid, CRUSH_BUCKET_STRAW2);
      }
    }
    if (!validate_crush_against_features(&newcrush, ss)) {
      err = -EINVAL;
      goto reply_no_propose;
    }
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd crush set-device-class") {
    string device_class;
    if (!cmd_getval(cmdmap, "class", device_class)) {
      err = -EINVAL; // no value!
      goto reply_no_propose;
    }

    bool stop = false;
    vector<string> idvec;
    cmd_getval(cmdmap, "ids", idvec);
    CrushWrapper newcrush = _get_pending_crush();
    set<int> updated;
    for (unsigned j = 0; j < idvec.size() && !stop; j++) {
      set<int> osds;
      // wildcard?
      if (j == 0 &&
          (idvec[0] == "any" || idvec[0] == "all" || idvec[0] == "*")) {
        osdmap.get_all_osds(osds);
        stop = true;
      } else {
        // try traditional single osd way
        long osd = parse_osd_id(idvec[j].c_str(), &ss);
        if (osd < 0) {
          // ss has reason for failure
          ss << ", unable to parse osd id:\"" << idvec[j] << "\". ";
          err = -EINVAL;
          continue;
        }
        osds.insert(osd);
      }

      for (auto &osd : osds) {
        if (!osdmap.exists(osd)) {
          ss << "osd." << osd << " does not exist. ";
          continue;
        }

        ostringstream oss;
        oss << "osd." << osd;
        string name = oss.str();

	if (newcrush.get_max_devices() < osd + 1) {
	  newcrush.set_max_devices(osd + 1);
	}
        string action;
        if (newcrush.item_exists(osd)) {
          action = "updating";
        } else {
          action = "creating";
          newcrush.set_item_name(osd, name);
        }

        dout(5) << action << " crush item id " << osd << " name '" << name
                << "' device_class '" << device_class << "'"
                << dendl;
        err = newcrush.update_device_class(osd, device_class, name, &ss);
        if (err < 0) {
          goto reply_no_propose;
        }
        if (err == 0 && !_have_pending_crush()) {
          if (!stop) {
            // for single osd only, wildcard makes too much noise
            ss << "set-device-class item id " << osd << " name '" << name
               << "' device_class '" << device_class << "': no change. ";
          }
        } else {
          updated.insert(osd);
        }
      }
    }

    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
    ss << "set osd(s) " << updated << " to class '" << device_class << "'";
    getline(ss, rs);
    wait_for_finished_proposal(
      op,
      new Monitor::C_Command(mon,op, 0, rs, get_last_committed() + 1));
    return true;
 } else if (prefix == "osd crush rm-device-class") {
    bool stop = false;
    vector<string> idvec;
    cmd_getval(cmdmap, "ids", idvec);
    CrushWrapper newcrush = _get_pending_crush();
    set<int> updated;

    for (unsigned j = 0; j < idvec.size() && !stop; j++) {
      set<int> osds;

      // wildcard?
      if (j == 0 &&
          (idvec[0] == "any" || idvec[0] == "all" || idvec[0] == "*")) {
        osdmap.get_all_osds(osds);
        stop = true;
      } else {
        // try traditional single osd way
        long osd = parse_osd_id(idvec[j].c_str(), &ss);
        if (osd < 0) {
          // ss has reason for failure
          ss << ", unable to parse osd id:\"" << idvec[j] << "\". ";
          err = -EINVAL;
          goto reply_no_propose;
        }
        osds.insert(osd);
      }

      for (auto &osd : osds) {
        if (!osdmap.exists(osd)) {
          ss << "osd." << osd << " does not exist. ";
          continue;
        }

        auto class_name = newcrush.get_item_class(osd);
        if (!class_name) {
          ss << "osd." << osd << " belongs to no class, ";
          continue;
        }
        // note that we do not verify if class_is_in_use here
        // in case the device is misclassified and user wants
        // to overridely reset...

        err = newcrush.remove_device_class(cct, osd, &ss);
        if (err < 0) {
          // ss has reason for failure
          goto reply_no_propose;
        }
        updated.insert(osd);
      }
    }

    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
    ss << "done removing class of osd(s): " << updated;
    getline(ss, rs);
    wait_for_finished_proposal(
      op,
      new Monitor::C_Command(mon,op, 0, rs, get_last_committed() + 1));
    return true;
  } else if (prefix == "osd crush class create") {
    string device_class;
    if (!cmd_getval(cmdmap, "class", device_class)) {
      err = -EINVAL; // no value!
      goto reply_no_propose;
    }
    if (osdmap.require_osd_release < ceph_release_t::luminous) {
      ss << "you must complete the upgrade and 'ceph osd require-osd-release "
         << "luminous' before using crush device classes";
      err = -EPERM;
      goto reply_no_propose;
    }
    if (!_have_pending_crush() &&
        _get_stable_crush().class_exists(device_class)) {
      ss << "class '" << device_class << "' already exists";
      goto reply_no_propose;
    }
     CrushWrapper newcrush = _get_pending_crush();
     if (newcrush.class_exists(device_class)) {
      ss << "class '" << device_class << "' already exists";
      goto update;
    }
    int class_id = newcrush.get_or_create_class_id(device_class);
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
    ss << "created class " << device_class << " with id " << class_id
       << " to crush map";
    goto update;
  } else if (prefix == "osd crush class rm") {
    string device_class;
    if (!cmd_getval(cmdmap, "class", device_class)) {
       err = -EINVAL; // no value!
       goto reply_no_propose;
     }
    if (osdmap.require_osd_release < ceph_release_t::luminous) {
       ss << "you must complete the upgrade and 'ceph osd require-osd-release "
         << "luminous' before using crush device classes";
       err = -EPERM;
       goto reply_no_propose;
     }

     if (!osdmap.crush->class_exists(device_class)) {
       err = 0;
       goto reply_no_propose;
     }

     CrushWrapper newcrush = _get_pending_crush();
     if (!newcrush.class_exists(device_class)) {
       err = 0; // make command idempotent
       goto wait;
     }
     int class_id = newcrush.get_class_id(device_class);
     stringstream ts;
     if (newcrush.class_is_in_use(class_id, &ts)) {
       err = -EBUSY;
       ss << "class '" << device_class << "' " << ts.str();
       goto reply_no_propose;
     }

     // check if class is used by any erasure-code-profiles
     mempool::osdmap::map<string,map<string,string>> old_ec_profiles =
       osdmap.get_erasure_code_profiles();
     auto ec_profiles = pending_inc.get_erasure_code_profiles();
#ifdef HAVE_STDLIB_MAP_SPLICING
     ec_profiles.merge(old_ec_profiles);
#else
     ec_profiles.insert(make_move_iterator(begin(old_ec_profiles)),
                        make_move_iterator(end(old_ec_profiles)));
#endif
     list<string> referenced_by;
     for (auto &i: ec_profiles) {
       for (auto &j: i.second) {
         if ("crush-device-class" == j.first && device_class == j.second) {
           referenced_by.push_back(i.first);
         }
       }
     }
     if (!referenced_by.empty()) {
       err = -EBUSY;
       ss << "class '" << device_class
          << "' is still referenced by erasure-code-profile(s): " << referenced_by;
       goto reply_no_propose;
     }

     set<int> osds;
     newcrush.get_devices_by_class(device_class, &osds);
     for (auto& p: osds) {
       err = newcrush.remove_device_class(cct, p, &ss);
       if (err < 0) {
         // ss has reason for failure
         goto reply_no_propose;
       }
     }

     if (osds.empty()) {
       // empty class, remove directly
       err = newcrush.remove_class_name(device_class);
       if (err < 0) {
         ss << "class '" << device_class << "' cannot be removed '"
            << cpp_strerror(err) << "'";
         goto reply_no_propose;
       }
     }

     pending_inc.crush.clear();
     newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
     ss << "removed class " << device_class << " with id " << class_id
        << " from crush map";
     goto update;
  } else if (prefix == "osd crush class rename") {
    string srcname, dstname;
    if (!cmd_getval(cmdmap, "srcname", srcname)) {
      err = -EINVAL;
      goto reply_no_propose;
    }
    if (!cmd_getval(cmdmap, "dstname", dstname)) {
      err = -EINVAL;
      goto reply_no_propose;
    }

    CrushWrapper newcrush = _get_pending_crush();
    if (!newcrush.class_exists(srcname) && newcrush.class_exists(dstname)) {
      // suppose this is a replay and return success
      // so command is idempotent
      ss << "already renamed to '" << dstname << "'";
      err = 0;
      goto reply_no_propose;
    }

    err = newcrush.rename_class(srcname, dstname);
    if (err < 0) {
      ss << "fail to rename '" << srcname << "' to '" << dstname << "' : "
         << cpp_strerror(err);
      goto reply_no_propose;
    }

    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
    ss << "rename class '" << srcname << "' to '" << dstname << "'";
    goto update;
  } else if (prefix == "osd crush add-bucket") {
    // os crush add-bucket <name> <type>
    string name, typestr;
    vector<string> argvec;
    cmd_getval(cmdmap, "name", name);
    cmd_getval(cmdmap, "type", typestr);
    cmd_getval(cmdmap, "args", argvec);
    map<string,string> loc;
    if (!argvec.empty()) {
      CrushWrapper::parse_loc_map(argvec, &loc);
      dout(0) << "will create and move bucket '" << name
              << "' to location " << loc << dendl;
    }

    if (!_have_pending_crush() &&
	_get_stable_crush().name_exists(name)) {
      ss << "bucket '" << name << "' already exists";
      goto reply_no_propose;
    }

    CrushWrapper newcrush = _get_pending_crush();

    if (newcrush.name_exists(name)) {
      ss << "bucket '" << name << "' already exists";
      goto update;
    }
    int type = newcrush.get_type_id(typestr);
    if (type < 0) {
      ss << "type '" << typestr << "' does not exist";
      err = -EINVAL;
      goto reply_no_propose;
    }
    if (type == 0) {
      ss << "type '" << typestr << "' is for devices, not buckets";
      err = -EINVAL;
      goto reply_no_propose;
    }
    int bucketno;
    err = newcrush.add_bucket(0, 0,
			      CRUSH_HASH_DEFAULT, type, 0, NULL,
			      NULL, &bucketno);
    if (err < 0) {
      ss << "add_bucket error: '" << cpp_strerror(err) << "'";
      goto reply_no_propose;
    }
    err = newcrush.set_item_name(bucketno, name);
    if (err < 0) {
      ss << "error setting bucket name to '" << name << "'";
      goto reply_no_propose;
    }

    if (!loc.empty()) {
      if (!newcrush.check_item_loc(cct, bucketno, loc,
          (int *)NULL)) {
        err = newcrush.move_bucket(cct, bucketno, loc);
        if (err < 0) {
          ss << "error moving bucket '" << name << "' to location " << loc;
          goto reply_no_propose;
        }
      } else {
        ss << "no need to move item id " << bucketno << " name '" << name
           << "' to location " << loc << " in crush map";
      }
    }

    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
    if (loc.empty()) {
      ss << "added bucket " << name << " type " << typestr
         << " to crush map";
    } else {
      ss << "added bucket " << name << " type " << typestr
         << " to location " << loc;
    }
    goto update;
  } else if (prefix == "osd crush rename-bucket") {
    string srcname, dstname;
    cmd_getval(cmdmap, "srcname", srcname);
    cmd_getval(cmdmap, "dstname", dstname);

    err = crush_rename_bucket(srcname, dstname, &ss);
    if (err) {
      // equivalent to success for idempotency
      if (err == -EALREADY) {
        err = 0;
      }
      goto reply_no_propose;
    } else {
      goto update;
    }
  } else if (prefix == "osd crush weight-set create" ||
	     prefix == "osd crush weight-set create-compat") {
    if (_have_pending_crush()) {
      dout(10) << " first waiting for pending crush changes to commit" << dendl;
      goto wait;
    }
    CrushWrapper newcrush = _get_pending_crush();
    int64_t pool;
    int positions;
    if (newcrush.has_non_straw2_buckets()) {
      ss << "crush map contains one or more bucket(s) that are not straw2";
      err = -EPERM;
      goto reply_no_propose;
    }
    if (prefix == "osd crush weight-set create") {
      if (osdmap.require_min_compat_client != ceph_release_t::unknown &&
	  osdmap.require_min_compat_client < ceph_release_t::luminous) {
	ss << "require_min_compat_client "
	   << osdmap.require_min_compat_client
	   << " < luminous, which is required for per-pool weight-sets. "
           << "Try 'ceph osd set-require-min-compat-client luminous' "
           << "before using the new interface";
	err = -EPERM;
	goto reply_no_propose;
      }
      string poolname, mode;
      cmd_getval(cmdmap, "pool", poolname);
      pool = osdmap.lookup_pg_pool_name(poolname.c_str());
      if (pool < 0) {
	ss << "pool '" << poolname << "' not found";
	err = -ENOENT;
	goto reply_no_propose;
      }
      cmd_getval(cmdmap, "mode", mode);
      if (mode != "flat" && mode != "positional") {
	ss << "unrecognized weight-set mode '" << mode << "'";
	err = -EINVAL;
	goto reply_no_propose;
      }
      positions = mode == "flat" ? 1 : osdmap.get_pg_pool(pool)->get_size();
    } else {
      pool = CrushWrapper::DEFAULT_CHOOSE_ARGS;
      positions = 1;
    }
    if (!newcrush.create_choose_args(pool, positions)) {
      if (pool == CrushWrapper::DEFAULT_CHOOSE_ARGS) {
        ss << "compat weight-set already created";
      } else {
        ss << "weight-set for pool '" << osdmap.get_pool_name(pool)
           << "' already created";
      }
      goto reply_no_propose;
    }
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
    goto update;

  } else if (prefix == "osd crush weight-set rm" ||
	     prefix == "osd crush weight-set rm-compat") {
    CrushWrapper newcrush = _get_pending_crush();
    int64_t pool;
    if (prefix == "osd crush weight-set rm") {
      string poolname;
      cmd_getval(cmdmap, "pool", poolname);
      pool = osdmap.lookup_pg_pool_name(poolname.c_str());
      if (pool < 0) {
	ss << "pool '" << poolname << "' not found";
	err = -ENOENT;
	goto reply_no_propose;
      }
    } else {
      pool = CrushWrapper::DEFAULT_CHOOSE_ARGS;
    }
    newcrush.rm_choose_args(pool);
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
    goto update;

  } else if (prefix == "osd crush weight-set reweight" ||
	     prefix == "osd crush weight-set reweight-compat") {
    string poolname, item;
    vector<double> weight;
    cmd_getval(cmdmap, "pool", poolname);
    cmd_getval(cmdmap, "item", item);
    cmd_getval(cmdmap, "weight", weight);
    CrushWrapper newcrush = _get_pending_crush();
    int64_t pool;
    if (prefix == "osd crush weight-set reweight") {
      pool = osdmap.lookup_pg_pool_name(poolname.c_str());
      if (pool < 0) {
	ss << "pool '" << poolname << "' not found";
	err = -ENOENT;
	goto reply_no_propose;
      }
      if (!newcrush.have_choose_args(pool)) {
	ss << "no weight-set for pool '" << poolname << "'";
	err = -ENOENT;
	goto reply_no_propose;
      }
      auto arg_map = newcrush.choose_args_get(pool);
      int positions = newcrush.get_choose_args_positions(arg_map);
      if (weight.size() != (size_t)positions) {
         ss << "must specify exact " << positions << " weight values";
         err = -EINVAL;
         goto reply_no_propose;
      }
    } else {
      pool = CrushWrapper::DEFAULT_CHOOSE_ARGS;
      if (!newcrush.have_choose_args(pool)) {
	ss << "no backward-compatible weight-set";
	err = -ENOENT;
	goto reply_no_propose;
      }
    }
    if (!newcrush.name_exists(item)) {
      ss << "item '" << item << "' does not exist";
      err = -ENOENT;
      goto reply_no_propose;
    }
    err = newcrush.choose_args_adjust_item_weightf(
      cct,
      newcrush.choose_args_get(pool),
      newcrush.get_item_id(item),
      weight,
      &ss);
    if (err < 0) {
      goto reply_no_propose;
    }
    err = 0;
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
    goto update;
  } else if (osdid_present &&
	     (prefix == "osd crush set" || prefix == "osd crush add")) {
    // <OsdName> is 'osd.<id>' or '<id>', passed as int64_t id
    // osd crush set <OsdName> <weight> <loc1> [<loc2> ...]
    // osd crush add <OsdName> <weight> <loc1> [<loc2> ...]

    if (!osdmap.exists(osdid)) {
      err = -ENOENT;
      ss << osd_name
	 << " does not exist. Create it before updating the crush map";
      goto reply_no_propose;
    }

    double weight;
    if (!cmd_getval(cmdmap, "weight", weight)) {
      ss << "unable to parse weight value '"
         << cmd_vartype_stringify(cmdmap.at("weight")) << "'";
      err = -EINVAL;
      goto reply_no_propose;
    }

    string args;
    vector<string> argvec;
    cmd_getval(cmdmap, "args", argvec);
    map<string,string> loc;
    CrushWrapper::parse_loc_map(argvec, &loc);

    if (prefix == "osd crush set"
        && !_get_stable_crush().item_exists(osdid)) {
      err = -ENOENT;
      ss << "unable to set item id " << osdid << " name '" << osd_name
         << "' weight " << weight << " at location " << loc
         << ": does not exist";
      goto reply_no_propose;
    }

    dout(5) << "adding/updating crush item id " << osdid << " name '"
      << osd_name << "' weight " << weight << " at location "
      << loc << dendl;
    CrushWrapper newcrush = _get_pending_crush();

    string action;
    if (prefix == "osd crush set" ||
        newcrush.check_item_loc(cct, osdid, loc, (int *)NULL)) {
      action = "set";
      err = newcrush.update_item(cct, osdid, weight, osd_name, loc);
    } else {
      action = "add";
      err = newcrush.insert_item(cct, osdid, weight, osd_name, loc);
      if (err == 0)
        err = 1;
    }

    if (err < 0)
      goto reply_no_propose;

    if (err == 0 && !_have_pending_crush()) {
      ss << action << " item id " << osdid << " name '" << osd_name
	 << "' weight " << weight << " at location " << loc << ": no change";
      goto reply_no_propose;
    }

    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
    ss << action << " item id " << osdid << " name '" << osd_name << "' weight "
       << weight << " at location " << loc << " to crush map";
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						      get_last_committed() + 1));
    return true;

  } else if (prefix == "osd crush create-or-move") {
    do {
      // osd crush create-or-move <OsdName> <initial_weight> <loc1> [<loc2> ...]
      if (!osdmap.exists(osdid)) {
	err = -ENOENT;
	ss << osd_name
	   << " does not exist.  create it before updating the crush map";
	goto reply_no_propose;
      }

      double weight;
      if (!cmd_getval(cmdmap, "weight", weight)) {
        ss << "unable to parse weight value '"
           << cmd_vartype_stringify(cmdmap.at("weight")) << "'";
        err = -EINVAL;
        goto reply_no_propose;
      }

      string args;
      vector<string> argvec;
      cmd_getval(cmdmap, "args", argvec);
      map<string,string> loc;
      CrushWrapper::parse_loc_map(argvec, &loc);

      dout(0) << "create-or-move crush item name '" << osd_name
	      << "' initial_weight " << weight << " at location " << loc
	      << dendl;

      CrushWrapper newcrush = _get_pending_crush();

      err = newcrush.create_or_move_item(cct, osdid, weight, osd_name, loc,
					 g_conf()->osd_crush_update_weight_set);
      if (err == 0) {
	ss << "create-or-move updated item name '" << osd_name
	   << "' weight " << weight
	   << " at location " << loc << " to crush map";
	break;
      }
      if (err > 0) {
	pending_inc.crush.clear();
	newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
	ss << "create-or-move updating item name '" << osd_name
	   << "' weight " << weight
	   << " at location " << loc << " to crush map";
	getline(ss, rs);
	wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						  get_last_committed() + 1));
	return true;
      }
    } while (false);

  } else if (prefix == "osd crush move") {
    do {
      // osd crush move <name> <loc1> [<loc2> ...]
      string name;
      vector<string> argvec;
      cmd_getval(cmdmap, "name", name);
      cmd_getval(cmdmap, "args", argvec);
      map<string,string> loc;
      CrushWrapper::parse_loc_map(argvec, &loc);

      dout(0) << "moving crush item name '" << name << "' to location " << loc << dendl;
      CrushWrapper newcrush = _get_pending_crush();

      if (!newcrush.name_exists(name)) {
	err = -ENOENT;
	ss << "item " << name << " does not exist";
	break;
      }
      int id = newcrush.get_item_id(name);

      if (!newcrush.check_item_loc(cct, id, loc, (int *)NULL)) {
	if (id >= 0) {
	  err = newcrush.create_or_move_item(
	    cct, id, 0, name, loc,
	    g_conf()->osd_crush_update_weight_set);
	} else {
	  err = newcrush.move_bucket(cct, id, loc);
	}
	if (err >= 0) {
	  ss << "moved item id " << id << " name '" << name << "' to location " << loc << " in crush map";
	  pending_inc.crush.clear();
	  newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
	  getline(ss, rs);
	  wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						   get_last_committed() + 1));
	  return true;
	}
      } else {
	ss << "no need to move item id " << id << " name '" << name << "' to location " << loc << " in crush map";
	err = 0;
      }
    } while (false);
  } else if (prefix == "osd crush swap-bucket") {
    string source, dest;
    cmd_getval(cmdmap, "source", source);
    cmd_getval(cmdmap, "dest", dest);

    bool force = false;
    cmd_getval(cmdmap, "yes_i_really_mean_it", force);

    CrushWrapper newcrush = _get_pending_crush();
    if (!newcrush.name_exists(source)) {
      ss << "source item " << source << " does not exist";
      err = -ENOENT;
      goto reply_no_propose;
    }
    if (!newcrush.name_exists(dest)) {
      ss << "dest item " << dest << " does not exist";
      err = -ENOENT;
      goto reply_no_propose;
    }
    int sid = newcrush.get_item_id(source);
    int did = newcrush.get_item_id(dest);
    int sparent;
    if (newcrush.get_immediate_parent_id(sid, &sparent) == 0 && !force) {
      ss << "source item " << source << " is not an orphan bucket; pass --yes-i-really-mean-it to proceed anyway";
      err = -EPERM;
      goto reply_no_propose;
    }
    if (newcrush.get_bucket_alg(sid) != newcrush.get_bucket_alg(did) &&
	!force) {
      ss << "source bucket alg " << crush_alg_name(newcrush.get_bucket_alg(sid)) << " != "
	 << "dest bucket alg " << crush_alg_name(newcrush.get_bucket_alg(did))
	 << "; pass --yes-i-really-mean-it to proceed anyway";
      err = -EPERM;
      goto reply_no_propose;
    }
    int r = newcrush.swap_bucket(cct, sid, did);
    if (r < 0) {
      ss << "failed to swap bucket contents: " << cpp_strerror(r);
      err = r;
      goto reply_no_propose;
    }
    ss << "swapped bucket of " << source << " to " << dest;
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
    wait_for_finished_proposal(op,
			       new Monitor::C_Command(mon, op, err, ss.str(),
						      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd crush link") {
    // osd crush link <name> <loc1> [<loc2> ...]
    string name;
    cmd_getval(cmdmap, "name", name);
    vector<string> argvec;
    cmd_getval(cmdmap, "args", argvec);
    map<string,string> loc;
    CrushWrapper::parse_loc_map(argvec, &loc);

    // Need an explicit check for name_exists because get_item_id returns
    // 0 on unfound.
    int id = osdmap.crush->get_item_id(name);
    if (!osdmap.crush->name_exists(name)) {
      err = -ENOENT;
      ss << "item " << name << " does not exist";
      goto reply_no_propose;
    } else {
      dout(5) << "resolved crush name '" << name << "' to id " << id << dendl;
    }
    if (osdmap.crush->check_item_loc(cct, id, loc, (int*) NULL)) {
      ss << "no need to move item id " << id << " name '" << name
	 << "' to location " << loc << " in crush map";
      err = 0;
      goto reply_no_propose;
    }

    dout(5) << "linking crush item name '" << name << "' at location " << loc << dendl;
    CrushWrapper newcrush = _get_pending_crush();

    if (!newcrush.name_exists(name)) {
      err = -ENOENT;
      ss << "item " << name << " does not exist";
      goto reply_no_propose;
    } else {
      int id = newcrush.get_item_id(name);
      if (!newcrush.check_item_loc(cct, id, loc, (int *)NULL)) {
	err = newcrush.link_bucket(cct, id, loc);
	if (err >= 0) {
	  ss << "linked item id " << id << " name '" << name
             << "' to location " << loc << " in crush map";
	  pending_inc.crush.clear();
	  newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
	} else {
	  ss << "cannot link item id " << id << " name '" << name
             << "' to location " << loc;
          goto reply_no_propose;
	}
      } else {
	ss << "no need to move item id " << id << " name '" << name
           << "' to location " << loc << " in crush map";
	err = 0;
      }
    }
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, err, ss.str(),
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd crush rm" ||
	     prefix == "osd crush remove" ||
	     prefix == "osd crush unlink") {
    do {
      // osd crush rm <id> [ancestor]
      CrushWrapper newcrush = _get_pending_crush();

      string name;
      cmd_getval(cmdmap, "name", name);

      if (!osdmap.crush->name_exists(name)) {
	err = 0;
	ss << "device '" << name << "' does not appear in the crush map";
	break;
      }
      if (!newcrush.name_exists(name)) {
	err = 0;
	ss << "device '" << name << "' does not appear in the crush map";
	getline(ss, rs);
	wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						  get_last_committed() + 1));
	return true;
      }
      int id = newcrush.get_item_id(name);
      int ancestor = 0;

      bool unlink_only = prefix == "osd crush unlink";
      string ancestor_str;
      if (cmd_getval(cmdmap, "ancestor", ancestor_str)) {
	if (!newcrush.name_exists(ancestor_str)) {
	  err = -ENOENT;
	  ss << "ancestor item '" << ancestor_str
	     << "' does not appear in the crush map";
	  break;
	}
        ancestor = newcrush.get_item_id(ancestor_str);
      }

      err = prepare_command_osd_crush_remove(
          newcrush,
          id, ancestor,
          (ancestor < 0), unlink_only);

      if (err == -ENOENT) {
	ss << "item " << id << " does not appear in that position";
	err = 0;
	break;
      }
      if (err == 0) {
        if (!unlink_only)
          pending_inc.new_crush_node_flags[id] = 0;
	ss << "removed item id " << id << " name '" << name << "' from crush map";
	getline(ss, rs);
	wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						  get_last_committed() + 1));
	return true;
      }
    } while (false);

  } else if (prefix == "osd crush reweight-all") {
    CrushWrapper newcrush = _get_pending_crush();

    newcrush.reweight(cct);
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
    ss << "reweighted crush hierarchy";
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						  get_last_committed() + 1));
    return true;
  } else if (prefix == "osd crush reweight") {
    // osd crush reweight <name> <weight>
    CrushWrapper newcrush = _get_pending_crush();

    string name;
    cmd_getval(cmdmap, "name", name);
    if (!newcrush.name_exists(name)) {
      err = -ENOENT;
      ss << "device '" << name << "' does not appear in the crush map";
      goto reply_no_propose;
    }

    int id = newcrush.get_item_id(name);
    if (id < 0) {
      ss << "device '" << name << "' is not a leaf in the crush map";
      err = -EINVAL;
      goto reply_no_propose;
    }
    double w;
    if (!cmd_getval(cmdmap, "weight", w)) {
      ss << "unable to parse weight value '"
	 << cmd_vartype_stringify(cmdmap.at("weight")) << "'";
      err = -EINVAL;
      goto reply_no_propose;
    }

    err = newcrush.adjust_item_weightf(cct, id, w,
				       g_conf()->osd_crush_update_weight_set);
    if (err < 0)
      goto reply_no_propose;
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
    ss << "reweighted item id " << id << " name '" << name << "' to " << w
       << " in crush map";
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						  get_last_committed() + 1));
    return true;
  } else if (prefix == "osd crush reweight-subtree") {
    // osd crush reweight <name> <weight>
    CrushWrapper newcrush = _get_pending_crush();

    string name;
    cmd_getval(cmdmap, "name", name);
    if (!newcrush.name_exists(name)) {
      err = -ENOENT;
      ss << "device '" << name << "' does not appear in the crush map";
      goto reply_no_propose;
    }

    int id = newcrush.get_item_id(name);
    if (id >= 0) {
      ss << "device '" << name << "' is not a subtree in the crush map";
      err = -EINVAL;
      goto reply_no_propose;
    }
    double w;
    if (!cmd_getval(cmdmap, "weight", w)) {
      ss << "unable to parse weight value '"
	 << cmd_vartype_stringify(cmdmap.at("weight")) << "'";
      err = -EINVAL;
      goto reply_no_propose;
    }

    err = newcrush.adjust_subtree_weightf(cct, id, w,
					  g_conf()->osd_crush_update_weight_set);
    if (err < 0)
      goto reply_no_propose;
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
    ss << "reweighted subtree id " << id << " name '" << name << "' to " << w
       << " in crush map";
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd crush tunables") {
    CrushWrapper newcrush = _get_pending_crush();

    err = 0;
    string profile;
    cmd_getval(cmdmap, "profile", profile);
    if (profile == "legacy" || profile == "argonaut") {
      newcrush.set_tunables_legacy();
    } else if (profile == "bobtail") {
      newcrush.set_tunables_bobtail();
    } else if (profile == "firefly") {
      newcrush.set_tunables_firefly();
    } else if (profile == "hammer") {
      newcrush.set_tunables_hammer();
    } else if (profile == "jewel") {
      newcrush.set_tunables_jewel();
    } else if (profile == "optimal") {
      newcrush.set_tunables_optimal();
    } else if (profile == "default") {
      newcrush.set_tunables_default();
    } else {
      ss << "unrecognized profile '" << profile << "'";
      err = -EINVAL;
      goto reply_no_propose;
    }

    if (!validate_crush_against_features(&newcrush, ss)) {
      err = -EINVAL;
      goto reply_no_propose;
    }

    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
    ss << "adjusted tunables profile to " << profile;
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd crush set-tunable") {
    CrushWrapper newcrush = _get_pending_crush();

    err = 0;
    string tunable;
    cmd_getval(cmdmap, "tunable", tunable);

    int64_t value = -1;
    if (!cmd_getval(cmdmap, "value", value)) {
      err = -EINVAL;
      ss << "failed to parse integer value "
	 << cmd_vartype_stringify(cmdmap.at("value"));
      goto reply_no_propose;
    }

    if (tunable == "straw_calc_version") {
      if (value != 0 && value != 1) {
	ss << "value must be 0 or 1; got " << value;
	err = -EINVAL;
	goto reply_no_propose;
      }
      newcrush.set_straw_calc_version(value);
    } else {
      ss << "unrecognized tunable '" << tunable << "'";
      err = -EINVAL;
      goto reply_no_propose;
    }

    if (!validate_crush_against_features(&newcrush, ss)) {
      err = -EINVAL;
      goto reply_no_propose;
    }

    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
    ss << "adjusted tunable " << tunable << " to " << value;
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;

  } else if (prefix == "osd crush rule create-simple") {
    string name, root, type, mode;
    cmd_getval(cmdmap, "name", name);
    cmd_getval(cmdmap, "root", root);
    cmd_getval(cmdmap, "type", type);
    cmd_getval(cmdmap, "mode", mode);
    if (mode == "")
      mode = "firstn";

    if (osdmap.crush->rule_exists(name)) {
      // The name is uniquely associated to a ruleid and the rule it contains
      // From the user point of view, the rule is more meaningfull.
      ss << "rule " << name << " already exists";
      err = 0;
      goto reply_no_propose;
    }

    CrushWrapper newcrush = _get_pending_crush();

    if (newcrush.rule_exists(name)) {
      // The name is uniquely associated to a ruleid and the rule it contains
      // From the user point of view, the rule is more meaningfull.
      ss << "rule " << name << " already exists";
      err = 0;
    } else {
      int ruleno = newcrush.add_simple_rule(name, root, type, "", mode,
					       pg_pool_t::TYPE_REPLICATED, &ss);
      if (ruleno < 0) {
	err = ruleno;
	goto reply_no_propose;
      }

      pending_inc.crush.clear();
      newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
    }
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;

  } else if (prefix == "osd crush rule create-replicated") {
    string name, root, type, device_class;
    cmd_getval(cmdmap, "name", name);
    cmd_getval(cmdmap, "root", root);
    cmd_getval(cmdmap, "type", type);
    cmd_getval(cmdmap, "class", device_class);

    if (osdmap.crush->rule_exists(name)) {
      // The name is uniquely associated to a ruleid and the rule it contains
      // From the user point of view, the rule is more meaningfull.
      ss << "rule " << name << " already exists";
      err = 0;
      goto reply_no_propose;
    }

    CrushWrapper newcrush = _get_pending_crush();

    if (newcrush.rule_exists(name)) {
      // The name is uniquely associated to a ruleid and the rule it contains
      // From the user point of view, the rule is more meaningfull.
      ss << "rule " << name << " already exists";
      err = 0;
    } else {
      int ruleno = newcrush.add_simple_rule(
	name, root, type, device_class,
	"firstn", pg_pool_t::TYPE_REPLICATED, &ss);
      if (ruleno < 0) {
	err = ruleno;
	goto reply_no_propose;
      }

      pending_inc.crush.clear();
      newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
    }
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;

  } else if (prefix == "osd erasure-code-profile rm") {
    string name;
    cmd_getval(cmdmap, "name", name);

    if (erasure_code_profile_in_use(pending_inc.new_pools, name, &ss))
      goto wait;

    if (erasure_code_profile_in_use(osdmap.pools, name, &ss)) {
      err = -EBUSY;
      goto reply_no_propose;
    }

    if (osdmap.has_erasure_code_profile(name) ||
	pending_inc.new_erasure_code_profiles.count(name)) {
      if (osdmap.has_erasure_code_profile(name)) {
	pending_inc.old_erasure_code_profiles.push_back(name);
      } else {
	dout(20) << "erasure code profile rm " << name << ": creation canceled" << dendl;
	pending_inc.new_erasure_code_profiles.erase(name);
      }

      getline(ss, rs);
      wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
							get_last_committed() + 1));
      return true;
    } else {
      ss << "erasure-code-profile " << name << " does not exist";
      err = 0;
      goto reply_no_propose;
    }

  } else if (prefix == "osd erasure-code-profile set") {
    string name;
    cmd_getval(cmdmap, "name", name);
    vector<string> profile;
    cmd_getval(cmdmap, "profile", profile);

    bool force = false;
    cmd_getval(cmdmap, "force", force);

    map<string,string> profile_map;
    err = parse_erasure_code_profile(profile, &profile_map, &ss);
    if (err)
      goto reply_no_propose;
    if (auto found = profile_map.find("crush-failure-domain");
	found != profile_map.end()) {
      const auto& failure_domain = found->second;
      int failure_domain_type = osdmap.crush->get_type_id(failure_domain);
      if (failure_domain_type < 0) {
	ss << "erasure-code-profile " << profile_map
	  << " contains an invalid failure-domain " << std::quoted(failure_domain);
	err = -EINVAL;
	goto reply_no_propose;
      }
    }

    if (profile_map.find("plugin") == profile_map.end()) {
      ss << "erasure-code-profile " << profile_map
	 << " must contain a plugin entry" << std::endl;
      err = -EINVAL;
      goto reply_no_propose;
    }
    string plugin = profile_map["plugin"];

    if (pending_inc.has_erasure_code_profile(name)) {
      dout(20) << "erasure code profile " << name << " try again" << dendl;
      goto wait;
    } else {
      err = normalize_profile(name, profile_map, force, &ss);
      if (err)
	goto reply_no_propose;

      if (osdmap.has_erasure_code_profile(name)) {
	ErasureCodeProfile existing_profile_map =
	  osdmap.get_erasure_code_profile(name);
	err = normalize_profile(name, existing_profile_map, force, &ss);
	if (err)
	  goto reply_no_propose;

	if (existing_profile_map == profile_map) {
	  err = 0;
	  goto reply_no_propose;
	}
	if (!force) {
	  err = -EPERM;
	  ss << "will not override erasure code profile " << name
	     << " because the existing profile "
	     << existing_profile_map
	     << " is different from the proposed profile "
	     << profile_map;
	  goto reply_no_propose;
	}
      }

      dout(20) << "erasure code profile set " << name << "="
	       << profile_map << dendl;
      pending_inc.set_erasure_code_profile(name, profile_map);
    }

    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
                                                      get_last_committed() + 1));
    return true;

  } else if (prefix == "osd crush rule create-erasure") {
    err = check_cluster_features(CEPH_FEATURE_CRUSH_V2, ss);
    if (err == -EAGAIN)
      goto wait;
    if (err)
      goto reply_no_propose;
    string name, poolstr;
    cmd_getval(cmdmap, "name", name);
    string profile;
    cmd_getval(cmdmap, "profile", profile);
    if (profile == "")
      profile = "default";
    if (profile == "default") {
      if (!osdmap.has_erasure_code_profile(profile)) {
	if (pending_inc.has_erasure_code_profile(profile)) {
	  dout(20) << "erasure code profile " << profile << " already pending" << dendl;
	  goto wait;
	}

	map<string,string> profile_map;
	err = osdmap.get_erasure_code_profile_default(cct,
						      profile_map,
						      &ss);
	if (err)
	  goto reply_no_propose;
	err = normalize_profile(name, profile_map, true, &ss);
	if (err)
	  goto reply_no_propose;
	dout(20) << "erasure code profile set " << profile << "="
		 << profile_map << dendl;
	pending_inc.set_erasure_code_profile(profile, profile_map);
	goto wait;
      }
    }

    int rule;
    err = crush_rule_create_erasure(name, profile, &rule, &ss);
    if (err < 0) {
      switch(err) {
      case -EEXIST: // return immediately
	ss << "rule " << name << " already exists";
	err = 0;
	goto reply_no_propose;
      case -EALREADY: // wait for pending to be proposed
	ss << "rule " << name << " already exists";
	err = 0;
	break;
      default: // non recoverable error
 	goto reply_no_propose;
      }
    } else {
      ss << "created rule " << name << " at " << rule;
    }

    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
                                                      get_last_committed() + 1));
    return true;

  } else if (prefix == "osd crush rule rm") {
    string name;
    cmd_getval(cmdmap, "name", name);

    if (!osdmap.crush->rule_exists(name)) {
      ss << "rule " << name << " does not exist";
      err = 0;
      goto reply_no_propose;
    }

    CrushWrapper newcrush = _get_pending_crush();

    if (!newcrush.rule_exists(name)) {
      ss << "rule " << name << " does not exist";
      err = 0;
    } else {
      int ruleno = newcrush.get_rule_id(name);
      ceph_assert(ruleno >= 0);

      // make sure it is not in use.
      // FIXME: this is ok in some situations, but let's not bother with that
      // complexity now.
      if (osdmap.crush_rule_in_use(ruleno)) {
	ss << "crush rule " << name << " (" << ruleno << ") is in use";
	err = -EBUSY;
	goto reply_no_propose;
      }

      err = newcrush.remove_rule(ruleno);
      if (err < 0) {
	goto reply_no_propose;
      }

      pending_inc.crush.clear();
      newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
    }
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;

  } else if (prefix == "osd crush rule rename") {
    string srcname;
    string dstname;
    cmd_getval(cmdmap, "srcname", srcname);
    cmd_getval(cmdmap, "dstname", dstname);
    if (srcname.empty() || dstname.empty()) {
      ss << "must specify both source rule name and destination rule name";
      err = -EINVAL;
      goto reply_no_propose;
    }
    if (srcname == dstname) {
      ss << "destination rule name is equal to source rule name";
      err = 0;
      goto reply_no_propose;
    }

    CrushWrapper newcrush = _get_pending_crush();
    if (!newcrush.rule_exists(srcname) && newcrush.rule_exists(dstname)) {
      // srcname does not exist and dstname already exists
      // suppose this is a replay and return success
      // (so this command is idempotent)
      ss << "already renamed to '" << dstname << "'";
      err = 0;
      goto reply_no_propose;
    }

    err = newcrush.rename_rule(srcname, dstname, &ss);
    if (err < 0) {
      // ss has reason for failure
      goto reply_no_propose;
    }
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
                               get_last_committed() + 1));
    return true;

  } else if (prefix == "osd setmaxosd") {
    int64_t newmax;
    if (!cmd_getval(cmdmap, "newmax", newmax)) {
      ss << "unable to parse 'newmax' value '"
         << cmd_vartype_stringify(cmdmap.at("newmax")) << "'";
      err = -EINVAL;
      goto reply_no_propose;
    }

    if (newmax > g_conf()->mon_max_osd) {
      err = -ERANGE;
      ss << "cannot set max_osd to " << newmax << " which is > conf.mon_max_osd ("
	 << g_conf()->mon_max_osd << ")";
      goto reply_no_propose;
    }

    // Don't allow shrinking OSD number as this will cause data loss
    // and may cause kernel crashes.
    // Note: setmaxosd sets the maximum OSD number and not the number of OSDs
    if (newmax < osdmap.get_max_osd()) {
      // Check if the OSDs exist between current max and new value.
      // If there are any OSDs exist, then don't allow shrinking number
      // of OSDs.
      for (int i = newmax; i < osdmap.get_max_osd(); i++) {
        if (osdmap.exists(i)) {
          err = -EBUSY;
          ss << "cannot shrink max_osd to " << newmax
             << " because osd." << i << " (and possibly others) still in use";
          goto reply_no_propose;
        }
      }
    }

    pending_inc.new_max_osd = newmax;
    ss << "set new max_osd = " << pending_inc.new_max_osd;
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;

  } else if (prefix == "osd set-full-ratio" ||
	     prefix == "osd set-backfillfull-ratio" ||
             prefix == "osd set-nearfull-ratio") {
    double n;
    if (!cmd_getval(cmdmap, "ratio", n)) {
      ss << "unable to parse 'ratio' value '"
         << cmd_vartype_stringify(cmdmap.at("ratio")) << "'";
      err = -EINVAL;
      goto reply_no_propose;
    }
    if (prefix == "osd set-full-ratio")
      pending_inc.new_full_ratio = n;
    else if (prefix == "osd set-backfillfull-ratio")
      pending_inc.new_backfillfull_ratio = n;
    else if (prefix == "osd set-nearfull-ratio")
      pending_inc.new_nearfull_ratio = n;
    ss << prefix << " " << n;
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd set-require-min-compat-client") {
    string v;
    cmd_getval(cmdmap, "version", v);
    ceph_release_t vno = ceph_release_from_name(v);
    if (!vno) {
      ss << "version " << v << " is not recognized";
      err = -EINVAL;
      goto reply_no_propose;
    }
    OSDMap newmap;
    newmap.deepish_copy_from(osdmap);
    newmap.apply_incremental(pending_inc);
    newmap.require_min_compat_client = vno;
    auto mvno = newmap.get_min_compat_client();
    if (vno < mvno) {
      ss << "osdmap current utilizes features that require " << mvno
	 << "; cannot set require_min_compat_client below that to " << vno;
      err = -EPERM;
      goto reply_no_propose;
    }
    bool sure = false;
    cmd_getval(cmdmap, "yes_i_really_mean_it", sure);
    if (!sure) {
      FeatureMap m;
      mon.get_combined_feature_map(&m);
      uint64_t features = ceph_release_features(to_integer<int>(vno));
      bool first = true;
      bool ok = true;
      for (int type : {
	    CEPH_ENTITY_TYPE_CLIENT,
	    CEPH_ENTITY_TYPE_MDS,
	    CEPH_ENTITY_TYPE_MGR }) {
	auto p = m.m.find(type);
	if (p == m.m.end()) {
	  continue;
	}
	for (auto& q : p->second) {
	  uint64_t missing = ~q.first & features;
	  if (missing) {
	    if (first) {
	      ss << "cannot set require_min_compat_client to " << v << ": ";
	    } else {
	      ss << "; ";
	    }
	    first = false;
	    ss << q.second << " connected " << ceph_entity_type_name(type)
	       << "(s) look like " << ceph_release_name(
		 ceph_release_from_features(q.first))
	       << " (missing 0x" << std::hex << missing << std::dec << ")";
	    ok = false;
	  }
	}
      }
      if (!ok) {
	ss << "; add --yes-i-really-mean-it to do it anyway";
	err = -EPERM;
	goto reply_no_propose;
      }
    }
    ss << "set require_min_compat_client to " << vno;
    pending_inc.new_require_min_compat_client = vno;
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
							  get_last_committed() + 1));
    return true;
  } else if (prefix == "osd pause") {
    return prepare_set_flag(op, CEPH_OSDMAP_PAUSERD | CEPH_OSDMAP_PAUSEWR);

  } else if (prefix == "osd unpause") {
    return prepare_unset_flag(op, CEPH_OSDMAP_PAUSERD | CEPH_OSDMAP_PAUSEWR);

  } else if (prefix == "osd set") {
    bool sure = false;
    cmd_getval(cmdmap, "yes_i_really_mean_it", sure);

    string key;
    cmd_getval(cmdmap, "key", key);
    if (key == "pause")
      return prepare_set_flag(op, CEPH_OSDMAP_PAUSERD | CEPH_OSDMAP_PAUSEWR);
    else if (key == "noup")
      return prepare_set_flag(op, CEPH_OSDMAP_NOUP);
    else if (key == "nodown")
      return prepare_set_flag(op, CEPH_OSDMAP_NODOWN);
    else if (key == "noout")
      return prepare_set_flag(op, CEPH_OSDMAP_NOOUT);
    else if (key == "noin")
      return prepare_set_flag(op, CEPH_OSDMAP_NOIN);
    else if (key == "nobackfill")
      return prepare_set_flag(op, CEPH_OSDMAP_NOBACKFILL);
    else if (key == "norebalance")
      return prepare_set_flag(op, CEPH_OSDMAP_NOREBALANCE);
    else if (key == "norecover")
      return prepare_set_flag(op, CEPH_OSDMAP_NORECOVER);
    else if (key == "noscrub")
      return prepare_set_flag(op, CEPH_OSDMAP_NOSCRUB);
    else if (key == "nodeep-scrub")
      return prepare_set_flag(op, CEPH_OSDMAP_NODEEP_SCRUB);
    else if (key == "notieragent")
      return prepare_set_flag(op, CEPH_OSDMAP_NOTIERAGENT);
    else if (key == "nosnaptrim")
      return prepare_set_flag(op, CEPH_OSDMAP_NOSNAPTRIM);
    else if (key == "pglog_hardlimit") {
      if (!osdmap.get_num_up_osds() && !sure) {
        ss << "Not advisable to continue since no OSDs are up. Pass "
           << "--yes-i-really-mean-it if you really wish to continue.";
        err = -EPERM;
        goto reply_no_propose;
      }
      // The release check here is required because for OSD_PGLOG_HARDLIMIT,
      // we are reusing a jewel feature bit that was retired in luminous.
      if (osdmap.require_osd_release >= ceph_release_t::luminous &&
         (HAVE_FEATURE(osdmap.get_up_osd_features(), OSD_PGLOG_HARDLIMIT)
          || sure)) {
	return prepare_set_flag(op, CEPH_OSDMAP_PGLOG_HARDLIMIT);
      } else {
	ss << "not all up OSDs have OSD_PGLOG_HARDLIMIT feature";
	err = -EPERM;
	goto reply_no_propose;
      }
    } else if (key == "noautoscale") {
      return prepare_set_flag(op, CEPH_OSDMAP_NOAUTOSCALE);
    } else {
      ss << "unrecognized flag '" << key << "'";
      err = -EINVAL;
    }

  } else if (prefix == "osd unset") {
    string key;
    cmd_getval(cmdmap, "key", key);
    if (key == "pause")
      return prepare_unset_flag(op, CEPH_OSDMAP_PAUSERD | CEPH_OSDMAP_PAUSEWR);
    else if (key == "noup")
      return prepare_unset_flag(op, CEPH_OSDMAP_NOUP);
    else if (key == "nodown")
      return prepare_unset_flag(op, CEPH_OSDMAP_NODOWN);
    else if (key == "noout")
      return prepare_unset_flag(op, CEPH_OSDMAP_NOOUT);
    else if (key == "noin")
      return prepare_unset_flag(op, CEPH_OSDMAP_NOIN);
    else if (key == "nobackfill")
      return prepare_unset_flag(op, CEPH_OSDMAP_NOBACKFILL);
    else if (key == "norebalance")
      return prepare_unset_flag(op, CEPH_OSDMAP_NOREBALANCE);
    else if (key == "norecover")
      return prepare_unset_flag(op, CEPH_OSDMAP_NORECOVER);
    else if (key == "noscrub")
      return prepare_unset_flag(op, CEPH_OSDMAP_NOSCRUB);
    else if (key == "nodeep-scrub")
      return prepare_unset_flag(op, CEPH_OSDMAP_NODEEP_SCRUB);
    else if (key == "notieragent")
      return prepare_unset_flag(op, CEPH_OSDMAP_NOTIERAGENT);
    else if (key == "nosnaptrim")
      return prepare_unset_flag(op, CEPH_OSDMAP_NOSNAPTRIM);
    else if (key == "noautoscale")
      return prepare_unset_flag(op, CEPH_OSDMAP_NOAUTOSCALE);
    else {
      ss << "unrecognized flag '" << key << "'";
      err = -EINVAL;
    }

  } else if (prefix == "osd require-osd-release") {
    string release;
    cmd_getval(cmdmap, "release", release);
    bool sure = false;
    cmd_getval(cmdmap, "yes_i_really_mean_it", sure);
    ceph_release_t rel = ceph_release_from_name(release.c_str());
    if (!rel) {
      ss << "unrecognized release " << release;
      err = -EINVAL;
      goto reply_no_propose;
    }
    if (rel == osdmap.require_osd_release) {
      // idempotent
      err = 0;
      goto reply_no_propose;
    }
    if (osdmap.require_osd_release < ceph_release_t::pacific && !sure) {
      ss << "Not advisable to continue since current 'require_osd_release' "
         << "refers to a very old Ceph release. Pass "
	 << "--yes-i-really-mean-it if you really wish to continue.";
      err = -EPERM;
      goto reply_no_propose;
    }
    if (!osdmap.get_num_up_osds() && !sure) {
      ss << "Not advisable to continue since no OSDs are up. Pass "
	 << "--yes-i-really-mean-it if you really wish to continue.";
      err = -EPERM;
      goto reply_no_propose;
    }
    if (rel == ceph_release_t::pacific) {
      if (!mon.monmap->get_required_features().contains_all(
	    ceph::features::mon::FEATURE_PACIFIC)) {
	ss << "not all mons are pacific";
	err = -EPERM;
	goto reply_no_propose;
      }
      if ((!HAVE_FEATURE(osdmap.get_up_osd_features(), SERVER_PACIFIC))
           && !sure) {
	ss << "not all up OSDs have CEPH_FEATURE_SERVER_PACIFIC feature";
	err = -EPERM;
	goto reply_no_propose;
      }
    } else if (rel == ceph_release_t::quincy) {
      if (!mon.monmap->get_required_features().contains_all(
	    ceph::features::mon::FEATURE_QUINCY)) {
	ss << "not all mons are quincy";
	err = -EPERM;
	goto reply_no_propose;
      }
      if ((!HAVE_FEATURE(osdmap.get_up_osd_features(), SERVER_QUINCY))
           && !sure) {
	ss << "not all up OSDs have CEPH_FEATURE_SERVER_QUINCY feature";
	err = -EPERM;
	goto reply_no_propose;
      }
    } else if (rel == ceph_release_t::reef) {
      if (!mon.monmap->get_required_features().contains_all(
	    ceph::features::mon::FEATURE_REEF)) {
	ss << "not all mons are reef";
	err = -EPERM;
	goto reply_no_propose;
      }
      if ((!HAVE_FEATURE(osdmap.get_up_osd_features(), SERVER_REEF))
           && !sure) {
	ss << "not all up OSDs have CEPH_FEATURE_SERVER_REEF feature";
	err = -EPERM;
	goto reply_no_propose;
      }
    } else {
      ss << "not supported for this release";
      err = -EPERM;
      goto reply_no_propose;
    }
    if (rel < osdmap.require_osd_release) {
      ss << "require_osd_release cannot be lowered once it has been set";
      err = -EPERM;
      goto reply_no_propose;
    }
    pending_inc.new_require_osd_release = rel;
    goto update;
  } else if (prefix == "osd down" ||
             prefix == "osd out" ||
             prefix == "osd in" ||
             prefix == "osd rm" ||
             prefix == "osd stop") {

    bool any = false;
    bool stop = false;
    bool verbose = true;
    bool definitely_dead = false;

    vector<string> idvec;
    cmd_getval(cmdmap, "ids", idvec);
    cmd_getval(cmdmap, "definitely_dead", definitely_dead);
    derr << "definitely_dead " << (int)definitely_dead << dendl;
    for (unsigned j = 0; j < idvec.size() && !stop; j++) {
      set<int> osds;

      // wildcard?
      if (j == 0 &&
          (idvec[0] == "any" || idvec[0] == "all" || idvec[0] == "*")) {
        if (prefix == "osd in") {
          // touch out osds only
          osdmap.get_out_existing_osds(osds);
        } else {
          osdmap.get_all_osds(osds);
        }
        stop = true;
        verbose = false; // so the output is less noisy.
      } else {
        long osd = parse_osd_id(idvec[j].c_str(), &ss);
        if (osd < 0) {
          ss << "invalid osd id" << osd;
          err = -EINVAL;
          continue;
        } else if (!osdmap.exists(osd)) {
          ss << "osd." << osd << " does not exist. ";
          continue;
        }

        osds.insert(osd);
      }

      for (auto &osd : osds) {
        if (prefix == "osd down") {
	  if (osdmap.is_down(osd)) {
            if (verbose)
	      ss << "osd." << osd << " is already down. ";
	  } else {
            pending_inc.pending_osd_state_set(osd, CEPH_OSD_UP);
	    ss << "marked down osd." << osd << ". ";
	    any = true;
	  }
	  if (definitely_dead) {
	    if (!pending_inc.new_xinfo.count(osd)) {
	      pending_inc.new_xinfo[osd] = osdmap.osd_xinfo[osd];
	    }
	    if (pending_inc.new_xinfo[osd].dead_epoch < pending_inc.epoch) {
	      any = true;
	    }
	    pending_inc.new_xinfo[osd].dead_epoch = pending_inc.epoch;
	  }
        } else if (prefix == "osd out") {
	  if (osdmap.is_out(osd)) {
            if (verbose)
	      ss << "osd." << osd << " is already out. ";
	  } else {
	    pending_inc.new_weight[osd] = CEPH_OSD_OUT;
	    if (osdmap.osd_weight[osd]) {
	      if (pending_inc.new_xinfo.count(osd) == 0) {
	        pending_inc.new_xinfo[osd] = osdmap.osd_xinfo[osd];
	      }
	      pending_inc.new_xinfo[osd].old_weight = osdmap.osd_weight[osd];
	    }
	    ss << "marked out osd." << osd << ". ";
            std::ostringstream msg;
            msg << "Client " << op->get_session()->entity_name
                << " marked osd." << osd << " out";
            if (osdmap.is_up(osd)) {
              msg << ", while it was still marked up";
            } else {
              auto period = ceph_clock_now() - down_pending_out[osd];
              msg << ", after it was down for " << int(period.sec())
                  << " seconds";
            }

            mon.clog->info() << msg.str();
	    any = true;
	  }
        } else if (prefix == "osd in") {
	  if (osdmap.is_in(osd)) {
            if (verbose)
	      ss << "osd." << osd << " is already in. ";
	  } else {
	    if (osdmap.osd_xinfo[osd].old_weight > 0) {
	      pending_inc.new_weight[osd] = osdmap.osd_xinfo[osd].old_weight;
	      if (pending_inc.new_xinfo.count(osd) == 0) {
	        pending_inc.new_xinfo[osd] = osdmap.osd_xinfo[osd];
	      }
	      pending_inc.new_xinfo[osd].old_weight = 0;
	    } else {
	      pending_inc.new_weight[osd] = CEPH_OSD_IN;
	    }
	    ss << "marked in osd." << osd << ". ";
	    any = true;
	  }
        } else if (prefix == "osd rm") {
          err = prepare_command_osd_remove(osd);

          if (err == -EBUSY) {
	    if (any)
	      ss << ", ";
            ss << "osd." << osd << " is still up; must be down before removal. ";
	  } else {
            ceph_assert(err == 0);
	    if (any) {
	      ss << ", osd." << osd;
            } else {
	      ss << "removed osd." << osd;
            }
	    any = true;
	  }
        } else if (prefix == "osd stop") {
          if (osdmap.is_stop(osd)) {
            if (verbose)
              ss << "osd." << osd << " is already stopped. ";
          } else if (osdmap.is_down(osd)) {
            pending_inc.pending_osd_state_set(osd, CEPH_OSD_STOP);
            ss << "stop down osd." << osd << ". ";
            any = true;
          } else {
            pending_inc.pending_osd_state_set(osd, CEPH_OSD_UP | CEPH_OSD_STOP);
            ss << "stop osd." << osd << ". ";
            any = true;
          }
        }
      }
    }
    if (any) {
      getline(ss, rs);
      wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, err, rs,
						get_last_committed() + 1));
      return true;
    }
  } else if (prefix == "osd set-group" ||
             prefix == "osd unset-group" ||
             prefix == "osd add-noup" ||
             prefix == "osd add-nodown" ||
             prefix == "osd add-noin" ||
             prefix == "osd add-noout" ||
             prefix == "osd rm-noup" ||
             prefix == "osd rm-nodown" ||
             prefix == "osd rm-noin" ||
             prefix == "osd rm-noout") {
    bool do_set = prefix == "osd set-group" ||
                  prefix.find("add") != string::npos;
    string flag_str;
    unsigned flags = 0;
    vector<string> who;
    if (prefix == "osd set-group" || prefix == "osd unset-group") {
      cmd_getval(cmdmap, "flags", flag_str);
      cmd_getval(cmdmap, "who", who);
      vector<string> raw_flags;
      boost::split(raw_flags, flag_str, boost::is_any_of(","));
      for (auto& f : raw_flags) {
        if (f == "noup")
          flags |= CEPH_OSD_NOUP;
        else if (f == "nodown")
          flags |= CEPH_OSD_NODOWN;
        else if (f == "noin")
          flags |= CEPH_OSD_NOIN;
        else if (f == "noout")
          flags |= CEPH_OSD_NOOUT;
        else {
          ss << "unrecognized flag '" << f << "', must be one of "
             << "{noup,nodown,noin,noout}";
          err = -EINVAL;
          goto reply_no_propose;
        }
      }
    } else {
      cmd_getval(cmdmap, "ids", who);
      if (prefix.find("noup") != string::npos)
        flags = CEPH_OSD_NOUP;
      else if (prefix.find("nodown") != string::npos)
        flags = CEPH_OSD_NODOWN;
      else if (prefix.find("noin") != string::npos)
        flags = CEPH_OSD_NOIN;
      else if (prefix.find("noout") != string::npos)
        flags = CEPH_OSD_NOOUT;
      else
        ceph_assert(0 == "Unreachable!");
    }
    if (flags == 0) {
      ss << "must specify flag(s) {noup,nodwon,noin,noout} to set/unset";
      err = -EINVAL;
      goto reply_no_propose;
    }
    if (who.empty()) {
      ss << "must specify at least one or more targets to set/unset";
      err = -EINVAL;
      goto reply_no_propose;
    }
    set<int> osds;
    set<int> crush_nodes;
    set<int> device_classes;
    for (auto& w : who) {
      if (w == "any" || w == "all" || w == "*") {
        osdmap.get_all_osds(osds);
        break;
      }
      std::stringstream ts;
      if (auto osd = parse_osd_id(w.c_str(), &ts); osd >= 0) {
        osds.insert(osd);
      } else if (osdmap.crush->name_exists(w)) {
        crush_nodes.insert(osdmap.crush->get_item_id(w));
      } else if (osdmap.crush->class_exists(w)) {
        device_classes.insert(osdmap.crush->get_class_id(w));
      } else {
        ss << "unable to parse osd id or crush node or device class: "
           << "\"" << w << "\". ";
      }
    }
    if (osds.empty() && crush_nodes.empty() && device_classes.empty()) {
      // ss has reason for failure
      err = -EINVAL;
      goto reply_no_propose;
    }
    bool any = false;
    for (auto osd : osds) {
      if (!osdmap.exists(osd)) {
        ss << "osd." << osd << " does not exist. ";
        continue;
      }
      if (do_set) {
        if (flags & CEPH_OSD_NOUP) {
          any |= osdmap.is_noup_by_osd(osd) ?
            pending_inc.pending_osd_state_clear(osd, CEPH_OSD_NOUP) :
            pending_inc.pending_osd_state_set(osd, CEPH_OSD_NOUP);
        }
        if (flags & CEPH_OSD_NODOWN) {
          any |= osdmap.is_nodown_by_osd(osd) ?
            pending_inc.pending_osd_state_clear(osd, CEPH_OSD_NODOWN) :
            pending_inc.pending_osd_state_set(osd, CEPH_OSD_NODOWN);
        }
        if (flags & CEPH_OSD_NOIN) {
          any |= osdmap.is_noin_by_osd(osd) ?
            pending_inc.pending_osd_state_clear(osd, CEPH_OSD_NOIN) :
            pending_inc.pending_osd_state_set(osd, CEPH_OSD_NOIN);
        }
        if (flags & CEPH_OSD_NOOUT) {
          any |= osdmap.is_noout_by_osd(osd) ?
            pending_inc.pending_osd_state_clear(osd, CEPH_OSD_NOOUT) :
            pending_inc.pending_osd_state_set(osd, CEPH_OSD_NOOUT);
        }
      } else {
        if (flags & CEPH_OSD_NOUP) {
          any |= osdmap.is_noup_by_osd(osd) ?
            pending_inc.pending_osd_state_set(osd, CEPH_OSD_NOUP) :
            pending_inc.pending_osd_state_clear(osd, CEPH_OSD_NOUP);
        }
        if (flags & CEPH_OSD_NODOWN) {
          any |= osdmap.is_nodown_by_osd(osd) ?
            pending_inc.pending_osd_state_set(osd, CEPH_OSD_NODOWN) :
            pending_inc.pending_osd_state_clear(osd, CEPH_OSD_NODOWN);
        }
        if (flags & CEPH_OSD_NOIN) {
          any |= osdmap.is_noin_by_osd(osd) ?
            pending_inc.pending_osd_state_set(osd, CEPH_OSD_NOIN) :
            pending_inc.pending_osd_state_clear(osd, CEPH_OSD_NOIN);
        }
        if (flags & CEPH_OSD_NOOUT) {
          any |= osdmap.is_noout_by_osd(osd) ?
            pending_inc.pending_osd_state_set(osd, CEPH_OSD_NOOUT) :
            pending_inc.pending_osd_state_clear(osd, CEPH_OSD_NOOUT);
        }
      }
    }
    for (auto& id : crush_nodes) {
      auto old_flags = osdmap.get_crush_node_flags(id);
      auto& pending_flags = pending_inc.new_crush_node_flags[id];
      pending_flags |= old_flags; // adopt existing flags first!
      if (do_set) {
        pending_flags |= flags;
      } else {
        pending_flags &= ~flags;
      }
      any = true;
    }
    for (auto& id : device_classes) {
      auto old_flags = osdmap.get_device_class_flags(id);
      auto& pending_flags = pending_inc.new_device_class_flags[id];
      pending_flags |= old_flags;
      if (do_set) {
        pending_flags |= flags;
      } else {
        pending_flags &= ~flags;
      }
      any = true;
    }
    if (any) {
      getline(ss, rs);
      wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, err, rs,
                                 get_last_committed() + 1));
      return true;
    }
  } else if (prefix == "osd pg-temp") {
    pg_t pgid;
    err = parse_pgid(cmdmap, ss, pgid);
    if (err < 0)
      goto reply_no_propose;
    if (pending_inc.new_pg_temp.count(pgid)) {
      dout(10) << __func__ << " waiting for pending update on " << pgid << dendl;
      goto wait;
    }

    vector<int64_t> id_vec;
    vector<int32_t> new_pg_temp;
    cmd_getval(cmdmap, "id", id_vec);
    if (id_vec.empty())  {
      pending_inc.new_pg_temp[pgid] = mempool::osdmap::vector<int>();
      ss << "done cleaning up pg_temp of " << pgid;
      goto update;
    }
    for (auto osd : id_vec) {
      if (!osdmap.exists(osd)) {
        ss << "osd." << osd << " does not exist";
        err = -ENOENT;
        goto reply_no_propose;
      }
      new_pg_temp.push_back(osd);
    }

    int pool_min_size = osdmap.get_pg_pool_min_size(pgid);
    if ((int)new_pg_temp.size() < pool_min_size) {
      ss << "num of osds (" << new_pg_temp.size() <<") < pool min size ("
         << pool_min_size << ")";
      err = -EINVAL;
      goto reply_no_propose;
    }

    int pool_size = osdmap.get_pg_pool_size(pgid);
    if ((int)new_pg_temp.size() > pool_size) {
      ss << "num of osds (" << new_pg_temp.size() <<") > pool size ("
         << pool_size << ")";
      err = -EINVAL;
      goto reply_no_propose;
    }

    pending_inc.new_pg_temp[pgid] = mempool::osdmap::vector<int>(
      new_pg_temp.begin(), new_pg_temp.end());
    ss << "set " << pgid << " pg_temp mapping to " << new_pg_temp;
    goto update;
  } else if (prefix == "osd primary-temp" ||
             prefix == "osd rm-primary-temp") {
    pg_t pgid;
    err = parse_pgid(cmdmap, ss, pgid);
    if (err < 0)
      goto reply_no_propose;

    int64_t osd;
    if (prefix == "osd primary-temp") {
      if (!cmd_getval(cmdmap, "id", osd)) {
        ss << "unable to parse 'id' value '"
           << cmd_vartype_stringify(cmdmap.at("id")) << "'";
        err = -EINVAL;
        goto reply_no_propose;
      }
      if (!osdmap.exists(osd)) {
        ss << "osd." << osd << " does not exist";
        err = -ENOENT;
        goto reply_no_propose;
      }
    }
    else if (prefix == "osd rm-primary-temp") {
      osd = -1;
    }
    else {
      ceph_assert(0 == "Unreachable!");
    }

    if (osdmap.require_min_compat_client != ceph_release_t::unknown &&
	osdmap.require_min_compat_client < ceph_release_t::firefly) {
      ss << "require_min_compat_client "
	 << osdmap.require_min_compat_client
	 << " < firefly, which is required for primary-temp";
      err = -EPERM;
      goto reply_no_propose;
    }

    pending_inc.new_primary_temp[pgid] = osd;
    ss << "set " << pgid << " primary_temp mapping to " << osd;
    goto update;
  } else if (prefix == "pg repeer") {
    pg_t pgid;
    err = parse_pgid(cmdmap, ss, pgid);
    if (err < 0)
      goto reply_no_propose;
    vector<int> acting;
    int primary;
    osdmap.pg_to_acting_osds(pgid, &acting, &primary);
    if (primary < 0) {
      err = -EAGAIN;
      ss << "pg currently has no primary";
      goto reply_no_propose;
    }
    if (acting.size() > 1) {
      // map to just primary; it will map back to what it wants
      pending_inc.new_pg_temp[pgid] = { primary };
    } else {
      // hmm, pick another arbitrary osd to induce a change.  Note
      // that this won't work if there is only one suitable OSD in the cluster.
      int i;
      bool done = false;
      for (i = 0; i < osdmap.get_max_osd(); ++i) {
	if (i == primary || !osdmap.is_up(i) || !osdmap.exists(i)) {
	  continue;
	}
	pending_inc.new_pg_temp[pgid] = { primary, i };
	done = true;
	break;
      }
      if (!done) {
	err = -EAGAIN;
	ss << "not enough up OSDs in the cluster to force repeer";
	goto reply_no_propose;
      }
    }
    goto update;
  } else if (prefix == "osd pg-upmap" ||
             prefix == "osd rm-pg-upmap" ||
             prefix == "osd pg-upmap-items" ||
             prefix == "osd rm-pg-upmap-items" ||
	     prefix == "osd pg-upmap-primary" ||
	     prefix == "osd rm-pg-upmap-primary") {
    enum {
      OP_PG_UPMAP,
      OP_RM_PG_UPMAP,
      OP_PG_UPMAP_ITEMS,
      OP_RM_PG_UPMAP_ITEMS,
      OP_PG_UPMAP_PRIMARY,
      OP_RM_PG_UPMAP_PRIMARY,
    } upmap_option;

    if (prefix == "osd pg-upmap") {
      upmap_option = OP_PG_UPMAP;
    } else if (prefix == "osd rm-pg-upmap") {
      upmap_option = OP_RM_PG_UPMAP;
    } else if (prefix == "osd pg-upmap-items") {
      upmap_option = OP_PG_UPMAP_ITEMS;
    } else if (prefix == "osd rm-pg-upmap-items") {
      upmap_option = OP_RM_PG_UPMAP_ITEMS;
    } else if (prefix == "osd pg-upmap-primary") {
      upmap_option = OP_PG_UPMAP_PRIMARY;
    } else if (prefix == "osd rm-pg-upmap-primary") {
      upmap_option = OP_RM_PG_UPMAP_PRIMARY;
    } else {
      ceph_abort_msg("invalid upmap option");
    }

    ceph_release_t min_release = ceph_release_t::unknown;
    string feature_name = "unknown";
    switch (upmap_option) {
    case OP_PG_UPMAP: 		// fall through
    case OP_RM_PG_UPMAP:	// fall through
    case OP_PG_UPMAP_ITEMS:	// fall through
    case OP_RM_PG_UPMAP_ITEMS:
      min_release = ceph_release_t::luminous;
      feature_name = "pg-upmap";
      break;

    case OP_PG_UPMAP_PRIMARY:	// fall through
    case OP_RM_PG_UPMAP_PRIMARY:
      min_release = ceph_release_t::reef;
      feature_name = "pg-upmap-primary";
      break;

    default:
      ceph_abort_msg("invalid upmap option");
    }
    uint64_t min_feature = CEPH_FEATUREMASK_OSDMAP_PG_UPMAP;
    string min_release_name = ceph_release_name(static_cast<int>(min_release));

    if (osdmap.require_min_compat_client < min_release) {
      ss << "min_compat_client "
	 << osdmap.require_min_compat_client
	 << " < " << min_release_name << ", which is required for " << feature_name << ". "
         << "Try 'ceph osd set-require-min-compat-client " << min_release_name << "' "
         << "before using the new interface";
      err = -EPERM;
      goto reply_no_propose;
    }

    //TODO: Should I add feature and test for upmap-primary?
    err = check_cluster_features(min_feature, ss);
    if (err == -EAGAIN)
      goto wait;
    if (err < 0)
      goto reply_no_propose;
    pg_t pgid;
    err = parse_pgid(cmdmap, ss, pgid);
    if (err < 0)
      goto reply_no_propose;
    if (pending_inc.old_pools.count(pgid.pool())) {
      ss << "pool of " << pgid << " is pending removal";
      err = -ENOENT;
      getline(ss, rs);
      wait_for_finished_proposal(op,
        new Monitor::C_Command(mon, op, err, rs, get_last_committed() + 1));
      return true;
    }

    // check pending upmap changes
    switch (upmap_option) {
    case OP_PG_UPMAP: // fall through
    case OP_RM_PG_UPMAP:
      if (pending_inc.new_pg_upmap.count(pgid) ||
          pending_inc.old_pg_upmap.count(pgid)) {
        dout(10) << __func__ << " waiting for pending update on "
                 << pgid << dendl;
        goto wait;
      }
      break;

    case OP_PG_UPMAP_PRIMARY:   // fall through
    case OP_RM_PG_UPMAP_PRIMARY:
      {
	const pg_pool_t *pt = osdmap.get_pg_pool(pgid.pool());
        if (! pt->is_replicated()) {
	  ss << "pg-upmap-primary is only supported for replicated pools";
	  err = -EINVAL;
	  goto reply_no_propose;
	}
      }
      // fall through
    case OP_PG_UPMAP_ITEMS:     // fall through
    case OP_RM_PG_UPMAP_ITEMS:  // fall through
      if (pending_inc.new_pg_upmap_items.count(pgid) ||
          pending_inc.old_pg_upmap_items.count(pgid)) {
        dout(10) << __func__ << " waiting for pending update on "
                 << pgid << dendl;
        goto wait;
      }
      break;

    default:
      ceph_abort_msg("invalid upmap option");
    }

    switch (upmap_option) {
    case OP_PG_UPMAP:
      {
        vector<int64_t> id_vec;
        if (!cmd_getval(cmdmap, "id", id_vec)) {
          ss << "unable to parse 'id' value(s) '"
             << cmd_vartype_stringify(cmdmap.at("id")) << "'";
          err = -EINVAL;
          goto reply_no_propose;
        }

        int pool_min_size = osdmap.get_pg_pool_min_size(pgid);
        if ((int)id_vec.size() < pool_min_size) {
          ss << "num of osds (" << id_vec.size() <<") < pool min size ("
             << pool_min_size << ")";
          err = -EINVAL;
          goto reply_no_propose;
        }

        int pool_size = osdmap.get_pg_pool_size(pgid);
        if ((int)id_vec.size() > pool_size) {
          ss << "num of osds (" << id_vec.size() <<") > pool size ("
             << pool_size << ")";
          err = -EINVAL;
          goto reply_no_propose;
        }

        vector<int32_t> new_pg_upmap;
        for (auto osd : id_vec) {
          if (osd != CRUSH_ITEM_NONE && !osdmap.exists(osd)) {
            ss << "osd." << osd << " does not exist";
            err = -ENOENT;
            goto reply_no_propose;
          }
          auto it = std::find(new_pg_upmap.begin(), new_pg_upmap.end(), osd);
          if (it != new_pg_upmap.end()) {
            ss << "osd." << osd << " already exists, ";
            continue;
          }
          new_pg_upmap.push_back(osd);
        }

        if (new_pg_upmap.empty()) {
          ss << "no valid upmap items(pairs) is specified";
          err = -EINVAL;
          goto reply_no_propose;
        }

        pending_inc.new_pg_upmap[pgid] = mempool::osdmap::vector<int32_t>(
          new_pg_upmap.begin(), new_pg_upmap.end());
        ss << "set " << pgid << " pg_upmap mapping to " << new_pg_upmap;
      }
      break;

    case OP_RM_PG_UPMAP:
      {
        pending_inc.old_pg_upmap.insert(pgid);
        ss << "clear " << pgid << " pg_upmap mapping";
      }
      break;

    case OP_PG_UPMAP_ITEMS:
      {
        vector<int64_t> id_vec;
        if (!cmd_getval(cmdmap, "id", id_vec)) {
          ss << "unable to parse 'id' value(s) '"
             << cmd_vartype_stringify(cmdmap.at("id")) << "'";
          err = -EINVAL;
          goto reply_no_propose;
        }

        if (id_vec.size() % 2) {
          ss << "you must specify pairs of osd ids to be remapped";
          err = -EINVAL;
          goto reply_no_propose;
        }

        int pool_size = osdmap.get_pg_pool_size(pgid);
        if ((int)(id_vec.size() / 2) > pool_size) {
          ss << "num of osd pairs (" << id_vec.size() / 2 <<") > pool size ("
             << pool_size << ")";
          err = -EINVAL;
          goto reply_no_propose;
        }

        vector<pair<int32_t,int32_t>> new_pg_upmap_items;
        ostringstream items;
        items << "[";
        for (auto p = id_vec.begin(); p != id_vec.end(); ++p) {
          int from = *p++;
          int to = *p;
          if (from == to) {
            ss << "from osd." << from << " == to osd." << to << ", ";
            continue;
          }
          if (!osdmap.exists(from)) {
            ss << "osd." << from << " does not exist";
            err = -ENOENT;
            goto reply_no_propose;
          }
          if (to != CRUSH_ITEM_NONE && !osdmap.exists(to)) {
            ss << "osd." << to << " does not exist";
            err = -ENOENT;
            goto reply_no_propose;
          }
          pair<int32_t,int32_t> entry = make_pair(from, to);
          auto it = std::find(new_pg_upmap_items.begin(),
            new_pg_upmap_items.end(), entry);
          if (it != new_pg_upmap_items.end()) {
            ss << "osd." << from << " -> osd." << to << " already exists, ";
            continue;
          }
          new_pg_upmap_items.push_back(entry);
          items << from << "->" << to << ",";
        }
        string out(items.str());
        out.resize(out.size() - 1); // drop last ','
        out += "]";

        if (new_pg_upmap_items.empty()) {
          ss << "no valid upmap items(pairs) is specified";
          err = -EINVAL;
          goto reply_no_propose;
        }

        pending_inc.new_pg_upmap_items[pgid] =
          mempool::osdmap::vector<pair<int32_t,int32_t>>(
          new_pg_upmap_items.begin(), new_pg_upmap_items.end());
        ss << "set " << pgid << " pg_upmap_items mapping to " << out;
      }
      break;

    case OP_RM_PG_UPMAP_ITEMS:
      {
        pending_inc.old_pg_upmap_items.insert(pgid);
        ss << "clear " << pgid << " pg_upmap_items mapping";
      }
      break;

    case OP_PG_UPMAP_PRIMARY:
      {
	int64_t id;
	if (!cmd_getval(cmdmap, "id", id)) {
	  ss << "invalid osd id value '"
             << cmd_vartype_stringify(cmdmap.at("id")) << "'";
	  err = -EINVAL;
	  goto reply_no_propose;
	}
        if (id != CRUSH_ITEM_NONE && !osdmap.exists(id)) {
          ss << "osd." << id << " does not exist";
          err = -ENOENT;
          goto reply_no_propose;
        }
    	vector<int> acting;
    	int primary;
    	osdmap.pg_to_acting_osds(pgid, &acting, &primary);
	if (id == primary) {
	  ss << "osd." << id << " is already primary for pg " << pgid;
	  err = -EINVAL;
	  goto reply_no_propose;
	}
	int found_idx = 0;
	for (int i = 1 ; i < (int)acting.size(); i++) {  // skip 0 on purpose
	  if (acting[i] == id) {
	    found_idx = i;
	    break;
	  }
	}
	if (found_idx == 0) {
	  ss << "osd." << id << " is not in acting set for pg " << pgid;
	  err = -EINVAL;
	  goto reply_no_propose;
	}
	vector<int> new_acting(acting);
	new_acting[found_idx] = new_acting[0];
	new_acting[0] = id;
	int pool_size = osdmap.get_pg_pool_size(pgid);
	if (osdmap.crush->verify_upmap(cct, osdmap.get_pg_pool_crush_rule(pgid),
	    pool_size, new_acting) >= 0) {
          ss << "change primary for pg " << pgid << " to osd." << id;
	}
	else {
	  ss << "can't change primary for pg " << pgid << " to osd." << id
	     << " - illegal pg after the change";
	  err = -EINVAL;
	  goto reply_no_propose;
	}
	pending_inc.new_pg_upmap_primary[pgid] = id;
	//TO-REMOVE: 
	ldout(cct, 20) << "pg " << pgid << ": set pg_upmap_primary to " << id << dendl;
      }
      break;

    case OP_RM_PG_UPMAP_PRIMARY:
      {
        pending_inc.old_pg_upmap_primary.insert(pgid);
        ss << "clear " << pgid << " pg_upmap_primary mapping";
      }
      break;

    default:
      ceph_abort_msg("invalid upmap option");
    }

    goto update;
  } else if (prefix == "osd primary-affinity") {
    int64_t id;
    if (!cmd_getval(cmdmap, "id", id)) {
      ss << "invalid osd id value '"
         << cmd_vartype_stringify(cmdmap.at("id")) << "'";
      err = -EINVAL;
      goto reply_no_propose;
    }
    double w;
    if (!cmd_getval(cmdmap, "weight", w)) {
      ss << "unable to parse 'weight' value '"
	 << cmd_vartype_stringify(cmdmap.at("weight")) << "'";
      err = -EINVAL;
      goto reply_no_propose;
    }
    long ww = (int)((double)CEPH_OSD_MAX_PRIMARY_AFFINITY*w);
    if (ww < 0L) {
      ss << "weight must be >= 0";
      err = -EINVAL;
      goto reply_no_propose;
    }
    if (osdmap.require_min_compat_client != ceph_release_t::unknown &&
	osdmap.require_min_compat_client < ceph_release_t::firefly) {
      ss << "require_min_compat_client "
	 << osdmap.require_min_compat_client
	 << " < firefly, which is required for primary-affinity";
      err = -EPERM;
      goto reply_no_propose;
    }
    if (osdmap.exists(id)) {
      pending_inc.new_primary_affinity[id] = ww;
      ss << "set osd." << id << " primary-affinity to " << w << " (" << std::ios::hex << ww << std::ios::dec << ")";
      getline(ss, rs);
      wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
                                                get_last_committed() + 1));
      return true;
    } else {
      ss << "osd." << id << " does not exist";
      err = -ENOENT;
      goto reply_no_propose;
    }
  } else if (prefix == "osd reweight") {
    int64_t id;
    if (!cmd_getval(cmdmap, "id", id)) {
      ss << "unable to parse osd id value '"
         << cmd_vartype_stringify(cmdmap.at("id")) << "'";
      err = -EINVAL;
      goto reply_no_propose;
    }
    double w;
    if (!cmd_getval(cmdmap, "weight", w)) {
      ss << "unable to parse weight value '"
         << cmd_vartype_stringify(cmdmap.at("weight")) << "'";
      err = -EINVAL;
      goto reply_no_propose;
    }
    long ww = (int)((double)CEPH_OSD_IN*w);
    if (ww < 0L) {
      ss << "weight must be >= 0";
      err = -EINVAL;
      goto reply_no_propose;
    }
    if (osdmap.exists(id)) {
      pending_inc.new_weight[id] = ww;
      ss << "reweighted osd." << id << " to " << w << " (" << std::hex << ww << std::dec << ")";
      getline(ss, rs);
      wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						get_last_committed() + 1));
      return true;
    } else {
      ss << "osd." << id << " does not exist";
      err = -ENOENT;
      goto reply_no_propose;
    }
  } else if (prefix == "osd reweightn") {
    map<int32_t, uint32_t> weights;
    err = parse_reweights(cct, cmdmap, osdmap, &weights);
    if (err) {
      ss << "unable to parse 'weights' value '"
         << cmd_vartype_stringify(cmdmap.at("weights")) << "'";
      goto reply_no_propose;
    }
    pending_inc.new_weight.insert(weights.begin(), weights.end());
    wait_for_finished_proposal(
	op,
	new Monitor::C_Command(mon, op, 0, rs, rdata, get_last_committed() + 1));
    return true;
  } else if (prefix == "osd lost") {
    int64_t id;
    if (!cmd_getval(cmdmap, "id", id)) {
      ss << "unable to parse osd id value '"
         << cmd_vartype_stringify(cmdmap.at("id")) << "'";
      err = -EINVAL;
      goto reply_no_propose;
    }
    bool sure = false;
    cmd_getval(cmdmap, "yes_i_really_mean_it", sure);
    if (!sure) {
      ss << "are you SURE?  this might mean real, permanent data loss.  pass "
	    "--yes-i-really-mean-it if you really do.";
      err = -EPERM;
      goto reply_no_propose;
    } else if (!osdmap.exists(id)) {
      ss << "osd." << id << " does not exist";
      err = -ENOENT;
      goto reply_no_propose;
    } else if (!osdmap.is_down(id)) {
      ss << "osd." << id << " is not down";
      err = -EBUSY;
      goto reply_no_propose;
    } else {
      epoch_t e = osdmap.get_info(id).down_at;
      pending_inc.new_lost[id] = e;
      ss << "marked osd lost in epoch " << e;
      getline(ss, rs);
      wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						get_last_committed() + 1));
      return true;
    }

  } else if (prefix == "osd destroy-actual" ||
	     prefix == "osd purge-actual" ||
	     prefix == "osd purge-new") {
    /* Destroying an OSD means that we don't expect to further make use of
     * the OSDs data (which may even become unreadable after this operation),
     * and that we are okay with scrubbing all its cephx keys and config-key
     * data (which may include lockbox keys, thus rendering the osd's data
     * unreadable).
     *
     * The OSD will not be removed. Instead, we will mark it as destroyed,
     * such that a subsequent call to `create` will not reuse the osd id.
     * This will play into being able to recreate the OSD, at the same
     * crush location, with minimal data movement.
     */

    // make sure authmon is writeable.
    if (!mon.authmon()->is_writeable()) {
      dout(10) << __func__ << " waiting for auth mon to be writeable for "
               << "osd destroy" << dendl;
      mon.authmon()->wait_for_writeable(op, new C_RetryMessage(this, op));
      return false;
    }

    int64_t id;
    if (!cmd_getval(cmdmap, "id", id)) {
      auto p = cmdmap.find("id");
      if (p == cmdmap.end()) {
	ss << "no osd id specified";
      } else {
	ss << "unable to parse osd id value '"
	   << cmd_vartype_stringify(cmdmap.at("id")) << "";
      }
      err = -EINVAL;
      goto reply_no_propose;
    }

    bool is_destroy = (prefix == "osd destroy-actual");
    if (!is_destroy) {
      ceph_assert("osd purge-actual" == prefix ||
	     "osd purge-new" == prefix);
    }

    bool sure = false;
    cmd_getval(cmdmap, "yes_i_really_mean_it", sure);
    if (!sure) {
      ss << "Are you SURE?  Did you verify with 'ceph osd safe-to-destroy'?  "
	 << "This will mean real, permanent data loss, as well "
         << "as deletion of cephx and lockbox keys. "
	 << "Pass --yes-i-really-mean-it if you really do.";
      err = -EPERM;
      goto reply_no_propose;
    } else if (!osdmap.exists(id)) {
      ss << "osd." << id << " does not exist";
      err = 0; // idempotent
      goto reply_no_propose;
    } else if (osdmap.is_up(id)) {
      ss << "osd." << id << " is not `down`.";
      err = -EBUSY;
      goto reply_no_propose;
    } else if (is_destroy && osdmap.is_destroyed(id)) {
      ss << "destroyed osd." << id;
      err = 0;
      goto reply_no_propose;
    }

    if (prefix == "osd purge-new" &&
	(osdmap.get_state(id) & CEPH_OSD_NEW) == 0) {
      ss << "osd." << id << " is not new";
      err = -EPERM;
      goto reply_no_propose;
    }

    bool goto_reply = false;

    paxos.plug();
    if (is_destroy) {
      err = prepare_command_osd_destroy(id, ss);
      // we checked above that it should exist.
      ceph_assert(err != -ENOENT);
    } else {
      err = prepare_command_osd_purge(id, ss);
      if (err == -ENOENT) {
        err = 0;
        ss << "osd." << id << " does not exist.";
        goto_reply = true;
      }
    }
    paxos.unplug();

    if (err < 0 || goto_reply) {
      goto reply_no_propose;
    }

    if (is_destroy) {
      ss << "destroyed osd." << id;
    } else {
      ss << "purged osd." << id;
    }

    getline(ss, rs);
    wait_for_finished_proposal(op,
        new Monitor::C_Command(mon, op, 0, rs, get_last_committed() + 1));
    force_immediate_propose();
    return true;

  } else if (prefix == "osd new") {

    // make sure authmon is writeable.
    if (!mon.authmon()->is_writeable()) {
      dout(10) << __func__ << " waiting for auth mon to be writeable for "
               << "osd new" << dendl;
      mon.authmon()->wait_for_writeable(op, new C_RetryMessage(this, op));
      return false;
    }

    // make sure kvmon is writeable.
    if (!mon.kvmon()->is_writeable()) {
      dout(10) << __func__ << " waiting for kv mon to be writeable for "
               << "osd new" << dendl;
      mon.kvmon()->wait_for_writeable(op, new C_RetryMessage(this, op));
      return false;
    }

    map<string,string> param_map;

    bufferlist bl = m->get_data();
    string param_json = bl.to_str();
    dout(20) << __func__ << " osd new json = " << param_json << dendl;

    err = get_json_str_map(param_json, ss, &param_map);
    if (err < 0)
      goto reply_no_propose;

    dout(20) << __func__ << " osd new params " << param_map << dendl;

    paxos.plug();
    err = prepare_command_osd_new(op, cmdmap, param_map, ss, f.get());
    paxos.unplug();

    if (err < 0) {
      goto reply_no_propose;
    }

    if (f) {
      f->flush(rdata);
    } else {
      rdata.append(ss);
    }

    if (err == EEXIST) {
      // idempotent operation
      err = 0;
      goto reply_no_propose;
    }

    wait_for_finished_proposal(op,
        new Monitor::C_Command(mon, op, 0, rs, rdata,
                               get_last_committed() + 1));
    force_immediate_propose();
    return true;

  } else if (prefix == "osd create") {

    // optional id provided?
    int64_t id = -1, cmd_id = -1;
    if (cmd_getval(cmdmap, "id", cmd_id)) {
      if (cmd_id < 0) {
	ss << "invalid osd id value '" << cmd_id << "'";
	err = -EINVAL;
	goto reply_no_propose;
      }
      dout(10) << " osd create got id " << cmd_id << dendl;
    }

    uuid_d uuid;
    string uuidstr;
    if (cmd_getval(cmdmap, "uuid", uuidstr)) {
      if (!uuid.parse(uuidstr.c_str())) {
        ss << "invalid uuid value '" << uuidstr << "'";
        err = -EINVAL;
        goto reply_no_propose;
      }
      // we only care about the id if we also have the uuid, to
      // ensure the operation's idempotency.
      id = cmd_id;
    }

    int32_t new_id = -1;
    err = prepare_command_osd_create(id, uuid, &new_id, ss);
    if (err < 0) {
      if (err == -EAGAIN) {
        goto wait;
      }
      // a check has failed; reply to the user.
      goto reply_no_propose;

    } else if (err == EEXIST) {
      // this is an idempotent operation; we can go ahead and reply.
      if (f) {
        f->open_object_section("created_osd");
        f->dump_int("osdid", new_id);
        f->close_section();
        f->flush(rdata);
      } else {
        ss << new_id;
        rdata.append(ss);
      }
      err = 0;
      goto reply_no_propose;
    }

    string empty_device_class;
    do_osd_create(id, uuid, empty_device_class, &new_id);

    if (f) {
      f->open_object_section("created_osd");
      f->dump_int("osdid", new_id);
      f->close_section();
      f->flush(rdata);
    } else {
      ss << new_id;
      rdata.append(ss);
    }
    wait_for_finished_proposal(op,
        new Monitor::C_Command(mon, op, 0, rs, rdata,
                               get_last_committed() + 1));
    return true;

  } else if (prefix == "osd blocklist clear" ||
	     prefix == "osd blacklist clear") {
    pending_inc.new_blocklist.clear();
    std::list<std::pair<entity_addr_t,utime_t > > blocklist;
    std::list<std::pair<entity_addr_t,utime_t > > range_b;
    osdmap.get_blocklist(&blocklist, &range_b);
    for (const auto &entry : blocklist) {
      pending_inc.old_blocklist.push_back(entry.first);
    }
    for (const auto &entry : range_b) {
      pending_inc.old_range_blocklist.push_back(entry.first);
    }
    ss << " removed all blocklist entries";
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
                                              get_last_committed() + 1));
    return true;
  } else if (prefix == "osd blocklist" ||
	     prefix == "osd blacklist") {
    string addrstr, rangestr;
    bool range = false;
    cmd_getval(cmdmap, "addr", addrstr);
    if (cmd_getval(cmdmap, "range", rangestr)) {
      if (rangestr == "range") {
	range = true;
      } else {
	ss << "Did you mean to specify \"osd blocklist range\"?";
	err = -EINVAL;
	goto reply_no_propose;
      }
    }
    entity_addr_t addr;
    if (!addr.parse(addrstr)) {
      ss << "unable to parse address " << addrstr;
      err = -EINVAL;
      goto reply_no_propose;
    }
    else {
      if (range) {
	if (!addr.maybe_cidr()) {
	  ss << "You specified a range command, but " << addr
	     << " does not parse as a CIDR range";
	  err = -EINVAL;
	  goto reply_no_propose;
	}
	addr.type = entity_addr_t::TYPE_CIDR;
	err = check_cluster_features(CEPH_FEATUREMASK_RANGE_BLOCKLIST, ss);
	if (err) {
	  goto reply_no_propose;
	}
	if ((addr.is_ipv4() && addr.get_nonce() > 32) ||
	    (addr.is_ipv6() && addr.get_nonce() > 128)) {
	  ss << "Too many bits in range for that protocol!";
	  err = -EINVAL;
	  goto reply_no_propose;
	}
      } else {
	if (osdmap.require_osd_release >= ceph_release_t::nautilus) {
	  // always blocklist type ANY
	  addr.set_type(entity_addr_t::TYPE_ANY);
	} else {
	  addr.set_type(entity_addr_t::TYPE_LEGACY);
	}
      }

      string blocklistop;
      if (!cmd_getval(cmdmap, "blocklistop", blocklistop)) {
	cmd_getval(cmdmap, "blacklistop", blocklistop);
      }
      if (blocklistop == "add") {
	utime_t expires = ceph_clock_now();
	// default one hour
	double d = cmd_getval_or<double>(cmdmap, "expire",
          g_conf()->mon_osd_blocklist_default_expire);
	expires += d;

	auto add_to_pending_blocklists = [](auto& nb, auto& ob,
					    const auto& addr,
					    const auto& expires) {
	  nb[addr] = expires;
	  // cancel any pending un-blocklisting request too
	  auto it = std::find(ob.begin(),
			      ob.end(), addr);
	  if (it != ob.end()) {
	    ob.erase(it);
	  }
	};
	if (range) {
	  add_to_pending_blocklists(pending_inc.new_range_blocklist,
				    pending_inc.old_range_blocklist,
				    addr, expires);

	} else {
	  add_to_pending_blocklists(pending_inc.new_blocklist,
				    pending_inc.old_blocklist,
				    addr, expires);
	}

	ss << "blocklisting " << addr << " until " << expires << " (" << d << " sec)";
	getline(ss, rs);
	wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						  get_last_committed() + 1));
	return true;
      } else if (blocklistop == "rm") {
	auto rm_from_pending_blocklists = [](const auto& addr,
					     auto& blocklist,
					     auto& ob, auto& pb) {
	  if (blocklist.count(addr)) {
	    ob.push_back(addr);
	    return true;
	  } else if (pb.count(addr)) {
	    pb.erase(addr);
	    return true;
	  }
	  return false;
	};
	if ((!range && rm_from_pending_blocklists(addr, osdmap.blocklist,
						  pending_inc.old_blocklist,
						  pending_inc.new_blocklist)) ||
	    (range && rm_from_pending_blocklists(addr, osdmap.range_blocklist,
						 pending_inc.old_range_blocklist,
						 pending_inc.new_range_blocklist))) {
	  ss << "un-blocklisting " << addr;
	  getline(ss, rs);
	  wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						    get_last_committed() + 1));
	  return true;
	}
	ss << addr << " isn't blocklisted";
	err = 0;
	goto reply_no_propose;
      }
    }
  } else if (prefix == "osd pool mksnap") {
    string poolstr;
    cmd_getval(cmdmap, "pool", poolstr);
    int64_t pool = osdmap.lookup_pg_pool_name(poolstr.c_str());
    if (pool < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply_no_propose;
    }
    string snapname;
    cmd_getval(cmdmap, "snap", snapname);
    const pg_pool_t *p = osdmap.get_pg_pool(pool);
    if (p->is_unmanaged_snaps_mode()) {
      ss << "pool " << poolstr << " is in unmanaged snaps mode";
      err = -EINVAL;
      goto reply_no_propose;
    } else if (p->snap_exists(snapname.c_str())) {
      ss << "pool " << poolstr << " snap " << snapname << " already exists";
      err = 0;
      goto reply_no_propose;
    } else if (p->is_tier()) {
      ss << "pool " << poolstr << " is a cache tier";
      err = -EINVAL;
      goto reply_no_propose;
    }
    pg_pool_t *pp = 0;
    if (pending_inc.new_pools.count(pool))
      pp = &pending_inc.new_pools[pool];
    if (!pp) {
      pp = &pending_inc.new_pools[pool];
      *pp = *p;
    }
    if (pp->snap_exists(snapname.c_str())) {
      ss << "pool " << poolstr << " snap " << snapname << " already exists";
    } else {
      if (const auto& fsmap = mon.mdsmon()->get_fsmap(); fsmap.pool_in_use(pool)) {
	dout(20) << "pool-level snapshots have been disabled for pools "
		    "attached to an fs - poolid:" << pool << dendl;
	err = -EOPNOTSUPP;
        goto reply_no_propose;
      }
      pp->add_snap(snapname.c_str(), ceph_clock_now());
      pp->set_snap_epoch(pending_inc.epoch);
      ss << "created pool " << poolstr << " snap " << snapname;
    }
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd pool rmsnap") {
    string poolstr;
    cmd_getval(cmdmap, "pool", poolstr);
    int64_t pool = osdmap.lookup_pg_pool_name(poolstr.c_str());
    if (pool < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply_no_propose;
    }
    string snapname;
    cmd_getval(cmdmap, "snap", snapname);
    const pg_pool_t *p = osdmap.get_pg_pool(pool);
    if (p->is_unmanaged_snaps_mode()) {
      ss << "pool " << poolstr << " is in unmanaged snaps mode";
      err = -EINVAL;
      goto reply_no_propose;
    } else if (!p->snap_exists(snapname.c_str())) {
      ss << "pool " << poolstr << " snap " << snapname << " does not exist";
      err = 0;
      goto reply_no_propose;
    }
    pg_pool_t *pp = 0;
    if (pending_inc.new_pools.count(pool))
      pp = &pending_inc.new_pools[pool];
    if (!pp) {
      pp = &pending_inc.new_pools[pool];
      *pp = *p;
    }
    snapid_t sn = pp->snap_exists(snapname.c_str());
    if (sn) {
      pp->remove_snap(sn);
      pp->set_snap_epoch(pending_inc.epoch);
      ss << "removed pool " << poolstr << " snap " << snapname;
    } else {
      ss << "already removed pool " << poolstr << " snap " << snapname;
    }
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd pool create") {
    int64_t pg_num = cmd_getval_or<int64_t>(cmdmap, "pg_num", 0);
    int64_t pg_num_min = cmd_getval_or<int64_t>(cmdmap, "pg_num_min", 0);
    int64_t pg_num_max = cmd_getval_or<int64_t>(cmdmap, "pg_num_max", 0);
    int64_t pgp_num = cmd_getval_or<int64_t>(cmdmap, "pgp_num", pg_num);
    string pool_type_str;
    cmd_getval(cmdmap, "pool_type", pool_type_str);
    if (pool_type_str.empty())
      pool_type_str = g_conf().get_val<string>("osd_pool_default_type");

    string poolstr;
    cmd_getval(cmdmap, "pool", poolstr);
    bool confirm = false;
    //confirmation may be set to true only by internal operations.
    cmd_getval(cmdmap, "yes_i_really_mean_it", confirm);
    if (poolstr[0] == '.' && !confirm) {
      ss << "pool names beginning with . are not allowed";
      err = 0;
      goto reply_no_propose;
    }
    int64_t pool_id = osdmap.lookup_pg_pool_name(poolstr);
    if (pool_id >= 0) {
      const pg_pool_t *p = osdmap.get_pg_pool(pool_id);
      if (pool_type_str != p->get_type_name()) {
	ss << "pool '" << poolstr << "' cannot change to type " << pool_type_str;
 	err = -EINVAL;
      } else {
	ss << "pool '" << poolstr << "' already exists";
	err = 0;
      }
      goto reply_no_propose;
    }

    int pool_type;
    if (pool_type_str == "replicated") {
      pool_type = pg_pool_t::TYPE_REPLICATED;
    } else if (pool_type_str == "erasure") {
      pool_type = pg_pool_t::TYPE_ERASURE;
    } else {
      ss << "unknown pool type '" << pool_type_str << "'";
      err = -EINVAL;
      goto reply_no_propose;
    }

    bool implicit_rule_creation = false;
    int64_t expected_num_objects = 0;
    string rule_name;
    cmd_getval(cmdmap, "rule", rule_name);
    string erasure_code_profile;
    cmd_getval(cmdmap, "erasure_code_profile", erasure_code_profile);

    if (pool_type == pg_pool_t::TYPE_ERASURE) {
      if (erasure_code_profile == "")
	erasure_code_profile = "default";
      //handle the erasure code profile
      if (erasure_code_profile == "default") {
	if (!osdmap.has_erasure_code_profile(erasure_code_profile)) {
	  if (pending_inc.has_erasure_code_profile(erasure_code_profile)) {
	    dout(20) << "erasure code profile " << erasure_code_profile << " already pending" << dendl;
	    goto wait;
	  }

	  map<string,string> profile_map;
	  err = osdmap.get_erasure_code_profile_default(cct,
						      profile_map,
						      &ss);
	  if (err)
	    goto reply_no_propose;
	  dout(20) << "erasure code profile " << erasure_code_profile << " set" << dendl;
	  pending_inc.set_erasure_code_profile(erasure_code_profile, profile_map);
	  goto wait;
	}
      }
      if (rule_name == "") {
	implicit_rule_creation = true;
	if (erasure_code_profile == "default") {
	  rule_name = "erasure-code";
	} else {
	  dout(1) << "implicitly use rule named after the pool: "
		<< poolstr << dendl;
	  rule_name = poolstr;
	}
      }
      expected_num_objects =
	cmd_getval_or<int64_t>(cmdmap, "expected_num_objects", 0);
    } else {
      //NOTE:for replicated pool,cmd_map will put rule_name to erasure_code_profile field
      //     and put expected_num_objects to rule field
      if (erasure_code_profile != "") { // cmd is from CLI
        if (rule_name != "") {
          string interr;
          expected_num_objects = strict_strtoll(rule_name.c_str(), 10, &interr);
          if (interr.length()) {
            ss << "error parsing integer value '" << rule_name << "': " << interr;
            err = -EINVAL;
            goto reply_no_propose;
          }
        }
        rule_name = erasure_code_profile;
      } else { // cmd is well-formed
        expected_num_objects =
	  cmd_getval_or<int64_t>(cmdmap, "expected_num_objects", 0);
      }
    }

    if (!implicit_rule_creation && rule_name != "") {
      int rule;
      err = get_crush_rule(rule_name, &rule, &ss);
      if (err == -EAGAIN) {
        goto wait;
      }
      if (err)
	goto reply_no_propose;
    }

    if (expected_num_objects < 0) {
      ss << "'expected_num_objects' must be non-negative";
      err = -EINVAL;
      goto reply_no_propose;
    }

    set<int32_t> osds;
    osdmap.get_all_osds(osds);
    bool has_filestore_osd = std::any_of(osds.begin(), osds.end(), [this](int osd) {
      string type;
      if (!get_osd_objectstore_type(osd, &type)) {
        return type == "filestore";
      } else {
        return false;
      }
    });

    if (has_filestore_osd &&
        expected_num_objects > 0 &&
        cct->_conf->filestore_merge_threshold > 0) {
      ss << "'expected_num_objects' requires 'filestore_merge_threshold < 0'";
      err = -EINVAL;
      goto reply_no_propose;
    }

    if (has_filestore_osd &&
        expected_num_objects == 0 &&
        cct->_conf->filestore_merge_threshold < 0) {
      int osds = osdmap.get_num_osds();
      bool sure = false;
      cmd_getval(cmdmap, "yes_i_really_mean_it", sure);
      if (!sure && osds && (pg_num >= 1024 || pg_num / osds >= 100)) {
        ss << "For better initial performance on pools expected to store a "
           << "large number of objects, consider supplying the "
           << "expected_num_objects parameter when creating the pool."
           << " Pass --yes-i-really-mean-it to ignore it";
        err = -EPERM;
        goto reply_no_propose;
      }
    }

    int64_t fast_read_param = cmd_getval_or<int64_t>(cmdmap, "fast_read", -1);
    FastReadType fast_read = FAST_READ_DEFAULT;
    if (fast_read_param == 0)
      fast_read = FAST_READ_OFF;
    else if (fast_read_param > 0)
      fast_read = FAST_READ_ON;

    int64_t repl_size = 0;
    cmd_getval(cmdmap, "size", repl_size);
    int64_t target_size_bytes = 0;
    double target_size_ratio = 0.0;
    cmd_getval(cmdmap, "target_size_bytes", target_size_bytes);
    cmd_getval(cmdmap, "target_size_ratio", target_size_ratio);

    string pg_autoscale_mode;
    cmd_getval(cmdmap, "autoscale_mode", pg_autoscale_mode);

    bool bulk = cmd_getval_or<bool>(cmdmap, "bulk", 0);

    bool crimson = cmd_getval_or<bool>(cmdmap, "crimson", false) ||
      cct->_conf.get_val<bool>("osd_pool_default_crimson");

    err = prepare_new_pool(poolstr,
			   -1, // default crush rule
			   rule_name,
			   pg_num, pgp_num, pg_num_min, pg_num_max,
                           repl_size, target_size_bytes, target_size_ratio,
			   erasure_code_profile, pool_type,
                           (uint64_t)expected_num_objects,
                           fast_read,
			   pg_autoscale_mode,
			   bulk,
			   crimson,
			   &ss);
    if (err < 0) {
      switch(err) {
      case -EEXIST:
	ss << "pool '" << poolstr << "' already exists";
        err = 0;
        goto reply_no_propose;
      case -EAGAIN:
        goto wait;
      case -ERANGE:
        goto reply_no_propose;
      default:
	goto reply_no_propose;
      }
    } else {
      ss << "pool '" << poolstr << "' created";
    }
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;

  } else if (prefix == "osd pool delete" ||
             prefix == "osd pool rm") {
    // osd pool delete/rm <poolname> <poolname again> --yes-i-really-really-mean-it
    string poolstr, poolstr2, sure;
    cmd_getval(cmdmap, "pool", poolstr);
    cmd_getval(cmdmap, "pool2", poolstr2);
    int64_t pool = osdmap.lookup_pg_pool_name(poolstr.c_str());
    if (pool < 0) {
      ss << "pool '" << poolstr << "' does not exist";
      err = 0;
      goto reply_no_propose;
    }

    bool force_no_fake = false;
    cmd_getval(cmdmap, "yes_i_really_really_mean_it", force_no_fake);
    bool force = false;
    cmd_getval(cmdmap, "yes_i_really_really_mean_it_not_faking", force);
    if (poolstr2 != poolstr ||
	(!force && !force_no_fake)) {
      ss << "WARNING: this will *PERMANENTLY DESTROY* all data stored in pool " << poolstr
	 << ".  If you are *ABSOLUTELY CERTAIN* that is what you want, pass the pool name *twice*, "
	 << "followed by --yes-i-really-really-mean-it.";
      err = -EPERM;
      goto reply_no_propose;
    }
    err = _prepare_remove_pool(pool, &ss, force_no_fake);
    if (err == -EAGAIN) {
      goto wait;
    }
    if (err < 0)
      goto reply_no_propose;
    goto update;
  } else if (prefix == "osd pool rename") {
    string srcpoolstr, destpoolstr;
    cmd_getval(cmdmap, "srcpool", srcpoolstr);
    cmd_getval(cmdmap, "destpool", destpoolstr);
    int64_t pool_src = osdmap.lookup_pg_pool_name(srcpoolstr.c_str());
    int64_t pool_dst = osdmap.lookup_pg_pool_name(destpoolstr.c_str());
    bool confirm = false;
    //confirmation may be set to true only by internal operations.
    cmd_getval(cmdmap, "yes_i_really_mean_it", confirm);
    if (destpoolstr[0] == '.' && !confirm) {
      ss << "pool names beginning with . are not allowed";
      err = 0;
      goto reply_no_propose;
    }
    if (pool_src < 0) {
      if (pool_dst >= 0) {
        // src pool doesn't exist, dst pool does exist: to ensure idempotency
        // of operations, assume this rename succeeded, as it is not changing
        // the current state.  Make sure we output something understandable
        // for whoever is issuing the command, if they are paying attention,
        // in case it was not intentional; or to avoid a "wtf?" and a bug
        // report in case it was intentional, while expecting a failure.
        ss << "pool '" << srcpoolstr << "' does not exist; pool '"
          << destpoolstr << "' does -- assuming successful rename";
        err = 0;
      } else {
        ss << "unrecognized pool '" << srcpoolstr << "'";
        err = -ENOENT;
      }
      goto reply_no_propose;
    } else if (pool_dst >= 0) {
      // source pool exists and so does the destination pool
      ss << "pool '" << destpoolstr << "' already exists";
      err = -EEXIST;
      goto reply_no_propose;
    }

    int ret = _prepare_rename_pool(pool_src, destpoolstr);
    if (ret == 0) {
      ss << "pool '" << srcpoolstr << "' renamed to '" << destpoolstr << "'";
    } else {
      ss << "failed to rename pool '" << srcpoolstr << "' to '" << destpoolstr << "': "
        << cpp_strerror(ret);
    }
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, ret, rs,
					      get_last_committed() + 1));
    return true;

  } else if (prefix == "osd pool set") {
    err = prepare_command_pool_set(cmdmap, ss);
    if (err == -EAGAIN)
      goto wait;
    if (err < 0)
      goto reply_no_propose;

    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						   get_last_committed() + 1));
    return true;
  } else if (prefix == "osd tier add") {
    err = check_cluster_features(CEPH_FEATURE_OSD_CACHEPOOL, ss);
    if (err == -EAGAIN)
      goto wait;
    if (err)
      goto reply_no_propose;
    string poolstr;
    cmd_getval(cmdmap, "pool", poolstr);
    int64_t pool_id = osdmap.lookup_pg_pool_name(poolstr);
    if (pool_id < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply_no_propose;
    }
    string tierpoolstr;
    cmd_getval(cmdmap, "tierpool", tierpoolstr);
    int64_t tierpool_id = osdmap.lookup_pg_pool_name(tierpoolstr);
    if (tierpool_id < 0) {
      ss << "unrecognized pool '" << tierpoolstr << "'";
      err = -ENOENT;
      goto reply_no_propose;
    }
    const pg_pool_t *p = osdmap.get_pg_pool(pool_id);
    ceph_assert(p);
    const pg_pool_t *tp = osdmap.get_pg_pool(tierpool_id);
    ceph_assert(tp);

    if (!_check_become_tier(tierpool_id, tp, pool_id, p, &err, &ss)) {
      goto reply_no_propose;
    }

    // make sure new tier is empty
    bool force_nonempty = false;
    cmd_getval_compat_cephbool(cmdmap, "force_nonempty", force_nonempty);
    const pool_stat_t *pstats = mon.mgrstatmon()->get_pool_stat(tierpool_id);
    if (pstats && pstats->stats.sum.num_objects != 0 &&
	!force_nonempty) {
      ss << "tier pool '" << tierpoolstr << "' is not empty; --force-nonempty to force";
      err = -ENOTEMPTY;
      goto reply_no_propose;
    }
    if (tp->is_erasure()) {
      ss << "tier pool '" << tierpoolstr
	 << "' is an ec pool, which cannot be a tier";
      err = -ENOTSUP;
      goto reply_no_propose;
    }
    if ((!tp->removed_snaps.empty() || !tp->snaps.empty()) &&
	(!force_nonempty ||
	 !g_conf()->mon_debug_unsafe_allow_tier_with_nonempty_snaps)) {
      ss << "tier pool '" << tierpoolstr << "' has snapshot state; it cannot be added as a tier without breaking the pool";
      err = -ENOTEMPTY;
      goto reply_no_propose;
    }
    // go
    pg_pool_t *np = pending_inc.get_new_pool(pool_id, p);
    pg_pool_t *ntp = pending_inc.get_new_pool(tierpool_id, tp);
    if (np->tiers.count(tierpool_id) || ntp->is_tier()) {
      goto wait;
    }
    np->tiers.insert(tierpool_id);
    np->set_snap_epoch(pending_inc.epoch); // tier will update to our snap info
    ntp->tier_of = pool_id;
    ss << "pool '" << tierpoolstr << "' is now (or already was) a tier of '" << poolstr << "'";
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, ss.str(),
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd tier remove" ||
             prefix == "osd tier rm") {
    string poolstr;
    cmd_getval(cmdmap, "pool", poolstr);
    int64_t pool_id = osdmap.lookup_pg_pool_name(poolstr);
    if (pool_id < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply_no_propose;
    }
    string tierpoolstr;
    cmd_getval(cmdmap, "tierpool", tierpoolstr);
    int64_t tierpool_id = osdmap.lookup_pg_pool_name(tierpoolstr);
    if (tierpool_id < 0) {
      ss << "unrecognized pool '" << tierpoolstr << "'";
      err = -ENOENT;
      goto reply_no_propose;
    }
    const pg_pool_t *p = osdmap.get_pg_pool(pool_id);
    ceph_assert(p);
    const pg_pool_t *tp = osdmap.get_pg_pool(tierpool_id);
    ceph_assert(tp);

    if (!_check_remove_tier(pool_id, p, tp, &err, &ss)) {
      goto reply_no_propose;
    }

    if (p->tiers.count(tierpool_id) == 0) {
      ss << "pool '" << tierpoolstr << "' is now (or already was) not a tier of '" << poolstr << "'";
      err = 0;
      goto reply_no_propose;
    }
    if (tp->tier_of != pool_id) {
      ss << "tier pool '" << tierpoolstr << "' is a tier of '"
         << osdmap.get_pool_name(tp->tier_of) << "': "
         // be scary about it; this is an inconsistency and bells must go off
         << "THIS SHOULD NOT HAVE HAPPENED AT ALL";
      err = -EINVAL;
      goto reply_no_propose;
    }
    if (p->read_tier == tierpool_id) {
      ss << "tier pool '" << tierpoolstr << "' is the overlay for '" << poolstr << "'; please remove-overlay first";
      err = -EBUSY;
      goto reply_no_propose;
    }
    // go
    pg_pool_t *np = pending_inc.get_new_pool(pool_id, p);
    pg_pool_t *ntp = pending_inc.get_new_pool(tierpool_id, tp);
    if (np->tiers.count(tierpool_id) == 0 ||
	ntp->tier_of != pool_id ||
	np->read_tier == tierpool_id) {
      goto wait;
    }
    np->tiers.erase(tierpool_id);
    ntp->clear_tier();
    ss << "pool '" << tierpoolstr << "' is now (or already was) not a tier of '" << poolstr << "'";
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, ss.str(),
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd tier set-overlay") {
    err = check_cluster_features(CEPH_FEATURE_OSD_CACHEPOOL, ss);
    if (err == -EAGAIN)
      goto wait;
    if (err)
      goto reply_no_propose;
    string poolstr;
    cmd_getval(cmdmap, "pool", poolstr);
    int64_t pool_id = osdmap.lookup_pg_pool_name(poolstr);
    if (pool_id < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply_no_propose;
    }
    string overlaypoolstr;
    cmd_getval(cmdmap, "overlaypool", overlaypoolstr);
    int64_t overlaypool_id = osdmap.lookup_pg_pool_name(overlaypoolstr);
    if (overlaypool_id < 0) {
      ss << "unrecognized pool '" << overlaypoolstr << "'";
      err = -ENOENT;
      goto reply_no_propose;
    }
    const pg_pool_t *p = osdmap.get_pg_pool(pool_id);
    ceph_assert(p);
    const pg_pool_t *overlay_p = osdmap.get_pg_pool(overlaypool_id);
    ceph_assert(overlay_p);
    if (p->tiers.count(overlaypool_id) == 0) {
      ss << "tier pool '" << overlaypoolstr << "' is not a tier of '" << poolstr << "'";
      err = -EINVAL;
      goto reply_no_propose;
    }
    if (p->read_tier == overlaypool_id) {
      err = 0;
      ss << "overlay for '" << poolstr << "' is now (or already was) '" << overlaypoolstr << "'";
      goto reply_no_propose;
    }
    if (p->has_read_tier()) {
      ss << "pool '" << poolstr << "' has overlay '"
	 << osdmap.get_pool_name(p->read_tier)
	 << "'; please remove-overlay first";
      err = -EINVAL;
      goto reply_no_propose;
    }

    // go
    pg_pool_t *np = pending_inc.get_new_pool(pool_id, p);
    np->read_tier = overlaypool_id;
    np->write_tier = overlaypool_id;
    np->set_last_force_op_resend(pending_inc.epoch);
    pg_pool_t *noverlay_p = pending_inc.get_new_pool(overlaypool_id, overlay_p);
    noverlay_p->set_last_force_op_resend(pending_inc.epoch);
    ss << "overlay for '" << poolstr << "' is now (or already was) '" << overlaypoolstr << "'";
    if (overlay_p->cache_mode == pg_pool_t::CACHEMODE_NONE)
      ss <<" (WARNING: overlay pool cache_mode is still NONE)";
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, ss.str(),
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd tier remove-overlay" ||
             prefix == "osd tier rm-overlay") {
    string poolstr;
    cmd_getval(cmdmap, "pool", poolstr);
    int64_t pool_id = osdmap.lookup_pg_pool_name(poolstr);
    if (pool_id < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply_no_propose;
    }
    const pg_pool_t *p = osdmap.get_pg_pool(pool_id);
    ceph_assert(p);
    if (!p->has_read_tier()) {
      err = 0;
      ss << "there is now (or already was) no overlay for '" << poolstr << "'";
      goto reply_no_propose;
    }

    if (!_check_remove_tier(pool_id, p, NULL, &err, &ss)) {
      goto reply_no_propose;
    }

    // go
    pg_pool_t *np = pending_inc.get_new_pool(pool_id, p);
    if (np->has_read_tier()) {
      const pg_pool_t *op = osdmap.get_pg_pool(np->read_tier);
      pg_pool_t *nop = pending_inc.get_new_pool(np->read_tier,op);
      nop->set_last_force_op_resend(pending_inc.epoch);
    }
    if (np->has_write_tier()) {
      const pg_pool_t *op = osdmap.get_pg_pool(np->write_tier);
      pg_pool_t *nop = pending_inc.get_new_pool(np->write_tier, op);
      nop->set_last_force_op_resend(pending_inc.epoch);
    }
    np->clear_read_tier();
    np->clear_write_tier();
    np->set_last_force_op_resend(pending_inc.epoch);
    ss << "there is now (or already was) no overlay for '" << poolstr << "'";
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, ss.str(),
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd tier cache-mode") {
    err = check_cluster_features(CEPH_FEATURE_OSD_CACHEPOOL, ss);
    if (err == -EAGAIN)
      goto wait;
    if (err)
      goto reply_no_propose;
    string poolstr;
    cmd_getval(cmdmap, "pool", poolstr);
    int64_t pool_id = osdmap.lookup_pg_pool_name(poolstr);
    if (pool_id < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply_no_propose;
    }
    const pg_pool_t *p = osdmap.get_pg_pool(pool_id);
    ceph_assert(p);
    if (!p->is_tier()) {
      ss << "pool '" << poolstr << "' is not a tier";
      err = -EINVAL;
      goto reply_no_propose;
    }
    string modestr;
    cmd_getval(cmdmap, "mode", modestr);
    pg_pool_t::cache_mode_t mode = pg_pool_t::get_cache_mode_from_str(modestr);
    if (int(mode) < 0) {
      ss << "'" << modestr << "' is not a valid cache mode";
      err = -EINVAL;
      goto reply_no_propose;
    }

    bool sure = false;
    cmd_getval(cmdmap, "yes_i_really_mean_it", sure);

    if (mode == pg_pool_t::CACHEMODE_FORWARD ||
	mode == pg_pool_t::CACHEMODE_READFORWARD) {
      ss << "'" << modestr << "' is no longer a supported cache mode";
      err = -EPERM;
      goto reply_no_propose;
    }
    if ((mode != pg_pool_t::CACHEMODE_WRITEBACK &&
	 mode != pg_pool_t::CACHEMODE_NONE &&
	 mode != pg_pool_t::CACHEMODE_PROXY &&
	 mode != pg_pool_t::CACHEMODE_READPROXY) &&
	 !sure) {
      ss << "'" << modestr << "' is not a well-supported cache mode and may "
	 << "corrupt your data.  pass --yes-i-really-mean-it to force.";
      err = -EPERM;
      goto reply_no_propose;
    }

    // pool already has this cache-mode set and there are no pending changes
    if (p->cache_mode == mode &&
	(pending_inc.new_pools.count(pool_id) == 0 ||
	 pending_inc.new_pools[pool_id].cache_mode == p->cache_mode)) {
      ss << "set cache-mode for pool '" << poolstr << "'"
         << " to " << pg_pool_t::get_cache_mode_name(mode);
      err = 0;
      goto reply_no_propose;
    }

    /* Mode description:
     *
     *  none:       No cache-mode defined
     *  forward:    Forward all reads and writes to base pool [removed]
     *  writeback:  Cache writes, promote reads from base pool
     *  readonly:   Forward writes to base pool
     *  readforward: Writes are in writeback mode, Reads are in forward mode [removed]
     *  proxy:       Proxy all reads and writes to base pool
     *  readproxy:   Writes are in writeback mode, Reads are in proxy mode
     *
     * Hence, these are the allowed transitions:
     *
     *  none -> any
     *  forward -> proxy || readforward || readproxy || writeback || any IF num_objects_dirty == 0
     *  proxy -> readproxy || writeback || any IF num_objects_dirty == 0
     *  readforward -> forward || proxy || readproxy || writeback || any IF num_objects_dirty == 0
     *  readproxy -> proxy || writeback || any IF num_objects_dirty == 0
     *  writeback -> readproxy || proxy
     *  readonly -> any
     */

    // We check if the transition is valid against the current pool mode, as
    // it is the only committed state thus far.  We will blantly squash
    // whatever mode is on the pending state.

    if (p->cache_mode == pg_pool_t::CACHEMODE_WRITEBACK &&
        (mode != pg_pool_t::CACHEMODE_PROXY &&
	  mode != pg_pool_t::CACHEMODE_READPROXY)) {
      ss << "unable to set cache-mode '" << pg_pool_t::get_cache_mode_name(mode)
         << "' on a '" << pg_pool_t::get_cache_mode_name(p->cache_mode)
         << "' pool; only '"
         << pg_pool_t::get_cache_mode_name(pg_pool_t::CACHEMODE_PROXY)
         << "','"
         << pg_pool_t::get_cache_mode_name(pg_pool_t::CACHEMODE_READPROXY)
        << "' allowed.";
      err = -EINVAL;
      goto reply_no_propose;
    }
    if ((p->cache_mode == pg_pool_t::CACHEMODE_READFORWARD &&
        (mode != pg_pool_t::CACHEMODE_WRITEBACK &&
	  mode != pg_pool_t::CACHEMODE_PROXY &&
	  mode != pg_pool_t::CACHEMODE_READPROXY)) ||

        (p->cache_mode == pg_pool_t::CACHEMODE_READPROXY &&
        (mode != pg_pool_t::CACHEMODE_WRITEBACK &&
	  mode != pg_pool_t::CACHEMODE_PROXY)) ||

        (p->cache_mode == pg_pool_t::CACHEMODE_PROXY &&
        (mode != pg_pool_t::CACHEMODE_WRITEBACK &&
	  mode != pg_pool_t::CACHEMODE_READPROXY)) ||

        (p->cache_mode == pg_pool_t::CACHEMODE_FORWARD &&
        (mode != pg_pool_t::CACHEMODE_WRITEBACK &&
	  mode != pg_pool_t::CACHEMODE_PROXY &&
	  mode != pg_pool_t::CACHEMODE_READPROXY))) {

      const pool_stat_t* pstats =
        mon.mgrstatmon()->get_pool_stat(pool_id);

      if (pstats && pstats->stats.sum.num_objects_dirty > 0) {
        ss << "unable to set cache-mode '"
           << pg_pool_t::get_cache_mode_name(mode) << "' on pool '" << poolstr
           << "': dirty objects found";
        err = -EBUSY;
        goto reply_no_propose;
      }
    }
    // go
    pg_pool_t *np = pending_inc.get_new_pool(pool_id, p);
    np->cache_mode = mode;
    // set this both when moving to and from cache_mode NONE.  this is to
    // capture legacy pools that were set up before this flag existed.
    np->flags |= pg_pool_t::FLAG_INCOMPLETE_CLONES;
    ss << "set cache-mode for pool '" << poolstr
	<< "' to " << pg_pool_t::get_cache_mode_name(mode);
    if (mode == pg_pool_t::CACHEMODE_NONE) {
      const pg_pool_t *base_pool = osdmap.get_pg_pool(np->tier_of);
      ceph_assert(base_pool);
      if (base_pool->read_tier == pool_id ||
	  base_pool->write_tier == pool_id)
	ss <<" (WARNING: pool is still configured as read or write tier)";
    }
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, ss.str(),
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd tier add-cache") {
    err = check_cluster_features(CEPH_FEATURE_OSD_CACHEPOOL, ss);
    if (err == -EAGAIN)
      goto wait;
    if (err)
      goto reply_no_propose;
    string poolstr;
    cmd_getval(cmdmap, "pool", poolstr);
    int64_t pool_id = osdmap.lookup_pg_pool_name(poolstr);
    if (pool_id < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply_no_propose;
    }
    string tierpoolstr;
    cmd_getval(cmdmap, "tierpool", tierpoolstr);
    int64_t tierpool_id = osdmap.lookup_pg_pool_name(tierpoolstr);
    if (tierpool_id < 0) {
      ss << "unrecognized pool '" << tierpoolstr << "'";
      err = -ENOENT;
      goto reply_no_propose;
    }
    const pg_pool_t *p = osdmap.get_pg_pool(pool_id);
    ceph_assert(p);
    const pg_pool_t *tp = osdmap.get_pg_pool(tierpool_id);
    ceph_assert(tp);

    if (!_check_become_tier(tierpool_id, tp, pool_id, p, &err, &ss)) {
      goto reply_no_propose;
    }

    int64_t size = 0;
    if (!cmd_getval(cmdmap, "size", size)) {
      ss << "unable to parse 'size' value '"
         << cmd_vartype_stringify(cmdmap.at("size")) << "'";
      err = -EINVAL;
      goto reply_no_propose;
    }
    // make sure new tier is empty
    const pool_stat_t *pstats =
      mon.mgrstatmon()->get_pool_stat(tierpool_id);
    if (pstats && pstats->stats.sum.num_objects != 0) {
      ss << "tier pool '" << tierpoolstr << "' is not empty";
      err = -ENOTEMPTY;
      goto reply_no_propose;
    }
    auto& modestr = g_conf().get_val<string>("osd_tier_default_cache_mode");
    pg_pool_t::cache_mode_t mode = pg_pool_t::get_cache_mode_from_str(modestr);
    if (int(mode) < 0) {
      ss << "osd tier cache default mode '" << modestr << "' is not a valid cache mode";
      err = -EINVAL;
      goto reply_no_propose;
    }
    HitSet::Params hsp;
    auto& cache_hit_set_type =
      g_conf().get_val<string>("osd_tier_default_cache_hit_set_type");
    if (cache_hit_set_type == "bloom") {
      BloomHitSet::Params *bsp = new BloomHitSet::Params;
      bsp->set_fpp(g_conf().get_val<double>("osd_pool_default_hit_set_bloom_fpp"));
      hsp = HitSet::Params(bsp);
    } else if (cache_hit_set_type == "explicit_hash") {
      hsp = HitSet::Params(new ExplicitHashHitSet::Params);
    } else if (cache_hit_set_type == "explicit_object") {
      hsp = HitSet::Params(new ExplicitObjectHitSet::Params);
    } else {
      ss << "osd tier cache default hit set type '"
	 << cache_hit_set_type << "' is not a known type";
      err = -EINVAL;
      goto reply_no_propose;
    }
    // go
    pg_pool_t *np = pending_inc.get_new_pool(pool_id, p);
    pg_pool_t *ntp = pending_inc.get_new_pool(tierpool_id, tp);
    if (np->tiers.count(tierpool_id) || ntp->is_tier()) {
      goto wait;
    }
    np->tiers.insert(tierpool_id);
    np->read_tier = np->write_tier = tierpool_id;
    np->set_snap_epoch(pending_inc.epoch); // tier will update to our snap info
    np->set_last_force_op_resend(pending_inc.epoch);
    ntp->set_last_force_op_resend(pending_inc.epoch);
    ntp->tier_of = pool_id;
    ntp->cache_mode = mode;
    ntp->hit_set_count = g_conf().get_val<uint64_t>("osd_tier_default_cache_hit_set_count");
    ntp->hit_set_period = g_conf().get_val<uint64_t>("osd_tier_default_cache_hit_set_period");
    ntp->min_read_recency_for_promote = g_conf().get_val<uint64_t>("osd_tier_default_cache_min_read_recency_for_promote");
    ntp->min_write_recency_for_promote = g_conf().get_val<uint64_t>("osd_tier_default_cache_min_write_recency_for_promote");
    ntp->hit_set_grade_decay_rate = g_conf().get_val<uint64_t>("osd_tier_default_cache_hit_set_grade_decay_rate");
    ntp->hit_set_search_last_n = g_conf().get_val<uint64_t>("osd_tier_default_cache_hit_set_search_last_n");
    ntp->hit_set_params = hsp;
    ntp->target_max_bytes = size;
    ss << "pool '" << tierpoolstr << "' is now (or already was) a cache tier of '" << poolstr << "'";
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, ss.str(),
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd pool set-quota") {
    string poolstr;
    cmd_getval(cmdmap, "pool", poolstr);
    int64_t pool_id = osdmap.lookup_pg_pool_name(poolstr);
    if (pool_id < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply_no_propose;
    }

    string field;
    cmd_getval(cmdmap, "field", field);
    if (field != "max_objects" && field != "max_bytes") {
      ss << "unrecognized field '" << field << "'; should be 'max_bytes' or 'max_objects'";
      err = -EINVAL;
      goto reply_no_propose;
    }

    // val could contain unit designations, so we treat as a string
    string val;
    cmd_getval(cmdmap, "val", val);
    string tss;
    int64_t value;
    if (field == "max_objects") {
      value = strict_si_cast<uint64_t>(val, &tss);
    } else if (field == "max_bytes") {
      value = strict_iecstrtoll(val, &tss);
    } else {
      ceph_abort_msg("unrecognized option");
    }
    if (!tss.empty()) {
      ss << "error parsing value '" << val << "': " << tss;
      err = -EINVAL;
      goto reply_no_propose;
    }

    pg_pool_t *pi = pending_inc.get_new_pool(pool_id, osdmap.get_pg_pool(pool_id));
    if (field == "max_objects") {
      pi->quota_max_objects = value;
    } else if (field == "max_bytes") {
      pi->quota_max_bytes = value;
    } else {
      ceph_abort_msg("unrecognized option");
    }
    ss << "set-quota " << field << " = " << value << " for pool " << poolstr;
    rs = ss.str();
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd pool application enable" ||
             prefix == "osd pool application disable" ||
             prefix == "osd pool application set" ||
             prefix == "osd pool application rm") {
    err = prepare_command_pool_application(prefix, cmdmap, ss);
    if (err == -EAGAIN) {
      goto wait;
    } else if (err < 0) {
      goto reply_no_propose;
    } else {
      goto update;
    }
  } else if (prefix == "osd force-create-pg") {
    pg_t pgid;
    string pgidstr;
    err = parse_pgid(cmdmap, ss, pgid, pgidstr);
    if (err < 0)
      goto reply_no_propose;
    bool sure = false;
    cmd_getval(cmdmap, "yes_i_really_mean_it", sure);
    if (!sure) {
      ss << "This command will recreate a lost (as in data lost) PG with data in it, such "
	 << "that the cluster will give up ever trying to recover the lost data.  Do this "
	 << "only if you are certain that all copies of the PG are in fact lost and you are "
	 << "willing to accept that the data is permanently destroyed.  Pass "
	 << "--yes-i-really-mean-it to proceed.";
      err = -EPERM;
      goto reply_no_propose;
    }
    bool creating_now;
    {
      std::lock_guard<std::mutex> l(creating_pgs_lock);
      auto emplaced = creating_pgs.pgs.emplace(
	pgid,
	creating_pgs_t::pg_create_info(osdmap.get_epoch(),
				       ceph_clock_now()));
      creating_now = emplaced.second;
    }
    if (creating_now) {
      ss << "pg " << pgidstr << " now creating, ok";
      // set the pool's CREATING flag so that (1) the osd won't ignore our
      // create message and (2) we won't propose any future pg_num changes
      // until after the PG has been instantiated.
      if (pending_inc.new_pools.count(pgid.pool()) == 0) {
	pending_inc.new_pools[pgid.pool()] = *osdmap.get_pg_pool(pgid.pool());
      }
      pending_inc.new_pools[pgid.pool()].flags |= pg_pool_t::FLAG_CREATING;
      err = 0;
      goto update;
    } else {
      ss << "pg " << pgid << " already creating";
      err = 0;
      goto reply_no_propose;
    }
  } else if (prefix == "osd force_healthy_stretch_mode") {
    bool sure = false;
    cmd_getval(cmdmap, "yes_i_really_mean_it", sure);
    if (!sure) {
      ss << "This command will require peering across multiple CRUSH buckets "
	"(probably two data centers or availability zones?) and may result in PGs "
	"going inactive until backfilling is complete. Pass --yes-i-really-mean-it to proceed.";
      err = -EPERM;
      goto reply_no_propose;
    }
    try_end_recovery_stretch_mode(true);
    ss << "Triggering healthy stretch mode";
    err = 0;
    goto reply_no_propose;
  } else if (prefix == "osd force_recovery_stretch_mode") {
    bool sure = false;
    cmd_getval(cmdmap, "yes_i_really_mean_it", sure);
    if (!sure) {
      ss << "This command will increase pool sizes to try and spread them "
	"across multiple CRUSH buckets (probably two data centers or "
	"availability zones?) and should have happened automatically"
	"Pass --yes-i-really-mean-it to proceed.";
      err = -EPERM;
      goto reply_no_propose;
    }
    mon.go_recovery_stretch_mode();
    ss << "Triggering recovery stretch mode";
    err = 0;
    goto reply_no_propose;
  } else if (prefix == "osd set-allow-crimson") {

    bool sure = false;
    cmd_getval(cmdmap, "yes_i_really_mean_it", sure);

    bool experimental_enabled =
      g_ceph_context->check_experimental_feature_enabled("crimson");
    if (!sure || !experimental_enabled) {
      ss << "This command will allow usage of crimson-osd osd daemons.  "
	 << "crimson-osd is not considered stable and will likely cause "
	 << "crashes or data corruption.  At this time, crimson-osd is mainly "
	 << "useful for performance evaluation, testing, and development.  "
	 << "If you are sure, add --yes-i-really-mean-it and add 'crimson' to "
	 << "the experimental features config.  This setting is irrevocable.";
      err = -EPERM;
      goto reply_no_propose;
    }

    err = 0;
    if (osdmap.get_allow_crimson()) {
      goto reply_no_propose;
    } else {
      pending_inc.set_allow_crimson();
      goto update;
    }
  } else {
    err = -EINVAL;
  }

 reply_no_propose:
  getline(ss, rs);
  if (err < 0 && rs.length() == 0)
    rs = cpp_strerror(err);
  mon.reply_command(op, err, rs, rdata, get_last_committed());
  return false; /* nothing to propose */

 update:
  getline(ss, rs);
  wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					    get_last_committed() + 1));
  return true;

 wait:
  // XXX
  // Some osd commands split changes across two epochs.
  // It seems this is mostly for crush rule changes. It doesn't need
  // to be this way but it's a bit of work to fix that. For now,
  // trigger a proposal by returning true and then retry the command
  // to complete the operation.
  wait_for_finished_proposal(op, new C_RetryMessage(this, op));
  return true;
}

bool OSDMonitor::enforce_pool_op_caps(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);

  auto m = op->get_req<MPoolOp>();
  MonSession *session = op->get_session();
  if (!session) {
    _pool_op_reply(op, -EPERM, osdmap.get_epoch());
    return true;
  }

  switch (m->op) {
  case POOL_OP_CREATE_UNMANAGED_SNAP:
  case POOL_OP_DELETE_UNMANAGED_SNAP:
    {
      const std::string* pool_name = nullptr;
      const pg_pool_t *pg_pool = osdmap.get_pg_pool(m->pool);
      if (pg_pool != nullptr) {
        pool_name = &osdmap.get_pool_name(m->pool);
      }

      if (!is_unmanaged_snap_op_permitted(cct, mon.key_server,
                                          session->entity_name, session->caps,
					  session->get_peer_socket_addr(),
                                          pool_name)) {
        dout(0) << "got unmanaged-snap pool op from entity with insufficient "
                << "privileges. message: " << *m  << std::endl
                << "caps: " << session->caps << dendl;
        _pool_op_reply(op, -EPERM, osdmap.get_epoch());
        return true;
      }
    }
    break;
  default:
    if (!session->is_capable("osd", MON_CAP_W)) {
      dout(0) << "got pool op from entity with insufficient privileges. "
              << "message: " << *m  << std::endl
              << "caps: " << session->caps << dendl;
      _pool_op_reply(op, -EPERM, osdmap.get_epoch());
      return true;
    }
    break;
  }

  return false;
}

bool OSDMonitor::preprocess_pool_op(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MPoolOp>();

  if (enforce_pool_op_caps(op)) {
    return true;
  }

  if (m->fsid != mon.monmap->fsid) {
    dout(0) << __func__ << " drop message on fsid " << m->fsid
            << " != " << mon.monmap->fsid << " for " << *m << dendl;
    _pool_op_reply(op, -EINVAL, osdmap.get_epoch());
    return true;
  }

  if (m->op == POOL_OP_CREATE)
    return preprocess_pool_op_create(op);

  const pg_pool_t *p = osdmap.get_pg_pool(m->pool);
  if (p == nullptr) {
    dout(10) << "attempt to operate on non-existent pool id " << m->pool << dendl;
    if (m->op == POOL_OP_DELETE) {
      _pool_op_reply(op, 0, osdmap.get_epoch());
    } else {
      _pool_op_reply(op, -ENOENT, osdmap.get_epoch());
    }
    return true;
  }

  // check if the snap and snapname exist
  bool snap_exists = false;
  if (p->snap_exists(m->name.c_str()))
    snap_exists = true;

  switch (m->op) {
  case POOL_OP_CREATE_SNAP:
    if (p->is_unmanaged_snaps_mode() || p->is_tier()) {
      _pool_op_reply(op, -EINVAL, osdmap.get_epoch());
      return true;
    }
    if (snap_exists) {
      _pool_op_reply(op, 0, osdmap.get_epoch());
      return true;
    }
    return false;
  case POOL_OP_CREATE_UNMANAGED_SNAP:
    if (p->is_pool_snaps_mode()) {
      _pool_op_reply(op, -EINVAL, osdmap.get_epoch());
      return true;
    }
    return false;
  case POOL_OP_DELETE_SNAP:
    if (p->is_unmanaged_snaps_mode()) {
      _pool_op_reply(op, -EINVAL, osdmap.get_epoch());
      return true;
    }
    if (!snap_exists) {
      _pool_op_reply(op, 0, osdmap.get_epoch());
      return true;
    }
    return false;
  case POOL_OP_DELETE_UNMANAGED_SNAP:
    if (p->is_pool_snaps_mode()) {
      _pool_op_reply(op, -EINVAL, osdmap.get_epoch());
      return true;
    }
    if (_is_removed_snap(m->pool, m->snapid)) {
      _pool_op_reply(op, 0, osdmap.get_epoch());
      return true;
    }
    return false;
  case POOL_OP_DELETE:
    if (osdmap.lookup_pg_pool_name(m->name.c_str()) >= 0) {
      _pool_op_reply(op, 0, osdmap.get_epoch());
      return true;
    }
    return false;
  case POOL_OP_AUID_CHANGE:
    return false;
  default:
    ceph_abort();
    break;
  }

  return false;
}

bool OSDMonitor::_is_removed_snap(int64_t pool, snapid_t snap)
{
  if (!osdmap.have_pg_pool(pool)) {
    dout(10) << __func__ << " pool " << pool << " snap " << snap
	     << " - pool dne" << dendl;
    return true;
  }
  if (osdmap.in_removed_snaps_queue(pool, snap)) {
    dout(10) << __func__ << " pool " << pool << " snap " << snap
	     << " - in osdmap removed_snaps_queue" << dendl;
    return true;
  }
  snapid_t begin, end;
  int r = lookup_purged_snap(pool, snap, &begin, &end);
  if (r == 0) {
    dout(10) << __func__ << " pool " << pool << " snap " << snap
	     << " - purged, [" << begin << "," << end << ")" << dendl;
    return true;
  }
  return false;
}

bool OSDMonitor::_is_pending_removed_snap(int64_t pool, snapid_t snap)
{
  if (pending_inc.old_pools.count(pool)) {
    dout(10) << __func__ << " pool " << pool << " snap " << snap
	     << " - pool pending deletion" << dendl;
    return true;
  }
  if (pending_inc.in_new_removed_snaps(pool, snap)) {
    dout(10) << __func__ << " pool " << pool << " snap " << snap
	     << " - in pending new_removed_snaps" << dendl;
    return true;
  }
  return false;
}

bool OSDMonitor::preprocess_pool_op_create(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MPoolOp>();
  int64_t pool = osdmap.lookup_pg_pool_name(m->name.c_str());
  if (pool >= 0) {
    _pool_op_reply(op, 0, osdmap.get_epoch());
    return true;
  }

  return false;
}

bool OSDMonitor::prepare_pool_op(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MPoolOp>();
  dout(10) << "prepare_pool_op " << *m << dendl;
  if (m->op == POOL_OP_CREATE) {
    return prepare_pool_op_create(op);
  } else if (m->op == POOL_OP_DELETE) {
    return prepare_pool_op_delete(op);
  }

  int ret = 0;
  bool changed = false;

  if (!osdmap.have_pg_pool(m->pool)) {
    _pool_op_reply(op, -ENOENT, osdmap.get_epoch());
    return false;
  }

  const pg_pool_t *pool = osdmap.get_pg_pool(m->pool);

  if (m->op == POOL_OP_CREATE_SNAP ||
      m->op == POOL_OP_CREATE_UNMANAGED_SNAP) {
    if (const auto& fsmap = mon.mdsmon()->get_fsmap(); fsmap.pool_in_use(m->pool)) {
      dout(20) << "monitor-managed snapshots have been disabled for pools "
		  " attached to an fs - pool:" << m->pool << dendl;
      _pool_op_reply(op, -EOPNOTSUPP, osdmap.get_epoch());
      return false;
    }
  }

  switch (m->op) {
    case POOL_OP_CREATE_SNAP:
      if (pool->is_tier()) {
        ret = -EINVAL;
        _pool_op_reply(op, ret, osdmap.get_epoch());
        return false;
      }  // else, fall through
    case POOL_OP_DELETE_SNAP:
      if (!pool->is_unmanaged_snaps_mode()) {
        bool snap_exists = pool->snap_exists(m->name.c_str());
        if ((m->op == POOL_OP_CREATE_SNAP && snap_exists)
          || (m->op == POOL_OP_DELETE_SNAP && !snap_exists)) {
          ret = 0;
        } else {
          break;
        }
      } else {
        ret = -EINVAL;
      }
      _pool_op_reply(op, ret, osdmap.get_epoch());
      return false;

    case POOL_OP_DELETE_UNMANAGED_SNAP:
      // we won't allow removal of an unmanaged snapshot from a pool
      // not in unmanaged snaps mode.
      if (!pool->is_unmanaged_snaps_mode()) {
        _pool_op_reply(op, -ENOTSUP, osdmap.get_epoch());
        return false;
      }
      /* fall-thru */
    case POOL_OP_CREATE_UNMANAGED_SNAP:
      // but we will allow creating an unmanaged snapshot on any pool
      // as long as it is not in 'pool' snaps mode.
      if (pool->is_pool_snaps_mode()) {
        _pool_op_reply(op, -EINVAL, osdmap.get_epoch());
        return false;
      }
  }

  // projected pool info
  pg_pool_t pp;
  if (pending_inc.new_pools.count(m->pool))
    pp = pending_inc.new_pools[m->pool];
  else
    pp = *osdmap.get_pg_pool(m->pool);

  bufferlist reply_data;

  // pool snaps vs unmanaged snaps are mutually exclusive
  switch (m->op) {
  case POOL_OP_CREATE_SNAP:
  case POOL_OP_DELETE_SNAP:
    if (pp.is_unmanaged_snaps_mode()) {
      ret = -EINVAL;
      goto out;
    }
    break;

  case POOL_OP_CREATE_UNMANAGED_SNAP:
  case POOL_OP_DELETE_UNMANAGED_SNAP:
    if (pp.is_pool_snaps_mode()) {
      ret = -EINVAL;
      goto out;
    }
  }

  switch (m->op) {
  case POOL_OP_CREATE_SNAP:
    if (!pp.snap_exists(m->name.c_str())) {
      pp.add_snap(m->name.c_str(), ceph_clock_now());
      dout(10) << "create snap in pool " << m->pool << " " << m->name
	       << " seq " << pp.get_snap_epoch() << dendl;
      changed = true;
    }
    break;

  case POOL_OP_DELETE_SNAP:
    {
      snapid_t s = pp.snap_exists(m->name.c_str());
      if (s) {
	pp.remove_snap(s);
	pending_inc.new_removed_snaps[m->pool].insert(s);
	changed = true;
      }
    }
    break;

  case POOL_OP_CREATE_UNMANAGED_SNAP:
    {
      uint64_t snapid = pp.add_unmanaged_snap(
	osdmap.require_osd_release < ceph_release_t::octopus);
      encode(snapid, reply_data);
      changed = true;
    }
    break;

  case POOL_OP_DELETE_UNMANAGED_SNAP:
    if (!_is_removed_snap(m->pool, m->snapid) &&
	!_is_pending_removed_snap(m->pool, m->snapid)) {
      if (m->snapid > pp.get_snap_seq()) {
        _pool_op_reply(op, -ENOENT, osdmap.get_epoch());
        return false;
      }
      pp.remove_unmanaged_snap(
	m->snapid,
	osdmap.require_osd_release < ceph_release_t::octopus);
      pending_inc.new_removed_snaps[m->pool].insert(m->snapid);
      // also record the new seq as purged: this avoids a discontinuity
      // after all of the snaps have been purged, since the seq assigned
      // during removal lives in the same namespace as the actual snaps.
      pending_pseudo_purged_snaps[m->pool].insert(pp.get_snap_seq());
      changed = true;
    }
    break;

  case POOL_OP_AUID_CHANGE:
    _pool_op_reply(op, -EOPNOTSUPP, osdmap.get_epoch());
    return false;

  default:
    ceph_abort();
    break;
  }

  if (changed) {
    pp.set_snap_epoch(pending_inc.epoch);
    pending_inc.new_pools[m->pool] = pp;
  }

 out:
  wait_for_finished_proposal(op, new OSDMonitor::C_PoolOp(this, op, ret, pending_inc.epoch, &reply_data));
  return true;
}

bool OSDMonitor::prepare_pool_op_create(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  int err = prepare_new_pool(op);
  wait_for_finished_proposal(op, new OSDMonitor::C_PoolOp(this, op, err, pending_inc.epoch));
  return true;
}

int OSDMonitor::_check_remove_pool(int64_t pool_id, const pg_pool_t& pool,
				   ostream *ss)
{
  const string& poolstr = osdmap.get_pool_name(pool_id);

  // If the Pool is in use by CephFS, refuse to delete it
  FSMap const &pending_fsmap = mon.mdsmon()->get_pending_fsmap();
  if (pending_fsmap.pool_in_use(pool_id)) {
    *ss << "pool '" << poolstr << "' is in use by CephFS";
    return -EBUSY;
  }

  if (pool.tier_of >= 0) {
    *ss << "pool '" << poolstr << "' is a tier of '"
	<< osdmap.get_pool_name(pool.tier_of) << "'";
    return -EBUSY;
  }
  if (!pool.tiers.empty()) {
    *ss << "pool '" << poolstr << "' has tiers";
    for(auto tier : pool.tiers) {
      *ss << " " << osdmap.get_pool_name(tier);
    }
    return -EBUSY;
  }

  if (!g_conf()->mon_allow_pool_delete) {
    *ss << "pool deletion is disabled; you must first set the mon_allow_pool_delete config option to true before you can destroy a pool";
    return -EPERM;
  }

  if (pool.has_flag(pg_pool_t::FLAG_NODELETE)) {
    *ss << "pool deletion is disabled; you must unset nodelete flag for the pool first";
    return -EPERM;
  }

  *ss << "pool '" << poolstr << "' removed";
  return 0;
}

/**
 * Check if it is safe to add a tier to a base pool
 *
 * @return
 * True if the operation should proceed, false if we should abort here
 * (abort doesn't necessarily mean error, could be idempotency)
 */
bool OSDMonitor::_check_become_tier(
    const int64_t tier_pool_id, const pg_pool_t *tier_pool,
    const int64_t base_pool_id, const pg_pool_t *base_pool,
    int *err,
    ostream *ss) const
{
  const std::string &tier_pool_name = osdmap.get_pool_name(tier_pool_id);
  const std::string &base_pool_name = osdmap.get_pool_name(base_pool_id);

  if (tier_pool->is_crimson()) {
    *ss << "pool '" << tier_pool_name << "' is a crimson pool, tiering "
	<< "features are not supported";
    *err = -EINVAL;
    return false;
  }
  if (base_pool->is_crimson()) {
    *ss << "pool '" << base_pool_name << "' is a crimson pool, tiering "
	<< "features are not supported";
    *err = -EINVAL;
    return false;
  }

  const FSMap &pending_fsmap = mon.mdsmon()->get_pending_fsmap();
  if (pending_fsmap.pool_in_use(tier_pool_id)) {
    *ss << "pool '" << tier_pool_name << "' is in use by CephFS";
    *err = -EBUSY;
    return false;
  }

  if (base_pool->tiers.count(tier_pool_id)) {
    ceph_assert(tier_pool->tier_of == base_pool_id);
    *err = 0;
    *ss << "pool '" << tier_pool_name << "' is now (or already was) a tier of '"
      << base_pool_name << "'";
    return false;
  }

  if (base_pool->is_tier()) {
    *ss << "pool '" << base_pool_name << "' is already a tier of '"
      << osdmap.get_pool_name(base_pool->tier_of) << "', "
      << "multiple tiers are not yet supported.";
    *err = -EINVAL;
    return false;
  }

  if (tier_pool->has_tiers()) {
    *ss << "pool '" << tier_pool_name << "' has following tier(s) already:";
    for (set<uint64_t>::iterator it = tier_pool->tiers.begin();
         it != tier_pool->tiers.end(); ++it)
      *ss << "'" << osdmap.get_pool_name(*it) << "',";
    *ss << " multiple tiers are not yet supported.";
    *err = -EINVAL;
    return false;
  }

  if (tier_pool->is_tier()) {
    *ss << "tier pool '" << tier_pool_name << "' is already a tier of '"
       << osdmap.get_pool_name(tier_pool->tier_of) << "'";
    *err = -EINVAL;
    return false;
  }

  *err = 0;
  return true;
}


/**
 * Check if it is safe to remove a tier from this base pool
 *
 * @return
 * True if the operation should proceed, false if we should abort here
 * (abort doesn't necessarily mean error, could be idempotency)
 */
bool OSDMonitor::_check_remove_tier(
    const int64_t base_pool_id, const pg_pool_t *base_pool,
    const pg_pool_t *tier_pool,
    int *err, ostream *ss) const
{
  const std::string &base_pool_name = osdmap.get_pool_name(base_pool_id);

  // Apply CephFS-specific checks
  const FSMap &pending_fsmap = mon.mdsmon()->get_pending_fsmap();
  if (pending_fsmap.pool_in_use(base_pool_id)) {
    if (base_pool->is_erasure() && !base_pool->allows_ecoverwrites()) {
      // If the underlying pool is erasure coded and does not allow EC
      // overwrites, we can't permit the removal of the replicated tier that
      // CephFS relies on to access it
      *ss << "pool '" << base_pool_name <<
          "' does not allow EC overwrites and is in use by CephFS"
          " via its tier";
      *err = -EBUSY;
      return false;
    }

    if (tier_pool && tier_pool->cache_mode == pg_pool_t::CACHEMODE_WRITEBACK) {
      *ss << "pool '" << base_pool_name << "' is in use by CephFS, and this "
             "tier is still in use as a writeback cache.  Change the cache "
             "mode and flush the cache before removing it";
      *err = -EBUSY;
      return false;
    }
  }

  *err = 0;
  return true;
}

int OSDMonitor::_prepare_remove_pool(
  int64_t pool, ostream *ss, bool no_fake)
{
  dout(10) << __func__ << " " << pool << dendl;
  const pg_pool_t *p = osdmap.get_pg_pool(pool);
  int r = _check_remove_pool(pool, *p, ss);
  if (r < 0)
    return r;

  auto new_pool = pending_inc.new_pools.find(pool);
  if (new_pool != pending_inc.new_pools.end()) {
    // if there is a problem with the pending info, wait and retry
    // this op.
    const auto& p = new_pool->second;
    int r = _check_remove_pool(pool, p, ss);
    if (r < 0)
      return -EAGAIN;
  }

  if (pending_inc.old_pools.count(pool)) {
    dout(10) << __func__ << " " << pool << " already pending removal"
	     << dendl;
    return 0;
  }

  if (g_conf()->mon_fake_pool_delete && !no_fake) {
    string old_name = osdmap.get_pool_name(pool);
    string new_name = old_name + "." + stringify(pool) + ".DELETED";
    dout(1) << __func__ << " faking pool deletion: renaming " << pool << " "
	    << old_name << " -> " << new_name << dendl;
    pending_inc.new_pool_names[pool] = new_name;
    return 0;
  }

  // remove
  pending_inc.old_pools.insert(pool);

  // remove any pg_temp mappings for this pool
  for (auto p = osdmap.pg_temp->begin();
       p != osdmap.pg_temp->end();
       ++p) {
    if (p->first.pool() == pool) {
      dout(10) << __func__ << " " << pool << " removing obsolete pg_temp "
	       << p->first << dendl;
      pending_inc.new_pg_temp[p->first].clear();
    }
  }
  // remove any primary_temp mappings for this pool
  for (auto p = osdmap.primary_temp->begin();
      p != osdmap.primary_temp->end();
      ++p) {
    if (p->first.pool() == pool) {
      dout(10) << __func__ << " " << pool
               << " removing obsolete primary_temp" << p->first << dendl;
      pending_inc.new_primary_temp[p->first] = -1;
    }
  }
  // remove any pg_upmap mappings for this pool
  for (auto& p : osdmap.pg_upmap) {
    if (p.first.pool() == pool) {
      dout(10) << __func__ << " " << pool
               << " removing obsolete pg_upmap "
               << p.first << dendl;
      pending_inc.old_pg_upmap.insert(p.first);
    }
  }
  // remove any pending pg_upmap mappings for this pool
  {
    auto it = pending_inc.new_pg_upmap.begin();
    while (it != pending_inc.new_pg_upmap.end()) {
      if (it->first.pool() == pool) {
        dout(10) << __func__ << " " << pool
                 << " removing pending pg_upmap "
                 << it->first << dendl;
        it = pending_inc.new_pg_upmap.erase(it);
      } else {
        it++;
      }
    }
  }
  // remove any pg_upmap_items mappings for this pool
  for (auto& p : osdmap.pg_upmap_items) {
    if (p.first.pool() == pool) {
      dout(10) << __func__ << " " << pool
               << " removing obsolete pg_upmap_items " << p.first
               << dendl;
      pending_inc.old_pg_upmap_items.insert(p.first);
    }
  }
  // remove any pending pg_upmap mappings for this pool
  {
    auto it = pending_inc.new_pg_upmap_items.begin();
    while (it != pending_inc.new_pg_upmap_items.end()) {
      if (it->first.pool() == pool) {
        dout(10) << __func__ << " " << pool
                 << " removing pending pg_upmap_items "
                 << it->first << dendl;
        it = pending_inc.new_pg_upmap_items.erase(it);
      } else {
        it++;
      }
    }
  }

  // remove any choose_args for this pool
  CrushWrapper newcrush = _get_pending_crush();
  if (newcrush.have_choose_args(pool)) {
    dout(10) << __func__ << " removing choose_args for pool " << pool << dendl;
    newcrush.rm_choose_args(pool);
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon.get_quorum_con_features());
  }
  return 0;
}

int OSDMonitor::_prepare_rename_pool(int64_t pool, string newname)
{
  dout(10) << "_prepare_rename_pool " << pool << dendl;
  if (pending_inc.old_pools.count(pool)) {
    dout(10) << "_prepare_rename_pool " << pool << " pending removal" << dendl;
    return -ENOENT;
  }
  for (map<int64_t,string>::iterator p = pending_inc.new_pool_names.begin();
       p != pending_inc.new_pool_names.end();
       ++p) {
    if (p->second == newname && p->first != pool) {
      return -EEXIST;
    }
  }

  pending_inc.new_pool_names[pool] = newname;
  return 0;
}

bool OSDMonitor::prepare_pool_op_delete(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MPoolOp>();
  ostringstream ss;
  int ret = _prepare_remove_pool(m->pool, &ss, false);
  if (ret == -EAGAIN) {
    wait_for_finished_proposal(op, new C_RetryMessage(this, op));
    return true;
  }
  if (ret < 0)
    dout(10) << __func__ << " got " << ret << " " << ss.str() << dendl;
  wait_for_finished_proposal(op, new OSDMonitor::C_PoolOp(this, op, ret,
						      pending_inc.epoch));
  return true;
}

void OSDMonitor::_pool_op_reply(MonOpRequestRef op,
                                int ret, epoch_t epoch, bufferlist *blp)
{
  op->mark_osdmon_event(__func__);
  auto m = op->get_req<MPoolOp>();
  dout(20) << "_pool_op_reply " << ret << dendl;
  MPoolOpReply *reply = new MPoolOpReply(m->fsid, m->get_tid(),
					 ret, epoch, get_last_committed(), blp);
  mon.send_reply(op, reply);
}

void OSDMonitor::convert_pool_priorities(void)
{
  pool_opts_t::key_t key = pool_opts_t::get_opt_desc("recovery_priority").key;
  int64_t max_prio = 0;
  int64_t min_prio = 0;
  for (const auto &i : osdmap.get_pools()) {
    const auto &pool = i.second;

    if (pool.opts.is_set(key)) {
      int64_t prio = 0;
      pool.opts.get(key, &prio);
      if (prio > max_prio)
	max_prio = prio;
      if (prio < min_prio)
	min_prio = prio;
    }
  }
  if (max_prio <= OSD_POOL_PRIORITY_MAX && min_prio >= OSD_POOL_PRIORITY_MIN) {
    dout(20) << __func__ << " nothing to fix" << dendl;
    return;
  }
  // Current pool priorities exceeds new maximum
  for (const auto &i : osdmap.get_pools()) {
    const auto pool_id = i.first;
    pg_pool_t pool = i.second;

    int64_t prio = 0;
    pool.opts.get(key, &prio);
    int64_t n;

    if (prio > 0 && max_prio > OSD_POOL_PRIORITY_MAX) { // Likely scenario
      // Scaled priority range 0 to OSD_POOL_PRIORITY_MAX
      n = (float)prio / max_prio * OSD_POOL_PRIORITY_MAX;
    } else if (prio < 0 && min_prio < OSD_POOL_PRIORITY_MIN) {
      // Scaled  priority range OSD_POOL_PRIORITY_MIN to 0
      n = (float)prio / min_prio * OSD_POOL_PRIORITY_MIN;
    } else {
      continue;
    }
    if (n == 0) {
      pool.opts.unset(key);
    } else {
      pool.opts.set(key, static_cast<int64_t>(n));
    }
    dout(10) << __func__ << " pool " << pool_id
	     << " recovery_priority adjusted "
	     << prio << " to " << n << dendl;
    pool.last_change = pending_inc.epoch;
    pending_inc.new_pools[pool_id] = pool;
  }
}

void OSDMonitor::try_enable_stretch_mode_pools(stringstream& ss, bool *okay,
					       int *errcode,
					       set<pg_pool_t*>* pools,
					       const string& new_crush_rule)
{
  dout(20) << __func__ << dendl;
  *okay = false;
  int new_crush_rule_result = osdmap.crush->get_rule_id(new_crush_rule);
  if (new_crush_rule_result < 0) {
    ss << "unrecognized crush rule " << new_crush_rule_result;
    *errcode = new_crush_rule_result;
    return;
  }
  __u8 new_rule = static_cast<__u8>(new_crush_rule_result);
  for (const auto& pooli : osdmap.pools) {
    int64_t poolid = pooli.first;
    const pg_pool_t *p = &pooli.second;
    if (!p->is_replicated()) {
      ss << "stretched pools must be replicated; '" << osdmap.pool_name[poolid] << "' is erasure-coded";
      *errcode = -EINVAL;
      return;
    }
    uint8_t default_size = g_conf().get_val<uint64_t>("osd_pool_default_size");
    if ((p->get_size() != default_size ||
	 (p->get_min_size() != g_conf().get_osd_pool_default_min_size(default_size))) &&
	(p->get_crush_rule() != new_rule)) {
      ss << "we currently require stretch mode pools start out with the"
	" default size/min_size, which '" << osdmap.pool_name[poolid] << "' does not";
      *errcode = -EINVAL;
      return;
    }
    pg_pool_t *pp = pending_inc.get_new_pool(poolid, p);
    // TODO: The part where we unconditionally copy the pools into pending_inc is bad
    // the attempt may fail and then we have these pool updates...but they won't do anything
    // if there is a failure, so if it's hard to change the interface, no need to bother
    pools->insert(pp);
  }
  *okay = true;
  return;
}

void OSDMonitor::try_enable_stretch_mode(stringstream& ss, bool *okay,
					 int *errcode, bool commit,
					 const string& dividing_bucket,
					 uint32_t bucket_count,
					 const set<pg_pool_t*>& pools,
					 const string& new_crush_rule)
{
  dout(20) << __func__ << dendl;
  *okay = false;
  CrushWrapper crush = _get_pending_crush();
  int dividing_id = -1;
  if (auto type_id = crush.get_validated_type_id(dividing_bucket);
      !type_id.has_value()) {
    ss << dividing_bucket << " is not a valid crush bucket type";
    *errcode = -ENOENT;
    ceph_assert(!commit);
    return;
  } else {
    dividing_id = *type_id;
  }
  vector<int> subtrees;
  crush.get_subtree_of_type(dividing_id, &subtrees);
  if (subtrees.size() != 2) {
    ss << "there are " << subtrees.size() << dividing_bucket
       << "'s in the cluster but stretch mode currently only works with 2!";
    *errcode = -EINVAL;
    ceph_assert(!commit || subtrees.size() == 2);
    return;
  }

  int new_crush_rule_result = crush.get_rule_id(new_crush_rule);
  if (new_crush_rule_result < 0) {
    ss << "unrecognized crush rule " << new_crush_rule;
    *errcode = new_crush_rule_result;
    ceph_assert(!commit || (new_crush_rule_result > 0));
    return;
  }
  __u8 new_rule = static_cast<__u8>(new_crush_rule_result);

  int weight1 = crush.get_item_weight(subtrees[0]);
  int weight2 = crush.get_item_weight(subtrees[1]);
  if (weight1 != weight2) {
    // TODO: I'm really not sure this is a good idea?
    ss << "the 2 " << dividing_bucket
       << "instances in the cluster have differing weights "
       << weight1 << " and " << weight2
       <<" but stretch mode currently requires they be the same!";
    *errcode = -EINVAL;
    ceph_assert(!commit || (weight1 == weight2));
    return;
  }
  if (bucket_count != 2) {
    ss << "currently we only support 2-site stretch clusters!";
    *errcode = -EINVAL;
    ceph_assert(!commit || bucket_count == 2);
    return;
  }
  // TODO: check CRUSH rules for pools so that we are appropriately divided
  if (commit) {
    for (auto pool : pools) {
      pool->crush_rule = new_rule;
      pool->peering_crush_bucket_count = bucket_count;
      pool->peering_crush_bucket_target = bucket_count;
      pool->peering_crush_bucket_barrier = dividing_id;
      pool->peering_crush_mandatory_member = CRUSH_ITEM_NONE;
      pool->size = g_conf().get_val<uint64_t>("mon_stretch_pool_size");
      pool->min_size = g_conf().get_val<uint64_t>("mon_stretch_pool_min_size");
    }
    pending_inc.change_stretch_mode = true;
    pending_inc.stretch_mode_enabled = true;
    pending_inc.new_stretch_bucket_count = bucket_count;
    pending_inc.new_degraded_stretch_mode = 0;
    pending_inc.new_stretch_mode_bucket = dividing_id;
  }
  *okay = true;
  return;
}

bool OSDMonitor::check_for_dead_crush_zones(const map<string,set<string>>& dead_buckets,
					    set<int> *really_down_buckets,
					    set<string> *really_down_mons)
{
  dout(20) << __func__ << " with dead mon zones " << dead_buckets << dendl;
  ceph_assert(is_readable());
  if (dead_buckets.empty()) return false;
  set<int> down_cache;
  bool really_down = false;
  for (auto dbi : dead_buckets) {
    const string& bucket_name = dbi.first;
    ceph_assert(osdmap.crush->name_exists(bucket_name));
    int bucket_id = osdmap.crush->get_item_id(bucket_name);
    dout(20) << "Checking " << bucket_name << " id " << bucket_id
	     << " to see if OSDs are also down" << dendl;
    bool subtree_down = osdmap.subtree_is_down(bucket_id, &down_cache);
    if (subtree_down) {
      dout(20) << "subtree is down!" << dendl;
      really_down = true;
      really_down_buckets->insert(bucket_id);
      really_down_mons->insert(dbi.second.begin(), dbi.second.end());
    }
  }
  dout(10) << "We determined CRUSH buckets " << *really_down_buckets
	   << " and mons " << *really_down_mons << " are really down" << dendl;
  return really_down;
}

void OSDMonitor::trigger_degraded_stretch_mode(const set<int>& dead_buckets,
					       const set<string>& live_zones)
{
  dout(20) << __func__ << dendl;
  stretch_recovery_triggered.set_from_double(0); // reset this; we can't go clean now!
  // update the general OSDMap changes
  pending_inc.change_stretch_mode = true;
  pending_inc.stretch_mode_enabled = osdmap.stretch_mode_enabled;
  pending_inc.new_stretch_bucket_count = osdmap.stretch_bucket_count;
  int new_site_count = osdmap.stretch_bucket_count - dead_buckets.size();
  ceph_assert(new_site_count == 1); // stretch count 2!
  pending_inc.new_degraded_stretch_mode = new_site_count;
  pending_inc.new_recovering_stretch_mode = 0;
  pending_inc.new_stretch_mode_bucket = osdmap.stretch_mode_bucket;

  // and then apply them to all the pg_pool_ts
  ceph_assert(live_zones.size() == 1); // only support 2 zones now
  const string& remaining_site_name = *(live_zones.begin());
  ceph_assert(osdmap.crush->name_exists(remaining_site_name));
  int remaining_site = osdmap.crush->get_item_id(remaining_site_name);
  for (auto pgi : osdmap.pools) {
    if (pgi.second.peering_crush_bucket_count) {
      pg_pool_t& newp = *pending_inc.get_new_pool(pgi.first, &pgi.second);
      newp.peering_crush_bucket_count = new_site_count;
      newp.peering_crush_mandatory_member = remaining_site;
      newp.min_size = pgi.second.min_size / 2; // only support 2 zones now
      newp.set_last_force_op_resend(pending_inc.epoch);
    }
  }
  propose_pending();
}

void OSDMonitor::trigger_recovery_stretch_mode()
{
  dout(20) << __func__ << dendl;
  stretch_recovery_triggered.set_from_double(0); // reset this so we don't go full-active prematurely
  pending_inc.change_stretch_mode = true;
  pending_inc.stretch_mode_enabled = osdmap.stretch_mode_enabled;
  pending_inc.new_stretch_bucket_count = osdmap.stretch_bucket_count;
  pending_inc.new_degraded_stretch_mode = osdmap.degraded_stretch_mode;
  pending_inc.new_recovering_stretch_mode = 1;
  pending_inc.new_stretch_mode_bucket = osdmap.stretch_mode_bucket;

  for (auto pgi : osdmap.pools) {
    if (pgi.second.peering_crush_bucket_count) {
      pg_pool_t& newp = *pending_inc.get_new_pool(pgi.first, &pgi.second);
      newp.set_last_force_op_resend(pending_inc.epoch);
    }
  }
  propose_pending();
}

void OSDMonitor::set_degraded_stretch_mode()
{
  stretch_recovery_triggered.set_from_double(0);
}

void OSDMonitor::set_recovery_stretch_mode()
{
  if (stretch_recovery_triggered.is_zero()) {
    stretch_recovery_triggered = ceph_clock_now();
  }
}

void OSDMonitor::set_healthy_stretch_mode()
{
  stretch_recovery_triggered.set_from_double(0);
}

void OSDMonitor::notify_new_pg_digest()
{
  dout(20) << __func__ << dendl;
  if (!stretch_recovery_triggered.is_zero()) {
    try_end_recovery_stretch_mode(false);
  }
}

struct CMonExitRecovery : public Context {
  OSDMonitor *m;
  bool force;
  CMonExitRecovery(OSDMonitor *mon, bool f) : m(mon), force(f) {}
  void finish(int r) {
    m->try_end_recovery_stretch_mode(force);
  }
};

void OSDMonitor::try_end_recovery_stretch_mode(bool force)
{
  dout(20) << __func__ << dendl;
  if (!mon.is_leader()) return;
  if (!mon.is_degraded_stretch_mode()) return;
  if (!mon.is_recovering_stretch_mode()) return;
  if (!is_readable()) {
    wait_for_readable_ctx(new CMonExitRecovery(this, force));
    return;
  }

  if (osdmap.recovering_stretch_mode &&
      ((!stretch_recovery_triggered.is_zero() &&
	ceph_clock_now() - g_conf().get_val<double>("mon_stretch_recovery_min_wait") >
	stretch_recovery_triggered) ||
       force)) {
    if (!mon.mgrstatmon()->is_readable()) {
      mon.mgrstatmon()->wait_for_readable_ctx(new CMonExitRecovery(this, force));
      return;
    }
    const PGMapDigest& pgd = mon.mgrstatmon()->get_digest();
    double misplaced, degraded, inactive, unknown;
    pgd.get_recovery_stats(&misplaced, &degraded, &inactive, &unknown);
    if (force || (degraded == 0.0 && inactive == 0.0 && unknown == 0.0)) {
      // we can exit degraded stretch mode!
      mon.trigger_healthy_stretch_mode();
    }
  }
}

void OSDMonitor::trigger_healthy_stretch_mode()
{
  ceph_assert(is_writeable());
  stretch_recovery_triggered.set_from_double(0);
  pending_inc.change_stretch_mode = true;
  pending_inc.stretch_mode_enabled = osdmap.stretch_mode_enabled;
  pending_inc.new_stretch_bucket_count = osdmap.stretch_bucket_count;
  pending_inc.new_degraded_stretch_mode = 0; // turn off degraded mode...
  pending_inc.new_recovering_stretch_mode = 0; //...and recovering mode!
  pending_inc.new_stretch_mode_bucket = osdmap.stretch_mode_bucket;
  for (auto pgi : osdmap.pools) {
    if (pgi.second.peering_crush_bucket_count) {
      pg_pool_t& newp = *pending_inc.get_new_pool(pgi.first, &pgi.second);
      newp.peering_crush_bucket_count = osdmap.stretch_bucket_count;
      newp.peering_crush_mandatory_member = CRUSH_ITEM_NONE;
      newp.min_size = g_conf().get_val<uint64_t>("mon_stretch_pool_min_size");
      newp.set_last_force_op_resend(pending_inc.epoch);
    }
  }
  propose_pending();
}
