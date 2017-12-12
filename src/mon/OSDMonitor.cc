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
#include <locale>
#include <sstream>

#include "mon/OSDMonitor.h"
#include "mon/Monitor.h"
#include "mon/MDSMonitor.h"
#include "mon/PGMonitor.h"
#include "mon/MgrStatMonitor.h"
#include "mon/AuthMonitor.h"
#include "mon/ConfigKeyService.h"

#include "mon/MonitorDBStore.h"
#include "mon/Session.h"

#include "crush/CrushWrapper.h"
#include "crush/CrushTester.h"
#include "crush/CrushTreeDumper.h"

#include "messages/MOSDBeacon.h"
#include "messages/MOSDFailure.h"
#include "messages/MOSDMarkMeDown.h"
#include "messages/MOSDFull.h"
#include "messages/MOSDMap.h"
#include "messages/MMonGetOSDMap.h"
#include "messages/MOSDBoot.h"
#include "messages/MOSDAlive.h"
#include "messages/MPoolOp.h"
#include "messages/MPoolOpReply.h"
#include "messages/MOSDPGCreate.h"
#include "messages/MOSDPGCreated.h"
#include "messages/MOSDPGTemp.h"
#include "messages/MMonCommand.h"
#include "messages/MRemoveSnaps.h"
#include "messages/MOSDScrub.h"
#include "messages/MRoute.h"

#include "common/TextTable.h"
#include "common/Timer.h"
#include "common/ceph_argparse.h"
#include "common/perf_counters.h"
#include "common/strtol.h"

#include "common/config.h"
#include "common/errno.h"

#include "erasure-code/ErasureCodePlugin.h"
#include "compressor/Compressor.h"
#include "common/Checksummer.h"

#include "include/compat.h"
#include "include/assert.h"
#include "include/stringify.h"
#include "include/util.h"
#include "common/cmdparse.h"
#include "include/str_list.h"
#include "include/str_map.h"
#include "include/scope_guard.h"

#include "json_spirit/json_spirit_reader.h"

#include <boost/algorithm/string/predicate.hpp>

#define dout_subsys ceph_subsys_mon
static const string OSD_PG_CREATING_PREFIX("osd_pg_creating");
static const string OSD_METADATA_PREFIX("osd_metadata");

namespace {

const uint32_t MAX_POOL_APPLICATIONS = 4;
const uint32_t MAX_POOL_APPLICATION_KEYS = 64;
const uint32_t MAX_POOL_APPLICATION_LENGTH = 128;

} // anonymous namespace

void LastEpochClean::Lec::report(ps_t ps, epoch_t last_epoch_clean)
{
  if (epoch_by_pg.size() <= ps) {
    epoch_by_pg.resize(ps + 1, 0);
  }
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

void LastEpochClean::report(const pg_t& pg, epoch_t last_epoch_clean)
{
  auto& lec = report_by_pool[pg.pool()];
  return lec.report(pg.ps(), last_epoch_clean);
}

epoch_t LastEpochClean::get_lower_bound(const OSDMap& latest) const
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


struct C_UpdateCreatingPGs : public Context {
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
static ostream& _prefix(std::ostream *_dout, Monitor *mon, const OSDMap& osdmap) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name()
		<< ").osd e" << osdmap.get_epoch() << " ";
}

OSDMonitor::OSDMonitor(
  CephContext *cct,
  Monitor *mn,
  Paxos *p,
  const string& service_name)
 : PaxosService(mn, p, service_name),
   cct(cct),
   inc_osd_cache(g_conf->mon_osd_cache_size),
   full_osd_cache(g_conf->mon_osd_cache_size),
   last_attempted_minwait_time(utime_t()),
   mapper(mn->cct, &mn->cpu_tp),
   op_tracker(cct, true, 1)
{}

bool OSDMonitor::_have_pending_crush()
{
  return pending_inc.crush.length() > 0;
}

CrushWrapper &OSDMonitor::_get_stable_crush()
{
  return *osdmap.crush;
}

void OSDMonitor::_get_pending_crush(CrushWrapper& newcrush)
{
  bufferlist bl;
  if (pending_inc.crush.length())
    bl = pending_inc.crush;
  else
    osdmap.crush->encode(bl, CEPH_FEATURES_SUPPORTED_DEFAULT);

  bufferlist::iterator p = bl.begin();
  newcrush.decode(p);
}

void OSDMonitor::create_initial()
{
  dout(10) << "create_initial for " << mon->monmap->fsid << dendl;

  OSDMap newmap;

  bufferlist bl;
  mon->store->get("mkfs", "osdmap", bl);

  if (bl.length()) {
    newmap.decode(bl);
    newmap.set_fsid(mon->monmap->fsid);
  } else {
    newmap.build_simple(g_ceph_context, 0, mon->monmap->fsid, 0);
  }
  newmap.set_epoch(1);
  newmap.created = newmap.modified = ceph_clock_now();

  // new clusters should sort bitwise by default.
  newmap.set_flag(CEPH_OSDMAP_SORTBITWISE);

  // new cluster should require latest by default
  if (g_conf->mon_debug_no_require_luminous) {
    newmap.require_osd_release = CEPH_RELEASE_KRAKEN;
    derr << __func__ << " mon_debug_no_require_luminous=true" << dendl;
  } else {
    newmap.require_osd_release = CEPH_RELEASE_LUMINOUS;
    newmap.flags |=
      CEPH_OSDMAP_RECOVERY_DELETES |
      CEPH_OSDMAP_PURGED_SNAPDIRS;
    newmap.full_ratio = g_conf->mon_osd_full_ratio;
    if (newmap.full_ratio > 1.0) newmap.full_ratio /= 100;
    newmap.backfillfull_ratio = g_conf->mon_osd_backfillfull_ratio;
    if (newmap.backfillfull_ratio > 1.0) newmap.backfillfull_ratio /= 100;
    newmap.nearfull_ratio = g_conf->mon_osd_nearfull_ratio;
    if (newmap.nearfull_ratio > 1.0) newmap.nearfull_ratio /= 100;
    int r = ceph_release_from_name(
      g_conf->mon_osd_initial_require_min_compat_client.c_str());
    if (r <= 0) {
      assert(0 == "mon_osd_initial_require_min_compat_client is not valid");
    }
    newmap.require_min_compat_client = r;
  }

  // encode into pending incremental
  newmap.encode(pending_inc.fullmap,
                mon->get_quorum_con_features() | CEPH_FEATURE_RESERVED);
  pending_inc.full_crc = newmap.get_crc();
  dout(20) << " full crc " << pending_inc.full_crc << dendl;
}

void OSDMonitor::get_store_prefixes(std::set<string>& s)
{
  s.insert(service_name);
  s.insert(OSD_PG_CREATING_PREFIX);
  s.insert(OSD_METADATA_PREFIX);
}

void OSDMonitor::update_from_paxos(bool *need_bootstrap)
{
  version_t version = get_last_committed();
  if (version == osdmap.epoch)
    return;
  assert(version > osdmap.epoch);

  dout(15) << "update_from_paxos paxos e " << version
	   << ", my e " << osdmap.epoch << dendl;

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
      if (mon->store->exists(get_service_name(), full_key)) {
        dout(10) << __func__ << " found latest full map v " << v << dendl;
        latest_full = v;
        break;
      }
    }

    assert(latest_full > 0);
    auto t(std::make_shared<MonitorDBStore::Transaction>());
    put_version_latest_full(t, latest_full);
    mon->store->apply_transaction(t);
    dout(10) << __func__ << " updated the on-disk full map version to "
             << latest_full << dendl;
  }

  if ((latest_full > 0) && (latest_full > osdmap.epoch)) {
    bufferlist latest_bl;
    get_version_full(latest_full, latest_bl);
    assert(latest_bl.length() != 0);
    dout(7) << __func__ << " loading latest full map e" << latest_full << dendl;
    osdmap.decode(latest_bl);
  }

  if (mon->monmap->get_required_features().contains_all(
	ceph::features::mon::FEATURE_LUMINOUS)) {
    bufferlist bl;
    if (!mon->store->get(OSD_PG_CREATING_PREFIX, "creating", bl)) {
      auto p = bl.begin();
      std::lock_guard<std::mutex> l(creating_pgs_lock);
      creating_pgs.decode(p);
      dout(7) << __func__ << " loading creating_pgs last_scan_epoch "
	      << creating_pgs.last_scan_epoch
	      << " with " << creating_pgs.pgs.size() << " pgs" << dendl;
    } else {
      dout(1) << __func__ << " missing creating pgs; upgrade from post-kraken?"
	      << dendl;
    }
  }

  // make sure we're using the right pg service.. remove me post-luminous!
  if (osdmap.require_osd_release >= CEPH_RELEASE_LUMINOUS) {
    dout(10) << __func__ << " pgservice is mgrstat" << dendl;
    mon->pgservice = mon->mgrstatmon()->get_pg_stat_service();
  } else {
    dout(10) << __func__ << " pgservice is pg" << dendl;
    mon->pgservice = mon->pgmon()->get_pg_stat_service();
  }

  // walk through incrementals
  MonitorDBStore::TransactionRef t;
  size_t tx_size = 0;
  while (version > osdmap.epoch) {
    bufferlist inc_bl;
    int err = get_version(osdmap.epoch+1, inc_bl);
    assert(err == 0);
    assert(inc_bl.length());

    dout(7) << "update_from_paxos  applying incremental " << osdmap.epoch+1
	    << dendl;
    OSDMap::Incremental inc(inc_bl);
    err = osdmap.apply_incremental(inc);
    assert(err == 0);

    if (!t)
      t.reset(new MonitorDBStore::Transaction);

    // Write out the full map for all past epochs.  Encode the full
    // map with the same features as the incremental.  If we don't
    // know, use the quorum features.  If we don't know those either,
    // encode with all features.
    uint64_t f = inc.encode_features;
    if (!f)
      f = mon->get_quorum_con_features();
    if (!f)
      f = -1;
    bufferlist full_bl;
    osdmap.encode(full_bl, f | CEPH_FEATURE_RESERVED);
    tx_size += full_bl.length();

    bufferlist orig_full_bl;
    get_version_full(osdmap.epoch, orig_full_bl);
    if (orig_full_bl.length()) {
      // the primary provided the full map
      assert(inc.have_crc);
      if (inc.full_crc != osdmap.crc) {
	// This will happen if the mons were running mixed versions in
	// the past or some other circumstance made the full encoded
	// maps divergent.  Reloading here will bring us back into
	// sync with the primary for this and all future maps.  OSDs
	// will also be brought back into sync when they discover the
	// crc mismatch and request a full map from a mon.
	derr << __func__ << " full map CRC mismatch, resetting to canonical"
	     << dendl;
	osdmap = OSDMap();
	osdmap.decode(orig_full_bl);
      }
    } else {
      assert(!inc.have_crc);
      put_version_full(t, osdmap.epoch, full_bl);
    }
    put_version_latest_full(t, osdmap.epoch);

    // share
    dout(1) << osdmap << dendl;

    if (osdmap.epoch == 1) {
      t->erase("mkfs", "osdmap");
    }

    // make sure we're using the right pg service.. remove me post-luminous!
    if (osdmap.require_osd_release >= CEPH_RELEASE_LUMINOUS) {
      dout(10) << __func__ << " pgservice is mgrstat" << dendl;
      mon->pgservice = mon->mgrstatmon()->get_pg_stat_service();
    } else {
      dout(10) << __func__ << " pgservice is pg" << dendl;
      mon->pgservice = mon->pgmon()->get_pg_stat_service();
    }

    if (tx_size > g_conf->mon_sync_max_payload_size*2) {
      mon->store->apply_transaction(t);
      t = MonitorDBStore::TransactionRef();
      tx_size = 0;
    }
    if (mon->monmap->get_required_features().contains_all(
          ceph::features::mon::FEATURE_LUMINOUS)) {
      for (const auto &osd_state : inc.new_state) {
	if (osd_state.second & CEPH_OSD_UP) {
	  // could be marked up *or* down, but we're too lazy to check which
	  last_osd_report.erase(osd_state.first);
	}
	if (osd_state.second & CEPH_OSD_EXISTS) {
	  // could be created *or* destroyed, but we can safely drop it
	  osd_epochs.erase(osd_state.first);
	}
      }
    }
  }

  if (t) {
    mon->store->apply_transaction(t);
  }

  for (int o = 0; o < osdmap.get_max_osd(); o++) {
    if (osdmap.is_out(o))
      continue;
    auto found = down_pending_out.find(o);
    if (osdmap.is_down(o)) {
      // populate down -> out map
      if (found == down_pending_out.end()) {
        dout(10) << " adding osd." << o << " to down_pending_out map" << dendl;
        down_pending_out[o] = ceph_clock_now();
      }
    } else {
      if (found != down_pending_out.end()) {
        dout(10) << " removing osd." << o << " from down_pending_out map" << dendl;
        down_pending_out.erase(found);
      }
    }
  }
  // XXX: need to trim MonSession connected with a osd whose id > max_osd?

  if (mon->is_leader()) {
    // kick pgmon, make sure it's seen the latest map
    mon->pgmon()->check_osd_map(osdmap.epoch);
  }

  check_osdmap_subs();
  check_pg_creates_subs();

  share_map_with_random_osd();
  update_logger();

  process_failures();

  // make sure our feature bits reflect the latest map
  update_msgr_features();

  if (!mon->is_leader()) {
    // will be called by on_active() on the leader, avoid doing so twice
    start_mapping();
  }
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
				       g_conf->mon_osd_mapping_pgs_per_chunk);
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
  set<int> types;
  types.insert((int)entity_name_t::TYPE_OSD);
  types.insert((int)entity_name_t::TYPE_CLIENT);
  types.insert((int)entity_name_t::TYPE_MDS);
  types.insert((int)entity_name_t::TYPE_MON);
  for (set<int>::iterator q = types.begin(); q != types.end(); ++q) {
    uint64_t mask;
    uint64_t features = osdmap.get_features(*q, &mask);
    if ((mon->messenger->get_policy(*q).features_required & mask) != features) {
      dout(0) << "crush map has features " << features << ", adjusting msgr requires" << dendl;
      Messenger::Policy p = mon->messenger->get_policy(*q);
      p.features_required = (p.features_required & ~mask) | features;
      mon->messenger->set_policy(*q, p);
    }
  }
}

void OSDMonitor::on_active()
{
  update_logger();

  if (mon->is_leader()) {
    mon->clog->debug() << "osdmap " << osdmap;
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

  mon->cluster_logger->set(l_cluster_num_osd, osdmap.get_num_osds());
  mon->cluster_logger->set(l_cluster_num_osd_up, osdmap.get_num_up_osds());
  mon->cluster_logger->set(l_cluster_num_osd_in, osdmap.get_num_in_osds());
  mon->cluster_logger->set(l_cluster_osd_epoch, osdmap.get_epoch());
}

void OSDMonitor::create_pending()
{
  pending_inc = OSDMap::Incremental(osdmap.epoch+1);
  pending_inc.fsid = mon->monmap->fsid;

  dout(10) << "create_pending e " << pending_inc.epoch << dendl;

  // clean up pg_temp, primary_temp
  OSDMap::clean_temps(g_ceph_context, osdmap, &pending_inc);
  dout(10) << "create_pending  did clean_temps" << dendl;

  // On upgrade OSDMap has new field set by mon_osd_backfillfull_ratio config
  // instead of osd_backfill_full_ratio config
  if (osdmap.backfillfull_ratio <= 0) {
    pending_inc.new_backfillfull_ratio = g_conf->mon_osd_backfillfull_ratio;
    if (pending_inc.new_backfillfull_ratio > 1.0)
      pending_inc.new_backfillfull_ratio /= 100;
    dout(1) << __func__ << " setting backfillfull_ratio = "
	    << pending_inc.new_backfillfull_ratio << dendl;
  }
  if (osdmap.get_epoch() > 0 &&
      osdmap.require_osd_release < CEPH_RELEASE_LUMINOUS) {
    // transition full ratios from PGMap to OSDMap (on upgrade)
    float full_ratio = mon->pgservice->get_full_ratio();
    float nearfull_ratio = mon->pgservice->get_nearfull_ratio();
    if (osdmap.full_ratio != full_ratio) {
      dout(10) << __func__ << " full_ratio " << osdmap.full_ratio
	       << " -> " << full_ratio << " (from pgmap)" << dendl;
      pending_inc.new_full_ratio = full_ratio;
    }
    if (osdmap.nearfull_ratio != nearfull_ratio) {
      dout(10) << __func__ << " nearfull_ratio " << osdmap.nearfull_ratio
	       << " -> " << nearfull_ratio << " (from pgmap)" << dendl;
      pending_inc.new_nearfull_ratio = nearfull_ratio;
    }
  } else {
    // safety check (this shouldn't really happen)
    if (osdmap.full_ratio <= 0) {
      pending_inc.new_full_ratio = g_conf->mon_osd_full_ratio;
      if (pending_inc.new_full_ratio > 1.0)
        pending_inc.new_full_ratio /= 100;
      dout(1) << __func__ << " setting full_ratio = "
	      << pending_inc.new_full_ratio << dendl;
    }
    if (osdmap.nearfull_ratio <= 0) {
      pending_inc.new_nearfull_ratio = g_conf->mon_osd_nearfull_ratio;
      if (pending_inc.new_nearfull_ratio > 1.0)
        pending_inc.new_nearfull_ratio /= 100;
      dout(1) << __func__ << " setting nearfull_ratio = "
	      << pending_inc.new_nearfull_ratio << dendl;
    }
  }

  // Rewrite CRUSH rule IDs if they are using legacy "ruleset"
  // structure.
  if (osdmap.crush->has_legacy_rule_ids()) {
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    // First, for all pools, work out which rule they really used
    // by resolving ruleset to rule.
    for (const auto &i : osdmap.get_pools()) {
      const auto pool_id = i.first;
      const auto &pool = i.second;
      int new_rule_id = newcrush.find_rule(pool.crush_rule,
					   pool.type, pool.size);

      dout(1) << __func__ << " rewriting pool "
	      << osdmap.get_pool_name(pool_id) << " crush ruleset "
	      << pool.crush_rule << " -> rule id " << new_rule_id << dendl;
      if (pending_inc.new_pools.count(pool_id) == 0) {
	pending_inc.new_pools[pool_id] = pool;
      }
      pending_inc.new_pools[pool_id].crush_rule = new_rule_id;
    }

    // Now, go ahead and renumber all the rules so that their
    // rule_id field corresponds to their position in the array
    auto old_to_new = newcrush.renumber_rules();
    dout(1) << __func__ << " Rewrote " << old_to_new << " crush IDs:" << dendl;
    for (const auto &i : old_to_new) {
      dout(1) << __func__ << " " << i.first << " -> " << i.second << dendl;
    }
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
  }
}

creating_pgs_t
OSDMonitor::update_pending_pgs(const OSDMap::Incremental& inc)
{
  dout(10) << __func__ << dendl;
  creating_pgs_t pending_creatings;
  {
    std::lock_guard<std::mutex> l(creating_pgs_lock);
    pending_creatings = creating_pgs;
  }
  // check for new or old pools
  if (pending_creatings.last_scan_epoch < inc.epoch) {
    if (osdmap.get_epoch() &&
	osdmap.require_osd_release < CEPH_RELEASE_LUMINOUS) {
      auto added =
	mon->pgservice->maybe_add_creating_pgs(creating_pgs.last_scan_epoch,
					       osdmap.get_pools(),
					       &pending_creatings);
      dout(7) << __func__ << " " << added << " pgs added from pgmap" << dendl;
    }
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

  // process queue
  unsigned max = MAX(1, g_conf->mon_osd_max_creating_pgs);
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
    int n = MIN(max - pending_creatings.pgs.size(),
		p->second.end - p->second.start);
    ps_t first = p->second.start;
    ps_t end = first + n;
    for (ps_t ps = first; ps < end; ++ps) {
      const pg_t pgid{ps, static_cast<uint64_t>(poolid)};
      // NOTE: use the *current* epoch as the PG creation epoch so that the
      // OSD does not have to generate a long set of PastIntervals.
      pending_creatings.pgs.emplace(pgid, make_pair(inc.epoch,
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
  for (map<int32_t,uint32_t>::iterator p = pending_inc.new_weight.begin();
       !all && p != pending_inc.new_weight.end();
       ++p) {
    if (p->second < osdmap.get_weight(p->first)) {
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
	g_conf->mon_osd_prime_pg_temp_max_estimate) {
      dout(10) << __func__ << " estimate " << estimate << " pgs on "
	       << osds.size() << " osds >= "
	       << g_conf->mon_osd_prime_pg_temp_max_estimate << " of total "
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
    mapper.queue(&job, g_conf->mon_osd_mapping_pgs_per_chunk);
    if (job.wait_for(g_conf->mon_osd_prime_pg_temp_max_time)) {
      dout(10) << __func__ << " done in " << job.get_duration() << dendl;
    } else {
      dout(10) << __func__ << " did not finish in "
	       << g_conf->mon_osd_prime_pg_temp_max_time
	       << ", stopping" << dendl;
      job.abort();
    }
  } else {
    dout(10) << __func__ << " " << osds.size() << " interesting osds" << dendl;
    utime_t stop = ceph_clock_now();
    stop += g_conf->mon_osd_prime_pg_temp_max_time;
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
		     << g_conf->mon_osd_prime_pg_temp_max_time
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
  if (mon->monmap->get_required_features().contains_all(
        ceph::features::mon::FEATURE_LUMINOUS)) {
    // TODO: remove this creating_pgs direct access?
    if (creating_pgs.pgs.count(pgid)) {
      return;
    }
  } else {
    if (mon->pgservice->is_creating_pg(pgid)) {
      return;
    }
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
  if (acting == next_acting && next_up != next_acting)
    return;  // no change since last epoch

  if (acting.empty())
    return;  // if previously empty now we can be no worse off
  const pg_pool_t *pool = next.get_pg_pool(pgid.pool());
  if (pool && acting.size() < pool->min_size)
    return;  // can be no worse off than before

  if (next_up == next_acting) {
    acting.clear();
    dout(20) << __func__ << "next_up === next_acting now, clear pg_temp"
             << dendl;
  }

  dout(20) << __func__ << " " << pgid << " " << up << "/" << acting
	   << " -> " << next_up << "/" << next_acting
	   << ", priming " << acting
	   << dendl;
  {
    Mutex::Locker l(prime_pg_temp_lock);
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

  // finalize up pending_inc
  pending_inc.modified = ceph_clock_now();

  int r = pending_inc.propagate_snaps_to_tiers(g_ceph_context, osdmap);
  assert(r == 0);

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
      if (g_conf->mon_osd_prime_pg_temp) {
	maybe_prime_pg_temp();
      }
    } 
  } else if (g_conf->mon_osd_prime_pg_temp) {
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
      ++p;
    }
  }

  bufferlist bl;

  {
    OSDMap tmp;
    tmp.deepish_copy_from(osdmap);
    tmp.apply_incremental(pending_inc);

    if (tmp.require_osd_release >= CEPH_RELEASE_LUMINOUS) {
      // remove any legacy osdmap nearfull/full flags
      {
        if (tmp.test_flag(CEPH_OSDMAP_FULL | CEPH_OSDMAP_NEARFULL)) {
          dout(10) << __func__ << " clearing legacy osdmap nearfull/full flag"
                   << dendl;
          remove_flag(CEPH_OSDMAP_NEARFULL);
          remove_flag(CEPH_OSDMAP_FULL);
        }
      }
      // collect which pools are currently affected by
      // the near/backfill/full osd(s),
      // and set per-pool near/backfill/full flag instead
      set<int64_t> full_pool_ids;
      set<int64_t> backfillfull_pool_ids;
      set<int64_t> nearfull_pool_ids;
      tmp.get_full_pools(g_ceph_context,
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
            if (tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_FULL_NO_QUOTA)) {
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
               tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_FULL_NO_QUOTA)) {
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
          if (tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_FULL_NO_QUOTA)) {
            // make sure FLAG_FULL is truly set, so we are safe not
            // to set a extra (redundant) FLAG_BACKFILLFULL flag
            assert(tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_FULL));
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
          if (tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_FULL_NO_QUOTA)) {
            // make sure FLAG_FULL is truly set, so we are safe not
            // to set a extra (redundant) FLAG_NEARFULL flag
            assert(tmp.get_pg_pool(p)->has_flag(pg_pool_t::FLAG_FULL));
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
      if (tmp.require_min_compat_client == 0) {
	auto mv = tmp.get_min_compat_client();
	dout(1) << __func__ << " setting require_min_compat_client to currently "
		<< "required " << ceph_release_name(mv) << dendl;
	mon->clog->info() << "setting require_min_compat_client to currently "
			  << "required " << ceph_release_name(mv);
	pending_inc.new_require_min_compat_client = mv;
      }

      if (osdmap.require_osd_release < CEPH_RELEASE_LUMINOUS) {
	// convert ec profile ruleset-* -> crush-*
	for (auto& p : tmp.erasure_code_profiles) {
	  bool changed = false;
	  map<string,string> newprofile;
	  for (auto& q : p.second) {
	    if (q.first.find("ruleset-") == 0) {
	      string key = "crush-";
	      key += q.first.substr(8);
	      newprofile[key] = q.second;
	      changed = true;
	      dout(20) << " updating ec profile " << p.first
		       << " key " << q.first << " -> " << key << dendl;
	    } else {
	      newprofile[q.first] = q.second;
	    }
	  }
	  if (changed) {
	    dout(10) << " updated ec profile " << p.first << ": "
		     << newprofile << dendl;
	    pending_inc.new_erasure_code_profiles[p.first] = newprofile;
	  }
	}

        // auto-enable pool applications upon upgrade
        // NOTE: this can be removed post-Luminous assuming upgrades need to
        // proceed through Luminous
        for (auto &pool_pair : tmp.pools) {
          int64_t pool_id = pool_pair.first;
          pg_pool_t pg_pool = pool_pair.second;
          if (pg_pool.is_tier()) {
            continue;
          }

          std::string pool_name = tmp.get_pool_name(pool_id);
          uint32_t match_count = 0;

          // CephFS
          FSMap const &pending_fsmap = mon->mdsmon()->get_pending();
          if (pending_fsmap.pool_in_use(pool_id)) {
            dout(10) << __func__ << " auto-enabling CephFS on pool '"
                     << pool_name << "'" << dendl;
            pg_pool.application_metadata.insert(
              {pg_pool_t::APPLICATION_NAME_CEPHFS, {}});
            ++match_count;
          }

          // RBD heuristics (default OpenStack pool names from docs and
          // ceph-ansible)
          if (boost::algorithm::contains(pool_name, "rbd") ||
              pool_name == "images" || pool_name == "volumes" ||
              pool_name == "backups" || pool_name == "vms") {
            dout(10) << __func__ << " auto-enabling RBD on pool '"
                     << pool_name << "'" << dendl;
            pg_pool.application_metadata.insert(
              {pg_pool_t::APPLICATION_NAME_RBD, {}});
            ++match_count;
          }

          // RGW heuristics
          if (boost::algorithm::contains(pool_name, ".rgw") ||
              boost::algorithm::contains(pool_name, ".log") ||
              boost::algorithm::contains(pool_name, ".intent-log") ||
              boost::algorithm::contains(pool_name, ".usage") ||
              boost::algorithm::contains(pool_name, ".users")) {
            dout(10) << __func__ << " auto-enabling RGW on pool '"
                     << pool_name << "'" << dendl;
            pg_pool.application_metadata.insert(
              {pg_pool_t::APPLICATION_NAME_RGW, {}});
            ++match_count;
          }

          // OpenStack gnocchi (from ceph-ansible)
          if (pool_name == "metrics" && match_count == 0) {
            dout(10) << __func__ << " auto-enabling OpenStack Gnocchi on pool '"
                     << pool_name << "'" << dendl;
            pg_pool.application_metadata.insert({"openstack_gnocchi", {}});
            ++match_count;
          }

          if (match_count == 1) {
            pg_pool.last_change = pending_inc.epoch;
            pending_inc.new_pools[pool_id] = pg_pool;
          } else if (match_count > 1) {
            auto pstat = mon->pgservice->get_pool_stat(pool_id);
            if (pstat != nullptr && pstat->stats.sum.num_objects > 0) {
              mon->clog->info() << "unable to auto-enable application for pool "
                                << "'" << pool_name << "'";
            }
          }
        }
      }
    }
  }

  // tell me about it
  for (auto i = pending_inc.new_state.begin();
       i != pending_inc.new_state.end();
       ++i) {
    int s = i->second ? i->second : CEPH_OSD_UP;
    if (s & CEPH_OSD_UP)
      dout(2) << " osd." << i->first << " DOWN" << dendl;
    if (s & CEPH_OSD_EXISTS)
      dout(2) << " osd." << i->first << " DNE" << dendl;
  }
  for (map<int32_t,entity_addr_t>::iterator i = pending_inc.new_up_client.begin();
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
  uint64_t features = mon->get_quorum_con_features();

  // encode full map and determine its crc
  OSDMap tmp;
  {
    tmp.deepish_copy_from(osdmap);
    tmp.apply_incremental(pending_inc);

    // determine appropriate features
    if (tmp.require_osd_release < CEPH_RELEASE_LUMINOUS) {
      dout(10) << __func__ << " encoding without feature SERVER_LUMINOUS"
	       << dendl;
      features &= ~CEPH_FEATURE_SERVER_LUMINOUS;
    }
    if (tmp.require_osd_release < CEPH_RELEASE_KRAKEN) {
      dout(10) << __func__ << " encoding without feature SERVER_KRAKEN | "
	       << "MSG_ADDR2" << dendl;
      features &= ~(CEPH_FEATURE_SERVER_KRAKEN |
		    CEPH_FEATURE_MSG_ADDR2);
    }
    if (tmp.require_osd_release < CEPH_RELEASE_JEWEL) {
      dout(10) << __func__ << " encoding without feature SERVER_JEWEL" << dendl;
      features &= ~CEPH_FEATURE_SERVER_JEWEL;
    }
    dout(10) << __func__ << " encoding full map with " << features << dendl;

    bufferlist fullbl;
    ::encode(tmp, fullbl, features | CEPH_FEATURE_RESERVED);
    pending_inc.full_crc = tmp.get_crc();

    // include full map in the txn.  note that old monitors will
    // overwrite this.  new ones will now skip the local full map
    // encode and reload from this.
    put_version_full(t, pending_inc.epoch, fullbl);
  }

  // encode
  assert(get_last_committed() + 1 == pending_inc.epoch);
  ::encode(pending_inc, bl, features | CEPH_FEATURE_RESERVED);

  dout(20) << " full_crc " << tmp.get_crc()
	   << " inc_crc " << pending_inc.inc_crc << dendl;

  /* put everything in the transaction */
  put_version(t, pending_inc.epoch, bl);
  put_last_committed(t, pending_inc.epoch);

  // metadata, too!
  for (map<int,bufferlist>::iterator p = pending_metadata.begin();
       p != pending_metadata.end();
       ++p)
    t->put(OSD_METADATA_PREFIX, stringify(p->first), p->second);
  for (set<int>::iterator p = pending_metadata_rm.begin();
       p != pending_metadata_rm.end();
       ++p)
    t->erase(OSD_METADATA_PREFIX, stringify(*p));
  pending_metadata.clear();
  pending_metadata_rm.clear();

  // and pg creating, also!
  if (mon->monmap->get_required_features().contains_all(
	ceph::features::mon::FEATURE_LUMINOUS)) {
    auto pending_creatings = update_pending_pgs(pending_inc);
    if (osdmap.get_epoch() &&
	osdmap.require_osd_release < CEPH_RELEASE_LUMINOUS) {
      dout(7) << __func__ << " in the middle of upgrading, "
	      << " trimming pending creating_pgs using pgmap" << dendl;
      mon->pgservice->maybe_trim_creating_pgs(&pending_creatings);
    }
    bufferlist creatings_bl;
    ::encode(pending_creatings, creatings_bl);
    t->put(OSD_PG_CREATING_PREFIX, "creating", creatings_bl);
  }

  // health
  health_check_map_t next;
  tmp.check_health(&next);
  encode_health(next, t);
}

void OSDMonitor::trim_creating_pgs(creating_pgs_t* creating_pgs,
				   const ceph::unordered_map<pg_t,pg_stat_t>& pg_stat)
{
  auto p = creating_pgs->pgs.begin();
  while (p != creating_pgs->pgs.end()) {
    auto q = pg_stat.find(p->first);
    if (q != pg_stat.end() &&
	!(q->second.state & PG_STATE_CREATING)) {
      dout(20) << __func__ << " pgmap shows " << p->first << " is created"
	       << dendl;
      p = creating_pgs->pgs.erase(p);
    } else {
      ++p;
    }
  }
}

int OSDMonitor::load_metadata(int osd, map<string, string>& m, ostream *err)
{
  bufferlist bl;
  int r = mon->store->get(OSD_METADATA_PREFIX, stringify(osd), bl);
  if (r < 0)
    return r;
  try {
    bufferlist::iterator p = bl.begin();
    ::decode(m, p);
  }
  catch (buffer::error& e) {
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

bool OSDMonitor::is_pool_currently_all_bluestore(int64_t pool_id,
						 const pg_pool_t &pool,
						 ostream *err)
{
  // just check a few pgs for efficiency - this can't give a guarantee anyway,
  // since filestore osds could always join the pool later
  set<int> checked_osds;
  for (unsigned ps = 0; ps < MIN(8, pool.get_pg_num()); ++ps) {
    vector<int> up, acting;
    pg_t pgid(ps, pool_id, -1);
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

  MonSession *s = mon->session_map.get_random_osd_session(&osdmap);
  if (!s) {
    dout(10) << __func__ << " no up osd on our session map" << dendl;
    return;
  }

  dout(10) << "committed, telling random " << s->inst << " all about it" << dendl;
  // whatev, they'll request more if they need it
  MOSDMap *m = build_incremental(osdmap.get_epoch() - 1, osdmap.get_epoch());
  s->con->send_message(m);
  // NOTE: do *not* record osd has up to this epoch (as we do
  // elsewhere) as they may still need to request older values.
}

version_t OSDMonitor::get_trim_to()
{
  if (mon->get_quorum().empty()) {
    dout(10) << __func__ << ": quorum not formed" << dendl;
    return 0;
  }

  epoch_t floor;
  if (mon->monmap->get_required_features().contains_all(
        ceph::features::mon::FEATURE_LUMINOUS)) {
    {
      // TODO: Get this hidden in PGStatService
      std::lock_guard<std::mutex> l(creating_pgs_lock);
      if (!creating_pgs.pgs.empty()) {
	return 0;
      }
    }
    floor = get_min_last_epoch_clean();
  } else {
    if (!mon->pgservice->is_readable())
      return 0;
    if (mon->pgservice->have_creating_pgs()) {
      return 0;
    }
    floor = mon->pgservice->get_min_last_epoch_clean();
  }
  {
    dout(10) << " min_last_epoch_clean " << floor << dendl;
    if (g_conf->mon_osd_force_trim_to > 0 &&
	g_conf->mon_osd_force_trim_to < (int)get_last_committed()) {
      floor = g_conf->mon_osd_force_trim_to;
      dout(10) << " explicit mon_osd_force_trim_to = " << floor << dendl;
    }
    unsigned min = g_conf->mon_min_osdmap_epochs;
    if (floor + min > get_last_committed()) {
      if (min < get_last_committed())
	floor = get_last_committed() - min;
      else
	floor = 0;
    }
    if (floor > get_first_committed())
      return floor;
  }
  return 0;
}

epoch_t OSDMonitor::get_min_last_epoch_clean() const
{
  auto floor = last_epoch_clean.get_lower_bound(osdmap);
  // also scan osd epochs
  // don't trim past the oldest reported osd epoch
  for (auto& osd_epoch : osd_epochs) {
    if (osd_epoch.second < floor) {
      floor = osd_epoch.second;
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
    return preprocess_command(op);
  case CEPH_MSG_MON_GET_OSDMAP:
    return preprocess_get_osdmap(op);

    // damp updates
  case MSG_OSD_MARK_ME_DOWN:
    return preprocess_mark_me_down(op);
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
  case MSG_OSD_PGTEMP:
    return preprocess_pgtemp(op);
  case MSG_OSD_BEACON:
    return preprocess_beacon(op);

  case CEPH_MSG_POOLOP:
    return preprocess_pool_op(op);

  case MSG_REMOVE_SNAPS:
    return preprocess_remove_snaps(op);

  default:
    ceph_abort();
    return true;
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
  case MSG_OSD_BEACON:
    return prepare_beacon(op);

  case MSG_MON_COMMAND:
    return prepare_command(op);

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

  // propose as fast as possible if updating up_thru or pg_temp
  // want to merge OSDMap changes as much as possible
  if ((pending_inc.new_primary_temp.size() == 1
      || pending_inc.new_up_thru.size() == 1)
      && pending_inc.new_state.size() < 2) {
    dout(15) << " propose as fast as possible for up_thru/pg_temp" << dendl;

    utime_t now = ceph_clock_now();
    if (now - last_attempted_minwait_time > g_conf->paxos_propose_interval
	&& now - paxos->get_last_commit_time() > g_conf->paxos_min_wait) {
      delay = g_conf->paxos_min_wait;
      last_attempted_minwait_time = now;
      return true;
    }
  }

  return PaxosService::should_propose(delay);
}



// ---------------------------
// READs

bool OSDMonitor::preprocess_get_osdmap(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MMonGetOSDMap *m = static_cast<MMonGetOSDMap*>(op->get_req());
  dout(10) << __func__ << " " << *m << dendl;
  MOSDMap *reply = new MOSDMap(mon->monmap->fsid);
  epoch_t first = get_first_committed();
  epoch_t last = osdmap.get_epoch();
  int max = g_conf->osd_map_message_max;
  for (epoch_t e = MAX(first, m->get_full_first());
       e <= MIN(last, m->get_full_last()) && max > 0;
       ++e, --max) {
    int r = get_version_full(e, reply->maps[e]);
    assert(r >= 0);
  }
  for (epoch_t e = MAX(first, m->get_inc_first());
       e <= MIN(last, m->get_inc_last()) && max > 0;
       ++e, --max) {
    int r = get_version(e, reply->incremental_maps[e]);
    assert(r >= 0);
  }
  reply->oldest_map = first;
  reply->newest_map = last;
  mon->send_reply(op, reply);
  return true;
}


// ---------------------------
// UPDATEs

// failure --

bool OSDMonitor::check_source(PaxosServiceMessage *m, uuid_d fsid) {
  // check permissions
  MonSession *session = m->get_session();
  if (!session)
    return true;
  if (!session->is_capable("osd", MON_CAP_X)) {
    dout(0) << "got MOSDFailure from entity with insufficient caps "
	    << session->caps << dendl;
    return true;
  }
  if (fsid != mon->monmap->fsid) {
    dout(0) << "check_source: on fsid " << fsid
	    << " != " << mon->monmap->fsid << dendl;
    return true;
  }
  return false;
}


bool OSDMonitor::preprocess_failure(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MOSDFailure *m = static_cast<MOSDFailure*>(op->get_req());
  // who is target_osd
  int badboy = m->get_target().name.num();

  // check permissions
  if (check_source(m, m->fsid))
    goto didit;

  // first, verify the reporting host is valid
  if (m->get_orig_source().is_osd()) {
    int from = m->get_orig_source().num();
    if (!osdmap.exists(from) ||
	osdmap.get_addr(from) != m->get_orig_source_inst().addr ||
	(osdmap.is_down(from) && m->if_osd_failed())) {
      dout(5) << "preprocess_failure from dead osd." << from << ", ignoring" << dendl;
      send_incremental(op, m->get_epoch()+1);
      goto didit;
    }
  }


  // weird?
  if (osdmap.is_down(badboy)) {
    dout(5) << "preprocess_failure dne(/dup?): " << m->get_target() << ", from " << m->get_orig_source_inst() << dendl;
    if (m->get_epoch() < osdmap.get_epoch())
      send_incremental(op, m->get_epoch()+1);
    goto didit;
  }
  if (osdmap.get_inst(badboy) != m->get_target()) {
    dout(5) << "preprocess_failure wrong osd: report " << m->get_target() << " != map's " << osdmap.get_inst(badboy)
	    << ", from " << m->get_orig_source_inst() << dendl;
    if (m->get_epoch() < osdmap.get_epoch())
      send_incremental(op, m->get_epoch()+1);
    goto didit;
  }

  // already reported?
  if (osdmap.is_down(badboy) ||
      osdmap.get_up_from(badboy) > m->get_epoch()) {
    dout(5) << "preprocess_failure dup/old: " << m->get_target() << ", from " << m->get_orig_source_inst() << dendl;
    if (m->get_epoch() < osdmap.get_epoch())
      send_incremental(op, m->get_epoch()+1);
    goto didit;
  }

  if (!can_mark_down(badboy)) {
    dout(5) << "preprocess_failure ignoring report of " << m->get_target() << " from " << m->get_orig_source_inst() << dendl;
    goto didit;
  }

  dout(10) << "preprocess_failure new: " << m->get_target() << ", from " << m->get_orig_source_inst() << dendl;
  return false;

 didit:
  return true;
}

class C_AckMarkedDown : public C_MonOp {
  OSDMonitor *osdmon;
public:
  C_AckMarkedDown(
    OSDMonitor *osdmon,
    MonOpRequestRef op)
    : C_MonOp(op), osdmon(osdmon) {}

  void _finish(int) override {
    MOSDMarkMeDown *m = static_cast<MOSDMarkMeDown*>(op->get_req());
    osdmon->mon->send_reply(
      op,
      new MOSDMarkMeDown(
	m->fsid,
	m->get_target(),
	m->get_epoch(),
	false));   // ACK itself does not request an ack
  }
  ~C_AckMarkedDown() override {
  }
};

bool OSDMonitor::preprocess_mark_me_down(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MOSDMarkMeDown *m = static_cast<MOSDMarkMeDown*>(op->get_req());
  int requesting_down = m->get_target().name.num();
  int from = m->get_orig_source().num();

  // check permissions
  if (check_source(m, m->fsid))
    goto reply;

  // first, verify the reporting host is valid
  if (!m->get_orig_source().is_osd())
    goto reply;

  if (!osdmap.exists(from) ||
      osdmap.is_down(from) ||
      osdmap.get_addr(from) != m->get_target().addr) {
    dout(5) << "preprocess_mark_me_down from dead osd."
	    << from << ", ignoring" << dendl;
    send_incremental(op, m->get_epoch()+1);
    goto reply;
  }

  // no down might be set
  if (!can_mark_down(requesting_down))
    goto reply;

  dout(10) << "MOSDMarkMeDown for: " << m->get_target() << dendl;
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
  MOSDMarkMeDown *m = static_cast<MOSDMarkMeDown*>(op->get_req());
  int target_osd = m->get_target().name.num();

  assert(osdmap.is_up(target_osd));
  assert(osdmap.get_addr(target_osd) == m->get_target().addr);

  mon->clog->info() << "osd." << target_osd << " marked itself down";
  pending_inc.new_state[target_osd] = CEPH_OSD_UP;
  if (m->request_ack)
    wait_for_finished_proposal(op, new C_AckMarkedDown(this, op));
  return true;
}

bool OSDMonitor::can_mark_down(int i)
{
  if (osdmap.test_flag(CEPH_OSDMAP_NODOWN)) {
    dout(5) << __func__ << " NODOWN flag set, will not mark osd." << i
            << " down" << dendl;
    return false;
  }

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
  if (up_ratio < g_conf->mon_osd_min_up_ratio) {
    dout(2) << __func__ << " current up_ratio " << up_ratio << " < min "
	    << g_conf->mon_osd_min_up_ratio
	    << ", will not mark osd." << i << " down" << dendl;
    return false;
  }
  return true;
}

bool OSDMonitor::can_mark_up(int i)
{
  if (osdmap.test_flag(CEPH_OSDMAP_NOUP)) {
    dout(5) << __func__ << " NOUP flag set, will not mark osd." << i
            << " up" << dendl;
    return false;
  }

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
  if (osdmap.test_flag(CEPH_OSDMAP_NOOUT)) {
    dout(5) << __func__ << " NOOUT flag set, will not mark osds out" << dendl;
    return false;
  }

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
  if (in_ratio < g_conf->mon_osd_min_in_ratio) {
    if (i >= 0)
      dout(5) << __func__ << " current in_ratio " << in_ratio << " < min "
	      << g_conf->mon_osd_min_in_ratio
	      << ", will not mark osd." << i << " out" << dendl;
    else
      dout(5) << __func__ << " current in_ratio " << in_ratio << " < min "
	      << g_conf->mon_osd_min_in_ratio
	      << ", will not mark osds out" << dendl;
    return false;
  }

  return true;
}

bool OSDMonitor::can_mark_in(int i)
{
  if (osdmap.test_flag(CEPH_OSDMAP_NOIN)) {
    dout(5) << __func__ << " NOIN flag set, will not mark osd." << i
            << " in" << dendl;
    return false;
  }

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
  for (map<int,failure_info_t>::iterator p = failure_info.begin();
       p != failure_info.end();
       ++p) {
    if (can_mark_down(p->first)) {
      found_failure |= check_failure(now, p->first, p->second);
    }
  }
  return found_failure;
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
  string reporter_subtree_level = g_conf->mon_osd_reporter_subtree_level;
  utime_t orig_grace(g_conf->osd_heartbeat_grace, 0);
  utime_t max_failed_since = fi.get_failed_since();
  utime_t failed_for = now - max_failed_since;

  utime_t grace = orig_grace;
  double my_grace = 0, peer_grace = 0;
  double decay_k = 0;
  if (g_conf->mon_osd_adjust_heartbeat_grace) {
    double halflife = (double)g_conf->mon_osd_laggy_halflife;
    decay_k = ::log(.5) / halflife;

    // scale grace period based on historical probability of 'lagginess'
    // (false positive failures due to slowness).
    const osd_xinfo_t& xi = osdmap.get_xinfo(target_osd);
    double decay = exp((double)failed_for * decay_k);
    dout(20) << " halflife " << halflife << " decay_k " << decay_k
	     << " failed_for " << failed_for << " decay " << decay << dendl;
    my_grace = decay * (double)xi.laggy_interval * xi.laggy_probability;
    grace += my_grace;
  }

  // consider the peers reporting a failure a proxy for a potential
  // 'subcluster' over the overall cluster that is similarly
  // laggy.  this is clearly not true in all cases, but will sometimes
  // help us localize the grace correction to a subset of the system
  // (say, a rack with a bad switch) that is unhappy.
  assert(fi.reporters.size());
  for (map<int,failure_reporter_t>::iterator p = fi.reporters.begin();
	p != fi.reporters.end();
	++p) {
    // get the parent bucket whose type matches with "reporter_subtree_level".
    // fall back to OSD if the level doesn't exist.
    map<string, string> reporter_loc = osdmap.crush->get_full_location(p->first);
    map<string, string>::iterator iter = reporter_loc.find(reporter_subtree_level);
    if (iter == reporter_loc.end()) {
      reporters_by_subtree.insert("osd." + to_string(p->first));
    } else {
      reporters_by_subtree.insert(iter->second);
    }
    if (g_conf->mon_osd_adjust_heartbeat_grace) {
      const osd_xinfo_t& xi = osdmap.get_xinfo(p->first);
      utime_t elapsed = now - xi.down_stamp;
      double decay = exp((double)elapsed * decay_k);
      peer_grace += decay * (double)xi.laggy_interval * xi.laggy_probability;
    }
  }
  
  if (g_conf->mon_osd_adjust_heartbeat_grace) {
    peer_grace /= (double)fi.reporters.size();
    grace += peer_grace;
  }

  dout(10) << " osd." << target_osd << " has "
	   << fi.reporters.size() << " reporters, "
	   << grace << " grace (" << orig_grace << " + " << my_grace
	   << " + " << peer_grace << "), max_failed_since " << max_failed_since
	   << dendl;

  if (failed_for >= grace &&
      (int)reporters_by_subtree.size() >= g_conf->mon_osd_min_down_reporters) {
    dout(1) << " we have enough reporters to mark osd." << target_osd
	    << " down" << dendl;
    pending_inc.new_state[target_osd] = CEPH_OSD_UP;

    mon->clog->info() << "osd." << target_osd << " failed ("
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

  mon->clog->info() << "osd." << target_osd << " failed ("
		    << osdmap.crush->get_full_location_ordered_string(target_osd)
		    << ") (connection refused reported by osd." << by << ")";
  return;
}

bool OSDMonitor::prepare_failure(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MOSDFailure *m = static_cast<MOSDFailure*>(op->get_req());
  dout(1) << "prepare_failure " << m->get_target()
	  << " from " << m->get_orig_source_inst()
          << " is reporting failure:" << m->if_osd_failed() << dendl;

  int target_osd = m->get_target().name.num();
  int reporter = m->get_orig_source().num();
  assert(osdmap.is_up(target_osd));
  assert(osdmap.get_addr(target_osd) == m->get_target().addr);

  if (m->if_osd_failed()) {
    // calculate failure time
    utime_t now = ceph_clock_now();
    utime_t failed_since =
      m->get_recv_stamp() - utime_t(m->failed_for, 0);

    // add a report
    if (m->is_immediate()) {
      mon->clog->debug() << m->get_target() << " reported immediately failed by "
            << m->get_orig_source_inst();
      force_failure(target_osd, reporter);
      return true;
    }
    mon->clog->debug() << m->get_target() << " reported failed by "
		      << m->get_orig_source_inst();

    failure_info_t& fi = failure_info[target_osd];
    MonOpRequestRef old_op = fi.add_report(reporter, failed_since, op);
    if (old_op) {
      mon->no_reply(old_op);
    }

    return check_failure(now, target_osd, fi);
  } else {
    // remove the report
    mon->clog->debug() << m->get_target() << " failure report canceled by "
		       << m->get_orig_source_inst();
    if (failure_info.count(target_osd)) {
      failure_info_t& fi = failure_info[target_osd];
      MonOpRequestRef report_op = fi.cancel_report(reporter);
      if (report_op) {
        mon->no_reply(report_op);
      }
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
    mon->no_reply(op);
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


// boot --

bool OSDMonitor::preprocess_boot(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MOSDBoot *m = static_cast<MOSDBoot*>(op->get_req());
  int from = m->get_orig_source_inst().name.num();

  // check permissions, ignore if failed (no response expected)
  MonSession *session = m->get_session();
  if (!session)
    goto ignore;
  if (!session->is_capable("osd", MON_CAP_X)) {
    dout(0) << "got preprocess_boot message from entity with insufficient caps"
	    << session->caps << dendl;
    goto ignore;
  }

  if (m->sb.cluster_fsid != mon->monmap->fsid) {
    dout(0) << "preprocess_boot on fsid " << m->sb.cluster_fsid
	    << " != " << mon->monmap->fsid << dendl;
    goto ignore;
  }

  if (m->get_orig_source_inst().addr.is_blank_ip()) {
    dout(0) << "preprocess_boot got blank addr for " << m->get_orig_source_inst() << dendl;
    goto ignore;
  }

  assert(m->get_orig_source_inst().name.is_osd());

  // check if osd has required features to boot
  if ((osdmap.get_features(CEPH_ENTITY_TYPE_OSD, NULL) &
       CEPH_FEATURE_OSD_ERASURE_CODES) &&
      !(m->get_connection()->get_features() & CEPH_FEATURE_OSD_ERASURE_CODES)) {
    dout(0) << __func__ << " osdmap requires erasure code but osd at "
            << m->get_orig_source_inst()
            << " doesn't announce support -- ignore" << dendl;
    goto ignore;
  }

  if ((osdmap.get_features(CEPH_ENTITY_TYPE_OSD, NULL) &
       CEPH_FEATURE_ERASURE_CODE_PLUGINS_V2) &&
      !(m->get_connection()->get_features() & CEPH_FEATURE_ERASURE_CODE_PLUGINS_V2)) {
    dout(0) << __func__ << " osdmap requires erasure code plugins v2 but osd at "
            << m->get_orig_source_inst()
            << " doesn't announce support -- ignore" << dendl;
    goto ignore;
  }

  if ((osdmap.get_features(CEPH_ENTITY_TYPE_OSD, NULL) &
       CEPH_FEATURE_ERASURE_CODE_PLUGINS_V3) &&
      !(m->get_connection()->get_features() & CEPH_FEATURE_ERASURE_CODE_PLUGINS_V3)) {
    dout(0) << __func__ << " osdmap requires erasure code plugins v3 but osd at "
            << m->get_orig_source_inst()
            << " doesn't announce support -- ignore" << dendl;
    goto ignore;
  }

  if (osdmap.require_osd_release >= CEPH_RELEASE_LUMINOUS &&
      !HAVE_FEATURE(m->osd_features, SERVER_LUMINOUS)) {
    mon->clog->info() << "disallowing boot of OSD "
		      << m->get_orig_source_inst()
		      << " because the osdmap requires"
		      << " CEPH_FEATURE_SERVER_LUMINOUS"
		      << " but the osd lacks CEPH_FEATURE_SERVER_LUMINOUS";
    goto ignore;
  }

  if (osdmap.require_osd_release >= CEPH_RELEASE_JEWEL &&
      !(m->osd_features & CEPH_FEATURE_SERVER_JEWEL)) {
    mon->clog->info() << "disallowing boot of OSD "
		      << m->get_orig_source_inst()
		      << " because the osdmap requires"
		      << " CEPH_FEATURE_SERVER_JEWEL"
		      << " but the osd lacks CEPH_FEATURE_SERVER_JEWEL";
    goto ignore;
  }

  if (osdmap.require_osd_release >= CEPH_RELEASE_KRAKEN &&
      !HAVE_FEATURE(m->osd_features, SERVER_KRAKEN)) {
    mon->clog->info() << "disallowing boot of OSD "
		      << m->get_orig_source_inst()
		      << " because the osdmap requires"
		      << " CEPH_FEATURE_SERVER_KRAKEN"
		      << " but the osd lacks CEPH_FEATURE_SERVER_KRAKEN";
    goto ignore;
  }

  if (osdmap.test_flag(CEPH_OSDMAP_SORTBITWISE) &&
      !(m->osd_features & CEPH_FEATURE_OSD_BITWISE_HOBJ_SORT)) {
    mon->clog->info() << "disallowing boot of OSD "
		      << m->get_orig_source_inst()
		      << " because 'sortbitwise' osdmap flag is set and OSD lacks the OSD_BITWISE_HOBJ_SORT feature";
    goto ignore;
  }

  if (osdmap.test_flag(CEPH_OSDMAP_RECOVERY_DELETES) &&
      !(m->osd_features & CEPH_FEATURE_OSD_RECOVERY_DELETES)) {
    mon->clog->info() << "disallowing boot of OSD "
		      << m->get_orig_source_inst()
		      << " because 'recovery_deletes' osdmap flag is set and OSD lacks the OSD_RECOVERY_DELETES feature";
    goto ignore;
  }

  if (any_of(osdmap.get_pools().begin(),
	     osdmap.get_pools().end(),
	     [](const std::pair<int64_t,pg_pool_t>& pool)
	     { return pool.second.use_gmt_hitset; })) {
    assert(osdmap.get_num_up_osds() == 0 ||
	   osdmap.get_up_osd_features() & CEPH_FEATURE_OSD_HITSET_GMT);
    if (!(m->osd_features & CEPH_FEATURE_OSD_HITSET_GMT)) {
      dout(0) << __func__ << " one or more pools uses GMT hitsets but osd at "
	      << m->get_orig_source_inst()
	      << " doesn't announce support -- ignore" << dendl;
      goto ignore;
    }
  }

  // make sure upgrades stop at luminous
  if (HAVE_FEATURE(m->osd_features, SERVER_M) &&
      osdmap.require_osd_release < CEPH_RELEASE_LUMINOUS) {
    mon->clog->info() << "disallowing boot of post-luminous OSD "
		      << m->get_orig_source_inst()
		      << " because require_osd_release < luminous";
    goto ignore;
  }

  // make sure upgrades stop at jewel
  if (HAVE_FEATURE(m->osd_features, SERVER_KRAKEN) &&
      osdmap.require_osd_release < CEPH_RELEASE_JEWEL) {
    mon->clog->info() << "disallowing boot of post-jewel OSD "
		      << m->get_orig_source_inst()
		      << " because require_osd_release < jewel";
    goto ignore;
  }

  // make sure upgrades stop at hammer
  //  * HAMMER_0_94_4 is the required hammer feature
  //  * MON_METADATA is the first post-hammer feature
  if (osdmap.get_num_up_osds() > 0) {
    if ((m->osd_features & CEPH_FEATURE_MON_METADATA) &&
	!(osdmap.get_up_osd_features() & CEPH_FEATURE_HAMMER_0_94_4)) {
      mon->clog->info() << "disallowing boot of post-hammer OSD "
			<< m->get_orig_source_inst()
			<< " because one or more up OSDs is pre-hammer v0.94.4";
      goto ignore;
    }
    if (!(m->osd_features & CEPH_FEATURE_HAMMER_0_94_4) &&
	(osdmap.get_up_osd_features() & CEPH_FEATURE_MON_METADATA)) {
      mon->clog->info() << "disallowing boot of pre-hammer v0.94.4 OSD "
			<< m->get_orig_source_inst()
			<< " because all up OSDs are post-hammer";
      goto ignore;
    }
  }

  // already booted?
  if (osdmap.is_up(from) &&
      osdmap.get_inst(from) == m->get_orig_source_inst() &&
      osdmap.get_cluster_addr(from) == m->cluster_addr) {
    // yup.
    dout(7) << "preprocess_boot dup from " << m->get_orig_source_inst()
	    << " == " << osdmap.get_inst(from) << dendl;
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
      osdmap.get_most_recent_inst(from) == m->get_orig_source_inst()) {
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
  MOSDBoot *m = static_cast<MOSDBoot*>(op->get_req());
  dout(7) << __func__ << " from " << m->get_orig_source_inst() << " sb " << m->sb
	  << " cluster_addr " << m->cluster_addr
	  << " hb_back_addr " << m->hb_back_addr
	  << " hb_front_addr " << m->hb_front_addr
	  << dendl;

  assert(m->get_orig_source().is_osd());
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
    dout(7) << __func__ << " was up, first marking down "
	    << osdmap.get_inst(from) << dendl;
    // preprocess should have caught these;  if not, assert.
    assert(osdmap.get_inst(from) != m->get_orig_source_inst() ||
           osdmap.get_cluster_addr(from) != m->cluster_addr);
    assert(osdmap.get_uuid(from) == m->sb.osd_fsid);

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
    pending_inc.new_up_client[from] = m->get_orig_source_addr();
    if (!m->cluster_addr.is_blank_ip())
      pending_inc.new_up_cluster[from] = m->cluster_addr;
    pending_inc.new_hb_back_up[from] = m->hb_back_addr;
    if (!m->hb_front_addr.is_blank_ip())
      pending_inc.new_hb_front_up[from] = m->hb_front_addr;

    down_pending_out.erase(from);  // if any

    if (m->sb.weight)
      osd_weight[from] = m->sb.weight;

    // set uuid?
    dout(10) << " setting osd." << from << " uuid to " << m->sb.osd_fsid
	     << dendl;
    if (!osdmap.exists(from) || osdmap.get_uuid(from) != m->sb.osd_fsid) {
      // preprocess should have caught this;  if not, assert.
      assert(!osdmap.exists(from) || osdmap.get_uuid(from).is_zero());
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
    ::encode(m->metadata, osd_metadata);
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

    osd_xinfo_t xi = osdmap.get_xinfo(from);
    if (m->boot_epoch == 0) {
      xi.laggy_probability *= (1.0 - g_conf->mon_osd_laggy_weight);
      xi.laggy_interval *= (1.0 - g_conf->mon_osd_laggy_weight);
      dout(10) << " not laggy, new xi " << xi << dendl;
    } else {
      if (xi.down_stamp.sec()) {
        int interval = ceph_clock_now().sec() -
	  xi.down_stamp.sec();
        if (g_conf->mon_osd_laggy_max_interval &&
	    (interval > g_conf->mon_osd_laggy_max_interval)) {
          interval =  g_conf->mon_osd_laggy_max_interval;
        }
        xi.laggy_interval =
	  interval * g_conf->mon_osd_laggy_weight +
	  xi.laggy_interval * (1.0 - g_conf->mon_osd_laggy_weight);
      }
      xi.laggy_probability =
	g_conf->mon_osd_laggy_weight +
	xi.laggy_probability * (1.0 - g_conf->mon_osd_laggy_weight);
      dout(10) << " laggy, now xi " << xi << dendl;
    }

    // set features shared by the osd
    if (m->osd_features)
      xi.features = m->osd_features;
    else
      xi.features = m->get_connection()->get_features();

    // mark in?
    if ((g_conf->mon_osd_auto_mark_auto_out_in &&
	 (oldstate & CEPH_OSD_AUTOOUT)) ||
	(g_conf->mon_osd_auto_mark_new_in && (oldstate & CEPH_OSD_NEW)) ||
	(g_conf->mon_osd_auto_mark_in)) {
      if (can_mark_in(from)) {
	if (osdmap.osd_xinfo[from].old_weight > 0) {
	  pending_inc.new_weight[from] = osdmap.osd_xinfo[from].old_weight;
	  xi.old_weight = 0;
	} else {
	  pending_inc.new_weight[from] = CEPH_OSD_IN;
	}
      } else {
	dout(7) << __func__ << " NOIN set, will not mark in "
		<< m->get_orig_source_addr() << dendl;
      }
    }

    pending_inc.new_xinfo[from] = xi;

    // wait
    wait_for_finished_proposal(op, new C_Booted(this, op));
  }
  return true;
}

void OSDMonitor::_booted(MonOpRequestRef op, bool logit)
{
  op->mark_osdmon_event(__func__);
  MOSDBoot *m = static_cast<MOSDBoot*>(op->get_req());
  dout(7) << "_booted " << m->get_orig_source_inst() 
	  << " w " << m->sb.weight << " from " << m->sb.current_epoch << dendl;

  if (logit) {
    mon->clog->info() << m->get_orig_source_inst() << " boot";
  }

  send_latest(op, m->sb.current_epoch+1);
}


// -------------
// full

bool OSDMonitor::preprocess_full(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MOSDFull *m = static_cast<MOSDFull*>(op->get_req());
  int from = m->get_orig_source().num();
  set<string> state;
  unsigned mask = CEPH_OSD_NEARFULL | CEPH_OSD_BACKFILLFULL | CEPH_OSD_FULL;

  // check permissions, ignore if failed
  MonSession *session = m->get_session();
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
       osdmap.get_most_recent_inst(from) == m->get_orig_source_inst()) ||
      (osdmap.is_up(from) &&
       osdmap.get_inst(from) != m->get_orig_source_inst())) {
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
  const MOSDFull *m = static_cast<MOSDFull*>(op->get_req());
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
  MOSDAlive *m = static_cast<MOSDAlive*>(op->get_req());
  int from = m->get_orig_source().num();

  // check permissions, ignore if failed
  MonSession *session = m->get_session();
  if (!session)
    goto ignore;
  if (!session->is_capable("osd", MON_CAP_X)) {
    dout(0) << "attempt to send MOSDAlive from entity with insufficient privileges:"
	    << session->caps << dendl;
    goto ignore;
  }

  if (!osdmap.is_up(from) ||
      osdmap.get_inst(from) != m->get_orig_source_inst()) {
    dout(7) << "preprocess_alive ignoring alive message from down " << m->get_orig_source_inst() << dendl;
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
  MOSDAlive *m = static_cast<MOSDAlive*>(op->get_req());
  int from = m->get_orig_source().num();

  if (0) {  // we probably don't care much about these
    mon->clog->debug() << m->get_orig_source_inst() << " alive";
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
  auto m = static_cast<MOSDPGCreated*>(op->get_req());
  dout(10) << __func__ << " " << *m << dendl;
  auto session = m->get_session();
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
  auto m = static_cast<MOSDPGCreated*>(op->get_req());
  dout(10) << __func__ << " " << *m << dendl;
  auto src = m->get_orig_source();
  auto from = src.num();
  if (!src.is_osd() ||
      !mon->osdmon()->osdmap.is_up(from) ||
      m->get_orig_source_inst() != mon->osdmon()->osdmap.get_inst(from)) {
    dout(1) << __func__ << " ignoring stats from non-active osd." << dendl;
    return false;
  }
  pending_created_pgs.push_back(m->pgid);
  return true;
}

// -------------
// pg_temp changes

bool OSDMonitor::preprocess_pgtemp(MonOpRequestRef op)
{
  MOSDPGTemp *m = static_cast<MOSDPGTemp*>(op->get_req());
  dout(10) << "preprocess_pgtemp " << *m << dendl;
  mempool::osdmap::vector<int> empty;
  int from = m->get_orig_source().num();
  size_t ignore_cnt = 0;

  // check caps
  MonSession *session = m->get_session();
  if (!session)
    goto ignore;
  if (!session->is_capable("osd", MON_CAP_X)) {
    dout(0) << "attempt to send MOSDPGTemp from entity with insufficient caps "
	    << session->caps << dendl;
    goto ignore;
  }

  if (!osdmap.is_up(from) ||
      osdmap.get_inst(from) != m->get_orig_source_inst()) {
    dout(7) << "ignoring pgtemp message from down " << m->get_orig_source_inst() << dendl;
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
	 !vectors_equal(osdmap.pg_temp->get(p->first), p->second) ||
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
  MOSDPGTemp *m = static_cast<MOSDPGTemp*>(op->get_req());
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
  MRemoveSnaps *m = static_cast<MRemoveSnaps*>(op->get_req());
  dout(7) << "preprocess_remove_snaps " << *m << dendl;

  // check privilege, ignore if failed
  MonSession *session = m->get_session();
  if (!session)
    goto ignore;
  if (!session->caps.is_capable(
	g_ceph_context,
	CEPH_ENTITY_TYPE_MON,
	session->entity_name,
        "osd", "osd pool rmsnap", {}, true, true, false)) {
    dout(0) << "got preprocess_remove_snaps from entity with insufficient caps "
	    << session->caps << dendl;
    goto ignore;
  }

  for (map<int, vector<snapid_t> >::iterator q = m->snaps.begin();
       q != m->snaps.end();
       ++q) {
    if (!osdmap.have_pg_pool(q->first)) {
      dout(10) << " ignoring removed_snaps " << q->second << " on non-existent pool " << q->first << dendl;
      continue;
    }
    const pg_pool_t *pi = osdmap.get_pg_pool(q->first);
    for (vector<snapid_t>::iterator p = q->second.begin();
	 p != q->second.end();
	 ++p) {
      if (*p > pi->get_snap_seq() ||
	  !pi->removed_snaps.contains(*p))
	return false;
    }
  }

 ignore:
  return true;
}

bool OSDMonitor::prepare_remove_snaps(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MRemoveSnaps *m = static_cast<MRemoveSnaps*>(op->get_req());
  dout(7) << "prepare_remove_snaps " << *m << dendl;

  for (map<int, vector<snapid_t> >::iterator p = m->snaps.begin();
       p != m->snaps.end();
       ++p) {

    if (!osdmap.have_pg_pool(p->first)) {
      dout(10) << " ignoring removed_snaps " << p->second << " on non-existent pool " << p->first << dendl;
      continue;
    }

    pg_pool_t& pi = osdmap.pools[p->first];
    for (vector<snapid_t>::iterator q = p->second.begin();
	 q != p->second.end();
	 ++q) {
      if (!pi.removed_snaps.contains(*q) &&
	  (!pending_inc.new_pools.count(p->first) ||
	   !pending_inc.new_pools[p->first].removed_snaps.contains(*q))) {
	pg_pool_t *newpi = pending_inc.get_new_pool(p->first, &pi);
	newpi->removed_snaps.insert(*q);
	dout(10) << " pool " << p->first << " removed_snaps added " << *q
		 << " (now " << newpi->removed_snaps << ")" << dendl;
	if (*q > newpi->get_snap_seq()) {
	  dout(10) << " pool " << p->first << " snap_seq " << newpi->get_snap_seq() << " -> " << *q << dendl;
	  newpi->set_snap_seq(*q);
	}
	newpi->set_snap_epoch(pending_inc.epoch);
      }
    }
  }
  return true;
}

// osd beacon
bool OSDMonitor::preprocess_beacon(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  auto beacon = static_cast<MOSDBeacon*>(op->get_req());
  // check caps
  auto session = beacon->get_session();
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
  const auto beacon = static_cast<MOSDBeacon*>(op->get_req());
  const auto src = beacon->get_orig_source();
  dout(10) << __func__ << " " << *beacon
	   << " from " << src << dendl;
  int from = src.num();

  if (!src.is_osd() ||
      !osdmap.is_up(from) ||
      beacon->get_orig_source_inst() != osdmap.get_inst(from)) {
    dout(1) << " ignoring beacon from non-active osd." << dendl;
    return false;
  }

  last_osd_report[from] = ceph_clock_now();
  osd_epochs[from] = beacon->version;

  for (const auto& pg : beacon->pgs) {
    last_epoch_clean.report(pg, beacon->min_last_epoch_clean);
  }
  return false;
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


MOSDMap *OSDMonitor::build_latest_full()
{
  MOSDMap *r = new MOSDMap(mon->monmap->fsid);
  get_version_full(osdmap.get_epoch(), r->maps[osdmap.get_epoch()]);
  r->oldest_map = get_first_committed();
  r->newest_map = osdmap.get_epoch();
  return r;
}

MOSDMap *OSDMonitor::build_incremental(epoch_t from, epoch_t to)
{
  dout(10) << "build_incremental [" << from << ".." << to << "]" << dendl;
  MOSDMap *m = new MOSDMap(mon->monmap->fsid);
  m->oldest_map = get_first_committed();
  m->newest_map = osdmap.get_epoch();

  for (epoch_t e = to; e >= from && e > 0; e--) {
    bufferlist bl;
    int err = get_version(e, bl);
    if (err == 0) {
      assert(bl.length());
      // if (get_version(e, bl) > 0) {
      dout(20) << "build_incremental    inc " << e << " "
	       << bl.length() << " bytes" << dendl;
      m->incremental_maps[e] = bl;
    } else {
      assert(err == -ENOENT);
      assert(!bl.length());
      get_version_full(e, bl);
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
  mon->send_reply(op, build_latest_full());
}

void OSDMonitor::send_incremental(MonOpRequestRef op, epoch_t first)
{
  op->mark_osdmon_event(__func__);

  MonSession *s = op->get_session();
  assert(s);

  if (s->proxy_con &&
      s->proxy_con->has_feature(CEPH_FEATURE_MON_ROUTE_OSDMAP)) {
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
	  << " to " << session->inst << dendl;

  if (first <= session->osd_epoch) {
    dout(10) << __func__ << " " << session->inst << " should already have epoch "
	     << session->osd_epoch << dendl;
    first = session->osd_epoch + 1;
  }

  if (first < get_first_committed()) {
    first = get_first_committed();
    bufferlist bl;
    int err = get_version_full(first, bl);
    assert(err == 0);
    assert(bl.length());

    dout(20) << "send_incremental starting with base full "
	     << first << " " << bl.length() << " bytes" << dendl;

    MOSDMap *m = new MOSDMap(osdmap.get_fsid());
    m->oldest_map = get_first_committed();
    m->newest_map = osdmap.get_epoch();
    m->maps[first] = bl;

    if (req) {
      mon->send_reply(req, m);
      session->osd_epoch = first;
      return;
    } else {
      session->con->send_message(m);
      session->osd_epoch = first;
    }
    first++;
  }

  while (first <= osdmap.get_epoch()) {
    epoch_t last = MIN(first + g_conf->osd_map_message_max - 1,
		       osdmap.get_epoch());
    MOSDMap *m = build_incremental(first, last);

    if (req) {
      // send some maps.  it may not be all of them, but it will get them
      // started.
      mon->send_reply(req, m);
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
    if (inc_osd_cache.lookup(ver, &bl)) {
      return 0;
    }
    int ret = PaxosService::get_version(ver, bl);
    if (!ret) {
      inc_osd_cache.add(ver, bl);
    }
    return ret;
}

int OSDMonitor::get_version_full(version_t ver, bufferlist& bl)
{
    if (full_osd_cache.lookup(ver, &bl)) {
      return 0;
    }
    int ret = PaxosService::get_version_full(ver, bl);
    if (!ret) {
      full_osd_cache.add(ver, bl);
    }
    return ret;
}

epoch_t OSDMonitor::blacklist(const entity_addr_t& a, utime_t until)
{
  dout(10) << "blacklist " << a << " until " << until << dendl;
  pending_inc.new_blacklist[a] = until;
  return pending_inc.epoch;
}


void OSDMonitor::check_osdmap_subs()
{
  dout(10) << __func__ << dendl;
  if (!osdmap.get_epoch()) {
    return;
  }
  auto osdmap_subs = mon->session_map.subs.find("osdmap");
  if (osdmap_subs == mon->session_map.subs.end()) {
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
      sub->session->con->send_message(build_latest_full());
    if (sub->onetime)
      mon->session_map.remove_sub(sub);
    else
      sub->next = osdmap.get_epoch() + 1;
  }
}

void OSDMonitor::check_pg_creates_subs()
{
  if (!mon->monmap->get_required_features().contains_all(
	ceph::features::mon::FEATURE_LUMINOUS)) {
    // PGMonitor takes care of this in pre-luminous era.
    return;
  }
  if (!osdmap.get_num_up_osds()) {
    return;
  }
  assert(osdmap.get_up_osd_features() & CEPH_FEATURE_MON_STATEFUL_SUB);
  mon->with_session_map([this](const MonSessionMap& session_map) {
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
  dout(20) << __func__ << " .. " << sub->session->inst << dendl;
  assert(sub->type == "osd_pg_creates");
  // only send these if the OSD is up.  we will check_subs() when they do
  // come up so they will get the creates then.
  if (sub->session->inst.name.is_osd() &&
      mon->osdmon()->osdmap.is_up(sub->session->inst.name.num())) {
    sub->next = send_pg_creates(sub->session->inst.name.num(),
				sub->session->con.get(),
				sub->next);
  }
}

void OSDMonitor::do_application_enable(int64_t pool_id,
                                       const std::string &app_name)
{
  assert(paxos->is_plugged() && is_writeable());

  dout(20) << __func__ << ": pool_id=" << pool_id << ", app_name=" << app_name
           << dendl;

  assert(osdmap.require_osd_release >= CEPH_RELEASE_LUMINOUS ||
	 pending_inc.new_require_osd_release >= CEPH_RELEASE_LUMINOUS);

  auto pp = osdmap.get_pg_pool(pool_id);
  assert(pp != nullptr);

  pg_pool_t p = *pp;
  if (pending_inc.new_pools.count(pool_id)) {
    p = pending_inc.new_pools[pool_id];
  }

  p.application_metadata.insert({app_name, {}});
  p.last_change = pending_inc.epoch;
  pending_inc.new_pools[pool_id] = p;
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
    const pg_pool_t& pool = p.second;
    int ruleno = osdmap.crush->find_rule(pool.get_crush_rule(),
					 pool.get_type(), pool.get_size());
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
    if (creating_pgs->create_pool(poolid, pool.get_pg_num(),
				  created, modified)) {
      queued++;
    }
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
    auto mapped = pg.second.first;
    dout(20) << __func__ << " looking up " << pgid << "@" << mapped << dendl;
    mapping.get(pgid, nullptr, nullptr, nullptr, &acting_primary);
    // check the previous creating_pgs, look for the target to whom the pg was
    // previously mapped
    for (const auto& pgs_by_epoch : creating_pgs_by_osd_epoch) {
      const auto last_acting_primary = pgs_by_epoch.first;
      for (auto& pgs: pgs_by_epoch.second) {
	if (pgs.second.count(pgid)) {
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
    new_pgs_by_osd_epoch[acting_primary][mapped].insert(pgid);
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
  assert(!creating_pgs_by_epoch->second.empty());

  MOSDPGCreate *m = nullptr;
  epoch_t last = 0;
  for (auto epoch_pgs = creating_pgs_by_epoch->second.lower_bound(next);
       epoch_pgs != creating_pgs_by_epoch->second.end(); ++epoch_pgs) {
    auto epoch = epoch_pgs->first;
    auto& pgs = epoch_pgs->second;
    dout(20) << __func__ << " osd." << osd << " from " << next
             << " : epoch " << epoch << " " << pgs.size() << " pgs" << dendl;
    last = epoch;
    for (auto& pg : pgs) {
      if (!m)
	m = new MOSDPGCreate(creating_pgs_epoch);
      // Need the create time from the monitor using its clock to set
      // last_scrub_stamp upon pg creation.
      auto create = creating_pgs.pgs.find(pg);
      assert(create != creating_pgs.pgs.end());
      m->mkpg.emplace(pg, pg_create_t{create->second.first, pg, 0});
      m->ctimes.emplace(pg, create->second.second);
      dout(20) << __func__ << " will create " << pg
	       << " at " << create->second.first << dendl;
    }
  }
  if (!m) {
    dout(20) << __func__ << " osd." << osd << " from " << next
             << " has nothing to send" << dendl;
    return next;
  }
  con->send_message(m);
  // sub is current through last + 1
  return last + 1;
}

// TICK


void OSDMonitor::tick()
{
  if (!is_active()) return;

  dout(10) << osdmap << dendl;

  if (!mon->is_leader()) return;

  bool do_propose = false;
  utime_t now = ceph_clock_now();

  if (osdmap.require_osd_release >= CEPH_RELEASE_LUMINOUS &&
      mon->monmap->get_required_features().contains_all(
	ceph::features::mon::FEATURE_LUMINOUS)) {
    if (handle_osd_timeouts(now, last_osd_report)) {
      do_propose = true;
    }
  }
  if (!osdmap.test_flag(CEPH_OSDMAP_PURGED_SNAPDIRS) &&
      osdmap.require_osd_release >= CEPH_RELEASE_LUMINOUS &&
      mon->mgrstatmon()->is_readable() &&
      mon->mgrstatmon()->definitely_converted_snapsets()) {
    dout(1) << __func__ << " all snapsets converted, setting purged_snapdirs"
	    << dendl;
    add_flag(CEPH_OSDMAP_PURGED_SNAPDIRS);
    do_propose = true;
  }

  // mark osds down?
  if (check_failures(now))
    do_propose = true;

  // mark down osds out?

  /* can_mark_out() checks if we can mark osds as being out. The -1 has no
   * influence at all. The decision is made based on the ratio of "in" osds,
   * and the function returns false if this ratio is lower that the minimum
   * ratio set by g_conf->mon_osd_min_in_ratio. So it's not really up to us.
   */
  if (can_mark_out(-1)) {
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
	utime_t orig_grace(g_conf->mon_osd_down_out_interval, 0);
	utime_t grace = orig_grace;
	double my_grace = 0.0;

	if (g_conf->mon_osd_adjust_down_out_interval) {
	  // scale grace period the same way we do the heartbeat grace.
	  const osd_xinfo_t& xi = osdmap.get_xinfo(o);
	  double halflife = (double)g_conf->mon_osd_laggy_halflife;
	  double decay_k = ::log(.5) / halflife;
	  double decay = exp((double)down * decay_k);
	  dout(20) << "osd." << o << " laggy halflife " << halflife << " decay_k " << decay_k
		   << " down for " << down << " decay " << decay << dendl;
	  my_grace = decay * (double)xi.laggy_interval * xi.laggy_probability;
	  grace += my_grace;
	}

	// is this an entire large subtree down?
	if (g_conf->mon_osd_down_out_subtree_limit.length()) {
	  int type = osdmap.crush->get_type_id(g_conf->mon_osd_down_out_subtree_limit);
	  if (type > 0) {
	    if (osdmap.containing_subtree_is_down(g_ceph_context, o, type, &down_cache)) {
	      dout(10) << "tick entire containing " << g_conf->mon_osd_down_out_subtree_limit
		       << " subtree for osd." << o << " is down; resetting timer" << dendl;
	      // reset timer, too.
	      down_pending_out[o] = now;
	      continue;
	    }
	  }
	}

        bool down_out = !osdmap.is_destroyed(o) &&
          g_conf->mon_osd_down_out_interval > 0 && down.sec() >= grace;
        bool destroyed_out = osdmap.is_destroyed(o) &&
          g_conf->mon_osd_destroyed_out_interval > 0 &&
        // this is not precise enough as we did not make a note when this osd
        // was marked as destroyed, but let's not bother with that
        // complexity for now.
          down.sec() >= g_conf->mon_osd_destroyed_out_interval;
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

	  mon->clog->info() << "Marking osd." << o << " out (has been down for "
                            << int(down.sec()) << " seconds)";
	} else
	  continue;
      }

      down_pending_out.erase(o);
    }
  } else {
    dout(10) << "tick NOOUT flag set, not checking down osds" << dendl;
  }

  // expire blacklisted items?
  for (ceph::unordered_map<entity_addr_t,utime_t>::iterator p = osdmap.blacklist.begin();
       p != osdmap.blacklist.end();
       ++p) {
    if (p->second < now) {
      dout(10) << "expiring blacklist item " << p->first << " expired " << p->second << " < now " << now << dendl;
      pending_inc.old_blacklist.push_back(p->first);
      do_propose = true;
    }
  }

  // if map full setting has changed, get that info out there!
  if (osdmap.require_osd_release < CEPH_RELEASE_LUMINOUS &&
      mon->pgservice->is_readable()) {
    // for pre-luminous compat only!
    if (mon->pgservice->have_full_osds()) {
      dout(5) << "There are full osds, setting full flag" << dendl;
      add_flag(CEPH_OSDMAP_FULL);
    } else if (osdmap.test_flag(CEPH_OSDMAP_FULL)){
      dout(10) << "No full osds, removing full flag" << dendl;
      remove_flag(CEPH_OSDMAP_FULL);
    }

    if (mon->pgservice->have_nearfull_osds()) {
      dout(5) << "There are near full osds, setting nearfull flag" << dendl;
      add_flag(CEPH_OSDMAP_NEARFULL);
    } else if (osdmap.test_flag(CEPH_OSDMAP_NEARFULL)){
      dout(10) << "No near full osds, removing nearfull flag" << dendl;
      remove_flag(CEPH_OSDMAP_NEARFULL);
    }
    if (pending_inc.new_flags != -1 &&
       (pending_inc.new_flags ^ osdmap.flags) & (CEPH_OSDMAP_FULL | CEPH_OSDMAP_NEARFULL)) {
      dout(1) << "New setting for" <<
              (pending_inc.new_flags & CEPH_OSDMAP_FULL ? " CEPH_OSDMAP_FULL" : "") <<
              (pending_inc.new_flags & CEPH_OSDMAP_NEARFULL ? " CEPH_OSDMAP_NEARFULL" : "")
              << " -- doing propose" << dendl;
      do_propose = true;
    }
  }

  if (update_pools_status())
    do_propose = true;

  if (do_propose ||
      !pending_inc.new_pg_temp.empty())  // also propose if we adjusted pg_temp
    propose_pending();
}

bool OSDMonitor::handle_osd_timeouts(const utime_t &now,
				     std::map<int,utime_t> &last_osd_report)
{
  utime_t timeo(g_conf->mon_osd_report_timeout, 0);
  if (now - mon->get_leader_since() < timeo) {
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
    const std::map<int,utime_t>::const_iterator t = last_osd_report.find(i);
    if (t == last_osd_report.end()) {
      // it wasn't in the map; start the timer.
      last_osd_report[i] = now;
    } else if (can_mark_down(i)) {
      utime_t diff = now - t->second;
      if (diff > timeo) {
	mon->clog->info() << "osd." << i << " marked down after no beacon for "
			  << diff << " seconds";
	derr << "no beacon from osd." << i << " since " << t->second
	     << ", " << diff << " seconds ago.  marking down" << dendl;
	pending_inc.new_state[i] = CEPH_OSD_UP;
	new_down = true;
      }
    }
  }
  return new_down;
}

void OSDMonitor::get_health(list<pair<health_status_t,string> >& summary,
			    list<pair<health_status_t,string> > *detail,
			    CephContext *cct) const
{
  int num_osds = osdmap.get_num_osds();

  if (num_osds == 0) {
    summary.push_back(make_pair(HEALTH_ERR, "no osds"));
  } else {
    int num_in_osds = 0;
    int num_down_in_osds = 0;
    set<int> osds;
    set<int> down_in_osds;
    set<int> up_in_osds;
    set<int> subtree_up;
    unordered_map<int, set<int> > subtree_type_down;
    unordered_map<int, int> num_osds_subtree;
    int max_type = osdmap.crush->get_max_type_id();

    for (int i = 0; i < osdmap.get_max_osd(); i++) {
      if (!osdmap.exists(i)) {
        if (osdmap.crush->item_exists(i)) {
          osds.insert(i);
        }
	continue;
      }
      if (osdmap.is_out(i))
        continue;
      ++num_in_osds;
      if (down_in_osds.count(i) || up_in_osds.count(i))
	continue;
      if (!osdmap.is_up(i)) {
	down_in_osds.insert(i);
	int parent_id = 0;
	int current = i;
	for (int type = 0; type <= max_type; type++) {
	  if (!osdmap.crush->get_type_name(type))
	    continue;
	  int r = osdmap.crush->get_immediate_parent_id(current, &parent_id);
	  if (r == -ENOENT)
	    break;
	  // break early if this parent is already marked as up
	  if (subtree_up.count(parent_id))
	    break;
	  type = osdmap.crush->get_bucket_type(parent_id);
	  if (!osdmap.subtree_type_is_down(
		g_ceph_context, parent_id, type,
		&down_in_osds, &up_in_osds, &subtree_up, &subtree_type_down))
	    break;
	  current = parent_id;
	}
      }
    }

    // calculate the number of down osds in each down subtree and
    // store it in num_osds_subtree
    for (int type = 1; type <= max_type; type++) {
      if (!osdmap.crush->get_type_name(type))
	continue;
      for (auto j = subtree_type_down[type].begin();
	   j != subtree_type_down[type].end();
	   ++j) {
	if (type == 1) {
          list<int> children;
          int num = osdmap.crush->get_children(*j, &children);
          num_osds_subtree[*j] = num;
        } else {
          list<int> children;
          int num = 0;
          int num_children = osdmap.crush->get_children(*j, &children);
          if (num_children == 0)
	    continue;
          for (auto l = children.begin(); l != children.end(); ++l) {
            if (num_osds_subtree[*l] > 0) {
              num = num + num_osds_subtree[*l];
            }
          }
          num_osds_subtree[*j] = num;
	}
      }
    }
    num_down_in_osds = down_in_osds.size();
    assert(num_down_in_osds <= num_in_osds);
    if (num_down_in_osds > 0) {
      // summary of down subtree types and osds
      for (int type = max_type; type > 0; type--) {
	if (!osdmap.crush->get_type_name(type))
	  continue;
	if (subtree_type_down[type].size() > 0) {
	  ostringstream ss;
	  ss << subtree_type_down[type].size() << " "
	     << osdmap.crush->get_type_name(type);
	  if (subtree_type_down[type].size() > 1) {
	    ss << "s";
	  }
	  int sum_down_osds = 0;
	  for (auto j = subtree_type_down[type].begin();
	       j != subtree_type_down[type].end();
	       ++j) {
	    sum_down_osds = sum_down_osds + num_osds_subtree[*j];
	  }
          ss << " (" << sum_down_osds << " osds) down";
	  summary.push_back(make_pair(HEALTH_WARN, ss.str()));
	}
      }
      ostringstream ss;
      ss << down_in_osds.size() << " osds down";
      summary.push_back(make_pair(HEALTH_WARN, ss.str()));

      if (detail) {
	// details of down subtree types
	for (int type = max_type; type > 0; type--) {
	  if (!osdmap.crush->get_type_name(type))
	    continue;
	  for (auto j = subtree_type_down[type].rbegin();
	       j != subtree_type_down[type].rend();
	       ++j) {
	    ostringstream ss;
	    ss << osdmap.crush->get_type_name(type);
	    ss << " ";
	    ss << osdmap.crush->get_item_name(*j);
	    // at the top level, do not print location
	    if (type != max_type) {
              ss << " (";
              ss << osdmap.crush->get_full_location_ordered_string(*j);
              ss << ")";
	    }
	    int num = num_osds_subtree[*j];
	    ss << " (" << num << " osds)";
	    ss << " is down";
	    detail->push_back(make_pair(HEALTH_WARN, ss.str()));
	  }
        }
	// details of down osds
	for (auto it = down_in_osds.begin(); it != down_in_osds.end(); ++it) {
	  ostringstream ss;
	  ss << "osd." << *it << " (";
	  ss << osdmap.crush->get_full_location_ordered_string(*it);
          ss << ") is down";
	  detail->push_back(make_pair(HEALTH_WARN, ss.str()));
	}
      }
    }

    if (!osds.empty()) {
      ostringstream ss;
      ss << osds.size() << " osds exist in the crush map but not in the osdmap";
      summary.push_back(make_pair(HEALTH_WARN, ss.str()));
      if (detail) {
        ss << " (osds: " << osds << ")";
        detail->push_back(make_pair(HEALTH_WARN, ss.str()));
      }
    }

    // note: we leave it to ceph-mgr to generate details health warnings
    // with actual osd utilizations

    // warn about flags
    uint64_t warn_flags =
      CEPH_OSDMAP_FULL |
      CEPH_OSDMAP_PAUSERD |
      CEPH_OSDMAP_PAUSEWR |
      CEPH_OSDMAP_PAUSEREC |
      CEPH_OSDMAP_NOUP |
      CEPH_OSDMAP_NODOWN |
      CEPH_OSDMAP_NOIN |
      CEPH_OSDMAP_NOOUT |
      CEPH_OSDMAP_NOBACKFILL |
      CEPH_OSDMAP_NORECOVER |
      CEPH_OSDMAP_NOSCRUB |
      CEPH_OSDMAP_NODEEP_SCRUB |
      CEPH_OSDMAP_NOTIERAGENT |
      CEPH_OSDMAP_NOREBALANCE;
    if (osdmap.test_flag(warn_flags)) {
      ostringstream ss;
      ss << osdmap.get_flag_string(osdmap.get_flags() & warn_flags)
	 << " flag(s) set";
      summary.push_back(make_pair(HEALTH_WARN, ss.str()));
      if (detail)
	detail->push_back(make_pair(HEALTH_WARN, ss.str()));
    }

    // old crush tunables?
    if (g_conf->mon_warn_on_legacy_crush_tunables) {
      string min = osdmap.crush->get_min_required_version();
      if (min < g_conf->mon_crush_min_required_version) {
	ostringstream ss;
	ss << "crush map has legacy tunables (require " << min
	   << ", min is " << g_conf->mon_crush_min_required_version << ")";
	summary.push_back(make_pair(HEALTH_WARN, ss.str()));
	if (detail) {
	  ss << "; see http://docs.ceph.com/docs/master/rados/operations/crush-map/#tunables";
	  detail->push_back(make_pair(HEALTH_WARN, ss.str()));
	}
      }
    }
    if (g_conf->mon_warn_on_crush_straw_calc_version_zero) {
      if (osdmap.crush->get_straw_calc_version() == 0) {
	ostringstream ss;
	ss << "crush map has straw_calc_version=0";
	summary.push_back(make_pair(HEALTH_WARN, ss.str()));
	if (detail) {
	  ss << "; see http://docs.ceph.com/docs/master/rados/operations/crush-map/#tunables";
	  detail->push_back(make_pair(HEALTH_WARN, ss.str()));
	}
      }
    }

    // hit_set-less cache_mode?
    if (g_conf->mon_warn_on_cache_pools_without_hit_sets) {
      int problem_cache_pools = 0;
      for (map<int64_t, pg_pool_t>::const_iterator p = osdmap.pools.begin();
	   p != osdmap.pools.end();
	   ++p) {
	const pg_pool_t& info = p->second;
	if (info.cache_mode_requires_hit_set() &&
	    info.hit_set_params.get_type() == HitSet::TYPE_NONE) {
	  ++problem_cache_pools;
	  if (detail) {
	    ostringstream ss;
	    ss << "pool '" << osdmap.get_pool_name(p->first)
	       << "' with cache_mode " << info.get_cache_mode_name()
	       << " needs hit_set_type to be set but it is not";
	    detail->push_back(make_pair(HEALTH_WARN, ss.str()));
	  }
	}
      }
      if (problem_cache_pools) {
	ostringstream ss;
	ss << problem_cache_pools << " cache pools are missing hit_sets";
	summary.push_back(make_pair(HEALTH_WARN, ss.str()));
      }
    }

    // Not using 'sortbitwise' and should be?
    if (!osdmap.test_flag(CEPH_OSDMAP_SORTBITWISE) &&
        (osdmap.get_up_osd_features() &
	 CEPH_FEATURE_OSD_BITWISE_HOBJ_SORT)) {
      ostringstream ss;
      ss << "no legacy OSD present but 'sortbitwise' flag is not set";
      summary.push_back(make_pair(HEALTH_WARN, ss.str()));
    }

    // Warn if 'mon_osd_down_out_interval' is set to zero.
    // Having this option set to zero on the leader acts much like the
    // 'noout' flag.  It's hard to figure out what's going wrong with clusters
    // without the 'noout' flag set but acting like that just the same, so
    // we report a HEALTH_WARN in case this option is set to zero.
    // This is an ugly hack to get the warning out, but until we find a way
    // to spread global options throughout the mon cluster and have all mons
    // using a base set of the same options, we need to work around this sort
    // of things.
    // There's also the obvious drawback that if this is set on a single
    // monitor on a 3-monitor cluster, this warning will only be shown every
    // third monitor connection.
    if (g_conf->mon_warn_on_osd_down_out_interval_zero &&
        g_conf->mon_osd_down_out_interval == 0) {
      ostringstream ss;
      ss << "mon." << mon->name << " has mon_osd_down_out_interval set to 0";
      summary.push_back(make_pair(HEALTH_WARN, ss.str()));
      if (detail) {
        ss << "; this has the same effect as the 'noout' flag";
        detail->push_back(make_pair(HEALTH_WARN, ss.str()));
      }
    }

    // warn about upgrade flags that can be set but are not.
    if (g_conf->mon_debug_no_require_luminous) {
      // ignore these checks
    } else if (HAVE_FEATURE(osdmap.get_up_osd_features(), SERVER_LUMINOUS) &&
	       osdmap.require_osd_release < CEPH_RELEASE_LUMINOUS) {
      string msg = "all OSDs are running luminous or later but"
	" require_osd_release < luminous";
      summary.push_back(make_pair(HEALTH_WARN, msg));
      if (detail) {
	detail->push_back(make_pair(HEALTH_WARN, msg));
      }
    } else if (HAVE_FEATURE(osdmap.get_up_osd_features(), SERVER_KRAKEN) &&
	       osdmap.require_osd_release < CEPH_RELEASE_KRAKEN) {
      string msg = "all OSDs are running kraken or later but"
	" require_osd_release < kraken";
      summary.push_back(make_pair(HEALTH_WARN, msg));
      if (detail) {
	detail->push_back(make_pair(HEALTH_WARN, msg));
      }
    } else if (HAVE_FEATURE(osdmap.get_up_osd_features(), SERVER_JEWEL) &&
	       osdmap.require_osd_release < CEPH_RELEASE_JEWEL) {
      string msg = "all OSDs are running jewel or later but"
	" require_osd_release < jewel";
      summary.push_back(make_pair(HEALTH_WARN, msg));
      if (detail) {
	detail->push_back(make_pair(HEALTH_WARN, msg));
      }
    }

    for (auto it : osdmap.get_pools()) {
      const pg_pool_t &pool = it.second;
      if (pool.has_flag(pg_pool_t::FLAG_FULL)) {
	const string& pool_name = osdmap.get_pool_name(it.first);
	stringstream ss;
	ss << "pool '" << pool_name << "' is full";
	summary.push_back(make_pair(HEALTH_WARN, ss.str()));
	if (detail)
	  detail->push_back(make_pair(HEALTH_WARN, ss.str()));
      }
    }
  }
}

void OSDMonitor::dump_info(Formatter *f)
{
  f->open_object_section("osdmap");
  osdmap.dump(f);
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

  f->dump_unsigned("osdmap_first_committed", get_first_committed());
  f->dump_unsigned("osdmap_last_committed", get_last_committed());

  f->open_object_section("crushmap");
  osdmap.crush->dump(f);
  f->close_section();
}

namespace {
  enum osd_pool_get_choices {
    SIZE, MIN_SIZE, CRASH_REPLAY_INTERVAL,
    PG_NUM, PGP_NUM, CRUSH_RULE, HASHPSPOOL,
    NODELETE, NOPGCHANGE, NOSIZECHANGE,
    WRITE_FADVISE_DONTNEED, NOSCRUB, NODEEP_SCRUB,
    HIT_SET_TYPE, HIT_SET_PERIOD, HIT_SET_COUNT, HIT_SET_FPP,
    USE_GMT_HITSET, AUID, TARGET_MAX_OBJECTS, TARGET_MAX_BYTES,
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
    CSUM_TYPE, CSUM_MAX_BLOCK, CSUM_MIN_BLOCK };

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
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  int r = 0;
  bufferlist rdata;
  stringstream ss, ds;

  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon->reply_command(op, -EINVAL, rs, get_last_committed());
    return true;
  }

  MonSession *session = m->get_session();
  if (!session) {
    mon->reply_command(op, -EACCES, "access denied", get_last_committed());
    return true;
  }

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  string format;
  cmd_getval(g_ceph_context, cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  if (prefix == "osd stat") {
    osdmap.print_summary(f.get(), ds, "");
    if (f)
      f->flush(rdata);
    else
      rdata.append(ds);
  }
  else if (prefix == "osd perf" ||
	   prefix == "osd blocked-by") {
    r = mon->pgservice->process_pg_command(prefix, cmdmap,
					   osdmap, f.get(), &ss, &rdata);
  }
  else if (prefix == "osd dump" ||
	   prefix == "osd tree" ||
	   prefix == "osd ls" ||
	   prefix == "osd getmap" ||
	   prefix == "osd getcrushmap" ||
	   prefix == "osd ls-tree") {
    string val;

    epoch_t epoch = 0;
    int64_t epochnum;
    cmd_getval(g_ceph_context, cmdmap, "epoch", epochnum, (int64_t)osdmap.get_epoch());
    epoch = epochnum;
    
    bufferlist osdmap_bl;
    int err = get_version_full(epoch, osdmap_bl);
    if (err == -ENOENT) {
      r = -ENOENT;
      ss << "there is no map for epoch " << epoch;
      goto reply;
    }
    assert(err == 0);
    assert(osdmap_bl.length());

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
	p->dump(f.get());
	f->close_section();
	f->flush(ds);
      } else {
	p->print(ds);
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
    } else if (prefix == "osd tree") {
      vector<string> states;
      cmd_getval(g_ceph_context, cmdmap, "states", states);
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
	p->print_tree(f.get(), NULL, filter);
	f->close_section();
	f->flush(ds);
      } else {
	p->print_tree(NULL, &ds, filter);
      }
      rdata.append(ds);
    } else if (prefix == "osd getmap") {
      rdata.append(osdmap_bl);
      ss << "got osdmap epoch " << p->get_epoch();
    } else if (prefix == "osd getcrushmap") {
      p->crush->encode(rdata, mon->get_quorum_con_features());
      ss << p->get_crush_version();
    } else if (prefix == "osd ls-tree") {
      string bucket_name;
      cmd_getval(g_ceph_context, cmdmap, "name", bucket_name);
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
  } else if (prefix == "osd df") {
    string method;
    cmd_getval(g_ceph_context, cmdmap, "output_method", method);
    print_osd_utilization(osdmap, mon->pgservice, ds,
			  f.get(), method == "tree");
    rdata.append(ds);
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
    if (!cmd_getval(g_ceph_context, cmdmap, "id", osd)) {
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
    cmd_getval(g_ceph_context, cmdmap, "format", format);
    boost::scoped_ptr<Formatter> f(Formatter::create(format, "json-pretty", "json-pretty"));
    f->open_object_section("osd_location");
    f->dump_int("osd", osd);
    f->dump_stream("ip") << osdmap.get_addr(osd);
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
        !cmd_getval(g_ceph_context, cmdmap, "id", osd)) {
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
    cmd_getval(g_ceph_context, cmdmap, "format", format);
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
    cmd_getval(g_ceph_context, cmdmap, "property", field);
    count_metadata(field, f.get());
    f->flush(rdata);
    r = 0;
  } else if (prefix == "osd map") {
    string poolstr, objstr, namespacestr;
    cmd_getval(g_ceph_context, cmdmap, "pool", poolstr);
    cmd_getval(g_ceph_context, cmdmap, "object", objstr);
    cmd_getval(g_ceph_context, cmdmap, "nspace", namespacestr);

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
    string pgidstr;
    cmd_getval(g_ceph_context, cmdmap, "pgid", pgidstr);
    if (!pgid.parse(pgidstr.c_str())) {
      ss << "invalid pgid '" << pgidstr << "'";
      r = -EINVAL;
      goto reply;
    }
    vector<int> up, acting;
    if (!osdmap.have_pg_pool(pgid.pool())) {
      ss << "pg '" << pgidstr << "' does not exist";
      r = -ENOENT;
      goto reply;
    }
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

  } else if (prefix == "osd scrub" ||
	     prefix == "osd deep-scrub" ||
	     prefix == "osd repair") {
    string whostr;
    cmd_getval(g_ceph_context, cmdmap, "who", whostr);
    vector<string> pvec;
    get_str_vec(prefix, pvec);

    if (whostr == "*" || whostr == "all" || whostr == "any") {
      ss << "osds ";
      int c = 0;
      for (int i = 0; i < osdmap.get_max_osd(); i++)
	if (osdmap.is_up(i)) {
	  ss << (c++ ? "," : "") << i;
	  mon->try_send_message(new MOSDScrub(osdmap.get_fsid(),
					      pvec.back() == "repair",
					      pvec.back() == "deep-scrub"),
				osdmap.get_inst(i));
	}
      r = 0;
      ss << " instructed to " << pvec.back();
    } else {
      long osd = parse_osd_id(whostr.c_str(), &ss);
      if (osd < 0) {
	r = -EINVAL;
      } else if (osdmap.is_up(osd)) {
	mon->try_send_message(new MOSDScrub(osdmap.get_fsid(),
					    pvec.back() == "repair",
					    pvec.back() == "deep-scrub"),
			      osdmap.get_inst(osd));
	ss << "osd." << osd << " instructed to " << pvec.back();
      } else {
	ss << "osd." << osd << " is not up";
	r = -EAGAIN;
      }
    }
  } else if (prefix == "osd lspools") {
    int64_t auid;
    cmd_getval(g_ceph_context, cmdmap, "auid", auid, int64_t(0));
    if (f)
      f->open_array_section("pools");
    for (map<int64_t, pg_pool_t>::iterator p = osdmap.pools.begin();
	 p != osdmap.pools.end();
	 ++p) {
      if (!auid || p->second.auid == (uint64_t)auid) {
	if (f) {
	  f->open_object_section("pool");
	  f->dump_int("poolnum", p->first);
	  f->dump_string("poolname", osdmap.pool_name[p->first]);
	  f->close_section();
	} else {
	  ds << p->first << ' ' << osdmap.pool_name[p->first] << ',';
	}
      }
    }
    if (f) {
      f->close_section();
      f->flush(ds);
    }
    rdata.append(ds);
  } else if (prefix == "osd blacklist ls") {
    if (f)
      f->open_array_section("blacklist");

    for (ceph::unordered_map<entity_addr_t,utime_t>::iterator p = osdmap.blacklist.begin();
	 p != osdmap.blacklist.end();
	 ++p) {
      if (f) {
	f->open_object_section("entry");
	f->dump_stream("addr") << p->first;
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
    ss << "listed " << osdmap.blacklist.size() << " entries";

  } else if (prefix == "osd pool ls") {
    string detail;
    cmd_getval(g_ceph_context, cmdmap, "detail", detail);
    if (!f && detail == "detail") {
      ostringstream ss;
      osdmap.print_pools(ss);
      rdata.append(ss.str());
    } else {
      if (f)
	f->open_array_section("pools");
      for (map<int64_t,pg_pool_t>::const_iterator it = osdmap.get_pools().begin();
	   it != osdmap.get_pools().end();
	   ++it) {
	if (f) {
	  if (detail == "detail") {
	    f->open_object_section("pool");
	    f->dump_string("pool_name", osdmap.get_pool_name(it->first));
	    it->second.dump(f.get());
	    f->close_section();
	  } else {
	    f->dump_string("pool_name", osdmap.get_pool_name(it->first));
	  }
	} else {
	  rdata.append(osdmap.get_pool_name(it->first) + "\n");
	}
      }
      if (f) {
	f->close_section();
	f->flush(rdata);
      }
    }

  } else if (prefix == "osd crush get-tunable") {
    string tunable;
    cmd_getval(g_ceph_context, cmdmap, "tunable", tunable);
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
    cmd_getval(g_ceph_context, cmdmap, "pool", poolstr);
    int64_t pool = osdmap.lookup_pg_pool_name(poolstr.c_str());
    if (pool < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      r = -ENOENT;
      goto reply;
    }

    const pg_pool_t *p = osdmap.get_pg_pool(pool);
    string var;
    cmd_getval(g_ceph_context, cmdmap, "var", var);

    typedef std::map<std::string, osd_pool_get_choices> choices_map_t;
    const choices_map_t ALL_CHOICES = {
      {"size", SIZE},
      {"min_size", MIN_SIZE},
      {"crash_replay_interval", CRASH_REPLAY_INTERVAL},
      {"pg_num", PG_NUM}, {"pgp_num", PGP_NUM},
      {"crush_rule", CRUSH_RULE},
      {"hashpspool", HASHPSPOOL}, {"nodelete", NODELETE},
      {"nopgchange", NOPGCHANGE}, {"nosizechange", NOSIZECHANGE},
      {"noscrub", NOSCRUB}, {"nodeep-scrub", NODEEP_SCRUB},
      {"write_fadvise_dontneed", WRITE_FADVISE_DONTNEED},
      {"hit_set_type", HIT_SET_TYPE}, {"hit_set_period", HIT_SET_PERIOD},
      {"hit_set_count", HIT_SET_COUNT}, {"hit_set_fpp", HIT_SET_FPP},
      {"use_gmt_hitset", USE_GMT_HITSET},
      {"auid", AUID}, {"target_max_objects", TARGET_MAX_OBJECTS},
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
      ERASURE_CODE_PROFILE
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

      selected_choices.insert(selected);
    }

    if (f) {
      for(choices_set_t::const_iterator it = selected_choices.begin();
	  it != selected_choices.end(); ++it) {
	choices_map_t::const_iterator i;
        for (i = ALL_CHOICES.begin(); i != ALL_CHOICES.end(); ++i) {
          if (i->second == *it) {
            break;
          }
        }
        assert(i != ALL_CHOICES.end());
        bool pool_opt = pool_opts_t::is_opt_name(i->first);
        if (!pool_opt) {
          f->open_object_section("pool");
          f->dump_string("pool", poolstr);
          f->dump_int("pool_id", pool);
        }
	switch(*it) {
	  case PG_NUM:
	    f->dump_int("pg_num", p->get_pg_num());
	    break;
	  case PGP_NUM:
	    f->dump_int("pgp_num", p->get_pgp_num());
	    break;
	  case AUID:
	    f->dump_int("auid", p->get_auid());
	    break;
	  case SIZE:
	    f->dump_int("size", p->get_size());
	    break;
	  case MIN_SIZE:
	    f->dump_int("min_size", p->get_min_size());
	    break;
	  case CRASH_REPLAY_INTERVAL:
	    f->dump_int("crash_replay_interval",
			p->get_crash_replay_interval());
	    break;
	  case CRUSH_RULE:
	    if (osdmap.crush->rule_exists(p->get_crush_rule())) {
	      f->dump_string("crush_rule", osdmap.crush->get_rule_name(
			       p->get_crush_rule()));
	    } else {
	      f->dump_string("crush_rule", stringify(p->get_crush_rule()));
	    }
	    break;
	  case HASHPSPOOL:
	  case NODELETE:
	  case NOPGCHANGE:
	  case NOSIZECHANGE:
	  case WRITE_FADVISE_DONTNEED:
	  case NOSCRUB:
	  case NODEEP_SCRUB:
	    f->dump_string(i->first.c_str(),
			   p->has_flag(pg_pool_t::get_flag_by_name(i->first)) ?
			   "true" : "false");
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
            pool_opts_t::key_t key = pool_opts_t::get_opt_desc(i->first).key;
            if (p->opts.is_set(key)) {
              f->open_object_section("pool");
              f->dump_string("pool", poolstr);
              f->dump_int("pool_id", pool);
              if(*it == CSUM_TYPE) {
                int val;
                p->opts.get(pool_opts_t::CSUM_TYPE, &val);
                f->dump_string(i->first.c_str(), Checksummer::get_csum_type_string(val));
              } else {
                p->opts.dump(i->first, f.get());
              }
              f->close_section();
              f->flush(rdata);
            }
            break;
	}
        if (!pool_opt) {
	  f->close_section();
	  f->flush(rdata);
        }
      }

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
	  case AUID:
	    ss << "auid: " << p->get_auid() << "\n";
	    break;
	  case SIZE:
	    ss << "size: " << p->get_size() << "\n";
	    break;
	  case MIN_SIZE:
	    ss << "min_size: " << p->get_min_size() << "\n";
	    break;
	  case CRASH_REPLAY_INTERVAL:
	    ss << "crash_replay_interval: " <<
	      p->get_crash_replay_interval() << "\n";
	    break;
	  case CRUSH_RULE:
	    if (osdmap.crush->rule_exists(p->get_crush_rule())) {
	      ss << "crush_rule: " << osdmap.crush->get_rule_name(
		p->get_crush_rule()) << "\n";
	    } else {
	      ss << "crush_rule: " << p->get_crush_rule() << "\n";
	    }
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
	  case HASHPSPOOL:
	  case NODELETE:
	  case NOPGCHANGE:
	  case NOSIZECHANGE:
	  case WRITE_FADVISE_DONTNEED:
	  case NOSCRUB:
	  case NODEEP_SCRUB:
	    for (i = ALL_CHOICES.begin(); i != ALL_CHOICES.end(); ++i) {
	      if (i->second == *it)
		break;
	    }
	    assert(i != ALL_CHOICES.end());
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
	    for (i = ALL_CHOICES.begin(); i != ALL_CHOICES.end(); ++i) {
	      if (i->second == *it)
		break;
	    }
	    assert(i != ALL_CHOICES.end());
	    {
	      pool_opts_t::key_t key = pool_opts_t::get_opt_desc(i->first).key;
	      if (p->opts.is_set(key)) {
                if(key == pool_opts_t::CSUM_TYPE) {
                  int val;
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
  } else if (prefix == "osd pool stats") {
    r = mon->pgservice->process_pg_command(prefix, cmdmap,
					   osdmap, f.get(), &ss, &rdata);
  } else if (prefix == "osd pool get-quota") {
    string pool_name;
    cmd_getval(g_ceph_context, cmdmap, "pool", pool_name);

    int64_t poolid = osdmap.lookup_pg_pool_name(pool_name);
    if (poolid < 0) {
      assert(poolid == -ENOENT);
      ss << "unrecognized pool '" << pool_name << "'";
      r = -ENOENT;
      goto reply;
    }
    const pg_pool_t *p = osdmap.get_pg_pool(poolid);

    if (f) {
      f->open_object_section("pool_quotas");
      f->dump_string("pool_name", pool_name);
      f->dump_unsigned("pool_id", poolid);
      f->dump_unsigned("quota_max_objects", p->quota_max_objects);
      f->dump_unsigned("quota_max_bytes", p->quota_max_bytes);
      f->close_section();
      f->flush(rdata);
    } else {
      stringstream rs;
      rs << "quotas for pool '" << pool_name << "':\n"
         << "  max objects: ";
      if (p->quota_max_objects == 0)
        rs << "N/A";
      else
        rs << si_t(p->quota_max_objects) << " objects";
      rs << "\n"
         << "  max bytes  : ";
      if (p->quota_max_bytes == 0)
        rs << "N/A";
      else
        rs << si_t(p->quota_max_bytes) << "B";
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
    cmd_getval(g_ceph_context, cmdmap, "class", class_name);
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
    cmd_getval(g_ceph_context, cmdmap, "name", name);
    string format;
    cmd_getval(g_ceph_context, cmdmap, "format", format);
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
    cmd_getval(g_ceph_context, cmdmap, "format", format);
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
    cmd_getval(g_ceph_context, cmdmap, "format", format);
    boost::scoped_ptr<Formatter> f(Formatter::create(format, "json-pretty", "json-pretty"));
    f->open_object_section("crush_map_tunables");
    osdmap.crush->dump_tunables(f.get());
    f->close_section();
    ostringstream rs;
    f->flush(rs);
    rs << "\n";
    rdata.append(rs.str());
  } else if (prefix == "osd crush tree") {
    string shadow;
    cmd_getval(g_ceph_context, cmdmap, "shadow", shadow);
    bool show_shadow = shadow == "--show-shadow";
    boost::scoped_ptr<Formatter> f(Formatter::create(format));
    if (f) {
      osdmap.crush->dump_tree(nullptr,
                              f.get(),
                              osdmap.get_pool_names(),
                              show_shadow);
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
    if (!cmd_getval(g_ceph_context, cmdmap, "node", name)) {
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
    cmd_getval(g_ceph_context, cmdmap, "class", name);
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
    cmd_getval(g_ceph_context, cmdmap, "name", name);
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
    cmd_getval(g_ceph_context, cmdmap, "pool", pool_name);
    string app;
    cmd_getval(g_ceph_context, cmdmap, "app", app);
    string key;
    cmd_getval(g_ceph_context, cmdmap, "key", key);

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
  } else {
    // try prepare update
    return false;
  }

 reply:
  string rs;
  getline(ss, rs);
  mon->reply_command(op, r, rs, rdata, get_last_committed());
  return true;
}

void OSDMonitor::set_pool_flags(int64_t pool_id, uint64_t flags)
{
  pg_pool_t *pool = pending_inc.get_new_pool(pool_id,
    osdmap.get_pg_pool(pool_id));
  assert(pool);
  pool->set_flag(flags);
}

void OSDMonitor::clear_pool_flags(int64_t pool_id, uint64_t flags)
{
  pg_pool_t *pool = pending_inc.get_new_pool(pool_id,
    osdmap.get_pg_pool(pool_id));
  assert(pool);
  pool->unset_flag(flags);
}

bool OSDMonitor::update_pools_status()
{
  if (!mon->pgservice->is_readable())
    return false;

  bool ret = false;

  auto& pools = osdmap.get_pools();
  for (auto it = pools.begin(); it != pools.end(); ++it) {
    const pool_stat_t *pstat = mon->pgservice->get_pool_stat(it->first);
    if (!pstat)
      continue;
    const object_stat_sum_t& sum = pstat->stats.sum;
    const pg_pool_t &pool = it->second;
    const string& pool_name = osdmap.get_pool_name(it->first);

    bool pool_is_full =
      (pool.quota_max_bytes > 0 && (uint64_t)sum.num_bytes >= pool.quota_max_bytes) ||
      (pool.quota_max_objects > 0 && (uint64_t)sum.num_objects >= pool.quota_max_objects);

    if (pool.has_flag(pg_pool_t::FLAG_FULL_NO_QUOTA)) {
      if (pool_is_full)
        continue;

      mon->clog->info() << "pool '" << pool_name
                       << "' no longer out of quota; removing NO_QUOTA flag";
      // below we cancel FLAG_FULL too, we'll set it again in
      // OSDMonitor::encode_pending if it still fails the osd-full checking.
      clear_pool_flags(it->first,
                       pg_pool_t::FLAG_FULL_NO_QUOTA | pg_pool_t::FLAG_FULL);
      ret = true;
    } else {
      if (!pool_is_full)
	continue;

      if (pool.quota_max_bytes > 0 &&
          (uint64_t)sum.num_bytes >= pool.quota_max_bytes) {
        mon->clog->warn() << "pool '" << pool_name << "' is full"
                         << " (reached quota's max_bytes: "
                         << si_t(pool.quota_max_bytes) << ")";
      }
      if (pool.quota_max_objects > 0 &&
		 (uint64_t)sum.num_objects >= pool.quota_max_objects) {
        mon->clog->warn() << "pool '" << pool_name << "' is full"
                         << " (reached quota's max_objects: "
                         << pool.quota_max_objects << ")";
      }
      // set both FLAG_FULL_NO_QUOTA and FLAG_FULL
      // note that below we try to cancel FLAG_BACKFILLFULL/NEARFULL too
      // since FLAG_FULL should always take precedence
      set_pool_flags(it->first,
                     pg_pool_t::FLAG_FULL_NO_QUOTA | pg_pool_t::FLAG_FULL);
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
  MPoolOp *m = static_cast<MPoolOp*>(op->get_req());
  dout(10) << "prepare_new_pool from " << m->get_connection() << dendl;
  MonSession *session = m->get_session();
  if (!session)
    return -EPERM;
  string erasure_code_profile;
  stringstream ss;
  string rule_name;
  if (m->auid)
    return prepare_new_pool(m->name, m->auid, m->crush_rule, rule_name,
			    0, 0,
                            erasure_code_profile,
			    pg_pool_t::TYPE_REPLICATED, 0, FAST_READ_OFF, &ss);
  else
    return prepare_new_pool(m->name, session->auid, m->crush_rule, rule_name,
			    0, 0,
                            erasure_code_profile,
			    pg_pool_t::TYPE_REPLICATED, 0, FAST_READ_OFF, &ss);
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

  CrushWrapper newcrush;
  _get_pending_crush(newcrush);

  ret = newcrush.rename_bucket(srcname,
			       dstname,
			       ss);
  if (ret)
    return ret;

  pending_inc.crush.clear();
  newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
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
			     g_conf->get_val<std::string>("erasure_code_dir"),
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
    uint32_t stripe_unit = strict_si_cast<uint32_t>(it->second.c_str(), &err_str);
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
    *rule = osdmap.crush->get_rule_mask_ruleset(ruleid);
    return -EEXIST;
  }

  CrushWrapper newcrush;
  _get_pending_crush(newcrush);

  ruleid = newcrush.get_rule_id(name);
  if (ruleid != -ENOENT) {
    *rule = newcrush.get_rule_mask_ruleset(ruleid);
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
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
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
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  return instance.factory(plugin->second,
			  g_conf->get_val<std::string>("erasure_code_dir"),
			  profile, erasure_code, ss);
}

int OSDMonitor::check_cluster_features(uint64_t features,
				       stringstream &ss)
{
  stringstream unsupported_ss;
  int unsupported_count = 0;
  if ((mon->get_quorum_con_features() & features) != features) {
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
  ::encode(*newcrush, new_pending.crush, mon->get_quorum_con_features());
  OSDMap newmap;
  newmap.deepish_copy_from(osdmap);
  newmap.apply_incremental(new_pending);

  // client compat
  if (newmap.require_min_compat_client > 0) {
    auto mv = newmap.get_min_compat_client();
    if (mv > newmap.require_min_compat_client) {
      ss << "new crush map requires client version " << ceph_release_name(mv)
	 << " but require_min_compat_client is "
	 << ceph_release_name(newmap.require_min_compat_client);
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
    if (p->second.erasure_code_profile == profile) {
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
  int r = get_json_str_map(g_conf->osd_pool_default_erasure_code_profile,
		           *ss,
		           erasure_code_profile_map);
  if (r)
    return r;
  assert((*erasure_code_profile_map).count("plugin"));
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
      string key = i->substr(0, equal);
      equal++;
      const string value = i->substr(equal);
      if (osdmap.require_osd_release >= CEPH_RELEASE_LUMINOUS &&
	  key.find("ruleset-") == 0) {
	if (g_conf->get_val<bool>("mon_fixup_legacy_erasure_code_profiles")) {
	  mon->clog->warn() << "erasure code profile property '" << key
			    << "' is no longer supported; try "
			    << "'crush-" << key.substr(8) << "' instead";
	  key = string("crush-") + key.substr(8);
	} else {
	  *ss << "property '" << key << "' is no longer supported; try "
	      << "'crush-" << key.substr(8) << "' instead";
	  return -EINVAL;
	}
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
				  unsigned *size, unsigned *min_size,
				  ostream *ss)
{
  int err = 0;
  switch (pool_type) {
  case pg_pool_t::TYPE_REPLICATED:
    *size = g_conf->osd_pool_default_size;
    *min_size = g_conf->get_osd_pool_default_min_size();
    break;
  case pg_pool_t::TYPE_ERASURE:
    {
      ErasureCodeInterfaceRef erasure_code;
      err = get_erasure_code(erasure_code_profile, &erasure_code, ss);
      if (err == 0) {
	*size = erasure_code->get_chunk_count();
	*min_size = MIN(erasure_code->get_data_chunk_count() + 1, *size);
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
      uint32_t stripe_unit = g_conf->osd_pool_erasure_code_stripe_unit;
      auto it = profile.find("stripe_unit");
      if (it != profile.end()) {
	string err_str;
	stripe_unit = strict_si_cast<uint32_t>(it->second.c_str(), &err_str);
	assert(err_str.empty());
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
	  // Use default rule
	  *crush_rule = osdmap.crush->get_osd_pool_default_crush_replicated_ruleset(g_ceph_context);
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
      break;
    }
  } else {
    if (!osdmap.crush->ruleset_exists(*crush_rule)) {
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
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

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

int OSDMonitor::check_pg_num(int64_t pool, int pg_num, int size, ostream *ss)
{
  auto max_pgs_per_osd = g_conf->get_val<uint64_t>("mon_max_pg_per_osd");
  auto num_osds = std::max(osdmap.get_num_in_osds(), 3u);   // assume min cluster size 3
  auto max_pgs = max_pgs_per_osd * num_osds;
  uint64_t projected = 0;
  if (pool < 0) {
    projected += pg_num * size;
  }
  for (const auto& i : osdmap.get_pools()) {
    if (i.first == pool) {
      projected += pg_num * size;
    } else {
      projected += i.second.get_pg_num() * i.second.get_size();
    }
  }
  if (projected > max_pgs) {
    if (pool >= 0) {
      *ss << "pool id " << pool;
    }
    *ss << " pg_num " << pg_num << " size " << size
	<< " would mean " << projected
	<< " total pgs, which exceeds max " << max_pgs
	<< " (mon_max_pg_per_osd " << max_pgs_per_osd
	<< " * num_in_osds " << num_osds << ")";
    return -ERANGE;
  }
  return 0;
}

/**
 * @param name The name of the new pool
 * @param auid The auid of the pool owner. Can be -1
 * @param crush_rule The crush rule to use. If <0, will use the system default
 * @param crush_rule_name The crush rule to use, if crush_rulset <0
 * @param pg_num The pg_num to use. If set to 0, will use the system default
 * @param pgp_num The pgp_num to use. If set to 0, will use the system default
 * @param erasure_code_profile The profile name in OSDMap to be used for erasure code
 * @param pool_type TYPE_ERASURE, or TYPE_REP
 * @param expected_num_objects expected number of objects on the pool
 * @param fast_read fast read type. 
 * @param ss human readable error message, if any.
 *
 * @return 0 on success, negative errno on failure.
 */
int OSDMonitor::prepare_new_pool(string& name, uint64_t auid,
				 int crush_rule,
				 const string &crush_rule_name,
                                 unsigned pg_num, unsigned pgp_num,
				 const string &erasure_code_profile,
                                 const unsigned pool_type,
                                 const uint64_t expected_num_objects,
                                 FastReadType fast_read,
				 ostream *ss)
{
  if (name.length() == 0)
    return -EINVAL;
  if (pg_num == 0)
    pg_num = g_conf->osd_pool_default_pg_num;
  if (pgp_num == 0)
    pgp_num = g_conf->osd_pool_default_pgp_num;
  if (pg_num > (unsigned)g_conf->mon_max_pool_pg_num) {
    *ss << "'pg_num' must be greater than 0 and less than or equal to "
        << g_conf->mon_max_pool_pg_num
        << " (you may adjust 'mon max pool pg num' for higher values)";
    return -ERANGE;
  }
  if (pgp_num > pg_num) {
    *ss << "'pgp_num' must be greater than 0 and lower or equal than 'pg_num'"
        << ", which in this case is " << pg_num;
    return -ERANGE;
  }
  if (pool_type == pg_pool_t::TYPE_REPLICATED && fast_read == FAST_READ_ON) {
    *ss << "'fast_read' can only apply to erasure coding pool";
    return -EINVAL;
  }
  int r;
  r = prepare_pool_crush_rule(pool_type, erasure_code_profile,
				 crush_rule_name, &crush_rule, ss);
  if (r) {
    dout(10) << " prepare_pool_crush_rule returns " << r << dendl;
    return r;
  }
  if (g_conf->mon_osd_crush_smoke_test) {
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);
    ostringstream err;
    CrushTester tester(newcrush, err);
    tester.set_min_x(0);
    tester.set_max_x(50);
    tester.set_rule(crush_rule);
    auto start = ceph::coarse_mono_clock::now();
    r = tester.test_with_fork(g_conf->mon_lease);
    auto duration = ceph::coarse_mono_clock::now() - start;
    if (r < 0) {
      dout(10) << " tester.test_with_fork returns " << r
	       << ": " << err.str() << dendl;
      *ss << "crush test failed with " << r << ": " << err.str();
      return r;
    }
    dout(10) << __func__ << " crush smoke test duration: "
             << duration << dendl;
  }
  unsigned size, min_size;
  r = prepare_pool_size(pool_type, erasure_code_profile, &size, &min_size, ss);
  if (r) {
    dout(10) << " prepare_pool_size returns " << r << dendl;
    return r;
  }
  r = check_pg_num(-1, pg_num, size, ss);
  if (r) {
    dout(10) << " prepare_pool_size returns " << r << dendl;
    return r;
  }

  if (!osdmap.crush->check_crush_rule(crush_rule, pool_type, size, *ss)) {
    return -EINVAL;
  }

  uint32_t stripe_width = 0;
  r = prepare_pool_stripe_width(pool_type, erasure_code_profile, &stripe_width, ss);
  if (r) {
    dout(10) << " prepare_pool_stripe_width returns " << r << dendl;
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
        fread = g_conf->mon_osd_pool_ec_fast_read;
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
  pi->type = pool_type;
  pi->fast_read = fread; 
  pi->flags = g_conf->osd_pool_default_flags;
  if (g_conf->osd_pool_default_flag_hashpspool)
    pi->set_flag(pg_pool_t::FLAG_HASHPSPOOL);
  if (g_conf->osd_pool_default_flag_nodelete)
    pi->set_flag(pg_pool_t::FLAG_NODELETE);
  if (g_conf->osd_pool_default_flag_nopgchange)
    pi->set_flag(pg_pool_t::FLAG_NOPGCHANGE);
  if (g_conf->osd_pool_default_flag_nosizechange)
    pi->set_flag(pg_pool_t::FLAG_NOSIZECHANGE);
  if (g_conf->osd_pool_use_gmt_hitset &&
      (osdmap.get_up_osd_features() & CEPH_FEATURE_OSD_HITSET_GMT))
    pi->use_gmt_hitset = true;
  else
    pi->use_gmt_hitset = false;

  pi->size = size;
  pi->min_size = min_size;
  pi->crush_rule = crush_rule;
  pi->expected_num_objects = expected_num_objects;
  pi->object_hash = CEPH_STR_HASH_RJENKINS;
  pi->set_pg_num(pg_num);
  pi->set_pgp_num(pgp_num);
  pi->last_change = pending_inc.epoch;
  pi->auid = auid;
  pi->erasure_code_profile = erasure_code_profile;
  pi->stripe_width = stripe_width;
  pi->cache_target_dirty_ratio_micro =
    g_conf->osd_pool_default_cache_target_dirty_ratio * 1000000;
  pi->cache_target_dirty_high_ratio_micro =
    g_conf->osd_pool_default_cache_target_dirty_high_ratio * 1000000;
  pi->cache_target_full_ratio_micro =
    g_conf->osd_pool_default_cache_target_full_ratio * 1000000;
  pi->cache_min_flush_age = g_conf->osd_pool_default_cache_min_flush_age;
  pi->cache_min_evict_age = g_conf->osd_pool_default_cache_min_evict_age;
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

int OSDMonitor::prepare_command_pool_set(map<string,cmd_vartype> &cmdmap,
                                         stringstream& ss)
{
  string poolstr;
  cmd_getval(g_ceph_context, cmdmap, "pool", poolstr);
  int64_t pool = osdmap.lookup_pg_pool_name(poolstr.c_str());
  if (pool < 0) {
    ss << "unrecognized pool '" << poolstr << "'";
    return -ENOENT;
  }
  string var;
  cmd_getval(g_ceph_context, cmdmap, "var", var);

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
  if (!cmd_getval(g_ceph_context, cmdmap, "val", val)) {
    // wasn't a string; maybe an older mon forwarded json with an int?
    if (!cmd_getval(g_ceph_context, cmdmap, "val", n))
      return -EINVAL;  // no value!
  } else {
    // we got a string.  see if it contains an int.
    n = strict_strtoll(val.c_str(), 10, &interr);
    // or a float
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
    int r = check_pg_num(pool, p.get_pg_num(), n, &ss);
    if (r < 0) {
      return r;
    }
    p.size = n;
    if (n < p.min_size)
      p.min_size = n;
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
	ss << "pool min_size must be between 1 and " << (int)p.size;
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
	 ss << __func__ << " get_erasure_code failed: " << tmp.rdbuf();
	 return err;
       }

       if (n < k || n > p.size) {
	 ss << "pool min_size must be between " << k << " and " << (int)p.size;
	 return -EINVAL;
       }
    }
    p.min_size = n;
  } else if (var == "auid") {
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    }
    p.auid = n;
  } else if (var == "crash_replay_interval") {
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    }
    p.crash_replay_interval = n;
  } else if (var == "pg_num") {
    if (p.has_flag(pg_pool_t::FLAG_NOPGCHANGE)) {
      ss << "pool pg_num change is disabled; you must unset nopgchange flag for the pool first";
      return -EPERM;
    }
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    }
    if (n <= (int)p.get_pg_num()) {
      ss << "specified pg_num " << n << " <= current " << p.get_pg_num();
      if (n < (int)p.get_pg_num())
	return -EEXIST;
      return 0;
    }
    if (n > (unsigned)g_conf->mon_max_pool_pg_num) {
      ss << "'pg_num' must be greater than 0 and less than or equal to "
         << g_conf->mon_max_pool_pg_num
         << " (you may adjust 'mon max pool pg num' for higher values)";
      return -ERANGE;
    }
    int r = check_pg_num(pool, n, p.get_size(), &ss);
    if (r) {
      return r;
    }
    string force;
    cmd_getval(g_ceph_context,cmdmap, "force", force);
    if (p.cache_mode != pg_pool_t::CACHEMODE_NONE &&
	force != "--yes-i-really-mean-it") {
      ss << "splits in cache pools must be followed by scrubs and leave sufficient free space to avoid overfilling.  use --yes-i-really-mean-it to force.";
      return -EPERM;
    }
    int expected_osds = MIN(p.get_pg_num(), osdmap.get_num_osds());
    int64_t new_pgs = n - p.get_pg_num();
    if (new_pgs > g_conf->mon_osd_max_split_count * expected_osds) {
      ss << "specified pg_num " << n << " is too large (creating "
	 << new_pgs << " new PGs on ~" << expected_osds
	 << " OSDs exceeds per-OSD max of " << g_conf->mon_osd_max_split_count
	 << ')';
      return -E2BIG;
    }
    p.set_pg_num(n);
    // force pre-luminous clients to resend their ops, since they
    // don't understand that split PGs now form a new interval.
    p.last_force_op_resend_preluminous = pending_inc.epoch;
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
    if (n > (int)p.get_pg_num()) {
      ss << "specified pgp_num " << n << " > pg_num " << p.get_pg_num();
      return -EINVAL;
    }
    p.set_pgp_num(n);
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
    if (!osdmap.crush->check_crush_rule(id, p.get_type(), p.get_size(), ss)) {
      return -EINVAL;
    }
    p.crush_rule = id;
  } else if (var == "nodelete" || var == "nopgchange" ||
	     var == "nosizechange" || var == "write_fadvise_dontneed" ||
	     var == "noscrub" || var == "nodeep-scrub") {
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
    string force;
    cmd_getval(g_ceph_context, cmdmap, "force", force);
    if (force != "--yes-i-really-mean-it") {
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
	bsp->set_fpp(g_conf->osd_pool_default_hit_set_bloom_fpp);
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
    }
    p.hit_set_period = n;
  } else if (var == "hit_set_count") {
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    }
    p.hit_set_count = n;
  } else if (var == "hit_set_fpp") {
    if (floaterr.length()) {
      ss << "error parsing floating point value '" << val << "': " << floaterr;
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
      string force;
      cmd_getval(g_ceph_context, cmdmap, "force", force);
      if (!osdmap.get_num_up_osds() && force != "--yes-i-really-mean-it") {
        ss << "Not advisable to continue since no OSDs are up. Pass "
           << "--yes-i-really-mean-it if you really wish to continue.";
        return -EPERM;
      }
      if (!(osdmap.get_up_osd_features() & CEPH_FEATURE_OSD_HITSET_GMT)
          && force != "--yes-i-really-mean-it") {
	ss << "not all OSDs support GMT hit set.";
	return -EINVAL;
      }
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
    if (!g_conf->mon_debug_no_require_bluestore_for_ec_overwrites &&
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
	p.opts.set(desc.key, static_cast<int>(n));
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
      assert(!"unknown type");
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
                                                 map<string,cmd_vartype> &cmdmap,
                                                 stringstream& ss)
{
  string pool_name;
  cmd_getval(g_ceph_context, cmdmap, "pool", pool_name);
  int64_t pool = osdmap.lookup_pg_pool_name(pool_name.c_str());
  if (pool < 0) {
    ss << "unrecognized pool '" << pool_name << "'";
    return -ENOENT;
  }

  pg_pool_t p = *osdmap.get_pg_pool(pool);
  if (pending_inc.new_pools.count(pool)) {
    p = pending_inc.new_pools[pool];
  }

  string app;
  cmd_getval(g_ceph_context, cmdmap, "app", app);
  bool app_exists = (p.application_metadata.count(app) > 0);

  if (boost::algorithm::ends_with(prefix, "enable")) {
    if (app.empty()) {
      ss << "application name must be provided";
      return -EINVAL;
    }

    if (p.is_tier()) {
      ss << "application must be enabled on base tier";
      return -EINVAL;
    }

    string force;
    cmd_getval(g_ceph_context, cmdmap, "force", force);

    if (!app_exists && !p.application_metadata.empty() &&
        force != "--yes-i-really-mean-it") {
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
    string force;
    cmd_getval(g_ceph_context, cmdmap, "force", force);

    if (force != "--yes-i-really-mean-it") {
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
    cmd_getval(g_ceph_context, cmdmap, "key", key);

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
    cmd_getval(g_ceph_context, cmdmap, "value", value);
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
    cmd_getval(g_ceph_context, cmdmap, "key", key);
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
    assert(false);
  }

  p.last_change = pending_inc.epoch;
  pending_inc.new_pools[pool] = p;
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
    err = newcrush.remove_item_under(g_ceph_context, id, ancestor,
        unlink_only);
  } else {
    err = newcrush.remove_item(g_ceph_context, id, unlink_only);
  }
  return err;
}

void OSDMonitor::do_osd_crush_remove(CrushWrapper& newcrush)
{
  pending_inc.crush.clear();
  newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
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

  assert(err == 0);
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
  assert(existing_id);
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
    int32_t* new_id)
{
  dout(10) << __func__ << " uuid " << uuid << dendl;
  assert(new_id);

  // We presume validation has been performed prior to calling this
  // function. We assert with prejudice.

  int32_t allocated_id = -1; // declare here so we can jump
  int32_t existing_id = -1;
  if (!uuid.is_zero()) {
    existing_id = osdmap.identify_osd(uuid);
    if (existing_id >= 0) {
      assert(id < 0 || id == existing_id);
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
    assert(existing_id < osdmap.get_max_osd());
    assert(allocated_id < 0);
    pending_inc.new_weight[existing_id] = CEPH_OSD_OUT;
    *new_id = existing_id;

  } else if (allocated_id >= 0) {
    assert(existing_id < 0);
    // raise max_osd
    if (pending_inc.new_max_osd < 0) {
      pending_inc.new_max_osd = osdmap.get_max_osd() + 1;
    } else {
      ++pending_inc.new_max_osd;
    }
    *new_id = pending_inc.new_max_osd - 1;
    assert(*new_id == allocated_id);
  } else {
    assert(0 == "unexpected condition");
  }

out:
  dout(10) << __func__ << " using id " << *new_id << dendl;
  if (osdmap.get_max_osd() <= *new_id && pending_inc.new_max_osd <= *new_id) {
    pending_inc.new_max_osd = *new_id + 1;
  }

  pending_inc.new_state[*new_id] |= CEPH_OSD_EXISTS | CEPH_OSD_NEW;
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

  assert(existing_id);

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

  assert(!uuid.is_zero());
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
  assert(existing_id);
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
    const map<string,cmd_vartype>& cmdmap,
    const map<string,string>& secrets,
    stringstream &ss,
    Formatter *f)
{
  uuid_d uuid;
  string uuidstr;
  int64_t id = -1;

  assert(paxos->is_plugged());

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
  if (!cmd_getval(g_ceph_context, cmdmap, "uuid", uuidstr)) {
    ss << "requires the OSD's UUID to be specified.";
    return -EINVAL;
  } else if (!uuid.parse(uuidstr.c_str())) {
    ss << "invalid UUID value '" << uuidstr << "'.";
    return -EINVAL;
  }

  if (cmd_getval(g_ceph_context, cmdmap, "id", id) &&
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
    assert(existing_id >= 0);
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
        assert(existing_id >= 0);
        id = existing_id;
      }
      dout(10) << __func__ << " found id " << id << " to use" << dendl;
    } else if (id >= 0 && osdmap.is_destroyed(id)) {
      dout(10) << __func__ << " recreating osd." << id << dendl;
    } else {
      dout(10) << __func__ << " creating new osd." << id << dendl;
    }
  } else {
    assert(id >= 0);
    assert(osdmap.exists(id));
  }

  // we are now able to either create a brand new osd or reuse an existing
  // osd that has been previously destroyed.

  dout(10) << __func__ << " id " << id << " uuid " << uuid << dendl;

  if (may_be_idempotent && secrets.empty()) {
    // nothing to do, really.
    dout(10) << __func__ << " idempotent and no secrets -- no op." << dendl;
    assert(id >= 0);
    if (f) {
      f->open_object_section("created_osd");
      f->dump_int("osdid", id);
      f->close_section();
    } else {
      ss << id;
    }
    return EEXIST;
  }

  string cephx_secret, lockbox_secret, dmcrypt_key;
  bool has_lockbox = false;
  bool has_secrets = (!secrets.empty());

  ConfigKeyService *svc = nullptr;
  AuthMonitor::auth_entity_t cephx_entity, lockbox_entity;

  if (has_secrets) {
    if (secrets.count("cephx_secret") == 0) {
      ss << "requires a cephx secret.";
      return -EINVAL;
    }
    cephx_secret = secrets.at("cephx_secret");

    bool has_lockbox_secret = (secrets.count("cephx_lockbox_secret") > 0);
    bool has_dmcrypt_key = (secrets.count("dmcrypt_key") > 0);

    dout(10) << __func__ << " has lockbox " << has_lockbox_secret
             << " dmcrypt " << has_dmcrypt_key << dendl;

    if (has_lockbox_secret && has_dmcrypt_key) {
      has_lockbox = true;
      lockbox_secret = secrets.at("cephx_lockbox_secret");
      dmcrypt_key = secrets.at("dmcrypt_key");
    } else if (!has_lockbox_secret != !has_dmcrypt_key) {
      ss << "requires both a cephx lockbox secret and a dm-crypt key.";
      return -EINVAL;
    }

    dout(10) << __func__ << " validate secrets using osd id " << id << dendl;

    err = mon->authmon()->validate_osd_new(id, uuid,
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
      assert(id >= 0);
      ss << "osd." << id << " exists but secrets do not match";
      return -EEXIST;
    }

    if (has_lockbox) {
      svc = (ConfigKeyService*)mon->config_key_service;
      err = svc->validate_osd_new(uuid, dmcrypt_key, ss);
      if (err < 0) {
        return err;
      } else if (may_be_idempotent && err != EEXIST) {
        assert(id >= 0);
        ss << "osd." << id << " exists but dm-crypt key does not match.";
        return -EEXIST;
      }
    }
  }
  assert(!has_secrets || !cephx_secret.empty());
  assert(!has_lockbox || !lockbox_secret.empty());

  if (may_be_idempotent) {
    // we have nothing to do for either the osdmon or the authmon,
    // and we have no lockbox - so the config key service will not be
    // touched. This is therefore an idempotent operation, and we can
    // just return right away.
    dout(10) << __func__ << " idempotent -- no op." << dendl;
    assert(id >= 0);
    if (f) {
      f->open_object_section("created_osd");
      f->dump_int("osdid", id);
      f->close_section();
    } else {
      ss << id;
    }
    return EEXIST;
  }
  assert(!may_be_idempotent);

  // perform updates.
  if (has_secrets) {
    assert(!cephx_secret.empty());
    assert((lockbox_secret.empty() && dmcrypt_key.empty()) ||
           (!lockbox_secret.empty() && !dmcrypt_key.empty()));

    err = mon->authmon()->do_osd_new(cephx_entity,
        lockbox_entity,
        has_lockbox);
    assert(0 == err);

    if (has_lockbox) {
      assert(nullptr != svc);
      svc->do_osd_new(uuid, dmcrypt_key);
    }
  }

  if (is_recreate_destroyed) {
    assert(id >= 0);
    assert(osdmap.is_destroyed(id));
    pending_inc.new_weight[id] = CEPH_OSD_OUT;
    pending_inc.new_state[id] |= CEPH_OSD_DESTROYED | CEPH_OSD_NEW;
    if (osdmap.get_state(id) & CEPH_OSD_UP) {
      // due to http://tracker.ceph.com/issues/20751 some clusters may
      // have UP set for non-existent OSDs; make sure it is cleared
      // for a newly created osd.
      pending_inc.new_state[id] |= CEPH_OSD_UP;
    }
    pending_inc.new_uuid[id] = uuid;
  } else {
    assert(id >= 0);
    int32_t new_id = -1;
    do_osd_create(id, uuid, &new_id);
    assert(new_id >= 0);
    assert(id == new_id);
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
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  stringstream ss;
  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon->reply_command(op, -EINVAL, rs, get_last_committed());
    return true;
  }

  MonSession *session = m->get_session();
  if (!session) {
    mon->reply_command(op, -EACCES, "access denied", get_last_committed());
    return true;
  }

  return prepare_command_impl(op, cmdmap);
}

static int parse_reweights(CephContext *cct,
			   const map<string,cmd_vartype> &cmdmap,
			   const OSDMap& osdmap,
			   map<int32_t, uint32_t>* weights)
{
  string weights_str;
  if (!cmd_getval(g_ceph_context, cmdmap, "weights", weights_str)) {
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
  assert(paxos->is_plugged());

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

  int err = mon->authmon()->validate_osd_destroy(id, uuid,
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

  ConfigKeyService *svc = (ConfigKeyService*)mon->config_key_service;
  err = svc->validate_osd_destroy(id, uuid);
  if (err < 0) {
    assert(err == -ENOENT);
    err = 0;
    idempotent_cks = true;
  }

  if (!idempotent_auth) {
    err = mon->authmon()->do_osd_destroy(cephx_entity, lockbox_entity);
    assert(0 == err);
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
  assert(err == 0);
  return 0;
}

int OSDMonitor::prepare_command_osd_purge(
    int32_t id,
    stringstream& ss)
{
  assert(paxos->is_plugged());
  dout(10) << __func__ << " purging osd." << id << dendl;

  assert(!osdmap.is_up(id));

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

  CrushWrapper newcrush;
  _get_pending_crush(newcrush);

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
  assert(0 == err);

  if (may_be_idempotent && !osdmap.exists(id)) {
    dout(10) << __func__ << " osd." << id << " does not exist and "
             << "we are idempotent." << dendl;
    return -ENOENT;
  }

  err = prepare_command_osd_remove(id);
  // we should not be busy, as we should have made sure this id is not up.
  assert(0 == err);

  do_osd_crush_remove(newcrush);
  return 0;
}

bool OSDMonitor::prepare_command_impl(MonOpRequestRef op,
				      map<string,cmd_vartype> &cmdmap)
{
  op->mark_osdmon_event(__func__);
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  bool ret = false;
  stringstream ss;
  string rs;
  bufferlist rdata;
  int err = 0;

  string format;
  cmd_getval(g_ceph_context, cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  int64_t osdid;
  string name;
  bool osdid_present = cmd_getval(g_ceph_context, cmdmap, "id", osdid);
  if (osdid_present) {
    ostringstream oss;
    oss << "osd." << osdid;
    name = oss.str();
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
      wait_for_finished_proposal(op, new C_RetryMessage(this, op));
      return true;
    }
    dout(10) << "prepare_command setting new crush map" << dendl;
    bufferlist data(m->get_data());
    CrushWrapper crush;
    try {
      bufferlist::iterator bl(data.begin());
      crush.decode(bl);
    }
    catch (const std::exception &e) {
      err = -EINVAL;
      ss << "Failed to parse crushmap: " << e.what();
      goto reply;
    }
  
    int64_t prior_version = 0;
    if (cmd_getval(g_ceph_context, cmdmap, "prior_version", prior_version)) {
      if (prior_version == osdmap.get_crush_version() - 1) {
	// see if we are a resend of the last update.  this is imperfect
	// (multiple racing updaters may not both get reliable success)
	// but we expect crush updaters (via this interface) to be rare-ish.
	bufferlist current, proposed;
	osdmap.crush->encode(current, mon->get_quorum_con_features());
	crush.encode(proposed, mon->get_quorum_con_features());
	if (current.contents_equal(proposed)) {
	  dout(10) << __func__
		   << " proposed matches current and version equals previous"
		   << dendl;
	  err = 0;
	  ss << osdmap.get_crush_version();
	  goto reply;
	}
      }
      if (prior_version != osdmap.get_crush_version()) {
	err = -EPERM;
	ss << "prior_version " << prior_version << " != crush version "
	   << osdmap.get_crush_version();
	goto reply;
      }
    }

    if (crush.has_legacy_rule_ids()) {
      err = -EINVAL;
      ss << "crush maps with ruleset != ruleid are no longer allowed";
      goto reply;
    }
    if (!validate_crush_against_features(&crush, ss)) {
      err = -EINVAL;
      goto reply;
    }

    err = osdmap.validate_crush_rules(&crush, &ss);
    if (err < 0) {
      goto reply;
    }

    if (g_conf->mon_osd_crush_smoke_test) {
      // sanity check: test some inputs to make sure this map isn't
      // totally broken
      dout(10) << " testing map" << dendl;
      stringstream ess;
      CrushTester tester(crush, ess);
      tester.set_min_x(0);
      tester.set_max_x(50);
      auto start = ceph::coarse_mono_clock::now();
      int r = tester.test_with_fork(g_conf->mon_lease);
      auto duration = ceph::coarse_mono_clock::now() - start;
      if (r < 0) {
	dout(10) << " tester.test_with_fork returns " << r
		 << ": " << ess.str() << dendl;
	ss << "crush smoke test failed with " << r << ": " << ess.str();
	err = r;
	goto reply;
      }
      dout(10) << __func__ << " crush somke test duration: "
               << duration << ", result: " << ess.str() << dendl;
    }

    pending_inc.crush = data;
    ss << osdmap.get_crush_version() + 1;
    goto update;

  } else if (prefix == "osd crush set-all-straw-buckets-to-straw2") {
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);
    for (int b = 0; b < newcrush.get_max_buckets(); ++b) {
      int bid = -1 - b;
      if (newcrush.bucket_exists(bid) &&
	  newcrush.get_bucket_alg(bid)) {
	dout(20) << " bucket " << bid << " is straw, can convert" << dendl;
	newcrush.bucket_set_alg(bid, CRUSH_BUCKET_STRAW2);
      }
    }
    if (!validate_crush_against_features(&newcrush, ss)) {
      err = -EINVAL;
      goto reply;
    }
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd crush set-device-class") {
    if (osdmap.require_osd_release < CEPH_RELEASE_LUMINOUS) {
      ss << "you must complete the upgrade and 'ceph osd require-osd-release "
         << "luminous' before using crush device classes";
      err = -EPERM;
      goto reply;
    }

    string device_class;
    if (!cmd_getval(g_ceph_context, cmdmap, "class", device_class)) {
      err = -EINVAL; // no value!
      goto reply;
    }

    bool stop = false;
    vector<string> idvec;
    cmd_getval(g_ceph_context, cmdmap, "ids", idvec);
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);
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
          goto reply;
        }
        if (err == 0 && !_have_pending_crush()) {
          if (!stop) {
            // for single osd only, wildcard makes too much noise
            ss << "set-device-class item id " << osd << " name '" << name
               << "' device_class '" << device_class << "': no change";
          }
        } else {
          updated.insert(osd);
        }
      }
    }

    if (!updated.empty()) {
      pending_inc.crush.clear();
      newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
      ss << "set osd(s) " << updated << " to class '" << device_class << "'";
      getline(ss, rs);
      wait_for_finished_proposal(op,
        new Monitor::C_Command(mon,op, 0, rs, get_last_committed() + 1));
      return true;
    }

 } else if (prefix == "osd crush rm-device-class") {
    bool stop = false;
    vector<string> idvec;
    cmd_getval(g_ceph_context, cmdmap, "ids", idvec);
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);
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
          goto reply;
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

        err = newcrush.remove_device_class(g_ceph_context, osd, &ss);
        if (err < 0) {
          // ss has reason for failure
          goto reply;
        }
        updated.insert(osd);
      }
    }

    if (!updated.empty()) {
      pending_inc.crush.clear();
      newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
      ss << "done removing class of osd(s): " << updated;
      getline(ss, rs);
      wait_for_finished_proposal(op,
        new Monitor::C_Command(mon,op, 0, rs, get_last_committed() + 1));
      return true;
    }
  } else if (prefix == "osd crush class rename") {
    string srcname, dstname;
    if (!cmd_getval(g_ceph_context, cmdmap, "srcname", srcname)) {
      err = -EINVAL;
      goto reply;
    }
    if (!cmd_getval(g_ceph_context, cmdmap, "dstname", dstname)) {
      err = -EINVAL;
      goto reply;
    }

    CrushWrapper newcrush;
    _get_pending_crush(newcrush);
    if (!newcrush.class_exists(srcname) && newcrush.class_exists(dstname)) {
      // suppose this is a replay and return success
      // so command is idempotent
      ss << "already renamed to '" << dstname << "'";
      err = 0;
      goto reply;
    }

    err = newcrush.rename_class(srcname, dstname);
    if (err < 0) {
      ss << "fail to rename '" << srcname << "' to '" << dstname << "' : "
         << cpp_strerror(err);
      goto reply;
    }

    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    ss << "rename class '" << srcname << "' to '" << dstname << "'";
    goto update;
  } else if (prefix == "osd crush add-bucket") {
    // os crush add-bucket <name> <type>
    string name, typestr;
    cmd_getval(g_ceph_context, cmdmap, "name", name);
    cmd_getval(g_ceph_context, cmdmap, "type", typestr);

    if (!_have_pending_crush() &&
	_get_stable_crush().name_exists(name)) {
      ss << "bucket '" << name << "' already exists";
      goto reply;
    }

    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    if (newcrush.name_exists(name)) {
      ss << "bucket '" << name << "' already exists";
      goto update;
    }
    int type = newcrush.get_type_id(typestr);
    if (type < 0) {
      ss << "type '" << typestr << "' does not exist";
      err = -EINVAL;
      goto reply;
    }
    if (type == 0) {
      ss << "type '" << typestr << "' is for devices, not buckets";
      err = -EINVAL;
      goto reply;
    }
    int bucketno;
    err = newcrush.add_bucket(0, 0,
			      CRUSH_HASH_DEFAULT, type, 0, NULL,
			      NULL, &bucketno);
    if (err < 0) {
      ss << "add_bucket error: '" << cpp_strerror(err) << "'";
      goto reply;
    }
    err = newcrush.set_item_name(bucketno, name);
    if (err < 0) {
      ss << "error setting bucket name to '" << name << "'";
      goto reply;
    }

    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    ss << "added bucket " << name << " type " << typestr
       << " to crush map";
    goto update;
  } else if (prefix == "osd crush rename-bucket") {
    string srcname, dstname;
    cmd_getval(g_ceph_context, cmdmap, "srcname", srcname);
    cmd_getval(g_ceph_context, cmdmap, "dstname", dstname);

    err = crush_rename_bucket(srcname, dstname, &ss);
    if (err == -EALREADY) // equivalent to success for idempotency
      err = 0;
    if (err)
      goto reply;
    else
      goto update;
  } else if (prefix == "osd crush weight-set create" ||
	     prefix == "osd crush weight-set create-compat") {
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);
    int64_t pool;
    int positions;
    if (newcrush.has_non_straw2_buckets()) {
      ss << "crush map contains one or more bucket(s) that are not straw2";
      err = -EPERM;
      goto reply;
    }
    if (prefix == "osd crush weight-set create") {
      if (osdmap.require_min_compat_client > 0 &&
	  osdmap.require_min_compat_client < CEPH_RELEASE_LUMINOUS) {
	ss << "require_min_compat_client "
	   << ceph_release_name(osdmap.require_min_compat_client)
	   << " < luminous, which is required for per-pool weight-sets. "
           << "Try 'ceph osd set-require-min-compat-client luminous' "
           << "before using the new interface";
	err = -EPERM;
	goto reply;
      }
      string poolname, mode;
      cmd_getval(g_ceph_context, cmdmap, "pool", poolname);
      pool = osdmap.lookup_pg_pool_name(poolname.c_str());
      if (pool < 0) {
	ss << "pool '" << poolname << "' not found";
	err = -ENOENT;
	goto reply;
      }
      cmd_getval(g_ceph_context, cmdmap, "mode", mode);
      if (mode != "flat" && mode != "positional") {
	ss << "unrecognized weight-set mode '" << mode << "'";
	err = -EINVAL;
	goto reply;
      }
      positions = mode == "flat" ? 1 : osdmap.get_pg_pool(pool)->get_size();
    } else {
      pool = CrushWrapper::DEFAULT_CHOOSE_ARGS;
      positions = 1;
    }
    newcrush.create_choose_args(pool, positions);
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    goto update;

  } else if (prefix == "osd crush weight-set rm" ||
	     prefix == "osd crush weight-set rm-compat") {
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);
    int64_t pool;
    if (prefix == "osd crush weight-set rm") {
      string poolname;
      cmd_getval(g_ceph_context, cmdmap, "pool", poolname);
      pool = osdmap.lookup_pg_pool_name(poolname.c_str());
      if (pool < 0) {
	ss << "pool '" << poolname << "' not found";
	err = -ENOENT;
	goto reply;
      }
    } else {
      pool = CrushWrapper::DEFAULT_CHOOSE_ARGS;
    }
    newcrush.rm_choose_args(pool);
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    goto update;

  } else if (prefix == "osd crush weight-set reweight" ||
	     prefix == "osd crush weight-set reweight-compat") {
    string poolname, item;
    vector<double> weight;
    cmd_getval(g_ceph_context, cmdmap, "pool", poolname);
    cmd_getval(g_ceph_context, cmdmap, "item", item);
    cmd_getval(g_ceph_context, cmdmap, "weight", weight);
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);
    int64_t pool;
    if (prefix == "osd crush weight-set reweight") {
      pool = osdmap.lookup_pg_pool_name(poolname.c_str());
      if (pool < 0) {
	ss << "pool '" << poolname << "' not found";
	err = -ENOENT;
	goto reply;
      }
      if (!newcrush.have_choose_args(pool)) {
	ss << "no weight-set for pool '" << poolname << "'";
	err = -ENOENT;
	goto reply;
      }
      auto arg_map = newcrush.choose_args_get(pool);
      int positions = newcrush.get_choose_args_positions(arg_map);
      if (weight.size() != (size_t)positions) {
         ss << "must specify exact " << positions << " weight values";
         err = -EINVAL;
         goto reply;
      }
    } else {
      pool = CrushWrapper::DEFAULT_CHOOSE_ARGS;
      if (!newcrush.have_choose_args(pool)) {
	ss << "no backward-compatible weight-set";
	err = -ENOENT;
	goto reply;
      }
    }
    if (!newcrush.name_exists(item)) {
      ss << "item '" << item << "' does not exist";
      err = -ENOENT;
      goto reply;
    }
    err = newcrush.choose_args_adjust_item_weightf(
      g_ceph_context,
      newcrush.choose_args_get(pool),
      newcrush.get_item_id(item),
      weight,
      &ss);
    if (err < 0) {
      goto reply;
    }
    err = 0;
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    goto update;
  } else if (osdid_present &&
	     (prefix == "osd crush set" || prefix == "osd crush add")) {
    // <OsdName> is 'osd.<id>' or '<id>', passed as int64_t id
    // osd crush set <OsdName> <weight> <loc1> [<loc2> ...]
    // osd crush add <OsdName> <weight> <loc1> [<loc2> ...]

    if (!osdmap.exists(osdid)) {
      err = -ENOENT;
      ss << name << " does not exist. Create it before updating the crush map";
      goto reply;
    }

    double weight;
    if (!cmd_getval(g_ceph_context, cmdmap, "weight", weight)) {
      ss << "unable to parse weight value '"
         << cmd_vartype_stringify(cmdmap["weight"]) << "'";
      err = -EINVAL;
      goto reply;
    }

    string args;
    vector<string> argvec;
    cmd_getval(g_ceph_context, cmdmap, "args", argvec);
    map<string,string> loc;
    CrushWrapper::parse_loc_map(argvec, &loc);

    if (prefix == "osd crush set"
        && !_get_stable_crush().item_exists(osdid)) {
      err = -ENOENT;
      ss << "unable to set item id " << osdid << " name '" << name
         << "' weight " << weight << " at location " << loc
         << ": does not exist";
      goto reply;
    }

    dout(5) << "adding/updating crush item id " << osdid << " name '"
      << name << "' weight " << weight << " at location "
      << loc << dendl;
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    string action;
    if (prefix == "osd crush set" ||
        newcrush.check_item_loc(g_ceph_context, osdid, loc, (int *)NULL)) {
      action = "set";
      err = newcrush.update_item(g_ceph_context, osdid, weight, name, loc);
    } else {
      action = "add";
      err = newcrush.insert_item(g_ceph_context, osdid, weight, name, loc);
      if (err == 0)
        err = 1;
    }

    if (err < 0)
      goto reply;

    if (err == 0 && !_have_pending_crush()) {
      ss << action << " item id " << osdid << " name '" << name << "' weight "
        << weight << " at location " << loc << ": no change";
      goto reply;
    }

    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    ss << action << " item id " << osdid << " name '" << name << "' weight "
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
	ss << name << " does not exist.  create it before updating the crush map";
	goto reply;
      }

      double weight;
      if (!cmd_getval(g_ceph_context, cmdmap, "weight", weight)) {
        ss << "unable to parse weight value '"
           << cmd_vartype_stringify(cmdmap["weight"]) << "'";
        err = -EINVAL;
        goto reply;
      }

      string args;
      vector<string> argvec;
      cmd_getval(g_ceph_context, cmdmap, "args", argvec);
      map<string,string> loc;
      CrushWrapper::parse_loc_map(argvec, &loc);

      dout(0) << "create-or-move crush item name '" << name << "' initial_weight " << weight
	      << " at location " << loc << dendl;

      CrushWrapper newcrush;
      _get_pending_crush(newcrush);

      err = newcrush.create_or_move_item(g_ceph_context, osdid, weight, name, loc);
      if (err == 0) {
	ss << "create-or-move updated item name '" << name << "' weight " << weight
	   << " at location " << loc << " to crush map";
	break;
      }
      if (err > 0) {
	pending_inc.crush.clear();
	newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
	ss << "create-or-move updating item name '" << name << "' weight " << weight
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

      string args;
      vector<string> argvec;
      cmd_getval(g_ceph_context, cmdmap, "name", name);
      cmd_getval(g_ceph_context, cmdmap, "args", argvec);
      map<string,string> loc;
      CrushWrapper::parse_loc_map(argvec, &loc);

      dout(0) << "moving crush item name '" << name << "' to location " << loc << dendl;
      CrushWrapper newcrush;
      _get_pending_crush(newcrush);

      if (!newcrush.name_exists(name)) {
	err = -ENOENT;
	ss << "item " << name << " does not exist";
	break;
      }
      int id = newcrush.get_item_id(name);

      if (!newcrush.check_item_loc(g_ceph_context, id, loc, (int *)NULL)) {
	if (id >= 0) {
	  err = newcrush.create_or_move_item(g_ceph_context, id, 0, name, loc);
	} else {
	  err = newcrush.move_bucket(g_ceph_context, id, loc);
	}
	if (err >= 0) {
	  ss << "moved item id " << id << " name '" << name << "' to location " << loc << " in crush map";
	  pending_inc.crush.clear();
	  newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
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
    string source, dest, force;
    cmd_getval(g_ceph_context, cmdmap, "source", source);
    cmd_getval(g_ceph_context, cmdmap, "dest", dest);
    cmd_getval(g_ceph_context, cmdmap, "force", force);
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);
    if (!newcrush.name_exists(source)) {
      ss << "source item " << source << " does not exist";
      err = -ENOENT;
      goto reply;
    }
    if (!newcrush.name_exists(dest)) {
      ss << "dest item " << dest << " does not exist";
      err = -ENOENT;
      goto reply;
    }
    int sid = newcrush.get_item_id(source);
    int did = newcrush.get_item_id(dest);
    int sparent;
    if (newcrush.get_immediate_parent_id(sid, &sparent) == 0 &&
	force != "--yes-i-really-mean-it") {
      ss << "source item " << source << " is not an orphan bucket; pass --yes-i-really-mean-it to proceed anyway";
      err = -EPERM;
      goto reply;
    }
    if (newcrush.get_bucket_alg(sid) != newcrush.get_bucket_alg(did) &&
	force != "--yes-i-really-mean-it") {
      ss << "source bucket alg " << crush_alg_name(newcrush.get_bucket_alg(sid)) << " != "
	 << "dest bucket alg " << crush_alg_name(newcrush.get_bucket_alg(did))
	 << "; pass --yes-i-really-mean-it to proceed anyway";
      err = -EPERM;
      goto reply;
    }
    int r = newcrush.swap_bucket(g_ceph_context, sid, did);
    if (r < 0) {
      ss << "failed to swap bucket contents: " << cpp_strerror(r);
      err = r;
      goto reply;
    }
    ss << "swapped bucket of " << source << " to " << dest;
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    wait_for_finished_proposal(op,
			       new Monitor::C_Command(mon, op, err, ss.str(),
						      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd crush link") {
    // osd crush link <name> <loc1> [<loc2> ...]
    string name;
    cmd_getval(g_ceph_context, cmdmap, "name", name);
    vector<string> argvec;
    cmd_getval(g_ceph_context, cmdmap, "args", argvec);
    map<string,string> loc;
    CrushWrapper::parse_loc_map(argvec, &loc);

    // Need an explicit check for name_exists because get_item_id returns
    // 0 on unfound.
    int id = osdmap.crush->get_item_id(name);
    if (!osdmap.crush->name_exists(name)) {
      err = -ENOENT;
      ss << "item " << name << " does not exist";
      goto reply;
    } else {
      dout(5) << "resolved crush name '" << name << "' to id " << id << dendl;
    }
    if (osdmap.crush->check_item_loc(g_ceph_context, id, loc, (int*) NULL)) {
      ss << "no need to move item id " << id << " name '" << name
	 << "' to location " << loc << " in crush map";
      err = 0;
      goto reply;
    }

    dout(5) << "linking crush item name '" << name << "' at location " << loc << dendl;
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    if (!newcrush.name_exists(name)) {
      err = -ENOENT;
      ss << "item " << name << " does not exist";
      goto reply;
    } else {
      int id = newcrush.get_item_id(name);
      if (!newcrush.check_item_loc(g_ceph_context, id, loc, (int *)NULL)) {
	err = newcrush.link_bucket(g_ceph_context, id, loc);
	if (err >= 0) {
	  ss << "linked item id " << id << " name '" << name
             << "' to location " << loc << " in crush map";
	  pending_inc.crush.clear();
	  newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
	} else {
	  ss << "cannot link item id " << id << " name '" << name
             << "' to location " << loc;
          goto reply;
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
      CrushWrapper newcrush;
      _get_pending_crush(newcrush);

      string name;
      cmd_getval(g_ceph_context, cmdmap, "name", name);

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
      if (cmd_getval(g_ceph_context, cmdmap, "ancestor", ancestor_str)) {
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
	ss << "removed item id " << id << " name '" << name << "' from crush map";
	getline(ss, rs);
	wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						  get_last_committed() + 1));
	return true;
      }
    } while (false);

  } else if (prefix == "osd crush reweight-all") {
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    newcrush.reweight(g_ceph_context);
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    ss << "reweighted crush hierarchy";
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						  get_last_committed() + 1));
    return true;
  } else if (prefix == "osd crush reweight") {
    // osd crush reweight <name> <weight>
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    string name;
    cmd_getval(g_ceph_context, cmdmap, "name", name);
    if (!newcrush.name_exists(name)) {
      err = -ENOENT;
      ss << "device '" << name << "' does not appear in the crush map";
      goto reply;
    }

    int id = newcrush.get_item_id(name);
    if (id < 0) {
      ss << "device '" << name << "' is not a leaf in the crush map";
      err = -EINVAL;
      goto reply;
    }
    double w;
    if (!cmd_getval(g_ceph_context, cmdmap, "weight", w)) {
      ss << "unable to parse weight value '"
	 << cmd_vartype_stringify(cmdmap["weight"]) << "'";
      err = -EINVAL;
      goto reply;
    }

    err = newcrush.adjust_item_weightf(g_ceph_context, id, w);
    if (err < 0)
      goto reply;
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    ss << "reweighted item id " << id << " name '" << name << "' to " << w
       << " in crush map";
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						  get_last_committed() + 1));
    return true;
  } else if (prefix == "osd crush reweight-subtree") {
    // osd crush reweight <name> <weight>
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    string name;
    cmd_getval(g_ceph_context, cmdmap, "name", name);
    if (!newcrush.name_exists(name)) {
      err = -ENOENT;
      ss << "device '" << name << "' does not appear in the crush map";
      goto reply;
    }

    int id = newcrush.get_item_id(name);
    if (id >= 0) {
      ss << "device '" << name << "' is not a subtree in the crush map";
      err = -EINVAL;
      goto reply;
    }
    double w;
    if (!cmd_getval(g_ceph_context, cmdmap, "weight", w)) {
      ss << "unable to parse weight value '"
	 << cmd_vartype_stringify(cmdmap["weight"]) << "'";
      err = -EINVAL;
      goto reply;
    }

    err = newcrush.adjust_subtree_weightf(g_ceph_context, id, w);
    if (err < 0)
      goto reply;
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    ss << "reweighted subtree id " << id << " name '" << name << "' to " << w
       << " in crush map";
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd crush tunables") {
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    err = 0;
    string profile;
    cmd_getval(g_ceph_context, cmdmap, "profile", profile);
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
      goto reply;
    }

    if (!validate_crush_against_features(&newcrush, ss)) {
      err = -EINVAL;
      goto reply;
    }

    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    ss << "adjusted tunables profile to " << profile;
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd crush set-tunable") {
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    err = 0;
    string tunable;
    cmd_getval(g_ceph_context, cmdmap, "tunable", tunable);

    int64_t value = -1;
    if (!cmd_getval(g_ceph_context, cmdmap, "value", value)) {
      err = -EINVAL;
      ss << "failed to parse integer value " << cmd_vartype_stringify(cmdmap["value"]);
      goto reply;
    }

    if (tunable == "straw_calc_version") {
      if (value != 0 && value != 1) {
	ss << "value must be 0 or 1; got " << value;
	err = -EINVAL;
	goto reply;
      }
      newcrush.set_straw_calc_version(value);
    } else {
      ss << "unrecognized tunable '" << tunable << "'";
      err = -EINVAL;
      goto reply;
    }

    if (!validate_crush_against_features(&newcrush, ss)) {
      err = -EINVAL;
      goto reply;
    }

    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    ss << "adjusted tunable " << tunable << " to " << value;
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;

  } else if (prefix == "osd crush rule create-simple") {
    string name, root, type, mode;
    cmd_getval(g_ceph_context, cmdmap, "name", name);
    cmd_getval(g_ceph_context, cmdmap, "root", root);
    cmd_getval(g_ceph_context, cmdmap, "type", type);
    cmd_getval(g_ceph_context, cmdmap, "mode", mode);
    if (mode == "")
      mode = "firstn";

    if (osdmap.crush->rule_exists(name)) {
      // The name is uniquely associated to a ruleid and the rule it contains
      // From the user point of view, the rule is more meaningfull.
      ss << "rule " << name << " already exists";
      err = 0;
      goto reply;
    }

    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

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
	goto reply;
      }

      pending_inc.crush.clear();
      newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    }
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;

  } else if (prefix == "osd crush rule create-replicated") {
    string name, root, type, device_class;
    cmd_getval(g_ceph_context, cmdmap, "name", name);
    cmd_getval(g_ceph_context, cmdmap, "root", root);
    cmd_getval(g_ceph_context, cmdmap, "type", type);
    cmd_getval(g_ceph_context, cmdmap, "class", device_class);

    if (!device_class.empty()) {
      if (osdmap.require_osd_release < CEPH_RELEASE_LUMINOUS) {
        ss << "you must complete the upgrade and 'ceph osd require-osd-release "
           << "luminous' before using crush device classes";
        err = -EPERM;
        goto reply;
      }
    }

    if (osdmap.crush->rule_exists(name)) {
      // The name is uniquely associated to a ruleid and the rule it contains
      // From the user point of view, the rule is more meaningfull.
      ss << "rule " << name << " already exists";
      err = 0;
      goto reply;
    }

    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

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
	goto reply;
      }

      pending_inc.crush.clear();
      newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    }
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;

  } else if (prefix == "osd erasure-code-profile rm") {
    string name;
    cmd_getval(g_ceph_context, cmdmap, "name", name);

    if (erasure_code_profile_in_use(pending_inc.new_pools, name, &ss))
      goto wait;

    if (erasure_code_profile_in_use(osdmap.pools, name, &ss)) {
      err = -EBUSY;
      goto reply;
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
      goto reply;
    }

  } else if (prefix == "osd erasure-code-profile set") {
    string name;
    cmd_getval(g_ceph_context, cmdmap, "name", name);
    vector<string> profile;
    cmd_getval(g_ceph_context, cmdmap, "profile", profile);
    bool force;
    if (profile.size() > 0 && profile.back() == "--force") {
      profile.pop_back();
      force = true;
    } else {
      force = false;
    }
    map<string,string> profile_map;
    err = parse_erasure_code_profile(profile, &profile_map, &ss);
    if (err)
      goto reply;
    if (profile_map.find("plugin") == profile_map.end()) {
      ss << "erasure-code-profile " << profile_map
	 << " must contain a plugin entry" << std::endl;
      err = -EINVAL;
      goto reply;
    }
    string plugin = profile_map["plugin"];

    if (pending_inc.has_erasure_code_profile(name)) {
      dout(20) << "erasure code profile " << name << " try again" << dendl;
      goto wait;
    } else {
      if (plugin == "isa" || plugin == "lrc") {
	err = check_cluster_features(CEPH_FEATURE_ERASURE_CODE_PLUGINS_V2, ss);
	if (err == -EAGAIN)
	  goto wait;
	if (err)
	  goto reply;
      } else if (plugin == "shec") {
	err = check_cluster_features(CEPH_FEATURE_ERASURE_CODE_PLUGINS_V3, ss);
	if (err == -EAGAIN)
	  goto wait;
	if (err)
	  goto reply;
      }
      err = normalize_profile(name, profile_map, force, &ss);
      if (err)
	goto reply;

      if (osdmap.has_erasure_code_profile(name)) {
	ErasureCodeProfile existing_profile_map =
	  osdmap.get_erasure_code_profile(name);
	err = normalize_profile(name, existing_profile_map, force, &ss);
	if (err)
	  goto reply;

	if (existing_profile_map == profile_map) {
	  err = 0;
	  goto reply;
	}
	if (!force) {
	  err = -EPERM;
	  ss << "will not override erasure code profile " << name
	     << " because the existing profile "
	     << existing_profile_map
	     << " is different from the proposed profile "
	     << profile_map;
	  goto reply;
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
      goto reply;
    string name, poolstr;
    cmd_getval(g_ceph_context, cmdmap, "name", name);
    string profile;
    cmd_getval(g_ceph_context, cmdmap, "profile", profile);
    if (profile == "")
      profile = "default";
    if (profile == "default") {
      if (!osdmap.has_erasure_code_profile(profile)) {
	if (pending_inc.has_erasure_code_profile(profile)) {
	  dout(20) << "erasure code profile " << profile << " already pending" << dendl;
	  goto wait;
	}

	map<string,string> profile_map;
	err = osdmap.get_erasure_code_profile_default(g_ceph_context,
						      profile_map,
						      &ss);
	if (err)
	  goto reply;
	err = normalize_profile(name, profile_map, true, &ss);
	if (err)
	  goto reply;
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
	goto reply;
	break;
      case -EALREADY: // wait for pending to be proposed
	ss << "rule " << name << " already exists";
	err = 0;
	break;
      default: // non recoverable error
 	goto reply;
	break;
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
    cmd_getval(g_ceph_context, cmdmap, "name", name);

    if (!osdmap.crush->rule_exists(name)) {
      ss << "rule " << name << " does not exist";
      err = 0;
      goto reply;
    }

    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    if (!newcrush.rule_exists(name)) {
      ss << "rule " << name << " does not exist";
      err = 0;
    } else {
      int ruleno = newcrush.get_rule_id(name);
      assert(ruleno >= 0);

      // make sure it is not in use.
      // FIXME: this is ok in some situations, but let's not bother with that
      // complexity now.
      int ruleset = newcrush.get_rule_mask_ruleset(ruleno);
      if (osdmap.crush_rule_in_use(ruleset)) {
	ss << "crush ruleset " << name << " " << ruleset << " is in use";
	err = -EBUSY;
	goto reply;
      }

      err = newcrush.remove_rule(ruleno);
      if (err < 0) {
	goto reply;
      }

      pending_inc.crush.clear();
      newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    }
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					      get_last_committed() + 1));
    return true;

  } else if (prefix == "osd crush rule rename") {
    string srcname;
    string dstname;
    cmd_getval(g_ceph_context, cmdmap, "srcname", srcname);
    cmd_getval(g_ceph_context, cmdmap, "dstname", dstname);
    if (srcname.empty() || dstname.empty()) {
      ss << "must specify both source rule name and destination rule name";
      err = -EINVAL;
      goto reply;
    }
    if (srcname == dstname) {
      ss << "destination rule name is equal to source rule name";
      err = 0;
      goto reply;
    }

    CrushWrapper newcrush;
    _get_pending_crush(newcrush);
    if (!newcrush.rule_exists(srcname) && newcrush.rule_exists(dstname)) {
      // srcname does not exist and dstname already exists
      // suppose this is a replay and return success
      // (so this command is idempotent)
      ss << "already renamed to '" << dstname << "'";
      err = 0;
      goto reply;
    }

    err = newcrush.rename_rule(srcname, dstname, &ss);
    if (err < 0) {
      // ss has reason for failure
      goto reply;
    }
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
                               get_last_committed() + 1));
    return true;

  } else if (prefix == "osd setmaxosd") {
    int64_t newmax;
    if (!cmd_getval(g_ceph_context, cmdmap, "newmax", newmax)) {
      ss << "unable to parse 'newmax' value '"
         << cmd_vartype_stringify(cmdmap["newmax"]) << "'";
      err = -EINVAL;
      goto reply;
    }

    if (newmax > g_conf->mon_max_osd) {
      err = -ERANGE;
      ss << "cannot set max_osd to " << newmax << " which is > conf.mon_max_osd ("
	 << g_conf->mon_max_osd << ")";
      goto reply;
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
          goto reply;
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
    if (osdmap.require_osd_release < CEPH_RELEASE_LUMINOUS) {
      ss << "you must complete the upgrade and 'ceph osd require-osd-release "
	 << "luminous' before using the new interface";
      err = -EPERM;
      goto reply;
    }
    double n;
    if (!cmd_getval(g_ceph_context, cmdmap, "ratio", n)) {
      ss << "unable to parse 'ratio' value '"
         << cmd_vartype_stringify(cmdmap["ratio"]) << "'";
      err = -EINVAL;
      goto reply;
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
    if (osdmap.require_osd_release < CEPH_RELEASE_LUMINOUS) {
      ss << "you must complete the upgrade and 'ceph osd require-osd-release "
	 << "luminous' before using the new interface";
      err = -EPERM;
      goto reply;
    }
    string v;
    cmd_getval(g_ceph_context, cmdmap, "version", v);
    int vno = ceph_release_from_name(v.c_str());
    if (vno <= 0) {
      ss << "version " << v << " is not recognized";
      err = -EINVAL;
      goto reply;
    }
    OSDMap newmap;
    newmap.deepish_copy_from(osdmap);
    newmap.apply_incremental(pending_inc);
    newmap.require_min_compat_client = vno;
    auto mvno = newmap.get_min_compat_client();
    if (vno < mvno) {
      ss << "osdmap current utilizes features that require "
	 << ceph_release_name(mvno)
	 << "; cannot set require_min_compat_client below that to "
	 << ceph_release_name(vno);
      err = -EPERM;
      goto reply;
    }
    string sure;
    cmd_getval(g_ceph_context, cmdmap, "sure", sure);
    if (sure != "--yes-i-really-mean-it") {
      FeatureMap m;
      mon->get_combined_feature_map(&m);
      uint64_t features = ceph_release_features(vno);
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
	goto reply;
      }
    }
    ss << "set require_min_compat_client to " << ceph_release_name(vno);
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
    string sure;
    cmd_getval(g_ceph_context, cmdmap, "sure", sure);
    string key;
    cmd_getval(g_ceph_context, cmdmap, "key", key);
    if (key == "full")
      return prepare_set_flag(op, CEPH_OSDMAP_FULL);
    else if (key == "pause")
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
    else if (key == "sortbitwise") {
      if (!osdmap.get_num_up_osds() && sure != "--yes-i-really-mean-it") {
        ss << "Not advisable to continue since no OSDs are up. Pass "
           << "--yes-i-really-mean-it if you really wish to continue.";
        err = -EPERM;
        goto reply;
      }
      if ((osdmap.get_up_osd_features() & CEPH_FEATURE_OSD_BITWISE_HOBJ_SORT)
          || sure == "--yes-i-really-mean-it") {
	return prepare_set_flag(op, CEPH_OSDMAP_SORTBITWISE);
      } else {
	ss << "not all up OSDs have OSD_BITWISE_HOBJ_SORT feature";
	err = -EPERM;
	goto reply;
      }
    } else if (key == "recovery_deletes") {
      if (!osdmap.get_num_up_osds() && sure != "--yes-i-really-mean-it") {
        ss << "Not advisable to continue since no OSDs are up. Pass "
           << "--yes-i-really-mean-it if you really wish to continue.";
        err = -EPERM;
        goto reply;
      }
      if (HAVE_FEATURE(osdmap.get_up_osd_features(), OSD_RECOVERY_DELETES)
          || sure == "--yes-i-really-mean-it") {
	return prepare_set_flag(op, CEPH_OSDMAP_RECOVERY_DELETES);
      } else {
	ss << "not all up OSDs have OSD_RECOVERY_DELETES feature";
	err = -EPERM;
	goto reply;
      }
    } else if (key == "require_jewel_osds") {
      if (!osdmap.get_num_up_osds() && sure != "--yes-i-really-mean-it") {
        ss << "Not advisable to continue since no OSDs are up. Pass "
           << "--yes-i-really-mean-it if you really wish to continue.";
        err = -EPERM;
        goto reply;
      }
      if (!osdmap.test_flag(CEPH_OSDMAP_SORTBITWISE)) {
	ss << "the sortbitwise flag must be set before require_jewel_osds";
	err = -EPERM;
	goto reply;
      } else if (osdmap.require_osd_release >= CEPH_RELEASE_JEWEL) {
	ss << "require_osd_release is already >= jewel";
	err = 0;
	goto reply;
      } else if (HAVE_FEATURE(osdmap.get_up_osd_features(), SERVER_JEWEL)
                 || sure == "--yes-i-really-mean-it") {
	return prepare_set_flag(op, CEPH_OSDMAP_REQUIRE_JEWEL);
      } else {
	ss << "not all up OSDs have CEPH_FEATURE_SERVER_JEWEL feature";
	err = -EPERM;
      }
    } else if (key == "require_kraken_osds") {
      if (!osdmap.get_num_up_osds() && sure != "--yes-i-really-mean-it") {
        ss << "Not advisable to continue since no OSDs are up. Pass "
           << "--yes-i-really-mean-it if you really wish to continue.";
        err = -EPERM;
        goto reply;
      }
      if (!osdmap.test_flag(CEPH_OSDMAP_SORTBITWISE)) {
	ss << "the sortbitwise flag must be set before require_kraken_osds";
	err = -EPERM;
	goto reply;
      } else if (osdmap.require_osd_release >= CEPH_RELEASE_KRAKEN) {
	ss << "require_osd_release is already >= kraken";
	err = 0;
	goto reply;
      } else if (HAVE_FEATURE(osdmap.get_up_osd_features(), SERVER_KRAKEN)
                 || sure == "--yes-i-really-mean-it") {
	bool r = prepare_set_flag(op, CEPH_OSDMAP_REQUIRE_KRAKEN);
	// ensure JEWEL is also set
	pending_inc.new_flags |= CEPH_OSDMAP_REQUIRE_JEWEL;
	return r;
      } else {
	ss << "not all up OSDs have CEPH_FEATURE_SERVER_KRAKEN feature";
	err = -EPERM;
      }
    } else {
      ss << "unrecognized flag '" << key << "'";
      err = -EINVAL;
    }

  } else if (prefix == "osd unset") {
    string key;
    cmd_getval(g_ceph_context, cmdmap, "key", key);
    if (key == "full")
      return prepare_unset_flag(op, CEPH_OSDMAP_FULL);
    else if (key == "pause")
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
    else {
      ss << "unrecognized flag '" << key << "'";
      err = -EINVAL;
    }

  } else if (prefix == "osd require-osd-release") {
    string release;
    cmd_getval(g_ceph_context, cmdmap, "release", release);
    string sure;
    cmd_getval(g_ceph_context, cmdmap, "sure", sure);
    if (!osdmap.test_flag(CEPH_OSDMAP_SORTBITWISE)) {
      ss << "the sortbitwise flag must be set first";
      err = -EPERM;
      goto reply;
    }
    int rel = ceph_release_from_name(release.c_str());
    if (rel <= 0) {
      ss << "unrecognized release " << release;
      err = -EINVAL;
      goto reply;
    }
    if (rel < CEPH_RELEASE_LUMINOUS) {
      ss << "use this command only for luminous and later";
      err = -EINVAL;
      goto reply;
    }
    if (rel == osdmap.require_osd_release) {
      // idempotent
      err = 0;
      goto reply;
    }
    if (rel == CEPH_RELEASE_LUMINOUS) {
      if (!HAVE_FEATURE(osdmap.get_up_osd_features(), SERVER_LUMINOUS)) {
	ss << "not all up OSDs have CEPH_FEATURE_SERVER_LUMINOUS feature";
	err = -EPERM;
	goto reply;
      }
    } else {
      ss << "not supported for this release yet";
      err = -EPERM;
      goto reply;
    }
    if (rel < osdmap.require_osd_release) {
      ss << "require_osd_release cannot be lowered once it has been set";
      err = -EPERM;
      goto reply;
    }
    pending_inc.new_require_osd_release = rel;
    if (rel >= CEPH_RELEASE_LUMINOUS &&
	!osdmap.test_flag(CEPH_OSDMAP_RECOVERY_DELETES)) {
      return prepare_set_flag(op, CEPH_OSDMAP_RECOVERY_DELETES);
    }
    goto update;
  } else if (prefix == "osd cluster_snap") {
    // ** DISABLE THIS FOR NOW **
    ss << "cluster snapshot currently disabled (broken implementation)";
    // ** DISABLE THIS FOR NOW **

  } else if (prefix == "osd down" ||
	     prefix == "osd out" ||
	     prefix == "osd in" ||
	     prefix == "osd rm") {

    bool any = false;
    bool stop = false;
    bool verbose = true;

    vector<string> idvec;
    cmd_getval(g_ceph_context, cmdmap, "ids", idvec);
    for (unsigned j = 0; j < idvec.size() && !stop; j++) {
      set<int> osds;

      // wildcard?
      if (j == 0 &&
          (idvec[0] == "any" || idvec[0] == "all" || idvec[0] == "*")) {
        if (prefix == "osd in") {
          // touch out osds only
          osdmap.get_out_osds(osds);
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

            mon->clog->info() << msg.str();
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
            assert(err == 0);
	    if (any) {
	      ss << ", osd." << osd;
            } else {
	      ss << "removed osd." << osd;
            }
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
  } else if (prefix == "osd add-noup" ||
             prefix == "osd add-nodown" ||
             prefix == "osd add-noin" ||
             prefix == "osd add-noout") {

    enum {
      OP_NOUP,
      OP_NODOWN,
      OP_NOIN,
      OP_NOOUT,
    } option;

    if (prefix == "osd add-noup") {
      option = OP_NOUP;
    } else if (prefix == "osd add-nodown") {
      option = OP_NODOWN;
    } else if (prefix == "osd add-noin") {
      option = OP_NOIN;
    } else {
      option = OP_NOOUT;
    }

    bool any = false;
    bool stop = false;

    vector<string> idvec;
    cmd_getval(g_ceph_context, cmdmap, "ids", idvec);
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

        switch (option) {
        case OP_NOUP:
          if (osdmap.is_up(osd)) {
            ss << "osd." << osd << " is already up. ";
            continue;
          }

          if (osdmap.is_noup(osd)) {
            if (pending_inc.pending_osd_state_clear(osd, CEPH_OSD_NOUP))
              any = true;
          } else {
            pending_inc.pending_osd_state_set(osd, CEPH_OSD_NOUP);
            any = true;
          }

          break;

        case OP_NODOWN:
          if (osdmap.is_down(osd)) {
            ss << "osd." << osd << " is already down. ";
            continue;
          }

          if (osdmap.is_nodown(osd)) {
            if (pending_inc.pending_osd_state_clear(osd, CEPH_OSD_NODOWN))
              any = true;
          } else {
            pending_inc.pending_osd_state_set(osd, CEPH_OSD_NODOWN);
            any = true;
          }

          break;

        case OP_NOIN:
          if (osdmap.is_in(osd)) {
            ss << "osd." << osd << " is already in. ";
            continue;
          }

          if (osdmap.is_noin(osd)) {
            if (pending_inc.pending_osd_state_clear(osd, CEPH_OSD_NOIN))
              any = true;
          } else {
            pending_inc.pending_osd_state_set(osd, CEPH_OSD_NOIN);
            any = true;
          }

          break;

        case OP_NOOUT:
          if (osdmap.is_out(osd)) {
            ss << "osd." << osd << " is already out. ";
            continue;
          }

          if (osdmap.is_noout(osd)) {
            if (pending_inc.pending_osd_state_clear(osd, CEPH_OSD_NOOUT))
              any = true;
          } else {
            pending_inc.pending_osd_state_set(osd, CEPH_OSD_NOOUT);
            any = true;
          }

          break;

        default:
	  assert(0 == "invalid option");
        }
      }
    }

    if (any) {
      getline(ss, rs);
      wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, err, rs,
                                 get_last_committed() + 1));
      return true;
    }
  } else if (prefix == "osd rm-noup" ||
             prefix == "osd rm-nodown" ||
             prefix == "osd rm-noin" ||
             prefix == "osd rm-noout") {

    enum {
      OP_NOUP,
      OP_NODOWN,
      OP_NOIN,
      OP_NOOUT,
    } option;

    if (prefix == "osd rm-noup") {
      option = OP_NOUP;
    } else if (prefix == "osd rm-nodown") {
      option = OP_NODOWN;
    } else if (prefix == "osd rm-noin") {
      option = OP_NOIN;
    } else {
      option = OP_NOOUT;
    }

    bool any = false;
    bool stop = false;

    vector<string> idvec;
    cmd_getval(g_ceph_context, cmdmap, "ids", idvec);

    for (unsigned j = 0; j < idvec.size() && !stop; j++) {

      vector<int> osds;

      // wildcard?
      if (j == 0 &&
          (idvec[0] == "any" || idvec[0] == "all" || idvec[0] == "*")) {

        // touch previous noup/nodown/noin/noout osds only
        switch (option) {
        case OP_NOUP:
          osdmap.get_noup_osds(&osds);
          break;
        case OP_NODOWN:
          osdmap.get_nodown_osds(&osds);
          break;
        case OP_NOIN:
          osdmap.get_noin_osds(&osds);
          break;
        case OP_NOOUT:
          osdmap.get_noout_osds(&osds);
          break;
        default:
          assert(0 == "invalid option");
        }

        // cancel any pending noup/nodown/noin/noout requests too
        vector<int> pending_state_osds;
        (void) pending_inc.get_pending_state_osds(&pending_state_osds);
        for (auto &p : pending_state_osds) {

          switch (option) {
          case OP_NOUP:
            if (!osdmap.is_noup(p) &&
                pending_inc.pending_osd_state_clear(p, CEPH_OSD_NOUP)) {
              any = true;
            }
            break;

          case OP_NODOWN:
            if (!osdmap.is_nodown(p) &&
                pending_inc.pending_osd_state_clear(p, CEPH_OSD_NODOWN)) {
              any = true;
            }
            break;

          case OP_NOIN:
            if (!osdmap.is_noin(p) &&
                pending_inc.pending_osd_state_clear(p, CEPH_OSD_NOIN)) {
              any = true;
            }
            break;

          case OP_NOOUT:
            if (!osdmap.is_noout(p) &&
                pending_inc.pending_osd_state_clear(p, CEPH_OSD_NOOUT)) {
              any = true;
            }
            break;

          default:
            assert(0 == "invalid option");
          }
        }

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

        osds.push_back(osd);
      }

      for (auto &osd : osds) {

        if (!osdmap.exists(osd)) {
          ss << "osd." << osd << " does not exist. ";
          continue;
        }

        switch (option) {
          case OP_NOUP:
            if (osdmap.is_noup(osd)) {
              pending_inc.pending_osd_state_set(osd, CEPH_OSD_NOUP);
              any = true;
            } else if (pending_inc.pending_osd_state_clear(
              osd, CEPH_OSD_NOUP)) {
              any = true;
            }
            break;

          case OP_NODOWN:
            if (osdmap.is_nodown(osd)) {
              pending_inc.pending_osd_state_set(osd, CEPH_OSD_NODOWN);
              any = true;
            } else if (pending_inc.pending_osd_state_clear(
              osd, CEPH_OSD_NODOWN)) {
              any = true;
            }
            break;

          case OP_NOIN:
            if (osdmap.is_noin(osd)) {
              pending_inc.pending_osd_state_set(osd, CEPH_OSD_NOIN);
              any = true;
            } else if (pending_inc.pending_osd_state_clear(
              osd, CEPH_OSD_NOIN)) {
              any = true;
            }
            break;

          case OP_NOOUT:
            if (osdmap.is_noout(osd)) {
              pending_inc.pending_osd_state_set(osd, CEPH_OSD_NOOUT);
              any = true;
            } else if (pending_inc.pending_osd_state_clear(
              osd, CEPH_OSD_NOOUT)) {
              any = true;
            }
            break;

          default:
            assert(0 == "invalid option");
        }
      }
    }

    if (any) {
      getline(ss, rs);
      wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, err, rs,
                                 get_last_committed() + 1));
      return true;
    }
  } else if (prefix == "osd pg-temp") {
    string pgidstr;
    if (!cmd_getval(g_ceph_context, cmdmap, "pgid", pgidstr)) {
      ss << "unable to parse 'pgid' value '"
         << cmd_vartype_stringify(cmdmap["pgid"]) << "'";
      err = -EINVAL;
      goto reply;
    }
    pg_t pgid;
    if (!pgid.parse(pgidstr.c_str())) {
      ss << "invalid pgid '" << pgidstr << "'";
      err = -EINVAL;
      goto reply;
    }
    if (!osdmap.pg_exists(pgid)) {
      ss << "pg " << pgid << " does not exist";
      err = -ENOENT;
      goto reply;
    }
    if (pending_inc.new_pg_temp.count(pgid)) {
      dout(10) << __func__ << " waiting for pending update on " << pgid << dendl;
      wait_for_finished_proposal(op, new C_RetryMessage(this, op));
      return true;
    }

    vector<int64_t> id_vec;
    vector<int32_t> new_pg_temp;
    if (!cmd_getval(g_ceph_context, cmdmap, "id", id_vec)) {
      ss << "unable to parse 'id' value(s) '"
         << cmd_vartype_stringify(cmdmap["id"]) << "'";
      err = -EINVAL;
      goto reply;
    }
    for (auto osd : id_vec) {
      if (!osdmap.exists(osd)) {
        ss << "osd." << osd << " does not exist";
        err = -ENOENT;
        goto reply;
      }
      new_pg_temp.push_back(osd);
    }

    int pool_min_size = osdmap.get_pg_pool_min_size(pgid);
    if ((int)new_pg_temp.size() < pool_min_size) {
      ss << "num of osds (" << new_pg_temp.size() <<") < pool min size ("
         << pool_min_size << ")";
      err = -EINVAL;
      goto reply;
    }

    int pool_size = osdmap.get_pg_pool_size(pgid);
    if ((int)new_pg_temp.size() > pool_size) {
      ss << "num of osds (" << new_pg_temp.size() <<") > pool size ("
         << pool_size << ")";
      err = -EINVAL;
      goto reply;
    }

    pending_inc.new_pg_temp[pgid] = mempool::osdmap::vector<int>(
      new_pg_temp.begin(), new_pg_temp.end());
    ss << "set " << pgid << " pg_temp mapping to " << new_pg_temp;
    goto update;
  } else if (prefix == "osd primary-temp") {
    string pgidstr;
    if (!cmd_getval(g_ceph_context, cmdmap, "pgid", pgidstr)) {
      ss << "unable to parse 'pgid' value '"
         << cmd_vartype_stringify(cmdmap["pgid"]) << "'";
      err = -EINVAL;
      goto reply;
    }
    pg_t pgid;
    if (!pgid.parse(pgidstr.c_str())) {
      ss << "invalid pgid '" << pgidstr << "'";
      err = -EINVAL;
      goto reply;
    }
    if (!osdmap.pg_exists(pgid)) {
      ss << "pg " << pgid << " does not exist";
      err = -ENOENT;
      goto reply;
    }

    int64_t osd;
    if (!cmd_getval(g_ceph_context, cmdmap, "id", osd)) {
      ss << "unable to parse 'id' value '"
         << cmd_vartype_stringify(cmdmap["id"]) << "'";
      err = -EINVAL;
      goto reply;
    }
    if (osd != -1 && !osdmap.exists(osd)) {
      ss << "osd." << osd << " does not exist";
      err = -ENOENT;
      goto reply;
    }

    if (osdmap.require_min_compat_client > 0 &&
	osdmap.require_min_compat_client < CEPH_RELEASE_FIREFLY) {
      ss << "require_min_compat_client "
	 << ceph_release_name(osdmap.require_min_compat_client)
	 << " < firefly, which is required for primary-temp";
      err = -EPERM;
      goto reply;
    } else if (!g_conf->mon_osd_allow_primary_temp) {
      ss << "you must enable 'mon osd allow primary temp = true' on the mons before you can set primary_temp mappings.  note that this is for developers only: older clients/OSDs will break and there is no feature bit infrastructure in place.";
      err = -EPERM;
      goto reply;
    }

    pending_inc.new_primary_temp[pgid] = osd;
    ss << "set " << pgid << " primary_temp mapping to " << osd;
    goto update;
  } else if (prefix == "osd pg-upmap" ||
             prefix == "osd rm-pg-upmap" ||
             prefix == "osd pg-upmap-items" ||
             prefix == "osd rm-pg-upmap-items") {
    if (osdmap.require_osd_release < CEPH_RELEASE_LUMINOUS) {
      ss << "you must complete the upgrade and 'ceph osd require-osd-release "
	 << "luminous' before using the new interface";
      err = -EPERM;
      goto reply;
    }
    if (osdmap.require_min_compat_client < CEPH_RELEASE_LUMINOUS) {
      ss << "min_compat_client "
	 << ceph_release_name(osdmap.require_min_compat_client)
	 << " < luminous, which is required for pg-upmap. "
         << "Try 'ceph osd set-require-min-compat-client luminous' "
         << "before using the new interface";
      err = -EPERM;
      goto reply;
    }
    err = check_cluster_features(CEPH_FEATUREMASK_OSDMAP_PG_UPMAP, ss);
    if (err == -EAGAIN)
      goto wait;
    if (err < 0)
      goto reply;
    string pgidstr;
    if (!cmd_getval(g_ceph_context, cmdmap, "pgid", pgidstr)) {
      ss << "unable to parse 'pgid' value '"
         << cmd_vartype_stringify(cmdmap["pgid"]) << "'";
      err = -EINVAL;
      goto reply;
    }
    pg_t pgid;
    if (!pgid.parse(pgidstr.c_str())) {
      ss << "invalid pgid '" << pgidstr << "'";
      err = -EINVAL;
      goto reply;
    }
    if (!osdmap.pg_exists(pgid)) {
      ss << "pg " << pgid << " does not exist";
      err = -ENOENT;
      goto reply;
    }

    enum {
      OP_PG_UPMAP,
      OP_RM_PG_UPMAP,
      OP_PG_UPMAP_ITEMS,
      OP_RM_PG_UPMAP_ITEMS,
    } option;

    if (prefix == "osd pg-upmap") {
      option = OP_PG_UPMAP;
    } else if (prefix == "osd rm-pg-upmap") {
      option = OP_RM_PG_UPMAP;
    } else if (prefix == "osd pg-upmap-items") {
      option = OP_PG_UPMAP_ITEMS;
    } else {
      option = OP_RM_PG_UPMAP_ITEMS;
    }

    // check pending upmap changes
    switch (option) {
    case OP_PG_UPMAP: // fall through
    case OP_RM_PG_UPMAP:
      if (pending_inc.new_pg_upmap.count(pgid) ||
          pending_inc.old_pg_upmap.count(pgid)) {
        dout(10) << __func__ << " waiting for pending update on "
                 << pgid << dendl;
        wait_for_finished_proposal(op, new C_RetryMessage(this, op));
        return true;
      }
      break;

    case OP_PG_UPMAP_ITEMS: // fall through
    case OP_RM_PG_UPMAP_ITEMS:
      if (pending_inc.new_pg_upmap_items.count(pgid) ||
          pending_inc.old_pg_upmap_items.count(pgid)) {
        dout(10) << __func__ << " waiting for pending update on "
                 << pgid << dendl;
        wait_for_finished_proposal(op, new C_RetryMessage(this, op));
        return true;
      }
      break;

    default:
      assert(0 == "invalid option");
    }

    switch (option) {
    case OP_PG_UPMAP:
      {
        vector<int64_t> id_vec;
        if (!cmd_getval(g_ceph_context, cmdmap, "id", id_vec)) {
          ss << "unable to parse 'id' value(s) '"
             << cmd_vartype_stringify(cmdmap["id"]) << "'";
          err = -EINVAL;
          goto reply;
        }

        int pool_min_size = osdmap.get_pg_pool_min_size(pgid);
        if ((int)id_vec.size() < pool_min_size) {
          ss << "num of osds (" << id_vec.size() <<") < pool min size ("
             << pool_min_size << ")";
          err = -EINVAL;
          goto reply;
        }

        int pool_size = osdmap.get_pg_pool_size(pgid);
        if ((int)id_vec.size() > pool_size) {
          ss << "num of osds (" << id_vec.size() <<") > pool size ("
             << pool_size << ")";
          err = -EINVAL;
          goto reply;
        }

        vector<int32_t> new_pg_upmap;
        for (auto osd : id_vec) {
          if (osd != CRUSH_ITEM_NONE && !osdmap.exists(osd)) {
            ss << "osd." << osd << " does not exist";
            err = -ENOENT;
            goto reply;
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
          goto reply;
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
        if (!cmd_getval(g_ceph_context, cmdmap, "id", id_vec)) {
          ss << "unable to parse 'id' value(s) '"
             << cmd_vartype_stringify(cmdmap["id"]) << "'";
          err = -EINVAL;
          goto reply;
        }

        if (id_vec.size() % 2) {
          ss << "you must specify pairs of osd ids to be remapped";
          err = -EINVAL;
          goto reply;
        }

        int pool_size = osdmap.get_pg_pool_size(pgid);
        if ((int)(id_vec.size() / 2) > pool_size) {
          ss << "num of osd pairs (" << id_vec.size() / 2 <<") > pool size ("
             << pool_size << ")";
          err = -EINVAL;
          goto reply;
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
            goto reply;
          }
          if (to != CRUSH_ITEM_NONE && !osdmap.exists(to)) {
            ss << "osd." << to << " does not exist";
            err = -ENOENT;
            goto reply;
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
          goto reply;
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

    default:
      assert(0 == "invalid option");
    }

    goto update;
  } else if (prefix == "osd primary-affinity") {
    int64_t id;
    if (!cmd_getval(g_ceph_context, cmdmap, "id", id)) {
      ss << "invalid osd id value '"
         << cmd_vartype_stringify(cmdmap["id"]) << "'";
      err = -EINVAL;
      goto reply;
    }
    double w;
    if (!cmd_getval(g_ceph_context, cmdmap, "weight", w)) {
      ss << "unable to parse 'weight' value '"
           << cmd_vartype_stringify(cmdmap["weight"]) << "'";
      err = -EINVAL;
      goto reply;
    }
    long ww = (int)((double)CEPH_OSD_MAX_PRIMARY_AFFINITY*w);
    if (ww < 0L) {
      ss << "weight must be >= 0";
      err = -EINVAL;
      goto reply;
    }
    if (osdmap.require_min_compat_client > 0 &&
	osdmap.require_min_compat_client < CEPH_RELEASE_FIREFLY) {
      ss << "require_min_compat_client "
	 << ceph_release_name(osdmap.require_min_compat_client)
	 << " < firefly, which is required for primary-affinity";
      err = -EPERM;
      goto reply;
    } else if (!g_conf->mon_osd_allow_primary_affinity) {
      ss << "you must enable 'mon osd allow primary affinity = true' on the mons before you can adjust primary-affinity.  note that older clients will no longer be able to communicate with the cluster.";
      err = -EPERM;
      goto reply;
    }
    err = check_cluster_features(CEPH_FEATURE_OSD_PRIMARY_AFFINITY, ss);
    if (err == -EAGAIN)
      goto wait;
    if (err < 0)
      goto reply;
    if (osdmap.exists(id)) {
      pending_inc.new_primary_affinity[id] = ww;
      ss << "set osd." << id << " primary-affinity to " << w << " (" << ios::hex << ww << ios::dec << ")";
      getline(ss, rs);
      wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
                                                get_last_committed() + 1));
      return true;
    } else {
      ss << "osd." << id << " does not exist";
      err = -ENOENT;
      goto reply;
    }
  } else if (prefix == "osd reweight") {
    int64_t id;
    if (!cmd_getval(g_ceph_context, cmdmap, "id", id)) {
      ss << "unable to parse osd id value '"
         << cmd_vartype_stringify(cmdmap["id"]) << "'";
      err = -EINVAL;
      goto reply;
    }
    double w;
    if (!cmd_getval(g_ceph_context, cmdmap, "weight", w)) {
      ss << "unable to parse weight value '"
         << cmd_vartype_stringify(cmdmap["weight"]) << "'";
      err = -EINVAL;
      goto reply;
    }
    long ww = (int)((double)CEPH_OSD_IN*w);
    if (ww < 0L) {
      ss << "weight must be >= 0";
      err = -EINVAL;
      goto reply;
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
      goto reply;
    }
  } else if (prefix == "osd reweightn") {
    map<int32_t, uint32_t> weights;
    err = parse_reweights(g_ceph_context, cmdmap, osdmap, &weights);
    if (err) {
      ss << "unable to parse 'weights' value '"
         << cmd_vartype_stringify(cmdmap["weights"]) << "'";
      goto reply;
    }
    pending_inc.new_weight.insert(weights.begin(), weights.end());
    wait_for_finished_proposal(
	op,
	new Monitor::C_Command(mon, op, 0, rs, rdata, get_last_committed() + 1));
    return true;
  } else if (prefix == "osd lost") {
    int64_t id;
    if (!cmd_getval(g_ceph_context, cmdmap, "id", id)) {
      ss << "unable to parse osd id value '"
         << cmd_vartype_stringify(cmdmap["id"]) << "'";
      err = -EINVAL;
      goto reply;
    }
    string sure;
    if (!cmd_getval(g_ceph_context, cmdmap, "sure", sure) || sure != "--yes-i-really-mean-it") {
      ss << "are you SURE?  this might mean real, permanent data loss.  pass "
	    "--yes-i-really-mean-it if you really do.";
      err = -EPERM;
      goto reply;
    } else if (!osdmap.exists(id)) {
      ss << "osd." << id << " does not exist";
      err = -ENOENT;
      goto reply;
    } else if (!osdmap.is_down(id)) {
      ss << "osd." << id << " is not down";
      err = -EBUSY;
      goto reply;
    } else {
      epoch_t e = osdmap.get_info(id).down_at;
      pending_inc.new_lost[id] = e;
      ss << "marked osd lost in epoch " << e;
      getline(ss, rs);
      wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						get_last_committed() + 1));
      return true;
    }

  } else if (prefix == "osd destroy" || prefix == "osd purge") {
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
    if (!mon->authmon()->is_writeable()) {
      dout(10) << __func__ << " waiting for auth mon to be writeable for "
               << "osd destroy" << dendl;
      mon->authmon()->wait_for_writeable(op, new C_RetryMessage(this, op));
      return false;
    }

    int64_t id;
    if (!cmd_getval(g_ceph_context, cmdmap, "id", id)) {
      ss << "unable to parse osd id value '"
         << cmd_vartype_stringify(cmdmap["id"]) << "";
      err = -EINVAL;
      goto reply;
    }

    bool is_destroy = (prefix == "osd destroy");
    if (!is_destroy) {
      assert("osd purge" == prefix);
    }

    string sure;
    if (!cmd_getval(g_ceph_context, cmdmap, "sure", sure) ||
        sure != "--yes-i-really-mean-it") {
      ss << "Are you SURE? This will mean real, permanent data loss, as well "
         << "as cephx and lockbox keys. Pass --yes-i-really-mean-it if you "
         << "really do.";
      err = -EPERM;
      goto reply;
    } else if (!osdmap.exists(id)) {
      ss << "osd." << id << " does not exist";
      err = 0; // idempotent
      goto reply;
    } else if (osdmap.is_up(id)) {
      ss << "osd." << id << " is not `down`.";
      err = -EBUSY;
      goto reply;
    } else if (is_destroy && osdmap.is_destroyed(id)) {
      ss << "destroyed osd." << id;
      err = 0;
      goto reply;
    }

    bool goto_reply = false;

    paxos->plug();
    if (is_destroy) {
      err = prepare_command_osd_destroy(id, ss);
      // we checked above that it should exist.
      assert(err != -ENOENT);
    } else {
      err = prepare_command_osd_purge(id, ss);
      if (err == -ENOENT) {
        err = 0;
        ss << "osd." << id << " does not exist.";
        goto_reply = true;
      }
    }
    paxos->unplug();

    if (err < 0 || goto_reply) {
      goto reply;
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
    if (!mon->authmon()->is_writeable()) {
      dout(10) << __func__ << " waiting for auth mon to be writeable for "
               << "osd new" << dendl;
      mon->authmon()->wait_for_writeable(op, new C_RetryMessage(this, op));
      return false;
    }

    map<string,string> secrets_map;

    bufferlist bl = m->get_data();
    string secrets_json = bl.to_str();
    dout(20) << __func__ << " osd new json = " << secrets_json << dendl;

    err = get_json_str_map(secrets_json, ss, &secrets_map);
    if (err < 0)
      goto reply;

    dout(20) << __func__ << " osd new secrets " << secrets_map << dendl;

    paxos->plug();
    err = prepare_command_osd_new(op, cmdmap, secrets_map, ss, f.get());
    paxos->unplug();

    if (err < 0) {
      goto reply;
    }

    if (f) {
      f->flush(rdata);
    } else {
      rdata.append(ss);
    }

    if (err == EEXIST) {
      // idempotent operation
      err = 0;
      goto reply;
    }

    wait_for_finished_proposal(op,
        new Monitor::C_Command(mon, op, 0, rs, rdata,
                               get_last_committed() + 1));
    force_immediate_propose();
    return true;

  } else if (prefix == "osd create") {

    // optional id provided?
    int64_t id = -1, cmd_id = -1;
    if (cmd_getval(g_ceph_context, cmdmap, "id", cmd_id)) {
      if (cmd_id < 0) {
	ss << "invalid osd id value '" << cmd_id << "'";
	err = -EINVAL;
	goto reply;
      }
      dout(10) << " osd create got id " << cmd_id << dendl;
    }

    uuid_d uuid;
    string uuidstr;
    if (cmd_getval(g_ceph_context, cmdmap, "uuid", uuidstr)) {
      if (!uuid.parse(uuidstr.c_str())) {
        ss << "invalid uuid value '" << uuidstr << "'";
        err = -EINVAL;
        goto reply;
      }
      // we only care about the id if we also have the uuid, to
      // ensure the operation's idempotency.
      id = cmd_id;
    }

    int32_t new_id = -1;
    err = prepare_command_osd_create(id, uuid, &new_id, ss);
    if (err < 0) {
      if (err == -EAGAIN) {
        wait_for_finished_proposal(op, new C_RetryMessage(this, op));
        return true;
      }
      // a check has failed; reply to the user.
      goto reply;

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
      goto reply;
    }

    do_osd_create(id, uuid, &new_id);

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

  } else if (prefix == "osd blacklist clear") {
    pending_inc.new_blacklist.clear();
    std::list<std::pair<entity_addr_t,utime_t > > blacklist;
    osdmap.get_blacklist(&blacklist);
    for (const auto &entry : blacklist) {
      pending_inc.old_blacklist.push_back(entry.first);
    }
    ss << " removed all blacklist entries";
    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
                                              get_last_committed() + 1));
    return true;
  } else if (prefix == "osd blacklist") {
    string addrstr;
    cmd_getval(g_ceph_context, cmdmap, "addr", addrstr);
    entity_addr_t addr;
    if (!addr.parse(addrstr.c_str(), 0)) {
      ss << "unable to parse address " << addrstr;
      err = -EINVAL;
      goto reply;
    }
    else {
      string blacklistop;
      cmd_getval(g_ceph_context, cmdmap, "blacklistop", blacklistop);
      if (blacklistop == "add") {
	utime_t expires = ceph_clock_now();
	double d;
	// default one hour
	cmd_getval(g_ceph_context, cmdmap, "expire", d,
          g_conf->mon_osd_blacklist_default_expire);
	expires += d;

	pending_inc.new_blacklist[addr] = expires;

        {
          // cancel any pending un-blacklisting request too
          auto it = std::find(pending_inc.old_blacklist.begin(),
            pending_inc.old_blacklist.end(), addr);
          if (it != pending_inc.old_blacklist.end()) {
            pending_inc.old_blacklist.erase(it);
          }
        }

	ss << "blacklisting " << addr << " until " << expires << " (" << d << " sec)";
	getline(ss, rs);
	wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						  get_last_committed() + 1));
	return true;
      } else if (blacklistop == "rm") {
	if (osdmap.is_blacklisted(addr) ||
	    pending_inc.new_blacklist.count(addr)) {
	  if (osdmap.is_blacklisted(addr))
	    pending_inc.old_blacklist.push_back(addr);
	  else
	    pending_inc.new_blacklist.erase(addr);
	  ss << "un-blacklisting " << addr;
	  getline(ss, rs);
	  wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						    get_last_committed() + 1));
	  return true;
	}
	ss << addr << " isn't blacklisted";
	err = 0;
	goto reply;
      }
    }
  } else if (prefix == "osd pool mksnap") {
    string poolstr;
    cmd_getval(g_ceph_context, cmdmap, "pool", poolstr);
    int64_t pool = osdmap.lookup_pg_pool_name(poolstr.c_str());
    if (pool < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply;
    }
    string snapname;
    cmd_getval(g_ceph_context, cmdmap, "snap", snapname);
    const pg_pool_t *p = osdmap.get_pg_pool(pool);
    if (p->is_unmanaged_snaps_mode()) {
      ss << "pool " << poolstr << " is in unmanaged snaps mode";
      err = -EINVAL;
      goto reply;
    } else if (p->snap_exists(snapname.c_str())) {
      ss << "pool " << poolstr << " snap " << snapname << " already exists";
      err = 0;
      goto reply;
    } else if (p->is_tier()) {
      ss << "pool " << poolstr << " is a cache tier";
      err = -EINVAL;
      goto reply;
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
    cmd_getval(g_ceph_context, cmdmap, "pool", poolstr);
    int64_t pool = osdmap.lookup_pg_pool_name(poolstr.c_str());
    if (pool < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply;
    }
    string snapname;
    cmd_getval(g_ceph_context, cmdmap, "snap", snapname);
    const pg_pool_t *p = osdmap.get_pg_pool(pool);
    if (p->is_unmanaged_snaps_mode()) {
      ss << "pool " << poolstr << " is in unmanaged snaps mode";
      err = -EINVAL;
      goto reply;
    } else if (!p->snap_exists(snapname.c_str())) {
      ss << "pool " << poolstr << " snap " << snapname << " does not exist";
      err = 0;
      goto reply;
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
    int64_t  pg_num;
    int64_t pgp_num;
    cmd_getval(g_ceph_context, cmdmap, "pg_num", pg_num, int64_t(0));
    cmd_getval(g_ceph_context, cmdmap, "pgp_num", pgp_num, pg_num);

    string pool_type_str;
    cmd_getval(g_ceph_context, cmdmap, "pool_type", pool_type_str);
    if (pool_type_str.empty())
      pool_type_str = g_conf->osd_pool_default_type;

    string poolstr;
    cmd_getval(g_ceph_context, cmdmap, "pool", poolstr);
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
      goto reply;
    }

    int pool_type;
    if (pool_type_str == "replicated") {
      pool_type = pg_pool_t::TYPE_REPLICATED;
    } else if (pool_type_str == "erasure") {
      err = check_cluster_features(CEPH_FEATURE_CRUSH_V2 |
				   CEPH_FEATURE_OSD_ERASURE_CODES,
				   ss);
      if (err == -EAGAIN)
	goto wait;
      if (err)
	goto reply;
      pool_type = pg_pool_t::TYPE_ERASURE;
    } else {
      ss << "unknown pool type '" << pool_type_str << "'";
      err = -EINVAL;
      goto reply;
    }

    bool implicit_rule_creation = false;
    string rule_name;
    cmd_getval(g_ceph_context, cmdmap, "rule", rule_name);
    string erasure_code_profile;
    cmd_getval(g_ceph_context, cmdmap, "erasure_code_profile", erasure_code_profile);

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
	  err = osdmap.get_erasure_code_profile_default(g_ceph_context,
						      profile_map,
						      &ss);
	  if (err)
	    goto reply;
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
    } else {
      //NOTE:for replicated pool,cmd_map will put rule_name to erasure_code_profile field
      rule_name = erasure_code_profile;
    }

    if (!implicit_rule_creation && rule_name != "") {
      int rule;
      err = get_crush_rule(rule_name, &rule, &ss);
      if (err == -EAGAIN) {
	wait_for_finished_proposal(op, new C_RetryMessage(this, op));
	return true;
      }
      if (err)
	goto reply;
    }

    int64_t expected_num_objects;
    cmd_getval(g_ceph_context, cmdmap, "expected_num_objects", expected_num_objects, int64_t(0));
    if (expected_num_objects < 0) {
      ss << "'expected_num_objects' must be non-negative";
      err = -EINVAL;
      goto reply;
    }

    int64_t fast_read_param;
    cmd_getval(g_ceph_context, cmdmap, "fast_read", fast_read_param, int64_t(-1));
    FastReadType fast_read = FAST_READ_DEFAULT;
    if (fast_read_param == 0)
      fast_read = FAST_READ_OFF;
    else if (fast_read_param > 0)
      fast_read = FAST_READ_ON;
    
    err = prepare_new_pool(poolstr, 0, // auid=0 for admin created pool
			   -1, // default crush rule
			   rule_name,
			   pg_num, pgp_num,
			   erasure_code_profile, pool_type,
                           (uint64_t)expected_num_objects,
                           fast_read,
			   &ss);
    if (err < 0) {
      switch(err) {
      case -EEXIST:
	ss << "pool '" << poolstr << "' already exists";
	break;
      case -EAGAIN:
	wait_for_finished_proposal(op, new C_RetryMessage(this, op));
	return true;
      case -ERANGE:
        goto reply;
      default:
	goto reply;
	break;
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
    cmd_getval(g_ceph_context, cmdmap, "pool", poolstr);
    cmd_getval(g_ceph_context, cmdmap, "pool2", poolstr2);
    cmd_getval(g_ceph_context, cmdmap, "sure", sure);
    int64_t pool = osdmap.lookup_pg_pool_name(poolstr.c_str());
    if (pool < 0) {
      ss << "pool '" << poolstr << "' does not exist";
      err = 0;
      goto reply;
    }

    bool force_no_fake = sure == "--yes-i-really-really-mean-it-not-faking";
    if (poolstr2 != poolstr ||
	(sure != "--yes-i-really-really-mean-it" && !force_no_fake)) {
      ss << "WARNING: this will *PERMANENTLY DESTROY* all data stored in pool " << poolstr
	 << ".  If you are *ABSOLUTELY CERTAIN* that is what you want, pass the pool name *twice*, "
	 << "followed by --yes-i-really-really-mean-it.";
      err = -EPERM;
      goto reply;
    }
    err = _prepare_remove_pool(pool, &ss, force_no_fake);
    if (err == -EAGAIN) {
      wait_for_finished_proposal(op, new C_RetryMessage(this, op));
      return true;
    }
    if (err < 0)
      goto reply;
    goto update;
  } else if (prefix == "osd pool rename") {
    string srcpoolstr, destpoolstr;
    cmd_getval(g_ceph_context, cmdmap, "srcpool", srcpoolstr);
    cmd_getval(g_ceph_context, cmdmap, "destpool", destpoolstr);
    int64_t pool_src = osdmap.lookup_pg_pool_name(srcpoolstr.c_str());
    int64_t pool_dst = osdmap.lookup_pg_pool_name(destpoolstr.c_str());

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
      goto reply;
    } else if (pool_dst >= 0) {
      // source pool exists and so does the destination pool
      ss << "pool '" << destpoolstr << "' already exists";
      err = -EEXIST;
      goto reply;
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
      goto reply;

    getline(ss, rs);
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						   get_last_committed() + 1));
    return true;
  } else if (prefix == "osd tier add") {
    err = check_cluster_features(CEPH_FEATURE_OSD_CACHEPOOL, ss);
    if (err == -EAGAIN)
      goto wait;
    if (err)
      goto reply;
    string poolstr;
    cmd_getval(g_ceph_context, cmdmap, "pool", poolstr);
    int64_t pool_id = osdmap.lookup_pg_pool_name(poolstr);
    if (pool_id < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply;
    }
    string tierpoolstr;
    cmd_getval(g_ceph_context, cmdmap, "tierpool", tierpoolstr);
    int64_t tierpool_id = osdmap.lookup_pg_pool_name(tierpoolstr);
    if (tierpool_id < 0) {
      ss << "unrecognized pool '" << tierpoolstr << "'";
      err = -ENOENT;
      goto reply;
    }
    const pg_pool_t *p = osdmap.get_pg_pool(pool_id);
    assert(p);
    const pg_pool_t *tp = osdmap.get_pg_pool(tierpool_id);
    assert(tp);

    if (!_check_become_tier(tierpool_id, tp, pool_id, p, &err, &ss)) {
      goto reply;
    }

    // make sure new tier is empty
    string force_nonempty;
    cmd_getval(g_ceph_context, cmdmap, "force_nonempty", force_nonempty);
    const pool_stat_t *pstats = mon->pgservice->get_pool_stat(tierpool_id);
    if (pstats && pstats->stats.sum.num_objects != 0 &&
	force_nonempty != "--force-nonempty") {
      ss << "tier pool '" << tierpoolstr << "' is not empty; --force-nonempty to force";
      err = -ENOTEMPTY;
      goto reply;
    }
    if (tp->ec_pool()) {
      ss << "tier pool '" << tierpoolstr
	 << "' is an ec pool, which cannot be a tier";
      err = -ENOTSUP;
      goto reply;
    }
    if ((!tp->removed_snaps.empty() || !tp->snaps.empty()) &&
	((force_nonempty != "--force-nonempty") ||
	 (!g_conf->mon_debug_unsafe_allow_tier_with_nonempty_snaps))) {
      ss << "tier pool '" << tierpoolstr << "' has snapshot state; it cannot be added as a tier without breaking the pool";
      err = -ENOTEMPTY;
      goto reply;
    }
    // go
    pg_pool_t *np = pending_inc.get_new_pool(pool_id, p);
    pg_pool_t *ntp = pending_inc.get_new_pool(tierpool_id, tp);
    if (np->tiers.count(tierpool_id) || ntp->is_tier()) {
      wait_for_finished_proposal(op, new C_RetryMessage(this, op));
      return true;
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
    cmd_getval(g_ceph_context, cmdmap, "pool", poolstr);
    int64_t pool_id = osdmap.lookup_pg_pool_name(poolstr);
    if (pool_id < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply;
    }
    string tierpoolstr;
    cmd_getval(g_ceph_context, cmdmap, "tierpool", tierpoolstr);
    int64_t tierpool_id = osdmap.lookup_pg_pool_name(tierpoolstr);
    if (tierpool_id < 0) {
      ss << "unrecognized pool '" << tierpoolstr << "'";
      err = -ENOENT;
      goto reply;
    }
    const pg_pool_t *p = osdmap.get_pg_pool(pool_id);
    assert(p);
    const pg_pool_t *tp = osdmap.get_pg_pool(tierpool_id);
    assert(tp);

    if (!_check_remove_tier(pool_id, p, tp, &err, &ss)) {
      goto reply;
    }

    if (p->tiers.count(tierpool_id) == 0) {
      ss << "pool '" << tierpoolstr << "' is now (or already was) not a tier of '" << poolstr << "'";
      err = 0;
      goto reply;
    }
    if (tp->tier_of != pool_id) {
      ss << "tier pool '" << tierpoolstr << "' is a tier of '"
         << osdmap.get_pool_name(tp->tier_of) << "': "
         // be scary about it; this is an inconsistency and bells must go off
         << "THIS SHOULD NOT HAVE HAPPENED AT ALL";
      err = -EINVAL;
      goto reply;
    }
    if (p->read_tier == tierpool_id) {
      ss << "tier pool '" << tierpoolstr << "' is the overlay for '" << poolstr << "'; please remove-overlay first";
      err = -EBUSY;
      goto reply;
    }
    // go
    pg_pool_t *np = pending_inc.get_new_pool(pool_id, p);
    pg_pool_t *ntp = pending_inc.get_new_pool(tierpool_id, tp);
    if (np->tiers.count(tierpool_id) == 0 ||
	ntp->tier_of != pool_id ||
	np->read_tier == tierpool_id) {
      wait_for_finished_proposal(op, new C_RetryMessage(this, op));
      return true;
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
      goto reply;
    string poolstr;
    cmd_getval(g_ceph_context, cmdmap, "pool", poolstr);
    int64_t pool_id = osdmap.lookup_pg_pool_name(poolstr);
    if (pool_id < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply;
    }
    string overlaypoolstr;
    cmd_getval(g_ceph_context, cmdmap, "overlaypool", overlaypoolstr);
    int64_t overlaypool_id = osdmap.lookup_pg_pool_name(overlaypoolstr);
    if (overlaypool_id < 0) {
      ss << "unrecognized pool '" << overlaypoolstr << "'";
      err = -ENOENT;
      goto reply;
    }
    const pg_pool_t *p = osdmap.get_pg_pool(pool_id);
    assert(p);
    const pg_pool_t *overlay_p = osdmap.get_pg_pool(overlaypool_id);
    assert(overlay_p);
    if (p->tiers.count(overlaypool_id) == 0) {
      ss << "tier pool '" << overlaypoolstr << "' is not a tier of '" << poolstr << "'";
      err = -EINVAL;
      goto reply;
    }
    if (p->read_tier == overlaypool_id) {
      err = 0;
      ss << "overlay for '" << poolstr << "' is now (or already was) '" << overlaypoolstr << "'";
      goto reply;
    }
    if (p->has_read_tier()) {
      ss << "pool '" << poolstr << "' has overlay '"
	 << osdmap.get_pool_name(p->read_tier)
	 << "'; please remove-overlay first";
      err = -EINVAL;
      goto reply;
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
    cmd_getval(g_ceph_context, cmdmap, "pool", poolstr);
    int64_t pool_id = osdmap.lookup_pg_pool_name(poolstr);
    if (pool_id < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply;
    }
    const pg_pool_t *p = osdmap.get_pg_pool(pool_id);
    assert(p);
    if (!p->has_read_tier()) {
      err = 0;
      ss << "there is now (or already was) no overlay for '" << poolstr << "'";
      goto reply;
    }

    if (!_check_remove_tier(pool_id, p, NULL, &err, &ss)) {
      goto reply;
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
      goto reply;
    string poolstr;
    cmd_getval(g_ceph_context, cmdmap, "pool", poolstr);
    int64_t pool_id = osdmap.lookup_pg_pool_name(poolstr);
    if (pool_id < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply;
    }
    const pg_pool_t *p = osdmap.get_pg_pool(pool_id);
    assert(p);
    if (!p->is_tier()) {
      ss << "pool '" << poolstr << "' is not a tier";
      err = -EINVAL;
      goto reply;
    }
    string modestr;
    cmd_getval(g_ceph_context, cmdmap, "mode", modestr);
    pg_pool_t::cache_mode_t mode = pg_pool_t::get_cache_mode_from_str(modestr);
    if (mode < 0) {
      ss << "'" << modestr << "' is not a valid cache mode";
      err = -EINVAL;
      goto reply;
    }

    string sure;
    cmd_getval(g_ceph_context, cmdmap, "sure", sure);
    if ((mode != pg_pool_t::CACHEMODE_WRITEBACK &&
	 mode != pg_pool_t::CACHEMODE_NONE &&
	 mode != pg_pool_t::CACHEMODE_PROXY &&
	 mode != pg_pool_t::CACHEMODE_READPROXY) &&
	sure != "--yes-i-really-mean-it") {
      ss << "'" << modestr << "' is not a well-supported cache mode and may "
	 << "corrupt your data.  pass --yes-i-really-mean-it to force.";
      err = -EPERM;
      goto reply;
    }

    // pool already has this cache-mode set and there are no pending changes
    if (p->cache_mode == mode &&
	(pending_inc.new_pools.count(pool_id) == 0 ||
	 pending_inc.new_pools[pool_id].cache_mode == p->cache_mode)) {
      ss << "set cache-mode for pool '" << poolstr << "'"
         << " to " << pg_pool_t::get_cache_mode_name(mode);
      err = 0;
      goto reply;
    }

    /* Mode description:
     *
     *  none:       No cache-mode defined
     *  forward:    Forward all reads and writes to base pool
     *  writeback:  Cache writes, promote reads from base pool
     *  readonly:   Forward writes to base pool
     *  readforward: Writes are in writeback mode, Reads are in forward mode
     *  proxy:       Proxy all reads and writes to base pool
     *  readproxy:   Writes are in writeback mode, Reads are in proxy mode
     *
     * Hence, these are the allowed transitions:
     *
     *  none -> any
     *  forward -> proxy || readforward || readproxy || writeback || any IF num_objects_dirty == 0
     *  proxy -> forward || readforward || readproxy || writeback || any IF num_objects_dirty == 0
     *  readforward -> forward || proxy || readproxy || writeback || any IF num_objects_dirty == 0
     *  readproxy -> forward || proxy || readforward || writeback || any IF num_objects_dirty == 0
     *  writeback -> readforward || readproxy || forward || proxy
     *  readonly -> any
     */

    // We check if the transition is valid against the current pool mode, as
    // it is the only committed state thus far.  We will blantly squash
    // whatever mode is on the pending state.

    if (p->cache_mode == pg_pool_t::CACHEMODE_WRITEBACK &&
        (mode != pg_pool_t::CACHEMODE_FORWARD &&
	  mode != pg_pool_t::CACHEMODE_PROXY &&
	  mode != pg_pool_t::CACHEMODE_READFORWARD &&
	  mode != pg_pool_t::CACHEMODE_READPROXY)) {
      ss << "unable to set cache-mode '" << pg_pool_t::get_cache_mode_name(mode)
         << "' on a '" << pg_pool_t::get_cache_mode_name(p->cache_mode)
         << "' pool; only '"
         << pg_pool_t::get_cache_mode_name(pg_pool_t::CACHEMODE_FORWARD)
	 << "','"
         << pg_pool_t::get_cache_mode_name(pg_pool_t::CACHEMODE_PROXY)
	 << "','"
         << pg_pool_t::get_cache_mode_name(pg_pool_t::CACHEMODE_READFORWARD)
	 << "','"
         << pg_pool_t::get_cache_mode_name(pg_pool_t::CACHEMODE_READPROXY)
        << "' allowed.";
      err = -EINVAL;
      goto reply;
    }
    if ((p->cache_mode == pg_pool_t::CACHEMODE_READFORWARD &&
        (mode != pg_pool_t::CACHEMODE_WRITEBACK &&
	  mode != pg_pool_t::CACHEMODE_FORWARD &&
	  mode != pg_pool_t::CACHEMODE_PROXY &&
	  mode != pg_pool_t::CACHEMODE_READPROXY)) ||

        (p->cache_mode == pg_pool_t::CACHEMODE_READPROXY &&
        (mode != pg_pool_t::CACHEMODE_WRITEBACK &&
	  mode != pg_pool_t::CACHEMODE_FORWARD &&
	  mode != pg_pool_t::CACHEMODE_READFORWARD &&
	  mode != pg_pool_t::CACHEMODE_PROXY)) ||

        (p->cache_mode == pg_pool_t::CACHEMODE_PROXY &&
        (mode != pg_pool_t::CACHEMODE_WRITEBACK &&
	  mode != pg_pool_t::CACHEMODE_FORWARD &&
	  mode != pg_pool_t::CACHEMODE_READFORWARD &&
	  mode != pg_pool_t::CACHEMODE_READPROXY)) ||

        (p->cache_mode == pg_pool_t::CACHEMODE_FORWARD &&
        (mode != pg_pool_t::CACHEMODE_WRITEBACK &&
	  mode != pg_pool_t::CACHEMODE_READFORWARD &&
	  mode != pg_pool_t::CACHEMODE_PROXY &&
	  mode != pg_pool_t::CACHEMODE_READPROXY))) {

      const pool_stat_t* pstats =
        mon->pgservice->get_pool_stat(pool_id);

      if (pstats && pstats->stats.sum.num_objects_dirty > 0) {
        ss << "unable to set cache-mode '"
           << pg_pool_t::get_cache_mode_name(mode) << "' on pool '" << poolstr
           << "': dirty objects found";
        err = -EBUSY;
        goto reply;
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
      assert(base_pool);
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
      goto reply;
    string poolstr;
    cmd_getval(g_ceph_context, cmdmap, "pool", poolstr);
    int64_t pool_id = osdmap.lookup_pg_pool_name(poolstr);
    if (pool_id < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply;
    }
    string tierpoolstr;
    cmd_getval(g_ceph_context, cmdmap, "tierpool", tierpoolstr);
    int64_t tierpool_id = osdmap.lookup_pg_pool_name(tierpoolstr);
    if (tierpool_id < 0) {
      ss << "unrecognized pool '" << tierpoolstr << "'";
      err = -ENOENT;
      goto reply;
    }
    const pg_pool_t *p = osdmap.get_pg_pool(pool_id);
    assert(p);
    const pg_pool_t *tp = osdmap.get_pg_pool(tierpool_id);
    assert(tp);

    if (!_check_become_tier(tierpool_id, tp, pool_id, p, &err, &ss)) {
      goto reply;
    }

    int64_t size = 0;
    if (!cmd_getval(g_ceph_context, cmdmap, "size", size)) {
      ss << "unable to parse 'size' value '"
         << cmd_vartype_stringify(cmdmap["size"]) << "'";
      err = -EINVAL;
      goto reply;
    }
    // make sure new tier is empty
    const pool_stat_t *pstats =
      mon->pgservice->get_pool_stat(tierpool_id);
    if (pstats && pstats->stats.sum.num_objects != 0) {
      ss << "tier pool '" << tierpoolstr << "' is not empty";
      err = -ENOTEMPTY;
      goto reply;
    }
    string modestr = g_conf->osd_tier_default_cache_mode;
    pg_pool_t::cache_mode_t mode = pg_pool_t::get_cache_mode_from_str(modestr);
    if (mode < 0) {
      ss << "osd tier cache default mode '" << modestr << "' is not a valid cache mode";
      err = -EINVAL;
      goto reply;
    }
    HitSet::Params hsp;
    if (g_conf->osd_tier_default_cache_hit_set_type == "bloom") {
      BloomHitSet::Params *bsp = new BloomHitSet::Params;
      bsp->set_fpp(g_conf->osd_pool_default_hit_set_bloom_fpp);
      hsp = HitSet::Params(bsp);
    } else if (g_conf->osd_tier_default_cache_hit_set_type == "explicit_hash") {
      hsp = HitSet::Params(new ExplicitHashHitSet::Params);
    }
    else if (g_conf->osd_tier_default_cache_hit_set_type == "explicit_object") {
      hsp = HitSet::Params(new ExplicitObjectHitSet::Params);
    } else {
      ss << "osd tier cache default hit set type '" <<
	g_conf->osd_tier_default_cache_hit_set_type << "' is not a known type";
      err = -EINVAL;
      goto reply;
    }
    // go
    pg_pool_t *np = pending_inc.get_new_pool(pool_id, p);
    pg_pool_t *ntp = pending_inc.get_new_pool(tierpool_id, tp);
    if (np->tiers.count(tierpool_id) || ntp->is_tier()) {
      wait_for_finished_proposal(op, new C_RetryMessage(this, op));
      return true;
    }
    np->tiers.insert(tierpool_id);
    np->read_tier = np->write_tier = tierpool_id;
    np->set_snap_epoch(pending_inc.epoch); // tier will update to our snap info
    np->set_last_force_op_resend(pending_inc.epoch);
    ntp->set_last_force_op_resend(pending_inc.epoch);
    ntp->tier_of = pool_id;
    ntp->cache_mode = mode;
    ntp->hit_set_count = g_conf->osd_tier_default_cache_hit_set_count;
    ntp->hit_set_period = g_conf->osd_tier_default_cache_hit_set_period;
    ntp->min_read_recency_for_promote = g_conf->osd_tier_default_cache_min_read_recency_for_promote;
    ntp->min_write_recency_for_promote = g_conf->osd_tier_default_cache_min_write_recency_for_promote;
    ntp->hit_set_grade_decay_rate = g_conf->osd_tier_default_cache_hit_set_grade_decay_rate;
    ntp->hit_set_search_last_n = g_conf->osd_tier_default_cache_hit_set_search_last_n;
    ntp->hit_set_params = hsp;
    ntp->target_max_bytes = size;
    ss << "pool '" << tierpoolstr << "' is now (or already was) a cache tier of '" << poolstr << "'";
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, ss.str(),
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd pool set-quota") {
    string poolstr;
    cmd_getval(g_ceph_context, cmdmap, "pool", poolstr);
    int64_t pool_id = osdmap.lookup_pg_pool_name(poolstr);
    if (pool_id < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply;
    }

    string field;
    cmd_getval(g_ceph_context, cmdmap, "field", field);
    if (field != "max_objects" && field != "max_bytes") {
      ss << "unrecognized field '" << field << "'; should be 'max_bytes' or 'max_objects'";
      err = -EINVAL;
      goto reply;
    }

    // val could contain unit designations, so we treat as a string
    string val;
    cmd_getval(g_ceph_context, cmdmap, "val", val);
    stringstream tss;
    int64_t value = unit_to_bytesize(val, &tss);
    if (value < 0) {
      ss << "error parsing value '" << value << "': " << tss.str();
      err = value;
      goto reply;
    }

    pg_pool_t *pi = pending_inc.get_new_pool(pool_id, osdmap.get_pg_pool(pool_id));
    if (field == "max_objects") {
      pi->quota_max_objects = value;
    } else if (field == "max_bytes") {
      pi->quota_max_bytes = value;
    } else {
      assert(0 == "unrecognized option");
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
    if (err == -EAGAIN)
      goto wait;
    if (err < 0)
      goto reply;

    getline(ss, rs);
    wait_for_finished_proposal(
      op, new Monitor::C_Command(mon, op, 0, rs, get_last_committed() + 1));
    return true;
  } else if (prefix == "osd reweight-by-pg" ||
	     prefix == "osd reweight-by-utilization" ||
	     prefix == "osd test-reweight-by-pg" ||
	     prefix == "osd test-reweight-by-utilization") {
    bool by_pg =
      prefix == "osd reweight-by-pg" || prefix == "osd test-reweight-by-pg";
    bool dry_run =
      prefix == "osd test-reweight-by-pg" ||
      prefix == "osd test-reweight-by-utilization";
    int64_t oload;
    cmd_getval(g_ceph_context, cmdmap, "oload", oload, int64_t(120));
    set<int64_t> pools;
    vector<string> poolnamevec;
    cmd_getval(g_ceph_context, cmdmap, "pools", poolnamevec);
    for (unsigned j = 0; j < poolnamevec.size(); j++) {
      int64_t pool = osdmap.lookup_pg_pool_name(poolnamevec[j]);
      if (pool < 0) {
	ss << "pool '" << poolnamevec[j] << "' does not exist";
	err = -ENOENT;
	goto reply;
      }
      pools.insert(pool);
    }
    double max_change = g_conf->mon_reweight_max_change;
    cmd_getval(g_ceph_context, cmdmap, "max_change", max_change);
    if (max_change <= 0.0) {
      ss << "max_change " << max_change << " must be positive";
      err = -EINVAL;
      goto reply;
    }
    int64_t max_osds = g_conf->mon_reweight_max_osds;
    cmd_getval(g_ceph_context, cmdmap, "max_osds", max_osds);
    if (max_osds <= 0) {
      ss << "max_osds " << max_osds << " must be positive";
      err = -EINVAL;
      goto reply;
    }
    string no_increasing;
    cmd_getval(g_ceph_context, cmdmap, "no_increasing", no_increasing);
    string out_str;
    mempool::osdmap::map<int32_t, uint32_t> new_weights;
    err = mon->pgservice->reweight_by_utilization(osdmap,
					     oload,
					     max_change,
					     max_osds,
					     by_pg,
					     pools.empty() ? NULL : &pools,
					     no_increasing == "--no-increasing",
					     &new_weights,
					     &ss, &out_str, f.get());
    if (err >= 0) {
      dout(10) << "reweight::by_utilization: finished with " << out_str << dendl;
    }
    if (f)
      f->flush(rdata);
    else
      rdata.append(out_str);
    if (err < 0) {
      ss << "FAILED reweight-by-pg";
    } else if (err == 0 || dry_run) {
      ss << "no change";
    } else {
      ss << "SUCCESSFUL reweight-by-pg";
      pending_inc.new_weight = std::move(new_weights);
      wait_for_finished_proposal(
	op,
	new Monitor::C_Command(mon, op, 0, rs, rdata, get_last_committed() + 1));
      return true;
    }
  } else if (prefix == "osd force-create-pg") {
    pg_t pgid;
    string pgidstr;
    cmd_getval(g_ceph_context, cmdmap, "pgid", pgidstr);
    if (!pgid.parse(pgidstr.c_str())) {
      ss << "invalid pgid '" << pgidstr << "'";
      err = -EINVAL;
      goto reply;
    }
    bool creating_now;
    {
      std::lock_guard<std::mutex> l(creating_pgs_lock);
      auto emplaced = creating_pgs.pgs.emplace(pgid,
					       make_pair(osdmap.get_epoch(),
							 ceph_clock_now()));
      creating_now = emplaced.second;
    }
    if (creating_now) {
      ss << "pg " << pgidstr << " now creating, ok";
      err = 0;
      goto update;
    } else {
      ss << "pg " << pgid << " already creating";
      err = 0;
      goto reply;
    }
  } else {
    err = -EINVAL;
  }

 reply:
  getline(ss, rs);
  if (err < 0 && rs.length() == 0)
    rs = cpp_strerror(err);
  mon->reply_command(op, err, rs, rdata, get_last_committed());
  return ret;

 update:
  getline(ss, rs);
  wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
					    get_last_committed() + 1));
  return true;

 wait:
  wait_for_finished_proposal(op, new C_RetryMessage(this, op));
  return true;
}

bool OSDMonitor::preprocess_pool_op(MonOpRequestRef op) 
{
  op->mark_osdmon_event(__func__);
  MPoolOp *m = static_cast<MPoolOp*>(op->get_req());
  
  if (m->fsid != mon->monmap->fsid) {
    dout(0) << __func__ << " drop message on fsid " << m->fsid
            << " != " << mon->monmap->fsid << " for " << *m << dendl;
    _pool_op_reply(op, -EINVAL, osdmap.get_epoch());
    return true;
  }

  if (m->op == POOL_OP_CREATE)
    return preprocess_pool_op_create(op);

  if (!osdmap.get_pg_pool(m->pool)) {
    dout(10) << "attempt to operate on non-existent pool id " << m->pool << dendl;
    _pool_op_reply(op, 0, osdmap.get_epoch());
    return true;
  }

  // check if the snap and snapname exist
  bool snap_exists = false;
  const pg_pool_t *p = osdmap.get_pg_pool(m->pool);
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
    if (p->is_removed_snap(m->snapid)) {
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

bool OSDMonitor::preprocess_pool_op_create(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  MPoolOp *m = static_cast<MPoolOp*>(op->get_req());
  MonSession *session = m->get_session();
  if (!session) {
    _pool_op_reply(op, -EPERM, osdmap.get_epoch());
    return true;
  }
  if (!session->is_capable("osd", MON_CAP_W)) {
    dout(5) << "attempt to create new pool without sufficient auid privileges!"
	    << "message: " << *m  << std::endl
	    << "caps: " << session->caps << dendl;
    _pool_op_reply(op, -EPERM, osdmap.get_epoch());
    return true;
  }

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
  MPoolOp *m = static_cast<MPoolOp*>(op->get_req());
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
      dout(10) << "create snap in pool " << m->pool << " " << m->name << " seq " << pp.get_snap_epoch() << dendl;
      changed = true;
    }
    break;

  case POOL_OP_DELETE_SNAP:
    {
      snapid_t s = pp.snap_exists(m->name.c_str());
      if (s) {
	pp.remove_snap(s);
	changed = true;
      }
    }
    break;

  case POOL_OP_CREATE_UNMANAGED_SNAP:
    {
      uint64_t snapid;
      pp.add_unmanaged_snap(snapid);
      ::encode(snapid, reply_data);
      changed = true;
    }
    break;

  case POOL_OP_DELETE_UNMANAGED_SNAP:
    if (!pp.is_removed_snap(m->snapid)) {
      pp.remove_unmanaged_snap(m->snapid);
      changed = true;
    }
    break;

  case POOL_OP_AUID_CHANGE:
    if (pp.auid != m->auid) {
      pp.auid = m->auid;
      changed = true;
    }
    break;

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
  FSMap const &pending_fsmap = mon->mdsmon()->get_pending();
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

  if (!g_conf->mon_allow_pool_delete) {
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

  const FSMap &pending_fsmap = mon->mdsmon()->get_pending();
  if (pending_fsmap.pool_in_use(tier_pool_id)) {
    *ss << "pool '" << tier_pool_name << "' is in use by CephFS";
    *err = -EBUSY;
    return false;
  }

  if (base_pool->tiers.count(tier_pool_id)) {
    assert(tier_pool->tier_of == base_pool_id);
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
  const FSMap &pending_fsmap = mon->mdsmon()->get_pending();
  if (pending_fsmap.pool_in_use(base_pool_id)) {
    if (base_pool->type != pg_pool_t::TYPE_REPLICATED) {
      // If the underlying pool is erasure coded, we can't permit the
      // removal of the replicated tier that CephFS relies on to access it
      *ss << "pool '" << base_pool_name << "' is in use by CephFS via its tier";
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

  if (g_conf->mon_fake_pool_delete && !no_fake) {
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
    if (p->first.pool() == (uint64_t)pool) {
      dout(10) << __func__ << " " << pool << " removing obsolete pg_temp "
	       << p->first << dendl;
      pending_inc.new_pg_temp[p->first].clear();
    }
  }
  // remove any primary_temp mappings for this pool
  for (auto p = osdmap.primary_temp->begin();
      p != osdmap.primary_temp->end();
      ++p) {
    if (p->first.pool() == (uint64_t)pool) {
      dout(10) << __func__ << " " << pool
               << " removing obsolete primary_temp" << p->first << dendl;
      pending_inc.new_primary_temp[p->first] = -1;
    }
  }
  // remove any pg_upmap mappings for this pool
  for (auto& p : osdmap.pg_upmap) {
    if (p.first.pool() == (uint64_t)pool) {
      dout(10) << __func__ << " " << pool
               << " removing obsolete pg_upmap "
               << p.first << dendl;
      pending_inc.old_pg_upmap.insert(p.first);
    }
  }
  // remove any pg_upmap_items mappings for this pool
  for (auto& p : osdmap.pg_upmap_items) {
    if (p.first.pool() == (uint64_t)pool) {
      dout(10) << __func__ << " " << pool
               << " removing obsolete pg_upmap_items " << p.first
               << dendl;
      pending_inc.old_pg_upmap_items.insert(p.first);
    }
  }

  // remove any choose_args for this pool
  CrushWrapper newcrush;
  _get_pending_crush(newcrush);
  if (newcrush.have_choose_args(pool)) {
    dout(10) << __func__ << " removing choose_args for pool " << pool << dendl;
    newcrush.rm_choose_args(pool);
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush, mon->get_quorum_con_features());
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
  MPoolOp *m = static_cast<MPoolOp*>(op->get_req());
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
  MPoolOp *m = static_cast<MPoolOp*>(op->get_req());
  dout(20) << "_pool_op_reply " << ret << dendl;
  MPoolOpReply *reply = new MPoolOpReply(m->fsid, m->get_tid(),
					 ret, epoch, get_last_committed(), blp);
  mon->send_reply(op, reply);
}
