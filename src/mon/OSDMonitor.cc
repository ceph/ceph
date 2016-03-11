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
#include <sstream>
#include <boost/assign.hpp>

#include "OSDMonitor.h"
#include "Monitor.h"
#include "MDSMonitor.h"
#include "PGMonitor.h"

#include "MonitorDBStore.h"

#include "crush/CrushWrapper.h"
#include "crush/CrushTester.h"
#include "crush/CrushTreeDumper.h"

#include "messages/MOSDFailure.h"
#include "messages/MOSDMarkMeDown.h"
#include "messages/MOSDMap.h"
#include "messages/MMonGetOSDMap.h"
#include "messages/MOSDBoot.h"
#include "messages/MOSDAlive.h"
#include "messages/MPoolOp.h"
#include "messages/MPoolOpReply.h"
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

#include "include/compat.h"
#include "include/assert.h"
#include "include/stringify.h"
#include "include/util.h"
#include "common/cmdparse.h"
#include "include/str_list.h"
#include "include/str_map.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, osdmap)
static ostream& _prefix(std::ostream *_dout, Monitor *mon, OSDMap& osdmap) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name()
		<< ").osd e" << osdmap.get_epoch() << " ";
}

OSDMonitor::OSDMonitor(CephContext *cct, Monitor *mn, Paxos *p, const string& service_name)
 : PaxosService(mn, p, service_name),
   inc_osd_cache(g_conf->mon_osd_cache_size),
   full_osd_cache(g_conf->mon_osd_cache_size),
   thrash_map(0), thrash_last_up_osd(-1),
   op_tracker(cct, true, 1)
{}

bool OSDMonitor::_have_pending_crush()
{
  return pending_inc.crush.length();
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
    osdmap.crush->encode(bl);

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
    newmap.build_simple(g_ceph_context, 0, mon->monmap->fsid, 0,
			g_conf->osd_pg_bits, g_conf->osd_pgp_bits);
  }
  newmap.set_epoch(1);
  newmap.created = newmap.modified = ceph_clock_now(g_ceph_context);

  // new clusters should sort bitwise by default.
  newmap.set_flag(CEPH_OSDMAP_SORTBITWISE);

  // encode into pending incremental
  newmap.encode(pending_inc.fullmap, mon->quorum_features | CEPH_FEATURE_RESERVED);
  pending_inc.full_crc = newmap.get_crc();
  dout(20) << " full crc " << pending_inc.full_crc << dendl;
}

void OSDMonitor::update_from_paxos(bool *need_bootstrap)
{
  version_t version = get_last_committed();
  if (version == osdmap.epoch)
    return;
  assert(version >= osdmap.epoch);

  dout(15) << "update_from_paxos paxos e " << version
	   << ", my e " << osdmap.epoch << dendl;


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

  if (latest_full > 0) {
    // make sure we can really believe get_version_latest_full(); see
    // 76cd7ac1c2094b34ad36bea89b2246fa90eb2f6d
    bufferlist test;
    get_version_full(latest_full, test);
    if (test.length() == 0) {
      dout(10) << __func__ << " ignoring recorded latest_full as it is missing; fallback to search" << dendl;
      latest_full = 0;
    }
  }
  if (get_first_committed() > 1 &&
      latest_full < get_first_committed()) {
    /* a bug introduced in 7fb3804fb860dcd0340dd3f7c39eec4315f8e4b6 would lead
     * us to not update the on-disk latest_full key.  Upon trim, the actual
     * version would cease to exist but we would still point to it.  This
     * makes sure we get it pointing to a proper version.
     */
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

    // if we trigger this, then there's something else going with the store
    // state, and we shouldn't want to work around it without knowing what
    // exactly happened.
    assert(latest_full > 0);
    MonitorDBStore::TransactionRef t(new MonitorDBStore::Transaction);
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

  // walk through incrementals
  MonitorDBStore::TransactionRef t;
  size_t tx_size = 0;
  while (version > osdmap.epoch) {
    bufferlist inc_bl;
    int err = get_version(osdmap.epoch+1, inc_bl);
    assert(err == 0);
    assert(inc_bl.length());

    dout(7) << "update_from_paxos  applying incremental " << osdmap.epoch+1 << dendl;
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
      f = mon->quorum_features;
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

    if (tx_size > g_conf->mon_sync_max_payload_size*2) {
      mon->store->apply_transaction(t);
      t = MonitorDBStore::TransactionRef();
      tx_size = 0;
    }
  }

  if (t) {
    mon->store->apply_transaction(t);
  }

  for (int o = 0; o < osdmap.get_max_osd(); o++) {
    if (osdmap.is_down(o)) {
      // populate down -> out map
      if (osdmap.is_in(o) &&
	  down_pending_out.count(o) == 0) {
	dout(10) << " adding osd." << o << " to down_pending_out map" << dendl;
	down_pending_out[o] = ceph_clock_now(g_ceph_context);
      }
    }
  }
  // XXX: need to trim MonSession connected with a osd whose id > max_osd?

  /** we don't have any of the feature bit infrastructure in place for
   * supporting primary_temp mappings without breaking old clients/OSDs.*/
  assert(g_conf->mon_osd_allow_primary_temp || osdmap.primary_temp->empty());

  if (mon->is_leader()) {
    // kick pgmon, make sure it's seen the latest map
    mon->pgmon()->check_osd_map(osdmap.epoch);
  }

  check_subs();

  share_map_with_random_osd();
  update_logger();

  process_failures();

  // make sure our feature bits reflect the latest map
  update_msgr_features();
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

bool OSDMonitor::thrash()
{
  if (!thrash_map)
    return false;

  thrash_map--;
  int o;

  // mark a random osd up_thru..
  if (rand() % 4 == 0 || thrash_last_up_osd < 0)
    o = rand() % osdmap.get_num_osds();
  else
    o = thrash_last_up_osd;
  if (osdmap.is_up(o)) {
    dout(5) << "thrash_map osd." << o << " up_thru" << dendl;
    pending_inc.new_up_thru[o] = osdmap.get_epoch();
  }

  // mark a random osd up/down
  o = rand() % osdmap.get_num_osds();
  if (osdmap.is_up(o)) {
    dout(5) << "thrash_map osd." << o << " down" << dendl;
    pending_inc.new_state[o] = CEPH_OSD_UP;
  } else if (osdmap.exists(o)) {
    dout(5) << "thrash_map osd." << o << " up" << dendl;
    pending_inc.new_state[o] = CEPH_OSD_UP;
    pending_inc.new_up_client[o] = entity_addr_t();
    pending_inc.new_up_cluster[o] = entity_addr_t();
    pending_inc.new_hb_back_up[o] = entity_addr_t();
    pending_inc.new_weight[o] = CEPH_OSD_IN;
    thrash_last_up_osd = o;
  }

  // mark a random osd in
  o = rand() % osdmap.get_num_osds();
  if (osdmap.exists(o)) {
    dout(5) << "thrash_map osd." << o << " in" << dendl;
    pending_inc.new_weight[o] = CEPH_OSD_IN;
  }

  // mark a random osd out
  o = rand() % osdmap.get_num_osds();
  if (osdmap.exists(o)) {
    dout(5) << "thrash_map osd." << o << " out" << dendl;
    pending_inc.new_weight[o] = CEPH_OSD_OUT;
  }

  // generate some pg_temp entries.
  // let's assume the ceph::unordered_map iterates in a random-ish order.
  int n = rand() % mon->pgmon()->pg_map.pg_stat.size();
  ceph::unordered_map<pg_t,pg_stat_t>::iterator p = mon->pgmon()->pg_map.pg_stat.begin();
  ceph::unordered_map<pg_t,pg_stat_t>::iterator e = mon->pgmon()->pg_map.pg_stat.end();
  while (n--)
    ++p;
  for (int i=0; i<50; i++) {
    unsigned size = osdmap.get_pg_size(p->first);
    vector<int> v;
    bool have_real_osd = false;
    for (int j=0; j < (int)size; j++) {
      o = rand() % osdmap.get_num_osds();
      if (osdmap.exists(o) && std::find(v.begin(), v.end(), o) == v.end()) {
	have_real_osd = true;
	v.push_back(o);
      }
    }
    for (vector<int>::iterator q = p->second.acting.begin();
	 q != p->second.acting.end() && v.size() < size;
	 ++q) {
      if (std::find(v.begin(), v.end(), *q) == v.end()) {
	if (*q != CRUSH_ITEM_NONE)
	  have_real_osd = true;
	v.push_back(*q);
      }
    }
    if (osdmap.pg_is_ec(p->first)) {
      while (v.size() < size)
	v.push_back(CRUSH_ITEM_NONE);
    }
    if (!v.empty() && have_real_osd)
      pending_inc.new_pg_temp[p->first] = v;
    dout(5) << "thrash_map pg " << p->first << " pg_temp remapped to " << v << dendl;

    ++p;
    if (p == e)
      p = mon->pgmon()->pg_map.pg_stat.begin();
  }
  return true;
}

void OSDMonitor::on_active()
{
  update_logger();

  if (thrash_map) {
    if (mon->is_leader()) {
      if (thrash())
	propose_pending();
    } else {
      thrash_map = 0;
    }
  }

  if (mon->is_leader())
    mon->clog->info() << "osdmap " << osdmap << "\n";

  if (!mon->is_leader()) {
    list<MonOpRequestRef> ls;
    take_all_failures(ls);
    while (!ls.empty()) {
      MonOpRequestRef op = ls.front();
      op->mark_osdmon_event(__func__);
      dispatch(op);
      ls.pop_front();
    }
  }
}

void OSDMonitor::on_shutdown()
{
  dout(10) << __func__ << dendl;

  // discard failure info, waiters
  list<MonOpRequestRef> ls;
  take_all_failures(ls);
  while (!ls.empty()) {
    ls.pop_front();
  }
}

void OSDMonitor::update_logger()
{
  dout(10) << "update_logger" << dendl;

  mon->cluster_logger->set(l_cluster_num_osd, osdmap.get_num_osds());
  mon->cluster_logger->set(l_cluster_num_osd_up, osdmap.get_num_up_osds());
  mon->cluster_logger->set(l_cluster_num_osd_in, osdmap.get_num_in_osds());
  mon->cluster_logger->set(l_cluster_osd_epoch, osdmap.get_epoch());
}

/* Assign a lower weight to overloaded OSDs.
 *
 * The osds that will get a lower weight are those with with a utilization
 * percentage 'oload' percent greater than the average utilization.
 */
int OSDMonitor::reweight_by_utilization(int oload, std::string& out_str,
					bool by_pg, const set<int64_t> *pools)
{
  if (oload <= 100) {
    ostringstream oss;
    oss << "You must give a percentage higher than 100. "
      "The reweighting threshold will be calculated as <average-utilization> "
      "times <input-percentage>. For example, an argument of 200 would "
      "reweight OSDs which are twice as utilized as the average OSD.\n";
    out_str = oss.str();
    return -EINVAL;
  }

  const PGMap &pgm = mon->pgmon()->pg_map;
  vector<int> pgs_by_osd(osdmap.get_max_osd());

  // Avoid putting a small number (or 0) in the denominator when calculating
  // average_util
  double average_util;
  if (by_pg) {
    // by pg mapping
    double weight_sum = 0.0;      // sum up the crush weights
    unsigned num_pg_copies = 0;
    int num_osds = 0;
    for (ceph::unordered_map<pg_t,pg_stat_t>::const_iterator p =
	   pgm.pg_stat.begin();
	 p != pgm.pg_stat.end();
	 ++p) {
      if (pools && pools->count(p->first.pool()) == 0)
	continue;
      for (vector<int>::const_iterator q = p->second.acting.begin();
	   q != p->second.acting.end();
	   ++q) {
	if (*q >= (int)pgs_by_osd.size())
	  pgs_by_osd.resize(*q);
	if (pgs_by_osd[*q] == 0) {
	  weight_sum += osdmap.crush->get_item_weightf(*q);
	  ++num_osds;
	}
	++pgs_by_osd[*q];
	++num_pg_copies;
      }
    }

    if (!num_osds || (num_pg_copies / num_osds < g_conf->mon_reweight_min_pgs_per_osd)) {
      ostringstream oss;
      oss << "Refusing to reweight: we only have " << num_pg_copies
	  << " PGs across " << num_osds << " osds!\n";
      out_str = oss.str();
      return -EDOM;
    }

    average_util = (double)num_pg_copies / weight_sum;
  } else {
    // by osd utilization
    int num_osd = MIN(1, pgm.osd_stat.size());
    if ((uint64_t)pgm.osd_sum.kb * 1024 / num_osd
	< g_conf->mon_reweight_min_bytes_per_osd) {
      ostringstream oss;
      oss << "Refusing to reweight: we only have " << pgm.osd_sum.kb
	  << " kb across all osds!\n";
      out_str = oss.str();
      return -EDOM;
    }
    if ((uint64_t)pgm.osd_sum.kb_used * 1024 / num_osd
	< g_conf->mon_reweight_min_bytes_per_osd) {
      ostringstream oss;
      oss << "Refusing to reweight: we only have " << pgm.osd_sum.kb_used
	  << " kb used across all osds!\n";
      out_str = oss.str();
      return -EDOM;
    }

    average_util = (double)pgm.osd_sum.kb_used / (double)pgm.osd_sum.kb;
  }

  // adjust down only if we are above the threshold
  double overload_util = average_util * (double)oload / 100.0;

  // but aggressively adjust weights up whenever possible.
  double underload_util = average_util;

  ostringstream oss;
  char buf[128];
  snprintf(buf, sizeof(buf), "average %04f, overload %04f. ",
	   average_util, overload_util);
  oss << buf;
  std::string sep;
  oss << "reweighted: ";
  bool changed = false;
  for (ceph::unordered_map<int,osd_stat_t>::const_iterator p =
	 pgm.osd_stat.begin();
       p != pgm.osd_stat.end();
       ++p) {
    float util;
    if (by_pg) {
      util = pgs_by_osd[p->first] / osdmap.crush->get_item_weightf(p->first);
    } else {
      util = (double)p->second.kb_used / (double)p->second.kb;
    }
    if (util >= overload_util) {
      sep = ", ";
      // Assign a lower weight to overloaded OSDs. The current weight
      // is a factor to take into account the original weights,
      // to represent e.g. differing storage capacities
      unsigned weight = osdmap.get_weight(p->first);
      unsigned new_weight = (unsigned)((average_util / util) * (float)weight);
      pending_inc.new_weight[p->first] = new_weight;
      char buf[128];
      snprintf(buf, sizeof(buf), "osd.%d [%04f -> %04f]", p->first,
	       (float)weight / (float)0x10000,
	       (float)new_weight / (float)0x10000);
      oss << buf << sep;
      changed = true;
    }
    if (util <= underload_util) {
      // assign a higher weight.. if we can.
      unsigned weight = osdmap.get_weight(p->first);
      unsigned new_weight = (unsigned)((average_util / util) * (float)weight);
      if (new_weight > 0x10000)
	new_weight = 0x10000;
      if (new_weight > weight) {
	sep = ", ";
	pending_inc.new_weight[p->first] = new_weight;
	char buf[128];
	snprintf(buf, sizeof(buf), "osd.%d [%04f -> %04f]", p->first,
		 (float)weight / (float)0x10000,
		 (float)new_weight / (float)0x10000);
	oss << buf << sep;
	changed = true;
      }
    }
  }
  if (sep.empty()) {
    oss << "(none)";
  }
  out_str = oss.str();
  dout(10) << "reweight_by_utilization: finished with " << out_str << dendl;
  return changed;
}

template <typename F>
class OSDUtilizationDumper : public CrushTreeDumper::Dumper<F> {
public:
  typedef CrushTreeDumper::Dumper<F> Parent;

  OSDUtilizationDumper(const CrushWrapper *crush, const OSDMap *osdmap_,
		       const PGMap *pgm_, bool tree_) :
    Parent(crush),
    osdmap(osdmap_),
    pgm(pgm_),
    tree(tree_),
    average_util(average_utilization()),
    min_var(-1),
    max_var(-1),
    stddev(0),
    sum(0) {
  }

protected:
  void dump_stray(F *f) {
    for (int i = 0; i <= osdmap->get_max_osd(); i++) {
      if (osdmap->exists(i) && !this->is_touched(i))
	dump_item(CrushTreeDumper::Item(i, 0, 0), f);
    }
  }

  virtual void dump_item(const CrushTreeDumper::Item &qi, F *f) {
    if (!tree && qi.is_bucket())
      return;

    float reweight = qi.is_bucket() ? -1 : osdmap->get_weightf(qi.id);
    int64_t kb = 0, kb_used = 0, kb_avail = 0;
    double util = 0;
    if (get_bucket_utilization(qi.id, &kb, &kb_used, &kb_avail))
      util = 100.0 * (double)kb_used / (double)kb;
    double var = 1.0;
    if (average_util)
      var = util / average_util;

    size_t num_pgs = pgm->get_num_pg_by_osd(qi.id);

    dump_item(qi, reweight, kb, kb_used, kb_avail, util, var, num_pgs, f);

    if (!qi.is_bucket() && reweight > 0) {
      if (min_var < 0 || var < min_var)
	min_var = var;
      if (max_var < 0 || var > max_var)
	max_var = var;

      double dev = util - average_util;
      dev *= dev;
      stddev += reweight * dev;
      sum += reweight;
    }
  }

  virtual void dump_item(const CrushTreeDumper::Item &qi,
			 float &reweight,
			 int64_t kb,
			 int64_t kb_used,
			 int64_t kb_avail,
			 double& util,
			 double& var,
			 const size_t num_pgs,
			 F *f) = 0;

  double dev() {
    return sum > 0 ? sqrt(stddev / sum) : 0;
  }

  double average_utilization() {
    int64_t kb = 0, kb_used = 0;
    for (int i = 0; i <= osdmap->get_max_osd(); i++) {
      if (!osdmap->exists(i) || osdmap->get_weight(i) == 0)
	continue;
      int64_t kb_i, kb_used_i, kb_avail_i;
      if (get_osd_utilization(i, &kb_i, &kb_used_i, &kb_avail_i)) {
	kb += kb_i;
	kb_used += kb_used_i;
      }
    }
    return kb > 0 ? 100.0 * (double)kb_used / (double)kb : 0;
  }

  bool get_osd_utilization(int id, int64_t* kb, int64_t* kb_used,
			   int64_t* kb_avail) const {
    typedef ceph::unordered_map<int32_t,osd_stat_t> OsdStat;
    OsdStat::const_iterator p = pgm->osd_stat.find(id);
    if (p == pgm->osd_stat.end())
      return false;
    *kb = p->second.kb;
    *kb_used = p->second.kb_used;
    *kb_avail = p->second.kb_avail;
    return *kb > 0;
  }

  bool get_bucket_utilization(int id, int64_t* kb, int64_t* kb_used,
			      int64_t* kb_avail) const {
    if (id >= 0)
      return get_osd_utilization(id, kb, kb_used, kb_avail);

    *kb = 0;
    *kb_used = 0;
    *kb_avail = 0;

    for (int k = osdmap->crush->get_bucket_size(id) - 1; k >= 0; k--) {
      int item = osdmap->crush->get_bucket_item(id, k);
      int64_t kb_i = 0, kb_used_i = 0, kb_avail_i;
      if (!get_bucket_utilization(item, &kb_i, &kb_used_i, &kb_avail_i))
	return false;
      *kb += kb_i;
      *kb_used += kb_used_i;
      *kb_avail += kb_avail_i;
    }
    return *kb > 0;
  }

protected:
  const OSDMap *osdmap;
  const PGMap *pgm;
  bool tree;
  double average_util;
  double min_var;
  double max_var;
  double stddev;
  double sum;
};

class OSDUtilizationPlainDumper : public OSDUtilizationDumper<TextTable> {
public:
  typedef OSDUtilizationDumper<TextTable> Parent;

  OSDUtilizationPlainDumper(const CrushWrapper *crush, const OSDMap *osdmap,
		     const PGMap *pgm, bool tree) :
    Parent(crush, osdmap, pgm, tree) {}

  void dump(TextTable *tbl) {
    tbl->define_column("ID", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("WEIGHT", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("REWEIGHT", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("SIZE", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("USE", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("AVAIL", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("%USE", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("VAR", TextTable::LEFT, TextTable::RIGHT);
    tbl->define_column("PGS", TextTable::LEFT, TextTable::RIGHT);
    if (tree)
      tbl->define_column("TYPE NAME", TextTable::LEFT, TextTable::LEFT);

    Parent::dump(tbl);

    dump_stray(tbl);

    *tbl << "" << "" << "TOTAL"
	 << si_t(pgm->osd_sum.kb << 10)
	 << si_t(pgm->osd_sum.kb_used << 10)
	 << si_t(pgm->osd_sum.kb_avail << 10)
	 << lowprecision_t(average_util)
	 << ""
	 << TextTable::endrow;
  }

protected:
  struct lowprecision_t {
    float v;
    explicit lowprecision_t(float _v) : v(_v) {}
  };
  friend std::ostream &operator<<(ostream& out, const lowprecision_t& v);

  using OSDUtilizationDumper<TextTable>::dump_item;
  virtual void dump_item(const CrushTreeDumper::Item &qi,
			 float &reweight,
			 int64_t kb,
			 int64_t kb_used,
			 int64_t kb_avail,
			 double& util,
			 double& var,
			 const size_t num_pgs,
			 TextTable *tbl) {
    *tbl << qi.id
	 << weightf_t(qi.weight)
	 << weightf_t(reweight)
	 << si_t(kb << 10)
	 << si_t(kb_used << 10)
	 << si_t(kb_avail << 10)
	 << lowprecision_t(util)
	 << lowprecision_t(var)
	 << num_pgs;

    if (tree) {
      ostringstream name;
      for (int k = 0; k < qi.depth; k++)
	name << "    ";
      if (qi.is_bucket()) {
	int type = crush->get_bucket_type(qi.id);
	name << crush->get_type_name(type) << " "
	     << crush->get_item_name(qi.id);
      } else {
	name << "osd." << qi.id;
      }
      *tbl << name.str();
    }

    *tbl << TextTable::endrow;
  }

public:
  string summary() {
    ostringstream out;
    out << "MIN/MAX VAR: " << lowprecision_t(min_var)
	<< "/" << lowprecision_t(max_var) << "  "
	<< "STDDEV: " << lowprecision_t(dev());
    return out.str();
  }
};

ostream& operator<<(ostream& out,
		    const OSDUtilizationPlainDumper::lowprecision_t& v)
{
  if (v.v < -0.01) {
    return out << "-";
  } else if (v.v < 0.001) {
    return out << "0";
  } else {
    std::streamsize p = out.precision();
    return out << std::fixed << std::setprecision(2) << v.v << std::setprecision(p);
  }
}

class OSDUtilizationFormatDumper : public OSDUtilizationDumper<Formatter> {
public:
  typedef OSDUtilizationDumper<Formatter> Parent;

  OSDUtilizationFormatDumper(const CrushWrapper *crush, const OSDMap *osdmap,
			     const PGMap *pgm, bool tree) :
    Parent(crush, osdmap, pgm, tree) {}

  void dump(Formatter *f) {
    f->open_array_section("nodes");
    Parent::dump(f);
    f->close_section();

    f->open_array_section("stray");
    dump_stray(f);
    f->close_section();
  }

protected:
  using OSDUtilizationDumper<Formatter>::dump_item;
  virtual void dump_item(const CrushTreeDumper::Item &qi,
			 float &reweight,
			 int64_t kb,
			 int64_t kb_used,
			 int64_t kb_avail,
			 double& util,
			 double& var,
			 const size_t num_pgs,
			 Formatter *f) {
    f->open_object_section("item");
    CrushTreeDumper::dump_item_fields(crush, qi, f);
    f->dump_float("reweight", reweight);
    f->dump_int("kb", kb);
    f->dump_int("kb_used", kb_used);
    f->dump_int("kb_avail", kb_avail);
    f->dump_float("utilization", util);
    f->dump_float("var", var);
    f->dump_unsigned("pgs", num_pgs);
    CrushTreeDumper::dump_bucket_children(crush, qi, f);
    f->close_section();
  }

public:
  void summary(Formatter *f) {
    f->open_object_section("summary");
    f->dump_int("total_kb", pgm->osd_sum.kb);
    f->dump_int("total_kb_used", pgm->osd_sum.kb_used);
    f->dump_int("total_kb_avail", pgm->osd_sum.kb_avail);
    f->dump_float("average_utilization", average_util);
    f->dump_float("min_var", min_var);
    f->dump_float("max_var", max_var);
    f->dump_float("dev", dev());
    f->close_section();
  }
};

void OSDMonitor::print_utilization(ostream &out, Formatter *f, bool tree) const
{
  const PGMap *pgm = &mon->pgmon()->pg_map;
  const CrushWrapper *crush = osdmap.crush.get();

  if (f) {
    f->open_object_section("df");
    OSDUtilizationFormatDumper d(crush, &osdmap, pgm, tree);
    d.dump(f);
    d.summary(f);
    f->close_section();
    f->flush(out);
  } else {
    OSDUtilizationPlainDumper d(crush, &osdmap, pgm, tree);
    TextTable tbl;
    d.dump(&tbl);
    out << tbl
	<< d.summary() << "\n";
  }
}

void OSDMonitor::create_pending()
{
  pending_inc = OSDMap::Incremental(osdmap.epoch+1);
  pending_inc.fsid = mon->monmap->fsid;

  dout(10) << "create_pending e " << pending_inc.epoch << dendl;

  // drop any redundant pg_temp entries
  OSDMap::remove_redundant_temporaries(g_ceph_context, osdmap, &pending_inc);

  // drop any pg or primary_temp entries with no up entries
  OSDMap::remove_down_temps(g_ceph_context, osdmap, &pending_inc);
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
  for (map<int32_t,uint8_t>::iterator p = pending_inc.new_state.begin();
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

  OSDMap next;
  next.deepish_copy_from(osdmap);
  next.apply_incremental(pending_inc);

  PGMap *pg_map = &mon->pgmon()->pg_map;

  utime_t stop = ceph_clock_now(NULL);
  stop += g_conf->mon_osd_prime_pg_temp_max_time;
  int chunk = 1000;
  int n = chunk;

  if (all) {
    for (ceph::unordered_map<pg_t, pg_stat_t>::iterator pp =
	   pg_map->pg_stat.begin();
	 pp != pg_map->pg_stat.end();
	 ++pp) {
      prime_pg_temp(next, pp);
      if (--n <= 0) {
	n = chunk;
	if (ceph_clock_now(NULL) > stop) {
	  dout(10) << __func__ << " consumed more than "
		   << g_conf->mon_osd_prime_pg_temp_max_time
		   << " seconds, stopping"
		   << dendl;
	  break;
	}
      }
    }
  } else {
    dout(10) << __func__ << " " << osds.size() << " interesting osds" << dendl;
    for (set<int>::iterator p = osds.begin(); p != osds.end(); ++p) {
      n -= prime_pg_temp(next, pg_map, *p);
      if (--n <= 0) {
	n = chunk;
	if (ceph_clock_now(NULL) > stop) {
	  dout(10) << __func__ << " consumed more than "
		   << g_conf->mon_osd_prime_pg_temp_max_time
		   << " seconds, stopping"
		   << dendl;
	  break;
	}
      }
    }
  }
}

void OSDMonitor::prime_pg_temp(OSDMap& next,
			       ceph::unordered_map<pg_t, pg_stat_t>::iterator pp)
{
  // do not prime creating pgs
  if (pp->second.state & PG_STATE_CREATING)
    return;
  // do not touch a mapping if a change is pending
  if (pending_inc.new_pg_temp.count(pp->first))
    return;
  vector<int> up, acting;
  int up_primary, acting_primary;
  next.pg_to_up_acting_osds(pp->first, &up, &up_primary, &acting, &acting_primary);
  if (acting == pp->second.acting)
    return;  // no change since last pg update, skip
  vector<int> cur_up, cur_acting;
  osdmap.pg_to_up_acting_osds(pp->first, &cur_up, &up_primary,
			      &cur_acting, &acting_primary);
  if (cur_acting == acting)
    return;  // no change this epoch; must be stale pg_stat
  if (cur_acting.empty())
    return;  // if previously empty now we can be no worse off
  const pg_pool_t *pool = next.get_pg_pool(pp->first.pool());
  if (pool && cur_acting.size() < pool->min_size)
    return;  // can be no worse off than before

  dout(20) << __func__ << " " << pp->first << " " << cur_up << "/" << cur_acting
	   << " -> " << up << "/" << acting
	   << ", priming " << cur_acting
	   << dendl;
  pending_inc.new_pg_temp[pp->first] = cur_acting;
}

int OSDMonitor::prime_pg_temp(OSDMap& next, PGMap *pg_map, int osd)
{
  dout(10) << __func__ << " osd." << osd << dendl;
  int num = 0;
  ceph::unordered_map<int, set<pg_t> >::iterator po = pg_map->pg_by_osd.find(osd);
  if (po != pg_map->pg_by_osd.end()) {
    for (set<pg_t>::iterator p = po->second.begin();
	 p != po->second.end();
	 ++p, ++num) {
      ceph::unordered_map<pg_t, pg_stat_t>::iterator pp = pg_map->pg_stat.find(*p);
      if (pp == pg_map->pg_stat.end())
	continue;
      prime_pg_temp(next, pp);
    }
  }
  return num;
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
  pending_inc.modified = ceph_clock_now(g_ceph_context);

  int r = pending_inc.propagate_snaps_to_tiers(g_ceph_context, osdmap);
  assert(r == 0);

  if (g_conf->mon_osd_prime_pg_temp)
    maybe_prime_pg_temp();

  bufferlist bl;

  // tell me about it
  for (map<int32_t,uint8_t>::iterator i = pending_inc.new_state.begin();
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

  // encode full map and determine its crc
  OSDMap tmp;
  {
    tmp.deepish_copy_from(osdmap);
    tmp.apply_incremental(pending_inc);
    bufferlist fullbl;
    ::encode(tmp, fullbl, mon->quorum_features | CEPH_FEATURE_RESERVED);
    pending_inc.full_crc = tmp.get_crc();

    // include full map in the txn.  note that old monitors will
    // overwrite this.  new ones will now skip the local full map
    // encode and reload from this.
    put_version_full(t, pending_inc.epoch, fullbl);
  }

  // encode
  assert(get_last_committed() + 1 == pending_inc.epoch);
  ::encode(pending_inc, bl, mon->quorum_features | CEPH_FEATURE_RESERVED);

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
  for (int osd = 0; osd <= osdmap.get_max_osd(); osd++) {
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
  if (mon->pgmon()->is_readable() &&
      mon->pgmon()->pg_map.creating_pgs.empty()) {
    epoch_t floor = mon->pgmon()->pg_map.get_min_last_epoch_clean();
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
  PaxosServiceMessage *m = static_cast<PaxosServiceMessage*>(op->get_req());
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
  case MSG_OSD_FAILURE:
    return preprocess_failure(op);
  case MSG_OSD_BOOT:
    return preprocess_boot(op);
  case MSG_OSD_ALIVE:
    return preprocess_alive(op);
  case MSG_OSD_PGTEMP:
    return preprocess_pgtemp(op);

  case CEPH_MSG_POOLOP:
    return preprocess_pool_op(op);

  case MSG_REMOVE_SNAPS:
    return preprocess_remove_snaps(op);

  default:
    assert(0);
    return true;
  }
}

bool OSDMonitor::prepare_update(MonOpRequestRef op)
{
  op->mark_osdmon_event(__func__);
  PaxosServiceMessage *m = static_cast<PaxosServiceMessage*>(op->get_req());
  dout(7) << "prepare_update " << *m << " from " << m->get_orig_source_inst() << dendl;

  switch (m->get_type()) {
    // damp updates
  case MSG_OSD_MARK_ME_DOWN:
    return prepare_mark_me_down(op);
  case MSG_OSD_FAILURE:
    return prepare_failure(op);
  case MSG_OSD_BOOT:
    return prepare_boot(op);
  case MSG_OSD_ALIVE:
    return prepare_alive(op);
  case MSG_OSD_PGTEMP:
    return prepare_pgtemp(op);

  case MSG_MON_COMMAND:
    return prepare_command(op);

  case CEPH_MSG_POOLOP:
    return prepare_pool_op(op);

  case MSG_REMOVE_SNAPS:
    return prepare_remove_snaps(op);

  default:
    assert(0);
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
  reply->oldest_map = get_first_committed();
  reply->newest_map = osdmap.get_epoch();
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
  if (!osdmap.have_inst(badboy)) {
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

  void _finish(int) {
    MOSDMarkMeDown *m = static_cast<MOSDMarkMeDown*>(op->get_req());
    osdmon->mon->send_reply(
      op,
      new MOSDMarkMeDown(
	m->fsid,
	m->get_target(),
	m->get_epoch(),
	false));   // ACK itself does not request an ack
  }
  ~C_AckMarkedDown() {
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

  mon->clog->info() << "osd." << target_osd << " marked itself down\n";
  pending_inc.new_state[target_osd] = CEPH_OSD_UP;
  if (m->request_ack)
    wait_for_finished_proposal(op, new C_AckMarkedDown(this, op));
  return true;
}

bool OSDMonitor::can_mark_down(int i)
{
  if (osdmap.test_flag(CEPH_OSDMAP_NODOWN)) {
    dout(5) << "can_mark_down NODOWN flag set, will not mark osd." << i << " down" << dendl;
    return false;
  }
  int num_osds = osdmap.get_num_osds();
  if (num_osds == 0) {
    dout(5) << "can_mark_down no osds" << dendl;
    return false;
  }
  int up = osdmap.get_num_up_osds() - pending_inc.get_net_marked_down(&osdmap);
  float up_ratio = (float)up / (float)num_osds;
  if (up_ratio < g_conf->mon_osd_min_up_ratio) {
    dout(2) << "can_mark_down current up_ratio " << up_ratio << " < min "
	    << g_conf->mon_osd_min_up_ratio
	    << ", will not mark osd." << i << " down" << dendl;
    return false;
  }
  return true;
}

bool OSDMonitor::can_mark_up(int i)
{
  if (osdmap.test_flag(CEPH_OSDMAP_NOUP)) {
    dout(5) << "can_mark_up NOUP flag set, will not mark osd." << i << " up" << dendl;
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
    dout(5) << "can_mark_out NOOUT flag set, will not mark osds out" << dendl;
    return false;
  }
  int num_osds = osdmap.get_num_osds();
  if (num_osds == 0) {
    dout(5) << "can_mark_out no osds" << dendl;
    return false;
  }
  int in = osdmap.get_num_in_osds() - pending_inc.get_net_marked_out(&osdmap);
  float in_ratio = (float)in / (float)num_osds;
  if (in_ratio < g_conf->mon_osd_min_in_ratio) {
    if (i >= 0)
      dout(5) << "can_mark_down current in_ratio " << in_ratio << " < min "
	      << g_conf->mon_osd_min_in_ratio
	      << ", will not mark osd." << i << " out" << dendl;
    else
      dout(5) << "can_mark_down current in_ratio " << in_ratio << " < min "
	      << g_conf->mon_osd_min_in_ratio
	      << ", will not mark osds out" << dendl;
    return false;
  }

  return true;
}

bool OSDMonitor::can_mark_in(int i)
{
  if (osdmap.test_flag(CEPH_OSDMAP_NOIN)) {
    dout(5) << "can_mark_in NOIN flag set, will not mark osd." << i << " in" << dendl;
    return false;
  }
  return true;
}

void OSDMonitor::check_failures(utime_t now)
{
  for (map<int,failure_info_t>::iterator p = failure_info.begin();
       p != failure_info.end();
       ++p) {
    if (can_mark_down(p->first)) {
      check_failure(now, p->first, p->second);
    }
  }
}

bool OSDMonitor::check_failure(utime_t now, int target_osd, failure_info_t& fi)
{
  set<string> reporters_by_subtree;
  string reporter_subtree_level = g_conf->mon_osd_reporter_subtree_level;
  utime_t orig_grace(g_conf->osd_heartbeat_grace, 0);
  utime_t max_failed_since = fi.get_failed_since();
  utime_t failed_for = now - max_failed_since;

  utime_t grace = orig_grace;
  double my_grace = 0, peer_grace = 0;
  if (g_conf->mon_osd_adjust_heartbeat_grace) {
    double halflife = (double)g_conf->mon_osd_laggy_halflife;
    double decay_k = ::log(.5) / halflife;

    // scale grace period based on historical probability of 'lagginess'
    // (false positive failures due to slowness).
    const osd_xinfo_t& xi = osdmap.get_xinfo(target_osd);
    double decay = exp((double)failed_for * decay_k);
    dout(20) << " halflife " << halflife << " decay_k " << decay_k
	     << " failed_for " << failed_for << " decay " << decay << dendl;
    my_grace = decay * (double)xi.laggy_interval * xi.laggy_probability;
    grace += my_grace;

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

      const osd_xinfo_t& xi = osdmap.get_xinfo(p->first);
      utime_t elapsed = now - xi.down_stamp;
      double decay = exp((double)elapsed * decay_k);
      peer_grace += decay * (double)xi.laggy_interval * xi.laggy_probability;
    }
    peer_grace /= (double)fi.reporters.size();
    grace += peer_grace;
  }

  dout(10) << " osd." << target_osd << " has "
	   << fi.reporters.size() << " reporters, "
	   << grace << " grace (" << orig_grace << " + " << my_grace
	   << " + " << peer_grace << "), max_failed_since " << max_failed_since
	   << dendl;

  // already pending failure?
  if (pending_inc.new_state.count(target_osd) &&
      pending_inc.new_state[target_osd] & CEPH_OSD_UP) {
    dout(10) << " already pending failure" << dendl;
    return true;
  }


  if (failed_for >= grace &&
      (int)reporters_by_subtree.size() >= g_conf->mon_osd_min_down_reporters) {
    dout(1) << " we have enough reporters to mark osd." << target_osd
	    << " down" << dendl;
    pending_inc.new_state[target_osd] = CEPH_OSD_UP;

    mon->clog->info() << osdmap.get_inst(target_osd) << " failed ("
		      << (int)reporters_by_subtree.size() << " reporters from different "
		      << reporter_subtree_level << " after "
		      << failed_for << " >= grace " << grace << ")\n";
    return true;
  }
  return false;
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

  // calculate failure time
  utime_t now = ceph_clock_now(g_ceph_context);
  utime_t failed_since =
    m->get_recv_stamp() -
    utime_t(m->failed_for ? m->failed_for : g_conf->osd_heartbeat_grace, 0);

  if (m->if_osd_failed()) {
    // add a report
    mon->clog->debug() << m->get_target() << " reported failed by "
		      << m->get_orig_source_inst() << "\n";
    failure_info_t& fi = failure_info[target_osd];
    MonOpRequestRef old_op = fi.add_report(reporter, failed_since, op);
    if (old_op) {
      mon->no_reply(old_op);
    }

    return check_failure(now, target_osd, fi);
  } else {
    // remove the report
    mon->clog->debug() << m->get_target() << " failure report canceled by "
		       << m->get_orig_source_inst() << "\n";
    if (failure_info.count(target_osd)) {
      failure_info_t& fi = failure_info[target_osd];
      list<MonOpRequestRef> ls;
      fi.take_report_messages(ls);
      fi.cancel_report(reporter);
      while (!ls.empty()) {
        if (ls.front())
          mon->no_reply(ls.front());
	ls.pop_front();
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

  if (osdmap.test_flag(CEPH_OSDMAP_SORTBITWISE) &&
      !(m->osd_features & CEPH_FEATURE_OSD_BITWISE_HOBJ_SORT)) {
    mon->clog->info() << "disallowing boot of OSD "
		      << m->get_orig_source_inst()
		      << " because 'sortbitwise' osdmap flag is set and OSD lacks the OSD_BITWISE_HOBJ_SORT feature\n";
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

  // make sure upgrades stop at hammer
  //  * HAMMER_0_94_4 is the required hammer feature
  //  * MON_METADATA is the first post-hammer feature
  if (osdmap.get_num_up_osds() > 0) {
    if ((m->osd_features & CEPH_FEATURE_MON_METADATA) &&
	!(osdmap.get_up_osd_features() & CEPH_FEATURE_HAMMER_0_94_4)) {
      mon->clog->info() << "disallowing boot of post-hammer OSD "
			<< m->get_orig_source_inst()
			<< " because one or more up OSDs is pre-hammer v0.94.4\n";
      goto ignore;
    }
    if (!(m->osd_features & CEPH_FEATURE_HAMMER_0_94_4) &&
	(osdmap.get_up_osd_features() & CEPH_FEATURE_MON_METADATA)) {
      mon->clog->info() << "disallowing boot of pre-hammer v0.94.4 OSD "
			<< m->get_orig_source_inst()
			<< " because all up OSDs are post-hammer\n";
      goto ignore;
    }
  }

  // already booted?
  if (osdmap.is_up(from) &&
      osdmap.get_inst(from) == m->get_orig_source_inst()) {
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
      osdmap.get_info(from).up_from > m->version) {
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
  dout(7) << "prepare_boot from " << m->get_orig_source_inst() << " sb " << m->sb
	  << " cluster_addr " << m->cluster_addr
	  << " hb_back_addr " << m->hb_back_addr
	  << " hb_front_addr " << m->hb_front_addr
	  << dendl;

  assert(m->get_orig_source().is_osd());
  int from = m->get_orig_source().num();

  // does this osd exist?
  if (from >= osdmap.get_max_osd()) {
    dout(1) << "boot from osd." << from << " >= max_osd " << osdmap.get_max_osd() << dendl;
    return false;
  }

  int oldstate = osdmap.exists(from) ? osdmap.get_state(from) : CEPH_OSD_NEW;
  if (pending_inc.new_state.count(from))
    oldstate ^= pending_inc.new_state[from];

  // already up?  mark down first?
  if (osdmap.is_up(from)) {
    dout(7) << "prepare_boot was up, first marking down " << osdmap.get_inst(from) << dendl;
    // preprocess should have caught these;  if not, assert.
    assert(osdmap.get_inst(from) != m->get_orig_source_inst());
    assert(osdmap.get_uuid(from) == m->sb.osd_fsid);

    if (pending_inc.new_state.count(from) == 0 ||
	(pending_inc.new_state[from] & CEPH_OSD_UP) == 0) {
      // mark previous guy down
      pending_inc.new_state[from] = CEPH_OSD_UP;
    }
    wait_for_finished_proposal(op, new C_RetryMessage(this, op));
  } else if (pending_inc.new_up_client.count(from)) { //FIXME: should this be using new_up_client?
    // already prepared, just wait
    dout(7) << "prepare_boot already prepared, waiting on " << m->get_orig_source_addr() << dendl;
    wait_for_finished_proposal(op, new C_RetryMessage(this, op));
  } else {
    // mark new guy up.
    pending_inc.new_up_client[from] = m->get_orig_source_addr();
    if (!m->cluster_addr.is_blank_ip())
      pending_inc.new_up_cluster[from] = m->cluster_addr;
    pending_inc.new_hb_back_up[from] = m->hb_back_addr;
    if (!m->hb_front_addr.is_blank_ip())
      pending_inc.new_hb_front_up[from] = m->hb_front_addr;

    // mark in?
    if ((g_conf->mon_osd_auto_mark_auto_out_in && (oldstate & CEPH_OSD_AUTOOUT)) ||
	(g_conf->mon_osd_auto_mark_new_in && (oldstate & CEPH_OSD_NEW)) ||
	(g_conf->mon_osd_auto_mark_in)) {
      if (can_mark_in(from)) {
	if (osdmap.osd_xinfo[from].old_weight > 0)
	  pending_inc.new_weight[from] = osdmap.osd_xinfo[from].old_weight;
	else
	  pending_inc.new_weight[from] = CEPH_OSD_IN;
      } else {
	dout(7) << "prepare_boot NOIN set, will not mark in " << m->get_orig_source_addr() << dendl;
      }
    }

    down_pending_out.erase(from);  // if any

    if (m->sb.weight)
      osd_weight[from] = m->sb.weight;

    // set uuid?
    dout(10) << " setting osd." << from << " uuid to " << m->sb.osd_fsid << dendl;
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

    // adjust last clean unmount epoch?
    const osd_info_t& info = osdmap.get_info(from);
    dout(10) << " old osd_info: " << info << dendl;
    if (m->sb.mounted > info.last_clean_begin ||
	(m->sb.mounted == info.last_clean_begin &&
	 m->sb.clean_thru > info.last_clean_end)) {
      epoch_t begin = m->sb.mounted;
      epoch_t end = m->sb.clean_thru;

      dout(10) << "prepare_boot osd." << from << " last_clean_interval "
	       << "[" << info.last_clean_begin << "," << info.last_clean_end << ")"
	       << " -> [" << begin << "-" << end << ")"
	       << dendl;
      pending_inc.new_last_clean_interval[from] = pair<epoch_t,epoch_t>(begin, end);
    }

    osd_xinfo_t xi = osdmap.get_xinfo(from);
    if (m->boot_epoch == 0) {
      xi.laggy_probability *= (1.0 - g_conf->mon_osd_laggy_weight);
      xi.laggy_interval *= (1.0 - g_conf->mon_osd_laggy_weight);
      dout(10) << " not laggy, new xi " << xi << dendl;
    } else {
      if (xi.down_stamp.sec()) {
        int interval = ceph_clock_now(g_ceph_context).sec() - xi.down_stamp.sec();
        if (g_conf->mon_osd_laggy_max_interval && (interval > g_conf->mon_osd_laggy_max_interval)) {
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
    mon->clog->info() << m->get_orig_source_inst() << " boot\n";
  }

  send_latest(op, m->sb.current_epoch+1);
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
    mon->clog->debug() << m->get_orig_source_inst() << " alive\n";
  }

  dout(7) << "prepare_alive want up_thru " << m->want << " have " << m->version
	  << " from " << m->get_orig_source_inst() << dendl;
  pending_inc.new_up_thru[from] = m->version;  // set to the latest map the OSD has
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

// -------------
// pg_temp changes

bool OSDMonitor::preprocess_pgtemp(MonOpRequestRef op)
{
  MOSDPGTemp *m = static_cast<MOSDPGTemp*>(op->get_req());
  dout(10) << "preprocess_pgtemp " << *m << dendl;
  vector<int> empty;
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

  for (map<pg_t,vector<int32_t> >::iterator p = m->pg_temp.begin(); p != m->pg_temp.end(); ++p) {
    dout(20) << " " << p->first
	     << (osdmap.pg_temp->count(p->first) ? (*osdmap.pg_temp)[p->first] : empty)
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

    // removal?
    if (p->second.empty() && (osdmap.pg_temp->count(p->first) ||
			      osdmap.primary_temp->count(p->first)))
      return false;
    // change?
    //  NOTE: we assume that this will clear pg_primary, so consider
    //        an existing pg_primary field to imply a change
    if (p->second.size() && (osdmap.pg_temp->count(p->first) == 0 ||
			     (*osdmap.pg_temp)[p->first] != p->second ||
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
    pending_inc.new_pg_temp[p->first] = p->second;

    // unconditionally clear pg_primary (until this message can encode
    // a change for that, too.. at which point we need to also fix
    // preprocess_pg_temp)
    if (osdmap.primary_temp->count(p->first) ||
	pending_inc.new_primary_temp.count(p->first))
      pending_inc.new_primary_temp[p->first] = -1;
  }
  pending_inc.new_up_thru[from] = m->map_epoch;   // set up_thru too, so the osd doesn't have to ask again
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
  if (!session->caps.is_capable(g_ceph_context, session->entity_name,
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
	assert(0);  // we should have all maps.
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
    dout(10) << __func__ << session->inst << " should already have epoch "
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
    epoch_t last = MIN(first + g_conf->osd_map_message_max, osdmap.get_epoch());
    MOSDMap *m = build_incremental(first, last);

    if (req) {
      // send some maps.  it may not be all of them, but it will get them
      // started.
      m->oldest_map = get_first_committed();
      m->newest_map = osdmap.get_epoch();
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


void OSDMonitor::check_subs()
{
  dout(10) << __func__ << dendl;
  string type = "osdmap";
  if (mon->session_map.subs.count(type) == 0)
    return;
  xlist<Subscription*>::iterator p = mon->session_map.subs[type]->begin();
  while (!p.end()) {
    Subscription *sub = *p;
    ++p;
    check_sub(sub);
  }
}

void OSDMonitor::check_sub(Subscription *sub)
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

// TICK


void OSDMonitor::tick()
{
  if (!is_active()) return;

  dout(10) << osdmap << dendl;

  if (!mon->is_leader()) return;

  bool do_propose = false;
  utime_t now = ceph_clock_now(g_ceph_context);

  // mark osds down?
  check_failures(now);

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

	if (g_conf->mon_osd_down_out_interval > 0 &&
	    down.sec() >= grace) {
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

	  mon->clog->info() << "osd." << o << " out (down for " << down << ")\n";
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

  //if map full setting has changed, get that info out there!
  if (mon->pgmon()->is_readable()) {
    if (!mon->pgmon()->pg_map.full_osds.empty()) {
      dout(5) << "There are full osds, setting full flag" << dendl;
      add_flag(CEPH_OSDMAP_FULL);
    } else if (osdmap.test_flag(CEPH_OSDMAP_FULL)){
      dout(10) << "No full osds, removing full flag" << dendl;
      remove_flag(CEPH_OSDMAP_FULL);
    }
    if (pending_inc.new_flags != -1 &&
	(pending_inc.new_flags ^ osdmap.flags) & CEPH_OSDMAP_FULL) {
      dout(1) << "New setting for CEPH_OSDMAP_FULL -- doing propose" << dendl;
      do_propose = true;
    }
  }
  // ---------------
#define SWAP_PRIMARIES_AT_START 0
#define SWAP_TIME 1
#if 0
  if (SWAP_PRIMARIES_AT_START) {
    // For all PGs that have OSD 0 as the primary,
    // switch them to use the first replca
    ps_t numps = osdmap.get_pg_num();
    for (int64_t pool=0; pool<1; pool++)
      for (ps_t ps = 0; ps < numps; ++ps) {
	pg_t pgid = pg_t(pg_t::TYPE_REPLICATED, ps, pool, -1);
	vector<int> osds;
	osdmap.pg_to_osds(pgid, osds);
	if (osds[0] == 0) {
	  pending_inc.new_pg_swap_primary[pgid] = osds[1];
	  dout(3) << "Changing primary for PG " << pgid << " from " << osds[0] << " to "
		  << osds[1] << dendl;
	  do_propose = true;
	}
      }
  }
#endif
  // ---------------

  if (update_pools_status())
    do_propose = true;

  if (do_propose ||
      !pending_inc.new_pg_temp.empty())  // also propose if we adjusted pg_temp
    propose_pending();
}

void OSDMonitor::handle_osd_timeouts(const utime_t &now,
				     std::map<int,utime_t> &last_osd_report)
{
  utime_t timeo(g_conf->mon_osd_report_timeout, 0);
  int max_osd = osdmap.get_max_osd();
  bool new_down = false;

  for (int i=0; i < max_osd; ++i) {
    dout(30) << "handle_osd_timeouts: checking up on osd " << i << dendl;
    if (!osdmap.exists(i))
      continue;
    if (!osdmap.is_up(i))
      continue;
    const std::map<int,utime_t>::const_iterator t = last_osd_report.find(i);
    if (t == last_osd_report.end()) {
      // it wasn't in the map; start the timer.
      last_osd_report[i] = now;
    } else if (can_mark_down(i)) {
      utime_t diff = now - t->second;
      if (diff > timeo) {
	mon->clog->info() << "osd." << i << " marked down after no pg stats for " << diff << "seconds\n";
	derr << "no osd or pg stats from osd." << i << " since " << t->second << ", " << diff
	     << " seconds ago.  marking down" << dendl;
	pending_inc.new_state[i] = CEPH_OSD_UP;
	new_down = true;
      }
    }
  }
  if (new_down) {
    propose_pending();
  }
}

void OSDMonitor::mark_all_down()
{
  assert(mon->is_leader());

  dout(7) << "mark_all_down" << dendl;

  set<int32_t> ls;
  osdmap.get_all_osds(ls);
  for (set<int32_t>::iterator it = ls.begin();
       it != ls.end();
       ++it) {
    if (osdmap.is_down(*it)) continue;
    pending_inc.new_state[*it] = CEPH_OSD_UP;
  }

  propose_pending();
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
    for (int i = 0; i < osdmap.get_max_osd(); i++) {
      if (!osdmap.exists(i) || osdmap.is_out(i))
	continue;
      ++num_in_osds;
      if (!osdmap.is_up(i)) {
	++num_down_in_osds;
	if (detail) {
	  const osd_info_t& info = osdmap.get_info(i);
	  ostringstream ss;
	  ss << "osd." << i << " is down since epoch " << info.down_at
	     << ", last address " << osdmap.get_addr(i);
	  detail->push_back(make_pair(HEALTH_WARN, ss.str()));
	}
      }
    }
    assert(num_down_in_osds <= num_in_osds);
    if (num_down_in_osds > 0) {
      ostringstream ss;
      ss << num_down_in_osds << "/" << num_in_osds << " in osds are down";
      summary.push_back(make_pair(HEALTH_WARN, ss.str()));
    }

    // warn about flags
    if (osdmap.test_flag(CEPH_OSDMAP_PAUSERD |
			 CEPH_OSDMAP_PAUSEWR |
			 CEPH_OSDMAP_NOUP |
			 CEPH_OSDMAP_NODOWN |
			 CEPH_OSDMAP_NOIN |
			 CEPH_OSDMAP_NOOUT |
			 CEPH_OSDMAP_NOBACKFILL |
			 CEPH_OSDMAP_NOREBALANCE |
			 CEPH_OSDMAP_NORECOVER |
			 CEPH_OSDMAP_NOSCRUB |
			 CEPH_OSDMAP_NODEEP_SCRUB |
			 CEPH_OSDMAP_NOTIERAGENT)) {
      ostringstream ss;
      ss << osdmap.get_flag_string() << " flag(s) set";
      summary.push_back(make_pair(HEALTH_WARN, ss.str()));
      if (detail)
	detail->push_back(make_pair(HEALTH_WARN, ss.str()));
    }

    // old crush tunables?
    if (g_conf->mon_warn_on_legacy_crush_tunables) {
      if (osdmap.crush->has_legacy_tunables()) {
	ostringstream ss;
	ss << "crush map has legacy tunables";
	summary.push_back(make_pair(HEALTH_WARN, ss.str()));
	if (detail) {
	  ss << "; see http://ceph.com/docs/master/rados/operations/crush-map/#tunables";
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

    get_pools_health(summary, detail);
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
    PG_NUM, PGP_NUM, CRUSH_RULESET, HASHPSPOOL,
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
    RECOVERY_PRIORITY, RECOVERY_OP_PRIORITY};

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
    mon->reply_command(op, -EACCES, "access denied", rdata, get_last_committed());
    return true;
  }

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  string format;
  cmd_getval(g_ceph_context, cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  if (prefix == "osd stat") {
    osdmap.print_summary(f.get(), ds);
    if (f)
      f->flush(rdata);
    else
      rdata.append(ds);
  }
  else if (prefix == "osd perf") {
    const PGMap &pgm = mon->pgmon()->pg_map;
    if (f) {
      f->open_object_section("osdstats");
      pgm.dump_osd_perf_stats(f.get());
      f->close_section();
      f->flush(ds);
    } else {
      pgm.print_osd_perf_stats(&ds);
    }
    rdata.append(ds);
  }
  else if (prefix == "osd blocked-by") {
    const PGMap &pgm = mon->pgmon()->pg_map;
    if (f) {
      f->open_object_section("osd_blocked_by");
      pgm.dump_osd_blocked_by_stats(f.get());
      f->close_section();
      f->flush(ds);
    } else {
      pgm.print_osd_blocked_by_stats(&ds);
    }
    rdata.append(ds);
  }
  else if (prefix == "osd dump" ||
	   prefix == "osd tree" ||
	   prefix == "osd ls" ||
	   prefix == "osd getmap" ||
	   prefix == "osd getcrushmap") {
    string val;

    epoch_t epoch = 0;
    int64_t epochnum;
    cmd_getval(g_ceph_context, cmdmap, "epoch", epochnum, (int64_t)0);
    epoch = epochnum;
    if (!epoch)
      epoch = osdmap.get_epoch();

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
      if (f) {
	f->open_object_section("tree");
	p->print_tree(f.get(), NULL);
	f->close_section();
	f->flush(ds);
      } else {
	p->print_tree(NULL, &ds);
      }
      rdata.append(ds);
    } else if (prefix == "osd getmap") {
      rdata.append(osdmap_bl);
      ss << "got osdmap epoch " << p->get_epoch();
    } else if (prefix == "osd getcrushmap") {
      p->crush->encode(rdata);
      ss << "got crush map from osdmap epoch " << p->get_epoch();
    }
    if (p != &osdmap)
      delete p;
  } else if (prefix == "osd df") {
    string method;
    cmd_getval(g_ceph_context, cmdmap, "output_method", method);
    print_utilization(ds, f ? f.get() : NULL, method == "tree");
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
      f->open_array_section("osd_metadata");
      for (int i=0; i<osdmap.get_max_osd(); ++i) {
        if (osdmap.exists(i)) {
          f->open_object_section("osd");
          f->dump_unsigned("id", i);
          r = dump_osd_metadata(i, f.get(), NULL);
          if (r < 0)
            goto reply;
          f->close_section();
        }
      }
      f->close_section();
    }
    f->flush(rdata);
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
  } else if ((prefix == "osd scrub" ||
	      prefix == "osd deep-scrub" ||
	      prefix == "osd repair")) {
    string whostr;
    cmd_getval(g_ceph_context, cmdmap, "who", whostr);
    vector<string> pvec;
    get_str_vec(prefix, pvec);

    if (whostr == "*") {
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
    const choices_map_t ALL_CHOICES = boost::assign::map_list_of
      ("size", SIZE)
      ("min_size", MIN_SIZE)
      ("crash_replay_interval", CRASH_REPLAY_INTERVAL)
      ("pg_num", PG_NUM)("pgp_num", PGP_NUM)("crush_ruleset", CRUSH_RULESET)
      ("hashpspool", HASHPSPOOL)("nodelete", NODELETE)
      ("nopgchange", NOPGCHANGE)("nosizechange", NOSIZECHANGE)
      ("noscrub", NOSCRUB)("nodeep-scrub", NODEEP_SCRUB)
      ("write_fadvise_dontneed", WRITE_FADVISE_DONTNEED)
      ("hit_set_type", HIT_SET_TYPE)("hit_set_period", HIT_SET_PERIOD)
      ("hit_set_count", HIT_SET_COUNT)("hit_set_fpp", HIT_SET_FPP)
      ("use_gmt_hitset", USE_GMT_HITSET)
      ("auid", AUID)("target_max_objects", TARGET_MAX_OBJECTS)
      ("target_max_bytes", TARGET_MAX_BYTES)
      ("cache_target_dirty_ratio", CACHE_TARGET_DIRTY_RATIO)
      ("cache_target_dirty_high_ratio", CACHE_TARGET_DIRTY_HIGH_RATIO)
      ("cache_target_full_ratio", CACHE_TARGET_FULL_RATIO)
      ("cache_min_flush_age", CACHE_MIN_FLUSH_AGE)
      ("cache_min_evict_age", CACHE_MIN_EVICT_AGE)
      ("erasure_code_profile", ERASURE_CODE_PROFILE)
      ("min_read_recency_for_promote", MIN_READ_RECENCY_FOR_PROMOTE)
      ("min_write_recency_for_promote", MIN_WRITE_RECENCY_FOR_PROMOTE)
      ("fast_read", FAST_READ)
      ("hit_set_grade_decay_rate", HIT_SET_GRADE_DECAY_RATE)
      ("hit_set_search_last_n", HIT_SET_SEARCH_LAST_N)
      ("scrub_min_interval", SCRUB_MIN_INTERVAL)
      ("scrub_max_interval", SCRUB_MAX_INTERVAL)
      ("deep_scrub_interval", DEEP_SCRUB_INTERVAL)
      ("recovery_priority", RECOVERY_PRIORITY)
      ("recovery_op_priority", RECOVERY_OP_PRIORITY);

    typedef std::set<osd_pool_get_choices> choices_set_t;

    const choices_set_t ONLY_TIER_CHOICES = boost::assign::list_of
      (HIT_SET_TYPE)(HIT_SET_PERIOD)(HIT_SET_COUNT)(HIT_SET_FPP)
      (TARGET_MAX_OBJECTS)(TARGET_MAX_BYTES)(CACHE_TARGET_FULL_RATIO)
      (CACHE_TARGET_DIRTY_RATIO)(CACHE_TARGET_DIRTY_HIGH_RATIO)
      (CACHE_MIN_FLUSH_AGE)(CACHE_MIN_EVICT_AGE)(MIN_READ_RECENCY_FOR_PROMOTE)
      (HIT_SET_GRADE_DECAY_RATE)(HIT_SET_SEARCH_LAST_N);
    const choices_set_t ONLY_ERASURE_CHOICES = boost::assign::list_of
      (ERASURE_CODE_PROFILE);

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
	f->open_object_section("pool");
	f->dump_string("pool", poolstr);
	f->dump_int("pool_id", pool);
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
	  case CRUSH_RULESET:
	    f->dump_int("crush_ruleset", p->get_crush_ruleset());
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
	    for (i = ALL_CHOICES.begin(); i != ALL_CHOICES.end(); ++i) {
	      if (i->second == *it)
		break;
	    }
	    assert(i != ALL_CHOICES.end());
	    p->opts.dump(i->first, f.get());
            break;
	}
	f->close_section();
	f->flush(rdata);
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
	  case CRUSH_RULESET:
	    ss << "crush_ruleset: " << p->get_crush_ruleset() << "\n";
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
	    for (i = ALL_CHOICES.begin(); i != ALL_CHOICES.end(); ++i) {
	      if (i->second == *it)
		break;
	    }
	    assert(i != ALL_CHOICES.end());
	    {
	      pool_opts_t::key_t key = pool_opts_t::get_opt_desc(i->first).key;
	      if (p->opts.is_set(key)) {
		ss << i->first << ": " << p->opts.get(key) << "\n";
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
    string pool_name;
    cmd_getval(g_ceph_context, cmdmap, "name", pool_name);

    PGMap& pg_map = mon->pgmon()->pg_map;

    int64_t poolid = -ENOENT;
    bool one_pool = false;
    if (!pool_name.empty()) {
      poolid = osdmap.lookup_pg_pool_name(pool_name);
      if (poolid < 0) {
        assert(poolid == -ENOENT);
        ss << "unrecognized pool '" << pool_name << "'";
        r = -ENOENT;
        goto reply;
      }
      one_pool = true;
    }

    stringstream rs;

    if (f)
      f->open_array_section("pool_stats");
    if (osdmap.get_pools().size() == 0) {
      if (!f)
        ss << "there are no pools!";
      goto stats_out;
    }

    for (map<int64_t,pg_pool_t>::const_iterator it = osdmap.get_pools().begin();
         it != osdmap.get_pools().end();
         ++it) {

      if (!one_pool)
        poolid = it->first;

      pool_name = osdmap.get_pool_name(poolid);

      if (f) {
        f->open_object_section("pool");
        f->dump_string("pool_name", pool_name.c_str());
        f->dump_int("pool_id", poolid);
        f->open_object_section("recovery");
      }

      list<string> sl;
      stringstream tss;
      pg_map.pool_recovery_summary(f.get(), &sl, poolid);
      if (!f && !sl.empty()) {
	for (list<string>::iterator p = sl.begin(); p != sl.end(); ++p)
	  tss << "  " << *p << "\n";
      }

      if (f) {
        f->close_section();
        f->open_object_section("recovery_rate");
      }

      ostringstream rss;
      pg_map.pool_recovery_rate_summary(f.get(), &rss, poolid);
      if (!f && !rss.str().empty())
        tss << "  recovery io " << rss.str() << "\n";

      if (f) {
        f->close_section();
        f->open_object_section("client_io_rate");
      }

      rss.clear();
      rss.str("");

      pg_map.pool_client_io_rate_summary(f.get(), &rss, poolid);
      if (!f && !rss.str().empty())
        tss << "  client io " << rss.str() << "\n";

      // dump cache tier IO rate for cache pool
      const pg_pool_t *pool = osdmap.get_pg_pool(poolid);
      if (pool->is_tier()) {
        if (f) {
          f->close_section();
          f->open_object_section("cache_io_rate");
        }

        rss.clear();
        rss.str("");

        pg_map.pool_cache_io_rate_summary(f.get(), &rss, poolid);
        if (!f && !rss.str().empty())
          tss << "  cache tier io " << rss.str() << "\n";
      }

      if (f) {
        f->close_section();
        f->close_section();
      } else {
        rs << "pool " << pool_name << " id " << poolid << "\n";
        if (!tss.str().empty())
          rs << tss.str() << "\n";
        else
          rs << "  nothing is going on\n\n";
      }

      if (one_pool)
        break;
    }

stats_out:
    if (f) {
      f->close_section();
      f->flush(rdata);
    } else {
      rdata.append(rs.str());
    }
    rdata.append("\n");
    r = 0;

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
    string format;
    cmd_getval(g_ceph_context, cmdmap, "format", format);
    boost::scoped_ptr<Formatter> f(Formatter::create(format, "json-pretty", "json-pretty"));
    f->open_array_section("rules");
    osdmap.crush->list_rules(f.get());
    f->close_section();
    ostringstream rs;
    f->flush(rs);
    rs << "\n";
    rdata.append(rs.str());
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
	ss << "unknown crush ruleset '" << name << "'";
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
    boost::scoped_ptr<Formatter> f(Formatter::create(format, "json-pretty", "json-pretty"));
    f->open_array_section("crush_map_roots");
    osdmap.crush->dump_tree(f.get());
    f->close_section();
    f->flush(rdata);
  } else if (prefix == "osd erasure-code-profile ls") {
    const map<string,map<string,string> > &profiles =
      osdmap.get_erasure_code_profiles();
    if (f)
      f->open_array_section("erasure-code-profiles");
    for(map<string,map<string,string> >::const_iterator i = profiles.begin();
	i != profiles.end();
	++i) {
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

void OSDMonitor::update_pool_flags(int64_t pool_id, uint64_t flags)
{
  const pg_pool_t *pool = osdmap.get_pg_pool(pool_id);
  pending_inc.get_new_pool(pool_id, pool)->flags = flags;
}

bool OSDMonitor::update_pools_status()
{
  if (!mon->pgmon()->is_readable())
    return false;

  bool ret = false;

  const map<int64_t,pg_pool_t>& pools = osdmap.get_pools();
  for (map<int64_t,pg_pool_t>::const_iterator it = pools.begin();
       it != pools.end();
       ++it) {
    if (!mon->pgmon()->pg_map.pg_pool_sum.count(it->first))
      continue;
    pool_stat_t& stats = mon->pgmon()->pg_map.pg_pool_sum[it->first];
    object_stat_sum_t& sum = stats.stats.sum;
    const pg_pool_t &pool = it->second;
    const string& pool_name = osdmap.get_pool_name(it->first);

    bool pool_is_full =
      (pool.quota_max_bytes > 0 && (uint64_t)sum.num_bytes >= pool.quota_max_bytes) ||
      (pool.quota_max_objects > 0 && (uint64_t)sum.num_objects >= pool.quota_max_objects);

    if (pool.has_flag(pg_pool_t::FLAG_FULL)) {
      if (pool_is_full)
        continue;

      mon->clog->info() << "pool '" << pool_name
                       << "' no longer full; removing FULL flag";

      update_pool_flags(it->first, pool.get_flags() & ~pg_pool_t::FLAG_FULL);
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
      
      update_pool_flags(it->first, pool.get_flags() | pg_pool_t::FLAG_FULL);
      ret = true;
    }
  }
  return ret;
}

void OSDMonitor::get_pools_health(
    list<pair<health_status_t,string> >& summary,
    list<pair<health_status_t,string> > *detail) const
{
  const map<int64_t,pg_pool_t>& pools = osdmap.get_pools();
  for (map<int64_t,pg_pool_t>::const_iterator it = pools.begin();
       it != pools.end(); ++it) {
    if (!mon->pgmon()->pg_map.pg_pool_sum.count(it->first))
      continue;
    pool_stat_t& stats = mon->pgmon()->pg_map.pg_pool_sum[it->first];
    object_stat_sum_t& sum = stats.stats.sum;
    const pg_pool_t &pool = it->second;
    const string& pool_name = osdmap.get_pool_name(it->first);

    if (pool.has_flag(pg_pool_t::FLAG_FULL)) {
      // uncomment these asserts if/when we update the FULL flag on pg_stat update
      //assert((pool.quota_max_objects > 0) || (pool.quota_max_bytes > 0));

      stringstream ss;
      ss << "pool '" << pool_name << "' is full";
      summary.push_back(make_pair(HEALTH_WARN, ss.str()));
      if (detail)
	detail->push_back(make_pair(HEALTH_WARN, ss.str()));
    }

    float warn_threshold = (float)g_conf->mon_pool_quota_warn_threshold/100;
    float crit_threshold = (float)g_conf->mon_pool_quota_crit_threshold/100;

    if (pool.quota_max_objects > 0) {
      stringstream ss;
      health_status_t status = HEALTH_OK;
      if ((uint64_t)sum.num_objects >= pool.quota_max_objects) {
	// uncomment these asserts if/when we update the FULL flag on pg_stat update
        //assert(pool.has_flag(pg_pool_t::FLAG_FULL));
      } else if (crit_threshold > 0 &&
		 sum.num_objects >= pool.quota_max_objects*crit_threshold) {
        ss << "pool '" << pool_name
           << "' has " << sum.num_objects << " objects"
           << " (max " << pool.quota_max_objects << ")";
        status = HEALTH_ERR;
      } else if (warn_threshold > 0 &&
		 sum.num_objects >= pool.quota_max_objects*warn_threshold) {
        ss << "pool '" << pool_name
           << "' has " << sum.num_objects << " objects"
           << " (max " << pool.quota_max_objects << ")";
        status = HEALTH_WARN;
      }
      if (status != HEALTH_OK) {
        pair<health_status_t,string> s(status, ss.str());
        summary.push_back(s);
        if (detail)
          detail->push_back(s);
      }
    }

    if (pool.quota_max_bytes > 0) {
      health_status_t status = HEALTH_OK;
      stringstream ss;
      if ((uint64_t)sum.num_bytes >= pool.quota_max_bytes) {
	// uncomment these asserts if/when we update the FULL flag on pg_stat update
	//assert(pool.has_flag(pg_pool_t::FLAG_FULL));
      } else if (crit_threshold > 0 &&
		 sum.num_bytes >= pool.quota_max_bytes*crit_threshold) {
        ss << "pool '" << pool_name
           << "' has " << si_t(sum.num_bytes) << " bytes"
           << " (max " << si_t(pool.quota_max_bytes) << ")";
        status = HEALTH_ERR;
      } else if (warn_threshold > 0 &&
		 sum.num_bytes >= pool.quota_max_bytes*warn_threshold) {
        ss << "pool '" << pool_name
           << "' has " << si_t(sum.num_bytes) << " bytes"
           << " (max " << si_t(pool.quota_max_bytes) << ")";
        status = HEALTH_WARN;
      }
      if (status != HEALTH_OK) {
        pair<health_status_t,string> s(status, ss.str());
        summary.push_back(s);
        if (detail)
          detail->push_back(s);
      }
    }
  }
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
  string ruleset_name;
  if (m->auid)
    return prepare_new_pool(m->name, m->auid, m->crush_rule, ruleset_name,
			    0, 0,
                            erasure_code_profile,
			    pg_pool_t::TYPE_REPLICATED, 0, FAST_READ_OFF, &ss);
  else
    return prepare_new_pool(m->name, session->auid, m->crush_rule, ruleset_name,
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
  newcrush.encode(pending_inc.crush);
  *ss << "renamed bucket " << srcname << " into " << dstname;
  return 0;
}

int OSDMonitor::normalize_profile(ErasureCodeProfile &profile, ostream *ss)
{
  ErasureCodeInterfaceRef erasure_code;
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  ErasureCodeProfile::const_iterator plugin = profile.find("plugin");
  int err = instance.factory(plugin->second,
			     g_conf->erasure_code_dir,
			     profile, &erasure_code, ss);
  if (err)
    return err;
  return erasure_code->init(profile, ss);
}

int OSDMonitor::crush_ruleset_create_erasure(const string &name,
					     const string &profile,
					     int *ruleset,
					     ostream *ss)
{
  int ruleid = osdmap.crush->get_rule_id(name);
  if (ruleid != -ENOENT) {
    *ruleset = osdmap.crush->get_rule_mask_ruleset(ruleid);
    return -EEXIST;
  }

  CrushWrapper newcrush;
  _get_pending_crush(newcrush);

  ruleid = newcrush.get_rule_id(name);
  if (ruleid != -ENOENT) {
    *ruleset = newcrush.get_rule_mask_ruleset(ruleid);
    return -EALREADY;
  } else {
    ErasureCodeInterfaceRef erasure_code;
    int err = get_erasure_code(profile, &erasure_code, ss);
    if (err) {
      *ss << "failed to load plugin using profile " << profile << std::endl;
      return err;
    }

    err = erasure_code->create_ruleset(name, newcrush, ss);
    erasure_code.reset();
    if (err < 0)
      return err;
    *ruleset = err;
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush);
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
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  return instance.factory(plugin->second,
			  g_conf->erasure_code_dir,
			  profile, erasure_code, ss);
}

int OSDMonitor::check_cluster_features(uint64_t features,
				       stringstream &ss)
{
  stringstream unsupported_ss;
  int unsupported_count = 0;
  if ((mon->get_quorum_features() & features) != features) {
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
  ::encode(*newcrush, new_pending.crush);
  OSDMap newmap;
  newmap.deepish_copy_from(osdmap);
  newmap.apply_incremental(new_pending);

  uint64_t features =
    newmap.get_features(CEPH_ENTITY_TYPE_MON, NULL) |
    newmap.get_features(CEPH_ENTITY_TYPE_OSD, NULL);

  stringstream features_ss;
  int r = check_cluster_features(features, features_ss);
  if (!r)
    return true;

  ss << "Could not change CRUSH: " << features_ss.str();
  return false;
}

bool OSDMonitor::erasure_code_profile_in_use(const map<int64_t, pg_pool_t> &pools,
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
      const string key = i->substr(0, equal);
      equal++;
      const string value = i->substr(equal);
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
	*min_size = erasure_code->get_data_chunk_count();
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
      ErasureCodeInterfaceRef erasure_code;
      err = get_erasure_code(erasure_code_profile, &erasure_code, ss);
      uint32_t desired_stripe_width = g_conf->osd_pool_erasure_code_stripe_width;
      if (err == 0)
	*stripe_width = erasure_code->get_data_chunk_count() *
	  erasure_code->get_chunk_size(desired_stripe_width);
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

int OSDMonitor::prepare_pool_crush_ruleset(const unsigned pool_type,
					   const string &erasure_code_profile,
					   const string &ruleset_name,
					   int *crush_ruleset,
					   ostream *ss)
{

  if (*crush_ruleset < 0) {
    switch (pool_type) {
    case pg_pool_t::TYPE_REPLICATED:
      {
	if (ruleset_name == "") {
	  //Use default ruleset
	  *crush_ruleset = osdmap.crush->get_osd_pool_default_crush_replicated_ruleset(g_ceph_context);
	  if (*crush_ruleset < 0) {
	    // Errors may happen e.g. if no valid ruleset is available
	    *ss << "No suitable CRUSH ruleset exists, check "
                << "'osd pool default crush *' config options";
	    return -ENOENT;
	  }
	} else {
	  return get_crush_ruleset(ruleset_name, crush_ruleset, ss);
	}
      }
      break;
    case pg_pool_t::TYPE_ERASURE:
      {
	int err = crush_ruleset_create_erasure(ruleset_name,
					       erasure_code_profile,
					       crush_ruleset, ss);
	switch (err) {
	case -EALREADY:
	  dout(20) << "prepare_pool_crush_ruleset: ruleset "
		   << ruleset_name << " try again" << dendl;
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
      *ss << "prepare_pool_crush_ruleset: " << pool_type
	 << " is not a known pool type";
      return -EINVAL;
      break;
    }
  } else {
    if (!osdmap.crush->ruleset_exists(*crush_ruleset)) {
      *ss << "CRUSH ruleset " << *crush_ruleset << " not found";
      return -ENOENT;
    }
  }

  return 0;
}

int OSDMonitor::get_crush_ruleset(const string &ruleset_name,
				  int *crush_ruleset,
				  ostream *ss)
{
  int ret;
  ret = osdmap.crush->get_rule_id(ruleset_name);
  if (ret != -ENOENT) {
    // found it, use it
    *crush_ruleset = ret;
  } else {
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    ret = newcrush.get_rule_id(ruleset_name);
    if (ret != -ENOENT) {
      // found it, wait for it to be proposed
      dout(20) << __func__ << ": ruleset " << ruleset_name
	       << " try again" << dendl;
      return -EAGAIN;
    } else {
      //Cannot find it , return error
      *ss << "specified ruleset " << ruleset_name << " doesn't exist";
      return ret;
    }
  }
  return 0;
}

/**
 * @param name The name of the new pool
 * @param auid The auid of the pool owner. Can be -1
 * @param crush_ruleset The crush rule to use. If <0, will use the system default
 * @param crush_ruleset_name The crush rule to use, if crush_rulset <0
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
				 int crush_ruleset,
				 const string &crush_ruleset_name,
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
  r = prepare_pool_crush_ruleset(pool_type, erasure_code_profile,
				 crush_ruleset_name, &crush_ruleset, ss);
  if (r) {
    dout(10) << " prepare_pool_crush_ruleset returns " << r << dendl;
    return r;
  }
  CrushWrapper newcrush;
  _get_pending_crush(newcrush);
  ostringstream err;
  CrushTester tester(newcrush, err);
  r = tester.test_with_crushtool(g_conf->crushtool.c_str(),
				 osdmap.get_max_osd(),
				 g_conf->mon_lease,
				 crush_ruleset);
  if (r) {
    dout(10) << " tester.test_with_crushtool returns " << r
	     << ": " << err.str() << dendl;
    *ss << "crushtool check failed with " << r << ": " << err.str();
    return r;
  }
  unsigned size, min_size;
  r = prepare_pool_size(pool_type, erasure_code_profile, &size, &min_size, ss);
  if (r) {
    dout(10) << " prepare_pool_size returns " << r << dendl;
    return r;
  }
  uint32_t stripe_width = 0;
  r = prepare_pool_stripe_width(pool_type, erasure_code_profile, &stripe_width, ss);
  if (r) {
    dout(10) << " prepare_pool_stripe_width returns " << r << dendl;
    return r;
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

  if (pool_type == pg_pool_t::TYPE_ERASURE) {
    switch (fast_read) {
      case FAST_READ_OFF:
        pi->fast_read = false;
        break;
      case FAST_READ_ON:
        pi->fast_read = true;
        break;
      case FAST_READ_DEFAULT:
        pi->fast_read = g_conf->mon_osd_pool_ec_fast_read;
        break;
      default:
        *ss << "invalid fast_read setting: " << fast_read;
        return -EINVAL;
    }
  }

  pi->size = size;
  pi->min_size = min_size;
  pi->crush_ruleset = crush_ruleset;
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
  ss << "set " << OSDMap::get_flag_string(flag);
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
  ss << "unset " << OSDMap::get_flag_string(flag);
  wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, ss.str(),
						    get_last_committed() + 1));
  return true;
}

int OSDMonitor::parse_osd_id(const char *s, stringstream *pss)
{
  // osd.NNN?
  if (strncmp(s, "osd.", 4) == 0) {
    s += 4;
  }

  // NNN?
  ostringstream ss;
  long id = parse_pos_long(s, &ss);
  if (id < 0) {
    *pss << ss.str();
    return id;
  }
  if (id > 0xffff) {
    *pss << "osd id " << id << " is too large";
    return -ERANGE;
  }
  return id;
}


/**
 * Special setter for crash_replay_interval on a pool.  Equivalent to
 * using prepare_command_pool_set, but in a form convenient for use
 * from MDSMonitor rather than from an administrative command.
 */
int OSDMonitor::set_crash_replay_interval(const int64_t pool_id, const uint32_t cri)
{
  pg_pool_t p;
  if (pending_inc.new_pools.count(pool_id)) {
    p = pending_inc.new_pools[pool_id];
  } else {
    const pg_pool_t *p_ptr = osdmap.get_pg_pool(pool_id);
    if (p_ptr == NULL) {
      return -ENOENT;
    } else {
      p = *p_ptr;
    }
  }

  dout(10) << "Set pool " << pool_id << " crash_replay_interval=" << cri << dendl;
  p.crash_replay_interval = cri;
  p.last_change = pending_inc.epoch;
  pending_inc.new_pools[pool_id] = p;

  return 0;
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
       var == "cache_target_dirty_high_ratio" ||
       var == "cache_min_flush_age" || var == "cache_min_evict_age" ||
       var == "hit_set_grade_decay_rate" || var == "hit_set_search_last_n")) {
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
	 return err;;
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
    string force;
    cmd_getval(g_ceph_context,cmdmap, "force", force);
    if (p.cache_mode != pg_pool_t::CACHEMODE_NONE &&
	force != "--yes-i-really-mean-it") {
      ss << "splits in cache pools must be followed by scrubs and leave sufficient free space to avoid overfilling.  use --yes-i-really-mean-it to force.";
      return -EPERM;
    }
    int expected_osds = MAX(1, MIN(p.get_pg_num(), osdmap.get_num_osds()));
    int64_t new_pgs = n - p.get_pg_num();
    int64_t pgs_per_osd = new_pgs / expected_osds;
    if (pgs_per_osd > g_conf->mon_osd_max_split_count) {
      ss << "specified pg_num " << n << " is too large (creating "
	 << new_pgs << " new PGs on ~" << expected_osds
	 << " OSDs exceeds per-OSD max of " << g_conf->mon_osd_max_split_count
	 << ')';
      return -E2BIG;
    }
    for(set<pg_t>::iterator i = mon->pgmon()->pg_map.creating_pgs.begin();
	i != mon->pgmon()->pg_map.creating_pgs.end();
	++i) {
      if (i->m_pool == static_cast<uint64_t>(pool)) {
	ss << "currently creating pgs, wait";
	return -EBUSY;
      }
    }
    p.set_pg_num(n);
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
    for(set<pg_t>::iterator i = mon->pgmon()->pg_map.creating_pgs.begin();
	i != mon->pgmon()->pg_map.creating_pgs.end();
	++i) {
      if (i->m_pool == static_cast<uint64_t>(pool)) {
	ss << "currently creating pgs, wait";
	return -EBUSY;
      }
    }
    p.set_pgp_num(n);
  } else if (var == "crush_ruleset") {
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    }
    if (!osdmap.crush->ruleset_exists(n)) {
      ss << "crush ruleset " << n << " does not exist";
      return -ENOENT;
    }
    const int64_t poolsize = p.get_size();
    const int64_t minsize = osdmap.crush->get_rule_mask_min_size(n);
    if (poolsize < minsize) {
      ss << "pool size " << poolsize << " is smaller than crush ruleset " 
         << n << " min size " << minsize;
      return -EINVAL;
    }
    const int64_t maxsize = osdmap.crush->get_rule_mask_max_size(n);
    if (poolsize > maxsize) {
      ss << "pool size " << poolsize << " is bigger than crush ruleset " 
         << n << " max size " << maxsize;
      return -EINVAL;
    }
    p.crush_ruleset = n;
  } else if (var == "hashpspool" || var == "nodelete" || var == "nopgchange" ||
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
      if (!(osdmap.get_up_osd_features() & CEPH_FEATURE_OSD_HITSET_GMT)) {
	ss << "not all OSDs support GMT hit set.";
	return -EINVAL;
      }
      p.use_gmt_hitset = true;
    } else {
      ss << "expecting value 'true' or '1'";
      return -EINVAL;
    }
  } else if (var == "debug_fake_ec_pool") {
    if (val == "true" || (interr.empty() && n == 1)) {
      p.flags |= pg_pool_t::FLAG_DEBUG_FAKE_EC_POOL;
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
    if (val == "true" || (interr.empty() && n == 1)) {
      if (p.is_replicated()) {
        ss << "fast read is not supported in replication pool";
        return -EINVAL;
      }
      p.fast_read = true;
    } else if (val == "false" || (interr.empty() && n == 0)) {
      p.fast_read = false;
    }
  } else if (pool_opts_t::is_opt_name(var)) {
    pool_opts_t::opt_desc_t desc = pool_opts_t::get_opt_desc(var);
    switch (desc.type) {
    case pool_opts_t::STR:
      if (val.empty()) {
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
  ss << "set pool " << pool << " " << var << " to " << val;
  p.last_change = pending_inc.epoch;
  pending_inc.new_pools[pool] = p;
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

    if (!validate_crush_against_features(&crush, ss)) {
      err = -EINVAL;
      goto reply;
    }

    // sanity check: test some inputs to make sure this map isn't totally broken
    dout(10) << " testing map" << dendl;
    stringstream ess;
    CrushTester tester(crush, ess);
    // XXX: Use mon_lease as a timeout value for crushtool.
    // If the crushtool consistently takes longer than 'mon_lease' seconds,
    // then we would consistently trigger an election before the command
    // finishes, having a flapping monitor unable to hold quorum.
    int r = tester.test_with_crushtool(g_conf->crushtool.c_str(),
				       osdmap.get_max_osd(),
				       g_conf->mon_lease);
    if (r < 0) {
      derr << "error on crush map: " << ess.str() << dendl;
      ss << "Failed crushmap test: " << ess.str();
      err = r;
      goto reply;
    }

    dout(10) << " result " << ess.str() << dendl;

    pending_inc.crush = data;
    ss << "set crush map";
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
    newcrush.encode(pending_inc.crush);
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
  } else if (osdid_present &&
	     (prefix == "osd crush set" || prefix == "osd crush add")) {
    // <OsdName> is 'osd.<id>' or '<id>', passed as int64_t id
    // osd crush set <OsdName> <weight> <loc1> [<loc2> ...]
    // osd crush add <OsdName> <weight> <loc1> [<loc2> ...]

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
    newcrush.encode(pending_inc.crush);
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
	newcrush.encode(pending_inc.crush);
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
	err = newcrush.move_bucket(g_ceph_context, id, loc);
	if (err >= 0) {
	  ss << "moved item id " << id << " name '" << name << "' to location " << loc << " in crush map";
	  pending_inc.crush.clear();
	  newcrush.encode(pending_inc.crush);
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
    } else {
      int id = newcrush.get_item_id(name);
      if (!newcrush.check_item_loc(g_ceph_context, id, loc, (int *)NULL)) {
	err = newcrush.link_bucket(g_ceph_context, id, loc);
	if (err >= 0) {
	  ss << "linked item id " << id << " name '" << name
             << "' to location " << loc << " in crush map";
	  pending_inc.crush.clear();
	  newcrush.encode(pending_inc.crush);
	} else {
	  ss << "cannot link item id " << id << " name '" << name
             << "' to location " << loc;
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
      bool unlink_only = prefix == "osd crush unlink";
      string ancestor_str;
      if (cmd_getval(g_ceph_context, cmdmap, "ancestor", ancestor_str)) {
	if (!newcrush.name_exists(ancestor_str)) {
	  err = -ENOENT;
	  ss << "ancestor item '" << ancestor_str
	     << "' does not appear in the crush map";
	  break;
	}
	int ancestor = newcrush.get_item_id(ancestor_str);
	err = newcrush.remove_item_under(g_ceph_context, id, ancestor,
					 unlink_only);
      } else {
	err = newcrush.remove_item(g_ceph_context, id, unlink_only);
      }
      if (err == -ENOENT) {
	ss << "item " << id << " does not appear in that position";
	err = 0;
	break;
      }
      if (err == 0) {
	pending_inc.crush.clear();
	newcrush.encode(pending_inc.crush);
	ss << "removed item id " << id << " name '" << name << "' from crush map";
	getline(ss, rs);
	wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						  get_last_committed() + 1));
	return true;
      }
    } while (false);

  } else if (prefix == "osd crush reweight-all") {
    // osd crush reweight <name> <weight>
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    newcrush.reweight(g_ceph_context);
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush);
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
    newcrush.encode(pending_inc.crush);
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
    newcrush.encode(pending_inc.crush);
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
    newcrush.encode(pending_inc.crush);
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
      if (value < 0 || value > 2) {
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
    newcrush.encode(pending_inc.crush);
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
      // The name is uniquely associated to a ruleid and the ruleset it contains
      // From the user point of view, the ruleset is more meaningfull.
      ss << "ruleset " << name << " already exists";
      err = 0;
      goto reply;
    }

    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    if (newcrush.rule_exists(name)) {
      // The name is uniquely associated to a ruleid and the ruleset it contains
      // From the user point of view, the ruleset is more meaningfull.
      ss << "ruleset " << name << " already exists";
      err = 0;
    } else {
      int ruleno = newcrush.add_simple_ruleset(name, root, type, mode,
					       pg_pool_t::TYPE_REPLICATED, &ss);
      if (ruleno < 0) {
	err = ruleno;
	goto reply;
      }

      pending_inc.crush.clear();
      newcrush.encode(pending_inc.crush);
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
      err = normalize_profile(profile_map, &ss);
      if (err)
	goto reply;

      if (osdmap.has_erasure_code_profile(name)) {
	ErasureCodeProfile existing_profile_map =
	  osdmap.get_erasure_code_profile(name);
	err = normalize_profile(existing_profile_map, &ss);
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
	err = normalize_profile(profile_map, &ss);
	if (err)
	  goto reply;
	dout(20) << "erasure code profile set " << profile << "="
		 << profile_map << dendl;
	pending_inc.set_erasure_code_profile(profile, profile_map);
	goto wait;
      }
    }

    int ruleset;
    err = crush_ruleset_create_erasure(name, profile, &ruleset, &ss);
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
      ss << "created ruleset " << name << " at " << ruleset;
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
      if (osdmap.crush_ruleset_in_use(ruleset)) {
	ss << "crush ruleset " << name << " " << ruleset << " is in use";
	err = -EBUSY;
	goto reply;
      }

      err = newcrush.remove_rule(ruleno);
      if (err < 0) {
	goto reply;
      }

      pending_inc.crush.clear();
      newcrush.encode(pending_inc.crush);
    }
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
      for (int i = newmax; i <= osdmap.get_max_osd(); i++) {
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

  } else if (prefix == "osd pause") {
    return prepare_set_flag(op, CEPH_OSDMAP_PAUSERD | CEPH_OSDMAP_PAUSEWR);

  } else if (prefix == "osd unpause") {
    return prepare_unset_flag(op, CEPH_OSDMAP_PAUSERD | CEPH_OSDMAP_PAUSEWR);

  } else if (prefix == "osd set") {
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
      if (osdmap.get_up_osd_features() & CEPH_FEATURE_OSD_BITWISE_HOBJ_SORT) {
	return prepare_set_flag(op, CEPH_OSDMAP_SORTBITWISE);
      } else {
	ss << "not all up OSDs have OSD_BITWISE_HOBJ_SORT feature";
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
    else if (key == "sortbitwise")
      return prepare_unset_flag(op, CEPH_OSDMAP_SORTBITWISE);
    else {
      ss << "unrecognized flag '" << key << "'";
      err = -EINVAL;
    }

  } else if (prefix == "osd cluster_snap") {
    // ** DISABLE THIS FOR NOW **
    ss << "cluster snapshot currently disabled (broken implementation)";
    // ** DISABLE THIS FOR NOW **

  } else if (prefix == "osd down" ||
	     prefix == "osd out" ||
	     prefix == "osd in" ||
	     prefix == "osd rm") {

    bool any = false;

    vector<string> idvec;
    cmd_getval(g_ceph_context, cmdmap, "ids", idvec);
    for (unsigned j = 0; j < idvec.size(); j++) {
      long osd = parse_osd_id(idvec[j].c_str(), &ss);
      if (osd < 0) {
	ss << "invalid osd id" << osd;
	err = -EINVAL;
	continue;
      } else if (!osdmap.exists(osd)) {
	ss << "osd." << osd << " does not exist. ";
	continue;
      }
      if (prefix == "osd down") {
	if (osdmap.is_down(osd)) {
	  ss << "osd." << osd << " is already down. ";
	} else {
	  pending_inc.new_state[osd] = CEPH_OSD_UP;
	  ss << "marked down osd." << osd << ". ";
	  any = true;
	}
      } else if (prefix == "osd out") {
	if (osdmap.is_out(osd)) {
	  ss << "osd." << osd << " is already out. ";
	} else {
	  pending_inc.new_weight[osd] = CEPH_OSD_OUT;
	  ss << "marked out osd." << osd << ". ";
	  any = true;
	}
      } else if (prefix == "osd in") {
	if (osdmap.is_in(osd)) {
	  ss << "osd." << osd << " is already in. ";
	} else {
	  pending_inc.new_weight[osd] = CEPH_OSD_IN;
	  ss << "marked in osd." << osd << ". ";
	  any = true;
	}
      } else if (prefix == "osd rm") {
	if (osdmap.is_up(osd)) {
	  if (any)
	    ss << ", ";
          ss << "osd." << osd << " is still up; must be down before removal. ";
	  err = -EBUSY;
	} else {
	  pending_inc.new_state[osd] = osdmap.get_state(osd);
          pending_inc.new_uuid[osd] = uuid_d();
	  pending_metadata_rm.insert(osd);
	  if (any) {
	    ss << ", osd." << osd;
          } else {
	    ss << "removed osd." << osd;
          }
	  any = true;
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
    PGMap& pg_map = mon->pgmon()->pg_map;
    if (!pg_map.pg_stat.count(pgid)) {
      ss << "pg " << pgid << " does not exist";
      err = -ENOENT;
      goto reply;
    }
    if (pending_inc.new_pg_temp.count(pgid)) {
      dout(10) << __func__ << " waiting for pending update on " << pgid << dendl;
      wait_for_finished_proposal(op, new C_RetryMessage(this, op));
      return true;
    }

    vector<string> id_vec;
    vector<int32_t> new_pg_temp;
    if (!cmd_getval(g_ceph_context, cmdmap, "id", id_vec)) {
      ss << "unable to parse 'id' value(s) '"
         << cmd_vartype_stringify(cmdmap["id"]) << "'";
      err = -EINVAL;
      goto reply;
    }
    for (unsigned i = 0; i < id_vec.size(); i++) {
      int32_t osd = parse_osd_id(id_vec[i].c_str(), &ss);
      if (osd < 0) {
        err = -EINVAL;
        goto reply;
      }
      if (!osdmap.exists(osd)) {
        ss << "osd." << osd << " does not exist";
        err = -ENOENT;
        goto reply;
      }

      new_pg_temp.push_back(osd);
    }

    pending_inc.new_pg_temp[pgid] = new_pg_temp;
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
    PGMap& pg_map = mon->pgmon()->pg_map;
    if (!pg_map.pg_stat.count(pgid)) {
      ss << "pg " << pgid << " does not exist";
      err = -ENOENT;
      goto reply;
    }

    string id;
    int32_t osd;
    if (!cmd_getval(g_ceph_context, cmdmap, "id", id)) {
      ss << "unable to parse 'id' value '"
         << cmd_vartype_stringify(cmdmap["id"]) << "'";
      err = -EINVAL;
      goto reply;
    }
    if (strcmp(id.c_str(), "-1")) {
      osd = parse_osd_id(id.c_str(), &ss);
      if (osd < 0) {
        err = -EINVAL;
        goto reply;
      }
      if (!osdmap.exists(osd)) {
        ss << "osd." << osd << " does not exist";
        err = -ENOENT;
        goto reply;
      }
    } else {
      osd = -1;
    }

    if (!g_conf->mon_osd_allow_primary_temp) {
      ss << "you must enable 'mon osd allow primary temp = true' on the mons before you can set primary_temp mappings.  note that this is for developers only: older clients/OSDs will break and there is no feature bit infrastructure in place.";
      err = -EPERM;
      goto reply;
    }

    pending_inc.new_primary_temp[pgid] = osd;
    ss << "set " << pgid << " primary_temp mapping to " << osd;
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
    if (!g_conf->mon_osd_allow_primary_affinity) {
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
    }

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
    } else if (!osdmap.exists(id) || !osdmap.is_down(id)) {
      ss << "osd." << id << " is not down or doesn't exist";
    } else {
      epoch_t e = osdmap.get_info(id).down_at;
      pending_inc.new_lost[id] = e;
      ss << "marked osd lost in epoch " << e;
      getline(ss, rs);
      wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						get_last_committed() + 1));
      return true;
    }

  } else if (prefix == "osd create") {
    int i = -1;

    // optional id provided?
    int64_t id = -1;
    if (cmd_getval(g_ceph_context, cmdmap, "id", id)) {
      if (id < 0) {
	ss << "invalid osd id value '" << id << "'";
	err = -EINVAL;
	goto reply;
      }
      dout(10) << " osd create got id " << id << dendl;
    }

    // optional uuid provided?
    uuid_d uuid;
    string uuidstr;
    if (cmd_getval(g_ceph_context, cmdmap, "uuid", uuidstr)) {
      if (!uuid.parse(uuidstr.c_str())) {
	ss << "invalid uuid value '" << uuidstr << "'";
	err = -EINVAL;
	goto reply;
      }
      dout(10) << " osd create got uuid " << uuid << dendl;
      i = osdmap.identify_osd(uuid);
      if (i >= 0) {
	// osd already exists
	if (id >= 0 && i != id) {
	  ss << "uuid " << uuidstr << " already in use for different id " << i;
	  err = -EINVAL;
	  goto reply;
	}
	err = 0;
	if (f) {
	  f->open_object_section("created_osd");
	  f->dump_int("osdid", i);
	  f->close_section();
	  f->flush(rdata);
	} else {
	  ss << i;
	  rdata.append(ss);
	}
	goto reply;
      }
      // i < 0
      if (id >= 0) {
	if (osdmap.exists(id)) {
	  ss << "id " << id << " already in use and does not match uuid "
	     << uuid;
	  err = -EINVAL;
	  goto reply;
	}
	if (pending_inc.new_state.count(id)) {
	  // osd is about to exist
	  wait_for_finished_proposal(op, new C_RetryMessage(this, op));
	  return true;
	}
	i = id;
      }
      if (pending_inc.identify_osd(uuid) >= 0) {
	// osd is about to exist
	wait_for_finished_proposal(op, new C_RetryMessage(this, op));
	return true;
      }
      if (i >= 0) {
	// raise max_osd
	if (osdmap.get_max_osd() <= i && pending_inc.new_max_osd <= i)
	  pending_inc.new_max_osd = i + 1;
	goto done;
      }
    }

    // allocate a new id
    for (i=0; i < osdmap.get_max_osd(); i++) {
      if (!osdmap.exists(i) &&
	  pending_inc.new_up_client.count(i) == 0 &&
	  (pending_inc.new_state.count(i) == 0 ||
	   (pending_inc.new_state[i] & CEPH_OSD_EXISTS) == 0))
	goto done;
    }

    // raise max_osd
    if (pending_inc.new_max_osd < 0)
      pending_inc.new_max_osd = osdmap.get_max_osd() + 1;
    else
      pending_inc.new_max_osd++;
    i = pending_inc.new_max_osd - 1;

done:
    dout(10) << " creating osd." << i << dendl;
    pending_inc.new_state[i] |= CEPH_OSD_EXISTS | CEPH_OSD_NEW;
    if (!uuid.is_zero())
      pending_inc.new_uuid[i] = uuid;
    if (f) {
      f->open_object_section("created_osd");
      f->dump_int("osdid", i);
      f->close_section();
      f->flush(rdata);
    } else {
      ss << i;
      rdata.append(ss);
    }
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs, rdata,
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
    if (!addr.parse(addrstr.c_str(), 0))
      ss << "unable to parse address " << addrstr;
    else {
      string blacklistop;
      cmd_getval(g_ceph_context, cmdmap, "blacklistop", blacklistop);
      if (blacklistop == "add") {
	utime_t expires = ceph_clock_now(g_ceph_context);
	double d;
	// default one hour
	cmd_getval(g_ceph_context, cmdmap, "expire", d, double(60*60));
	expires += d;

	pending_inc.new_blacklist[addr] = expires;
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
      pp->add_snap(snapname.c_str(), ceph_clock_now(g_ceph_context));
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
      pool_type_str = pg_pool_t::get_default_type();

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

    bool implicit_ruleset_creation = false;
    string ruleset_name;
    cmd_getval(g_ceph_context, cmdmap, "ruleset", ruleset_name);
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
      if (ruleset_name == "") {
	implicit_ruleset_creation = true;
	if (erasure_code_profile == "default") {
	  ruleset_name = "erasure-code";
	} else {
	  dout(1) << "implicitly use ruleset named after the pool: "
		<< poolstr << dendl;
	  ruleset_name = poolstr;
	}
      }
    } else {
      //NOTE:for replicated pool,cmd_map will put ruleset_name to erasure_code_profile field
      ruleset_name = erasure_code_profile;
    }

    if (!implicit_ruleset_creation && ruleset_name != "") {
      int ruleset;
      err = get_crush_ruleset(ruleset_name, &ruleset, &ss);
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
			   ruleset_name,
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

  } else if (prefix == "osd pool delete") {
    // osd pool delete <poolname> <poolname again> --yes-i-really-really-mean-it
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

    if (poolstr2 != poolstr || sure != "--yes-i-really-really-mean-it") {
      ss << "WARNING: this will *PERMANENTLY DESTROY* all data stored in pool " << poolstr
	 << ".  If you are *ABSOLUTELY CERTAIN* that is what you want, pass the pool name *twice*, "
	 << "followed by --yes-i-really-really-mean-it.";
      err = -EPERM;
      goto reply;
    }
    err = _prepare_remove_pool(pool, &ss);
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
    const pool_stat_t& tier_stats =
      mon->pgmon()->pg_map.get_pg_pool_sum_stat(tierpool_id);
    if (tier_stats.stats.sum.num_objects != 0 &&
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
  } else if (prefix == "osd tier remove") {
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
    np->last_force_op_resend = pending_inc.epoch;
    ss << "overlay for '" << poolstr << "' is now (or already was) '" << overlaypoolstr << "'";
    if (overlay_p->cache_mode == pg_pool_t::CACHEMODE_NONE)
      ss <<" (WARNING: overlay pool cache_mode is still NONE)";
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, ss.str(),
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd tier remove-overlay") {
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
    np->clear_read_tier();
    np->clear_write_tier();
    np->last_force_op_resend = pending_inc.epoch;
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
     *  readproxy:   Writes are in writeback mode, Reads are in proxy mode
     *
     * Hence, these are the allowed transitions:
     *
     *  none -> any
     *  forward -> readforward || readproxy || writeback || any IF num_objects_dirty == 0
     *  readforward -> forward || readproxy || writeback || any IF num_objects_dirty == 0
     *  readproxy -> forward || readforward || writeback || any IF num_objects_dirty == 0
     *  writeback -> readforward || readproxy || forward
     *  readonly -> any
     */

    // We check if the transition is valid against the current pool mode, as
    // it is the only committed state thus far.  We will blantly squash
    // whatever mode is on the pending state.

    if (p->cache_mode == pg_pool_t::CACHEMODE_WRITEBACK &&
        (mode != pg_pool_t::CACHEMODE_FORWARD &&
	  mode != pg_pool_t::CACHEMODE_READFORWARD &&
	  mode != pg_pool_t::CACHEMODE_READPROXY)) {
      ss << "unable to set cache-mode '" << pg_pool_t::get_cache_mode_name(mode)
         << "' on a '" << pg_pool_t::get_cache_mode_name(p->cache_mode)
         << "' pool; only '"
         << pg_pool_t::get_cache_mode_name(pg_pool_t::CACHEMODE_FORWARD)
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
	  mode != pg_pool_t::CACHEMODE_READPROXY)) ||

        (p->cache_mode == pg_pool_t::CACHEMODE_READPROXY &&
        (mode != pg_pool_t::CACHEMODE_WRITEBACK &&
	  mode != pg_pool_t::CACHEMODE_FORWARD &&
	  mode != pg_pool_t::CACHEMODE_READFORWARD)) ||

        (p->cache_mode == pg_pool_t::CACHEMODE_FORWARD &&
        (mode != pg_pool_t::CACHEMODE_WRITEBACK &&
	  mode != pg_pool_t::CACHEMODE_READFORWARD &&
	  mode != pg_pool_t::CACHEMODE_READPROXY))) {

      const pool_stat_t& tier_stats =
        mon->pgmon()->pg_map.get_pg_pool_sum_stat(pool_id);

      if (tier_stats.stats.sum.num_objects_dirty > 0) {
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
    const pool_stat_t& tier_stats =
      mon->pgmon()->pg_map.get_pg_pool_sum_stat(tierpool_id);
    if (tier_stats.stats.sum.num_objects != 0) {
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
      ss << "unrecognized field '" << field << "'; max_bytes of max_objects";
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

  } else if (prefix == "osd reweight-by-utilization") {
    int64_t oload;
    cmd_getval(g_ceph_context, cmdmap, "oload", oload, int64_t(120));
    string out_str;
    err = reweight_by_utilization(oload, out_str, false, NULL);
    if (err < 0) {
      ss << "FAILED reweight-by-utilization: " << out_str;
    } else if (err == 0) {
      ss << "no change: " << out_str;
    } else {
      ss << "SUCCESSFUL reweight-by-utilization: " << out_str;
      getline(ss, rs);
      wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						get_last_committed() + 1));
      return true;
    }
  } else if (prefix == "osd reweight-by-pg") {
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
    string out_str;
    err = reweight_by_utilization(oload, out_str, true,
				  pools.empty() ? NULL : &pools);
    if (err < 0) {
      ss << "FAILED reweight-by-pg: " << out_str;
    } else if (err == 0) {
      ss << "no change: " << out_str;
    } else {
      ss << "SUCCESSFUL reweight-by-pg: " << out_str;
      getline(ss, rs);
      wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, 0, rs,
						get_last_committed() + 1));
      return true;
    }
  } else if (prefix == "osd thrash") {
    int64_t num_epochs;
    cmd_getval(g_ceph_context, cmdmap, "num_epochs", num_epochs, int64_t(0));
    // thrash_map is a member var
    thrash_map = num_epochs;
    ss << "will thrash map for " << thrash_map << " epochs";
    ret = thrash();
    err = 0;
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
  if (m->op == POOL_OP_CREATE)
    return preprocess_pool_op_create(op);

  if (!osdmap.get_pg_pool(m->pool)) {
    dout(10) << "attempt to delete non-existent pool id " << m->pool << dendl;
    _pool_op_reply(op, 0, osdmap.get_epoch());
    return true;
  }

  // check if the snap and snapname exists
  bool snap_exists = false;
  const pg_pool_t *p = osdmap.get_pg_pool(m->pool);
  if (p->snap_exists(m->name.c_str()))
    snap_exists = true;

  switch (m->op) {
  case POOL_OP_CREATE_SNAP:
    if (p->is_unmanaged_snaps_mode()) {
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
    assert(0);
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
      pp.add_snap(m->name.c_str(), ceph_clock_now(g_ceph_context));
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
    assert(0);
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

int OSDMonitor::_check_remove_pool(int64_t pool, const pg_pool_t *p,
				   ostream *ss)
{
  const string& poolstr = osdmap.get_pool_name(pool);

  // If the Pool is in use by CephFS, refuse to delete it
  MDSMap const &pending_mdsmap = mon->mdsmon()->pending_mdsmap;
  if (pending_mdsmap.pool_in_use(pool)) {
    *ss << "pool '" << poolstr << "' is in use by CephFS";
    return -EBUSY;
  }

  if (p->tier_of >= 0) {
    *ss << "pool '" << poolstr << "' is a tier of '"
	<< osdmap.get_pool_name(p->tier_of) << "'";
    return -EBUSY;
  }
  if (!p->tiers.empty()) {
    *ss << "pool '" << poolstr << "' has tiers";
    for(std::set<uint64_t>::iterator i = p->tiers.begin(); i != p->tiers.end(); ++i) {
      *ss << " " << osdmap.get_pool_name(*i);
    }
    return -EBUSY;
  }

  if (!g_conf->mon_allow_pool_delete) {
    *ss << "pool deletion is disabled; you must first set the mon_allow_pool_delete config option to true before you can destroy a pool";
    return -EPERM;
  }

  if (p->has_flag(pg_pool_t::FLAG_NODELETE)) {
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

  const MDSMap &pending_mdsmap = mon->mdsmon()->pending_mdsmap;
  if (pending_mdsmap.pool_in_use(tier_pool_id)) {
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
  const MDSMap &pending_mdsmap = mon->mdsmon()->pending_mdsmap;
  if (pending_mdsmap.pool_in_use(base_pool_id)) {
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

int OSDMonitor::_prepare_remove_pool(int64_t pool, ostream *ss)
{
  dout(10) << "_prepare_remove_pool " << pool << dendl;
  const pg_pool_t *p = osdmap.get_pg_pool(pool);
  int r = _check_remove_pool(pool, p, ss);
  if (r < 0)
    return r;

  if (pending_inc.new_pools.count(pool)) {
    // if there is a problem with the pending info, wait and retry
    // this op.
    pg_pool_t *p = &pending_inc.new_pools[pool];
    int r = _check_remove_pool(pool, p, ss);
    if (r < 0)
      return -EAGAIN;
  }

  if (pending_inc.old_pools.count(pool)) {
    dout(10) << "_prepare_remove_pool " << pool << " already pending removal"
	     << dendl;
    return 0;
  }

  // remove
  pending_inc.old_pools.insert(pool);

  // remove any pg_temp mappings for this pool too
  for (map<pg_t,vector<int32_t> >::iterator p = osdmap.pg_temp->begin();
       p != osdmap.pg_temp->end();
       ++p) {
    if (p->first.pool() == (uint64_t)pool) {
      dout(10) << "_prepare_remove_pool " << pool << " removing obsolete pg_temp "
	       << p->first << dendl;
      pending_inc.new_pg_temp[p->first].clear();
    }
  }
  for (map<pg_t,int32_t>::iterator p = osdmap.primary_temp->begin();
      p != osdmap.primary_temp->end();
      ++p) {
    if (p->first.pool() == (uint64_t)pool) {
      dout(10) << "_prepare_remove_pool " << pool
               << " removing obsolete primary_temp" << p->first << dendl;
      pending_inc.new_primary_temp[p->first] = -1;
    }
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
  int ret = _prepare_remove_pool(m->pool, &ss);
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
