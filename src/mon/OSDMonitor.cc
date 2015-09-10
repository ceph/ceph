// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include <sstream>

#include "OSDMonitor.h"
#include "Monitor.h"
#include "MDSMonitor.h"
#include "PGMonitor.h"

#include "MonitorDBStore.h"

#include "crush/CrushWrapper.h"
#include "crush/CrushTester.h"

#include "messages/MOSDFailure.h"
#include "messages/MOSDMarkMeDown.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDBoot.h"
#include "messages/MOSDAlive.h"
#include "messages/MPoolOp.h"
#include "messages/MPoolOpReply.h"
#include "messages/MOSDPGTemp.h"
#include "messages/MMonCommand.h"
#include "messages/MRemoveSnaps.h"
#include "messages/MOSDScrub.h"

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

  // encode into pending incremental
  newmap.encode(pending_inc.fullmap, mon->quorum_features);
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
    MonitorDBStore::Transaction t;
    put_version_latest_full(&t, latest_full);
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
  MonitorDBStore::Transaction *t = NULL;
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

    if (t == NULL)
      t = new MonitorDBStore::Transaction;

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
    osdmap.encode(full_bl, f);
    tx_size += full_bl.length();

    put_version_full(t, osdmap.epoch, full_bl);
    put_version_latest_full(t, osdmap.epoch);

    // share
    dout(1) << osdmap << dendl;

    if (osdmap.epoch == 1) {
      t->erase("mkfs", "osdmap");
    }

    if (tx_size > g_conf->mon_sync_max_payload_size*2) {
      mon->store->apply_transaction(*t);
      delete t;
      t = NULL;
      tx_size = 0;
    }
  }

  if (t != NULL) {
    mon->store->apply_transaction(*t);
    delete t;
  }

  for (int o = 0; o < osdmap.get_max_osd(); o++) {
    if (osdmap.is_down(o)) {
      // invalidate osd_epoch cache
      osd_epoch.erase(o);

      // populate down -> out map
      if (osdmap.is_in(o) &&
	  down_pending_out.count(o) == 0) {
	dout(10) << " adding osd." << o << " to down_pending_out map" << dendl;
	down_pending_out[o] = ceph_clock_now(g_ceph_context);
      }
    }
  }
  // blow away any osd_epoch items beyond max_osd
  map<int,epoch_t>::iterator p = osd_epoch.upper_bound(osdmap.get_max_osd());
  while (p != osd_epoch.end()) {
    osd_epoch.erase(p++);
  }

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
    mon->clog.info() << "osdmap " << osdmap << "\n"; 

  if (!mon->is_leader()) {
    list<MOSDFailure*> ls;
    take_all_failures(ls);
    while (!ls.empty()) {
      dispatch(ls.front());
      ls.pop_front();
    }
  }
}

void OSDMonitor::on_shutdown()
{
  dout(10) << __func__ << dendl;

  // discard failure info, waiters
  list<MOSDFailure*> ls;
  take_all_failures(ls);
  while (!ls.empty()) {
    ls.front()->put();
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
int OSDMonitor::reweight_by_utilization(int oload, std::string& out_str)
{
  if (oload <= 100) {
    ostringstream oss;
    oss << "You must give a percentage higher than 100. "
      "The reweighting threshold will be calculated as <average-utilization> "
      "times <input-percentage>. For example, an argument of 200 would "
      "reweight OSDs which are twice as utilized as the average OSD.\n";
    out_str = oss.str();
    dout(0) << "reweight_by_utilization: " << out_str << dendl;
    return -EINVAL;
  }

  // Avoid putting a small number (or 0) in the denominator when calculating
  // average_util
  const PGMap &pgm = mon->pgmon()->pg_map;
  if (pgm.osd_sum.kb < 1024) {
    ostringstream oss;
    oss << "Refusing to reweight: we only have " << pgm.osd_sum << " kb "
      "across all osds!\n";
    out_str = oss.str();
    dout(0) << "reweight_by_utilization: " << out_str << dendl;
    return -EDOM;
  }

  if (pgm.osd_sum.kb_used < 5 * 1024) {
    ostringstream oss;
    oss << "Refusing to reweight: we only have " << pgm.osd_sum << " kb "
      "used across all osds!\n";
    out_str = oss.str();
    dout(0) << "reweight_by_utilization: " << out_str << dendl;
    return -EDOM;
  }

  float average_util = pgm.osd_sum.kb_used;
  average_util /= pgm.osd_sum.kb;
  float overload_util = average_util * oload / 100.0;

  ostringstream oss;
  char buf[128];
  snprintf(buf, sizeof(buf), "average_util: %04f, overload_util: %04f. ",
	   average_util, overload_util);
  oss << buf;
  std::string sep;
  oss << "overloaded osds: ";
  bool changed = false;
  for (ceph::unordered_map<int,osd_stat_t>::const_iterator p = pgm.osd_stat.begin();
       p != pgm.osd_stat.end();
       ++p) {
    float util = p->second.kb_used;
    util /= p->second.kb;
    if (util >= overload_util) {
      sep = ", ";
      // Assign a lower weight to overloaded OSDs. The current weight
      // is a factor to take into account the original weights,
      // to represent e.g. differing storage capacities
      unsigned weight = osdmap.get_weight(p->first);
      unsigned new_weight = (unsigned)((average_util / util) * (float)weight);
      pending_inc.new_weight[p->first] = new_weight;
      char buf[128];
      snprintf(buf, sizeof(buf), "%d [%04f -> %04f]", p->first,
	       (float)weight / (float)0x10000,
	       (float)new_weight / (float)0x10000);
      oss << buf << sep;
      changed = true;
    }
  }
  if (sep.empty()) {
    oss << "(none)";
  }
  out_str = oss.str();
  dout(0) << "reweight_by_utilization: finished with " << out_str << dendl;
  return changed;
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

/**
 * @note receiving a transaction in this function gives a fair amount of
 * freedom to the service implementation if it does need it. It shouldn't.
 */
void OSDMonitor::encode_pending(MonitorDBStore::Transaction *t)
{
  dout(10) << "encode_pending e " << pending_inc.epoch
	   << dendl;
  
  // finalize up pending_inc
  pending_inc.modified = ceph_clock_now(g_ceph_context);

  int r = pending_inc.propagate_snaps_to_tiers(g_ceph_context, osdmap);
  assert(r == 0);

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

  // encode
  assert(get_last_committed() + 1 == pending_inc.epoch);
  ::encode(pending_inc, bl, mon->quorum_features);

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

int OSDMonitor::dump_osd_metadata(int osd, Formatter *f, ostream *err)
{
  bufferlist bl;
  int r = mon->store->get(OSD_METADATA_PREFIX, stringify(osd), bl);
  if (r < 0)
    return r;
  map<string,string> m;
  try {
    bufferlist::iterator p = bl.begin();
    ::decode(m, p);
  }
  catch (buffer::error& e) {
    if (err)
      *err << "osd." << osd << " metadata is corrupt";
    return -EIO;
  }
  for (map<string,string>::iterator p = m.begin(); p != m.end(); ++p)
    f->dump_string(p->first.c_str(), p->second);
  return 0;
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
  mon->messenger->send_message(m, s->inst);
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

void OSDMonitor::encode_trim_extra(MonitorDBStore::Transaction *tx, version_t first)
{
  dout(10) << __func__ << " including full map for e " << first << dendl;
  bufferlist bl;
  get_version_full(first, bl);
  put_version_full(tx, first, bl);
}

// -------------

bool OSDMonitor::preprocess_query(PaxosServiceMessage *m)
{
  dout(10) << "preprocess_query " << *m << " from " << m->get_orig_source_inst() << dendl;

  switch (m->get_type()) {
    // READs
  case MSG_MON_COMMAND:
    return preprocess_command(static_cast<MMonCommand*>(m));

    // damp updates
  case MSG_OSD_MARK_ME_DOWN:
    return preprocess_mark_me_down(static_cast<MOSDMarkMeDown*>(m));
  case MSG_OSD_FAILURE:
    return preprocess_failure(static_cast<MOSDFailure*>(m));
  case MSG_OSD_BOOT:
    return preprocess_boot(static_cast<MOSDBoot*>(m));
  case MSG_OSD_ALIVE:
    return preprocess_alive(static_cast<MOSDAlive*>(m));
  case MSG_OSD_PGTEMP:
    return preprocess_pgtemp(static_cast<MOSDPGTemp*>(m));

  case CEPH_MSG_POOLOP:
    return preprocess_pool_op(static_cast<MPoolOp*>(m));

  case MSG_REMOVE_SNAPS:
    return preprocess_remove_snaps(static_cast<MRemoveSnaps*>(m));
    
  default:
    assert(0);
    m->put();
    return true;
  }
}

bool OSDMonitor::prepare_update(PaxosServiceMessage *m)
{
  dout(7) << "prepare_update " << *m << " from " << m->get_orig_source_inst() << dendl;
  
  switch (m->get_type()) {
    // damp updates
  case MSG_OSD_MARK_ME_DOWN:
    return prepare_mark_me_down(static_cast<MOSDMarkMeDown*>(m));
  case MSG_OSD_FAILURE:
    return prepare_failure(static_cast<MOSDFailure*>(m));
  case MSG_OSD_BOOT:
    return prepare_boot(static_cast<MOSDBoot*>(m));
  case MSG_OSD_ALIVE:
    return prepare_alive(static_cast<MOSDAlive*>(m));
  case MSG_OSD_PGTEMP:
    return prepare_pgtemp(static_cast<MOSDPGTemp*>(m));

  case MSG_MON_COMMAND:
    return prepare_command(static_cast<MMonCommand*>(m));
    
  case CEPH_MSG_POOLOP:
    return prepare_pool_op(static_cast<MPoolOp*>(m));

  case MSG_REMOVE_SNAPS:
    return prepare_remove_snaps(static_cast<MRemoveSnaps*>(m));

  default:
    assert(0);
    m->put();
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


bool OSDMonitor::preprocess_failure(MOSDFailure *m)
{
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
	osdmap.is_down(from)) {
      dout(5) << "preprocess_failure from dead osd." << from << ", ignoring" << dendl;
      send_incremental(m, m->get_epoch()+1);
      goto didit;
    }
  }
  

  // weird?
  if (!osdmap.have_inst(badboy)) {
    dout(5) << "preprocess_failure dne(/dup?): " << m->get_target() << ", from " << m->get_orig_source_inst() << dendl;
    if (m->get_epoch() < osdmap.get_epoch())
      send_incremental(m, m->get_epoch()+1);
    goto didit;
  }
  if (osdmap.get_inst(badboy) != m->get_target()) {
    dout(5) << "preprocess_failure wrong osd: report " << m->get_target() << " != map's " << osdmap.get_inst(badboy)
	    << ", from " << m->get_orig_source_inst() << dendl;
    if (m->get_epoch() < osdmap.get_epoch())
      send_incremental(m, m->get_epoch()+1);
    goto didit;
  }

  // already reported?
  if (osdmap.is_down(badboy) ||
      osdmap.get_up_from(badboy) > m->get_epoch()) {
    dout(5) << "preprocess_failure dup/old: " << m->get_target() << ", from " << m->get_orig_source_inst() << dendl;
    if (m->get_epoch() < osdmap.get_epoch())
      send_incremental(m, m->get_epoch()+1);
    goto didit;
  }

  if (!can_mark_down(badboy)) {
    dout(5) << "preprocess_failure ignoring report of " << m->get_target() << " from " << m->get_orig_source_inst() << dendl;
    goto didit;
  }

  dout(10) << "preprocess_failure new: " << m->get_target() << ", from " << m->get_orig_source_inst() << dendl;
  return false;

 didit:
  m->put();
  return true;
}

class C_AckMarkedDown : public Context {
  OSDMonitor *osdmon;
  MOSDMarkMeDown *m;
public:
  C_AckMarkedDown(
    OSDMonitor *osdmon,
    MOSDMarkMeDown *m)
    : osdmon(osdmon), m(m) {}

  void finish(int) {
    osdmon->mon->send_reply(
      m,
      new MOSDMarkMeDown(
	m->fsid,
	m->get_target(),
	m->get_epoch(),
	m->ack));
  }
  ~C_AckMarkedDown() {
    m->put();
  }
};

bool OSDMonitor::preprocess_mark_me_down(MOSDMarkMeDown *m)
{
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
    send_incremental(m, m->get_epoch()+1);
    goto reply;
  }

  // no down might be set
  if (!can_mark_down(requesting_down))
    goto reply;

  dout(10) << "MOSDMarkMeDown for: " << m->get_target() << dendl;
  return false;

 reply:
  Context *c(new C_AckMarkedDown(this, m));
  c->complete(0);
  return true;
}

bool OSDMonitor::prepare_mark_me_down(MOSDMarkMeDown *m)
{
  int target_osd = m->get_target().name.num();

  assert(osdmap.is_up(target_osd));
  assert(osdmap.get_addr(target_osd) == m->get_target().addr);

  mon->clog.info() << "osd." << target_osd << " marked itself down\n";
  pending_inc.new_state[target_osd] = CEPH_OSD_UP;
  wait_for_finished_proposal(new C_AckMarkedDown(this, m));
  return true;
}

bool OSDMonitor::can_mark_down(int i)
{
  if (osdmap.test_flag(CEPH_OSDMAP_NODOWN)) {
    dout(5) << "can_mark_down NODOWN flag set, will not mark osd." << i << " down" << dendl;
    return false;
  }
  int up = osdmap.get_num_up_osds() - pending_inc.get_net_marked_down(&osdmap);
  float up_ratio = (float)up / (float)osdmap.get_num_osds();
  if (up_ratio < g_conf->mon_osd_min_up_ratio) {
    dout(5) << "can_mark_down current up_ratio " << up_ratio << " < min "
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
  int in = osdmap.get_num_in_osds() - pending_inc.get_net_marked_out(&osdmap);
  float in_ratio = (float)in / (float)osdmap.get_num_osds();
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
    check_failure(now, p->first, p->second);
  }
}

bool OSDMonitor::check_failure(utime_t now, int target_osd, failure_info_t& fi)
{
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
      const osd_xinfo_t& xi = osdmap.get_xinfo(p->first);
      utime_t elapsed = now - xi.down_stamp;
      double decay = exp((double)elapsed * decay_k);
      peer_grace += decay * (double)xi.laggy_interval * xi.laggy_probability;
    }
    peer_grace /= (double)fi.reporters.size();
    grace += peer_grace;
  }

  dout(10) << " osd." << target_osd << " has "
	   << fi.reporters.size() << " reporters and "
	   << fi.num_reports << " reports, "
	   << grace << " grace (" << orig_grace << " + " << my_grace << " + " << peer_grace << "), max_failed_since " << max_failed_since
	   << dendl;

  // already pending failure?
  if (pending_inc.new_state.count(target_osd) &&
      pending_inc.new_state[target_osd] & CEPH_OSD_UP) {
    dout(10) << " already pending failure" << dendl;
    return true;
  }

  if (failed_for >= grace &&
      ((int)fi.reporters.size() >= g_conf->mon_osd_min_down_reporters) &&
      (fi.num_reports >= g_conf->mon_osd_min_down_reports)) {
    dout(1) << " we have enough reports/reporters to mark osd." << target_osd << " down" << dendl;
    pending_inc.new_state[target_osd] = CEPH_OSD_UP;

    mon->clog.info() << osdmap.get_inst(target_osd) << " failed ("
		     << fi.num_reports << " reports from " << (int)fi.reporters.size() << " peers after "
		     << failed_for << " >= grace " << grace << ")\n";
    return true;
  }
  return false;
}

bool OSDMonitor::prepare_failure(MOSDFailure *m)
{
  dout(1) << "prepare_failure " << m->get_target() << " from " << m->get_orig_source_inst()
          << " is reporting failure:" << m->if_osd_failed() << dendl;

  int target_osd = m->get_target().name.num();
  int reporter = m->get_orig_source().num();
  assert(osdmap.is_up(target_osd));
  assert(osdmap.get_addr(target_osd) == m->get_target().addr);

  // calculate failure time
  utime_t now = ceph_clock_now(g_ceph_context);
  utime_t failed_since = m->get_recv_stamp() - utime_t(m->failed_for ? m->failed_for : g_conf->osd_heartbeat_grace, 0);
  
  if (m->if_osd_failed()) {
    // add a report
    mon->clog.debug() << m->get_target() << " reported failed by "
		      << m->get_orig_source_inst() << "\n";
    failure_info_t& fi = failure_info[target_osd];
    MOSDFailure *old = fi.add_report(reporter, failed_since, m);
    if (old) {
      mon->no_reply(old);
      old->put();
    }

    return check_failure(now, target_osd, fi);
  } else {
    // remove the report
    mon->clog.debug() << m->get_target() << " failure report canceled by "
		      << m->get_orig_source_inst() << "\n";
    if (failure_info.count(target_osd)) {
      failure_info_t& fi = failure_info[target_osd];
      list<MOSDFailure*> ls;
      fi.take_report_messages(ls);
      fi.cancel_report(reporter);
      while (!ls.empty()) {
	mon->no_reply(ls.front());
	ls.front()->put();
	ls.pop_front();
      }
      if (fi.reporters.empty()) {
	dout(10) << " removing last failure_info for osd." << target_osd << dendl;
	failure_info.erase(target_osd);
      } else {
	dout(10) << " failure_info for osd." << target_osd << " now "
		 << fi.reporters.size() << " reporters and "
		 << fi.num_reports << " reports" << dendl;
      }
    } else {
      dout(10) << " no failure_info for osd." << target_osd << dendl;
    }
    mon->no_reply(m);
    m->put();
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
      list<MOSDFailure*> ls;
      p->second.take_report_messages(ls);
      failure_info.erase(p++);

      while (!ls.empty()) {
	send_latest(ls.front(), ls.front()->get_epoch());
	ls.pop_front();
      }
    }
  }
}

void OSDMonitor::take_all_failures(list<MOSDFailure*>& ls)
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

bool OSDMonitor::preprocess_boot(MOSDBoot *m)
{
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
    dout(0) << __func__ << " osdmap requires Erasure Codes but osd at "
            << m->get_orig_source_inst()
            << " doesn't announce support -- ignore" << dendl;
    goto ignore;
  }
  
  // already booted?
  if (osdmap.is_up(from) &&
      osdmap.get_inst(from) == m->get_orig_source_inst()) {
    // yup.
    dout(7) << "preprocess_boot dup from " << m->get_orig_source_inst()
	    << " == " << osdmap.get_inst(from) << dendl;
    _booted(m, false);
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
    send_latest(m, m->sb.current_epoch+1);
    return true;
  }

  // noup?
  if (!can_mark_up(from)) {
    dout(7) << "preprocess_boot ignoring boot from " << m->get_orig_source_inst() << dendl;
    send_latest(m, m->sb.current_epoch+1);
    return true;
  }

  dout(10) << "preprocess_boot from " << m->get_orig_source_inst() << dendl;
  return false;

 ignore:
  m->put();
  return true;
}

bool OSDMonitor::prepare_boot(MOSDBoot *m)
{
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
    m->put();
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
    wait_for_finished_proposal(new C_RetryMessage(this, m));
  } else if (pending_inc.new_up_client.count(from)) { //FIXME: should this be using new_up_client?
    // already prepared, just wait
    dout(7) << "prepare_boot already prepared, waiting on " << m->get_orig_source_addr() << dendl;
    wait_for_finished_proposal(new C_RetryMessage(this, m));
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
    wait_for_finished_proposal(new C_Booted(this, m));
  }
  return true;
}

void OSDMonitor::_booted(MOSDBoot *m, bool logit)
{
  dout(7) << "_booted " << m->get_orig_source_inst() 
	  << " w " << m->sb.weight << " from " << m->sb.current_epoch << dendl;

  if (logit) {
    mon->clog.info() << m->get_orig_source_inst() << " boot\n";
  }

  send_latest(m, m->sb.current_epoch+1);
}


// -------------
// alive

bool OSDMonitor::preprocess_alive(MOSDAlive *m)
{
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
    _reply_map(m, m->version);
    return true;
  }

  dout(10) << "preprocess_alive want up_thru " << m->want
	   << " from " << m->get_orig_source_inst() << dendl;
  return false;

 ignore:
  m->put();
  return true;
}

bool OSDMonitor::prepare_alive(MOSDAlive *m)
{
  int from = m->get_orig_source().num();

  if (0) {  // we probably don't care much about these
    mon->clog.debug() << m->get_orig_source_inst() << " alive\n";
  }

  dout(7) << "prepare_alive want up_thru " << m->want << " have " << m->version
	  << " from " << m->get_orig_source_inst() << dendl;
  pending_inc.new_up_thru[from] = m->version;  // set to the latest map the OSD has
  wait_for_finished_proposal(new C_ReplyMap(this, m, m->version));
  return true;
}

void OSDMonitor::_reply_map(PaxosServiceMessage *m, epoch_t e)
{
  dout(7) << "_reply_map " << e
	  << " from " << m->get_orig_source_inst()
	  << dendl;
  send_latest(m, e);
}

// -------------
// pg_temp changes

bool OSDMonitor::preprocess_pgtemp(MOSDPGTemp *m)
{
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

  for (map<pg_t,vector<int> >::iterator p = m->pg_temp.begin(); p != m->pg_temp.end(); ++p) {
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
  _reply_map(m, m->map_epoch);
  return true;

 ignore:
  m->put();
  return true;
}

bool OSDMonitor::prepare_pgtemp(MOSDPGTemp *m)
{
  int from = m->get_orig_source().num();
  dout(7) << "prepare_pgtemp e" << m->map_epoch << " from " << m->get_orig_source_inst() << dendl;
  for (map<pg_t,vector<int> >::iterator p = m->pg_temp.begin(); p != m->pg_temp.end(); ++p) {
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
  wait_for_finished_proposal(new C_ReplyMap(this, m, m->map_epoch));
  return true;
}


// ---

bool OSDMonitor::preprocess_remove_snaps(MRemoveSnaps *m)
{
  dout(7) << "preprocess_remove_snaps " << *m << dendl;

  // check privilege, ignore if failed
  MonSession *session = m->get_session();
  if (!session)
    goto ignore;
  if (!session->is_capable("osd", MON_CAP_R | MON_CAP_W)) {
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
  m->put();
  return true;
}

bool OSDMonitor::prepare_remove_snaps(MRemoveSnaps *m)
{
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

  m->put();
  return true;
}


// ---------------
// map helpers

void OSDMonitor::send_latest(PaxosServiceMessage *m, epoch_t start)
{
  dout(5) << "send_latest to " << m->get_orig_source_inst()
	  << " start " << start << dendl;
  if (start == 0)
    send_full(m);
  else
    send_incremental(m, start);
  m->put();
}


MOSDMap *OSDMonitor::build_latest_full()
{
  MOSDMap *r = new MOSDMap(mon->monmap->fsid, &osdmap);
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

void OSDMonitor::send_full(PaxosServiceMessage *m)
{
  dout(5) << "send_full to " << m->get_orig_source_inst() << dendl;
  mon->send_reply(m, build_latest_full());
}

/* TBH, I'm fairly certain these two functions could somehow be using a single
 * helper function to do the heavy lifting. As this is not our main focus right
 * now, I'm leaving it to the next near-future iteration over the services'
 * code. We should not forget it though.
 *
 * TODO: create a helper function and get rid of the duplicated code.
 */
void OSDMonitor::send_incremental(PaxosServiceMessage *req, epoch_t first)
{
  dout(5) << "send_incremental [" << first << ".." << osdmap.get_epoch() << "]"
	  << " to " << req->get_orig_source_inst()
	  << dendl;

  int osd = -1;
  if (req->get_source().is_osd()) {
    osd = req->get_source().num();
    map<int,epoch_t>::iterator p = osd_epoch.find(osd);
    if (p != osd_epoch.end()) {
      if (first <= p->second) {
	dout(10) << __func__ << " osd." << osd << " should already have epoch "
		 << p->second << dendl;
	first = p->second + 1;
	if (first > osdmap.get_epoch())
	  return;
      }
    }
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
    m->oldest_map = first;
    m->newest_map = osdmap.get_epoch();
    m->maps[first] = bl;
    mon->send_reply(req, m);

    if (osd >= 0)
      osd_epoch[osd] = osdmap.get_epoch();
    return;
  }

  // send some maps.  it may not be all of them, but it will get them
  // started.
  epoch_t last = MIN(first + g_conf->osd_map_message_max, osdmap.get_epoch());
  MOSDMap *m = build_incremental(first, last);
  m->oldest_map = get_first_committed();
  m->newest_map = osdmap.get_epoch();
  mon->send_reply(req, m);

  if (osd >= 0)
    osd_epoch[osd] = last;
}

void OSDMonitor::send_incremental(epoch_t first, entity_inst_t& dest, bool onetime)
{
  dout(5) << "send_incremental [" << first << ".." << osdmap.get_epoch() << "]"
	  << " to " << dest << dendl;

  if (first < get_first_committed()) {
    first = get_first_committed();
    bufferlist bl;
    int err = get_version_full(first, bl);
    assert(err == 0);
    assert(bl.length());

    dout(20) << "send_incremental starting with base full "
	     << first << " " << bl.length() << " bytes" << dendl;

    MOSDMap *m = new MOSDMap(osdmap.get_fsid());
    m->oldest_map = first;
    m->newest_map = osdmap.get_epoch();
    m->maps[first] = bl;
    mon->messenger->send_message(m, dest);
    first++;
  }

  while (first <= osdmap.get_epoch()) {
    epoch_t last = MIN(first + g_conf->osd_map_message_max, osdmap.get_epoch());
    MOSDMap *m = build_incremental(first, last);
    mon->messenger->send_message(m, dest);
    first = last + 1;
    if (onetime)
      break;
  }
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
      send_incremental(sub->next, sub->session->inst, sub->incremental_onetime);
    else
      mon->messenger->send_message(build_latest_full(),
				   sub->session->inst);
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

	  do_propose = true;
	
	  mon->clog.info() << "osd." << o << " out (down for " << down << ")\n";
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
	mon->clog.info() << "osd." << i << " marked down after no pg stats for " << diff << "seconds\n";
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
			    list<pair<health_status_t,string> > *detail) const
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

bool OSDMonitor::preprocess_command(MMonCommand *m)
{
  int r = 0;
  bufferlist rdata;
  stringstream ss, ds;

  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon->reply_command(m, -EINVAL, rs, get_last_committed());
    return true;
  }

  MonSession *session = m->get_session();
  if (!session) {
    mon->reply_command(m, -EACCES, "access denied", rdata, get_last_committed());
    return true;
  }

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  string format;
  cmd_getval(g_ceph_context, cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(new_formatter(format));

  if (prefix == "osd stat") {
    osdmap.print_summary(f.get(), ds);
    if (f)
      f->flush(rdata);
    else
      rdata.append(ds);
  }
  else if (prefix == "osd dump" ||
	   prefix == "osd tree" ||
	   prefix == "osd ls" ||
	   prefix == "osd getmap" ||
	   prefix == "osd getcrushmap" ||
	   prefix == "osd perf") {
    string val;

    epoch_t epoch = 0;
    int64_t epochnum;
    cmd_getval(g_ceph_context, cmdmap, "epoch", epochnum, (int64_t)0);
    epoch = epochnum;

    OSDMap *p = &osdmap;
    if (epoch) {
      bufferlist b;
      int err = get_version_full(epoch, b);
      if (err == -ENOENT) {
	r = -ENOENT;
        ss << "there is no map for epoch " << epoch;
	goto reply;
      }
      assert(err == 0);
      assert(b.length());
      p = new OSDMap;
      p->decode(b);
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
	p->print_tree(NULL, f.get());
	f->close_section();
	f->flush(ds);
      } else {
	p->print_tree(&ds, NULL);
      } 
      rdata.append(ds);
    } else if (prefix == "osd getmap") {
      p->encode(rdata, m->get_connection()->get_features());
      ss << "got osdmap epoch " << p->get_epoch();
    } else if (prefix == "osd getcrushmap") {
      p->crush->encode(rdata);
      ss << "got crush map from osdmap epoch " << p->get_epoch();
    } else if (prefix == "osd perf") {
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
    if (p != &osdmap)
      delete p;
  } else if (prefix == "osd getmaxosd") {
    if (f) {
      f->open_object_section("getmaxosd");
      f->dump_int("epoch", osdmap.get_epoch());
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
    cmd_getval(g_ceph_context, cmdmap, "format", format, string("json-pretty"));
    boost::scoped_ptr<Formatter> f(new_formatter(format));
    if (!f)
      f.reset(new_formatter("json-pretty"));

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
    cmd_getval(g_ceph_context, cmdmap, "format", format, string("json-pretty"));
    boost::scoped_ptr<Formatter> f(new_formatter(format));
    if (!f)
      f.reset(new_formatter("json-pretty"));
    f->open_object_section("osd_metadata");
    r = dump_osd_metadata(osd, f.get(), &ss);
    if (r < 0)
      goto reply;
    f->close_section();
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
      f->dump_int("epoch", osdmap.get_epoch());
      f->dump_string("pool", poolstr);
      f->dump_int("pool_id", pool);
      f->dump_stream("objname") << fullobjname;
      f->dump_stream("raw_pgid") << pgid;
      f->dump_stream("pgid") << mpgid;
      f->dump_stream("up") << up;
      f->dump_int("up_primary", up_p);
      f->dump_stream("acting") << acting;
      f->dump_int("acting_primary", acting_p);
      f->close_section(); // osd_map
      f->flush(rdata);
    } else {
      ds << "osdmap e" << osdmap.get_epoch()
        << " pool '" << poolstr << "' (" << pool << ")"
        << " object '" << fullobjname << "' ->"
        << " pg " << pgid << " (" << mpgid << ")"
        << " -> up (" << up << ", p" << up_p << ") acting ("
        << acting << ", p" << acting_p << ")";
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

    if (!p->is_tier() &&
        (var == "hit_set_type" || var == "hit_set_period" ||
         var == "hit_set_count" || var == "hit_set_fpp" ||
         var == "target_max_objects" || var == "target_max_bytes" ||
         var == "cache_target_full_ratio" ||
         var == "cache_target_dirty_ratio" ||
         var == "cache_min_flush_age" || var == "cache_min_evict_age")) {
      ss << "pool '" << poolstr
         << "' is not a tier pool: variable not applicable";
      r = -EACCES;
      goto reply;
    }

    if (!p->is_erasure() && var == "erasure_code_profile") {
      ss << "pool '" << poolstr
         << "' is not a erasure pool: variable not applicable";
      r = -EACCES;
      goto reply;
    }

    if (f) {
      f->open_object_section("pool");
      f->dump_string("pool", poolstr);
      f->dump_int("pool_id", pool);

      if (var == "pg_num") {
        f->dump_int("pg_num", p->get_pg_num());
      } else if (var == "pgp_num") {
        f->dump_int("pgp_num", p->get_pgp_num());
      } else if (var == "auid") {
        f->dump_int("auid", p->get_auid());
      } else if (var == "size") {
        f->dump_int("size", p->get_size());
      } else if (var == "min_size") {
        f->dump_int("min_size", p->get_min_size());
      } else if (var == "crash_replay_interval") {
        f->dump_int("crash_replay_interval", p->get_crash_replay_interval());
      } else if (var == "crush_ruleset") {
        f->dump_int("crush_ruleset", p->get_crush_ruleset());
      } else if (var == "hit_set_period") {
	f->dump_int("hit_set_period", p->hit_set_period);
      } else if (var == "hit_set_count") {
	f->dump_int("hit_set_count", p->hit_set_count);
      } else if (var == "hit_set_type") {
	f->dump_string("hit_set_type", HitSet::get_type_name(p->hit_set_params.get_type()));
      } else if (var == "hit_set_fpp") {
	if (p->hit_set_params.get_type() != HitSet::TYPE_BLOOM) {
	  f->close_section();
	  ss << "hit set is no of type Bloom; invalid to get a false positive rate!";
	  r = -EINVAL;
	  goto reply;
	} else {
	  BloomHitSet::Params *bloomp = static_cast<BloomHitSet::Params*>(p->hit_set_params.impl.get());
	  f->dump_float("hit_set_fpp", bloomp->get_fpp());
	}
      } else if (var == "target_max_objects") {
        f->dump_unsigned("target_max_objects", p->target_max_objects);
      } else if (var == "target_max_bytes") {
        f->dump_unsigned("target_max_bytes", p->target_max_bytes);
      } else if (var == "cache_target_dirty_ratio") {
        f->dump_unsigned("cache_target_dirty_ratio_micro",
                         p->cache_target_dirty_ratio_micro);
        f->dump_float("cache_target_dirty_ratio",
                      ((float)p->cache_target_dirty_ratio_micro/1000000));
      } else if (var == "cache_target_full_ratio") {
        f->dump_unsigned("cache_target_full_ratio_micro",
                         p->cache_target_full_ratio_micro);
        f->dump_float("cache_target_full_ratio",
                      ((float)p->cache_target_full_ratio_micro/1000000));
      } else if (var == "cache_min_flush_age") {
        f->dump_unsigned("cache_min_flush_age", p->cache_min_flush_age);
      } else if (var == "cache_min_evict_age") {
        f->dump_unsigned("cache_min_evict_age", p->cache_min_evict_age);
      } else if (var == "erasure_code_profile") {
       f->dump_string("erasure_code_profile", p->erasure_code_profile);
      } else if (var == "min_read_recency_for_promote") {
	f->dump_int("min_read_recency_for_promote", p->min_read_recency_for_promote);
      }

      f->close_section();
      f->flush(rdata);
    } else {
      if (var == "pg_num") {
        ss << "pg_num: " << p->get_pg_num();
      } else if (var == "pgp_num") {
        ss << "pgp_num: " << p->get_pgp_num();
      } else if (var == "auid") {
        ss << "auid: " << p->get_auid();
      } else if (var == "size") {
        ss << "size: " << p->get_size();
      } else if (var == "min_size") {
        ss << "min_size: " << p->get_min_size();
      } else if (var == "crash_replay_interval") {
        ss << "crash_replay_interval: " << p->get_crash_replay_interval();
      } else if (var == "crush_ruleset") {
        ss << "crush_ruleset: " << p->get_crush_ruleset();
      } else if (var == "hit_set_period") {
	ss << "hit_set_period: " << p->hit_set_period;
      } else if (var == "hit_set_count") {
	ss << "hit_set_count: " << p->hit_set_count;
      } else if (var == "hit_set_type") {
	ss << "hit_set_type: " <<  HitSet::get_type_name(p->hit_set_params.get_type());
      } else if (var == "hit_set_fpp") {
	if (p->hit_set_params.get_type() != HitSet::TYPE_BLOOM) {
	  ss << "hit set is no of type Bloom; invalid to get a false positive rate!";
	  r = -EINVAL;
	  goto reply;
	}
	BloomHitSet::Params *bloomp = static_cast<BloomHitSet::Params*>(p->hit_set_params.impl.get());
	ss << "hit_set_fpp: " << bloomp->get_fpp();
      } else if (var == "target_max_objects") {
        ss << "target_max_objects: " << p->target_max_objects;
      } else if (var == "target_max_bytes") {
        ss << "target_max_bytes: " << p->target_max_bytes;
      } else if (var == "cache_target_dirty_ratio") {
        ss << "cache_target_dirty_ratio: "
          << ((float)p->cache_target_dirty_ratio_micro/1000000);
      } else if (var == "cache_target_full_ratio") {
        ss << "cache_target_full_ratio: "
          << ((float)p->cache_target_full_ratio_micro/1000000);
      } else if (var == "cache_min_flush_age") {
        ss << "cache_min_flush_age: " << p->cache_min_flush_age;
      } else if (var == "cache_min_evict_age") {
        ss << "cache_min_evict_age: " << p->cache_min_evict_age;
      } else if (var == "erasure_code_profile") {
       ss << "erasure_code_profile: " << p->erasure_code_profile;
      } else if (var == "min_read_recency_for_promote") {
	ss << "min_read_recency_for_promote: " << p->min_read_recency_for_promote;
      }

      rdata.append(ss);
      ss.str("");
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

      stringstream rss, tss;
      pg_map.pool_recovery_summary(f.get(), &rss, poolid);
      if (!f && !rss.str().empty())
        tss << "  " << rss.str() << "\n";

      if (f) {
        f->close_section();
        f->open_object_section("recovery_rate");
      }

      rss.clear();
      rss.str("");

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
    cmd_getval(g_ceph_context, cmdmap, "format", format, string("json-pretty"));
    Formatter *fp = new_formatter(format);
    if (!fp)
      fp = new_formatter("json-pretty");
    boost::scoped_ptr<Formatter> f(fp);
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
    cmd_getval(g_ceph_context, cmdmap, "format", format, string("json-pretty"));
    Formatter *fp = new_formatter(format);
    if (!fp)
      fp = new_formatter("json-pretty");
    boost::scoped_ptr<Formatter> f(fp);
    if (name == "") {
      f->open_array_section("rules");
      osdmap.crush->dump_rules(f.get());
      f->close_section();
    } else {
      int ruleset = osdmap.crush->get_rule_id(name);
      if (ruleset < 0) {
	ss << "unknown crush ruleset '" << name << "'";
	r = ruleset;
	goto reply;
      }
      osdmap.crush->dump_rule(ruleset, f.get());
    }
    ostringstream rs;
    f->flush(rs);
    rs << "\n";
    rdata.append(rs.str());
  } else if (prefix == "osd crush dump") {
    string format;
    cmd_getval(g_ceph_context, cmdmap, "format", format, string("json-pretty"));
    Formatter *fp = new_formatter(format);
    if (!fp)
      fp = new_formatter("json-pretty");
    boost::scoped_ptr<Formatter> f(fp);
    f->open_object_section("crush_map");
    osdmap.crush->dump(f.get());
    f->close_section();
    ostringstream rs;
    f->flush(rs);
    rs << "\n";
    rdata.append(rs.str());
  } else if (prefix == "osd crush show-tunables") {
    string format;
    cmd_getval(g_ceph_context, cmdmap, "format", format, string("json-pretty"));
    Formatter *fp = new_formatter(format);
    if (!fp)
      fp = new_formatter("json-pretty");
    boost::scoped_ptr<Formatter> f(fp);
    f->open_object_section("crush_map_tunables");
    osdmap.crush->dump_tunables(f.get());
    f->close_section();
    ostringstream rs;
    f->flush(rs);
    rs << "\n";
    rdata.append(rs.str());
  } else if (prefix == "osd erasure-code-profile ls") {
    const map<string,map<string,string> > &profiles =
      osdmap.get_erasure_code_profiles();
    if (f)
      f->open_array_section("erasure-code-profiles");
    for(map<string,map<string,string> >::const_iterator i = profiles.begin();
	i != profiles.end();
	i++) {
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
	 i++) {
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
  mon->reply_command(m, r, rs, rdata, get_last_committed());
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
    const char *pool_name = osdmap.get_pool_name(it->first);

    bool pool_is_full =
      (pool.quota_max_bytes > 0 && (uint64_t)sum.num_bytes >= pool.quota_max_bytes) ||
      (pool.quota_max_objects > 0 && (uint64_t)sum.num_objects >= pool.quota_max_objects);

    if (pool.get_flags() & pg_pool_t::FLAG_FULL) {
      if (pool_is_full)
        continue;

      mon->clog.info() << "pool '" << pool_name
                       << "' no longer full; removing FULL flag";

      update_pool_flags(it->first, pool.get_flags() & ~pg_pool_t::FLAG_FULL);
      ret = true;
    } else {
      if (!pool_is_full)
	continue;

      if (pool.quota_max_bytes > 0 &&
          (uint64_t)sum.num_bytes >= pool.quota_max_bytes) {
        mon->clog.warn() << "pool '" << pool_name << "' is full"
                         << " (reached quota's max_bytes: "
                         << si_t(pool.quota_max_bytes) << ")";
      } else if (pool.quota_max_objects > 0 &&
		 (uint64_t)sum.num_objects >= pool.quota_max_objects) {
        mon->clog.warn() << "pool '" << pool_name << "' is full"
                         << " (reached quota's max_objects: "
                         << pool.quota_max_objects << ")";
      } else {
        assert(0 == "we shouldn't reach this");
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
    const char *pool_name = osdmap.get_pool_name(it->first);

    if (pool.get_flags() & pg_pool_t::FLAG_FULL) {
      // uncomment these asserts if/when we update the FULL flag on pg_stat update
      //assert((pool.quota_max_objects > 0) || (pool.quota_max_bytes > 0));

      stringstream ss;
      ss << "pool '" << pool_name << "' is full";
      summary.push_back(make_pair(HEALTH_WARN, ss.str()));
      if (detail)
	detail->push_back(make_pair(HEALTH_WARN, ss.str()));
    }

    float warn_threshold = g_conf->mon_pool_quota_warn_threshold/100;
    float crit_threshold = g_conf->mon_pool_quota_crit_threshold/100;

    if (pool.quota_max_objects > 0) {
      stringstream ss;
      health_status_t status = HEALTH_OK;
      if ((uint64_t)sum.num_objects >= pool.quota_max_objects) {
	// uncomment these asserts if/when we update the FULL flag on pg_stat update
        //assert(pool.get_flags() & pg_pool_t::FLAG_FULL);
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
	//assert(pool.get_flags() & pg_pool_t::FLAG_FULL);
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


int OSDMonitor::prepare_new_pool(MPoolOp *m)
{
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
			    pg_pool_t::TYPE_REPLICATED, ss);
  else
    return prepare_new_pool(m->name, session->auid, m->crush_rule, ruleset_name,
			    0, 0,
                            erasure_code_profile,
			    pg_pool_t::TYPE_REPLICATED, ss);
}

int OSDMonitor::crush_ruleset_create_erasure(const string &name,
					     const string &profile,
					     int *ruleset,
					     stringstream &ss)
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
      ss << "failed to load plugin using profile " << profile;
      return err;
    }

    err = erasure_code->create_ruleset(name, newcrush, &ss);
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
				 stringstream &ss) const
{
  if (pending_inc.has_erasure_code_profile(erasure_code_profile))
    return -EAGAIN;
  const map<string,string> &profile =
    osdmap.get_erasure_code_profile(erasure_code_profile);
  map<string,string>::const_iterator plugin =
    profile.find("plugin");
  if (plugin == profile.end()) {
    ss << "cannot determine the erasure code plugin"
       << " because there is no 'plugin' entry in the erasure_code_profile "
       << profile;
    return -EINVAL;
  }
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  return instance.factory(plugin->second, profile, erasure_code, ss);
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
  uint64_t features = newmap.get_features(CEPH_ENTITY_TYPE_MON, NULL);

  stringstream features_ss;

  int r = check_cluster_features(features, features_ss);

  if (!r)
    return true;

  ss << "Could not change CRUSH: " << features_ss.str();
  return false;
}

bool OSDMonitor::erasure_code_profile_in_use(const map<int64_t, pg_pool_t> &pools,
					     const string &profile,
					     ostream &ss)
{
  bool found = false;
  for (map<int64_t, pg_pool_t>::const_iterator p = pools.begin();
       p != pools.end();
       ++p) {
    if (p->second.erasure_code_profile == profile) {
      ss << osdmap.pool_name[p->first] << " ";
      found = true;
    }
  }
  if (found) {
    ss << "pool(s) are using the erasure code profile '" << profile << "'";
  }
  return found;
}

int OSDMonitor::parse_erasure_code_profile(const vector<string> &erasure_code_profile,
					   map<string,string> *erasure_code_profile_map,
					   stringstream &ss)
{
  int r = get_str_map(g_conf->osd_pool_default_erasure_code_profile,
		      ss,
		      erasure_code_profile_map);
  if (r)
    return r;
  (*erasure_code_profile_map)["directory"] =
    g_conf->osd_pool_default_erasure_code_directory;

  for (vector<string>::const_iterator i = erasure_code_profile.begin();
       i != erasure_code_profile.end();
       ++i) {
    size_t equal = i->find('=');
    if (equal == string::npos)
      (*erasure_code_profile_map)[*i] = string();
    else {
      const string key = i->substr(0, equal);
      equal++;
      const string value = i->substr(equal);
      (*erasure_code_profile_map)[key] = value;
    }
  }

  return 0;
}

int OSDMonitor::prepare_pool_size(const unsigned pool_type,
				  const string &erasure_code_profile,
				  unsigned *size, unsigned *min_size,
				  stringstream &ss)
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
    ss << "prepare_pool_size: " << pool_type << " is not a known pool type";
    err = -EINVAL;
    break;
  }
  return err;
}

int OSDMonitor::prepare_pool_stripe_width(const unsigned pool_type,
					  const string &erasure_code_profile,
					  uint32_t *stripe_width,
					  stringstream &ss)
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
    ss << "prepare_pool_stripe_width: "
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
					   stringstream &ss)
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
	    ss << "No suitable CRUSH ruleset exists";
	    return *crush_ruleset;
	  }
	} else {
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
	      dout(20) << "prepare_pool_crush_ruleset: ruleset "
		   << ruleset_name << " is pending, try again" << dendl;
	      return -EAGAIN;
	    } else {
	      //Cannot find it , return error
	      ss << "Specified ruleset " << ruleset_name << " doesn't exist";
	      return ret;
	    }
	  }
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
      ss << "prepare_pool_crush_ruleset: " << pool_type
	 << " is not a known pool type";
      return -EINVAL;
      break;
    }
  } else {
    if (!osdmap.crush->ruleset_exists(*crush_ruleset)) {
      ss << "CRUSH ruleset " << *crush_ruleset << " not found";
      return -ENOENT;
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
 * @param pool_type TYPE_ERASURE, TYPE_REP or TYPE_RAID4
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
				 stringstream &ss)
{
  int r;
  r = prepare_pool_crush_ruleset(pool_type, erasure_code_profile,
				 crush_ruleset_name, &crush_ruleset, ss);
  if (r)
    return r;
  unsigned size, min_size;
  r = prepare_pool_size(pool_type, erasure_code_profile, &size, &min_size, ss);
  if (r)
    return r;
  uint32_t stripe_width = 0;
  r = prepare_pool_stripe_width(pool_type, erasure_code_profile, &stripe_width, ss);
  if (r)
    return r;

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
    pi->flags |= pg_pool_t::FLAG_HASHPSPOOL;

  pi->size = size;
  pi->min_size = min_size;
  pi->crush_ruleset = crush_ruleset;
  pi->object_hash = CEPH_STR_HASH_RJENKINS;
  pi->set_pg_num(pg_num ? pg_num : g_conf->osd_pool_default_pg_num);
  pi->set_pgp_num(pgp_num ? pgp_num : g_conf->osd_pool_default_pgp_num);
  pi->last_change = pending_inc.epoch;
  pi->auid = auid;
  pi->erasure_code_profile = erasure_code_profile;
  pi->stripe_width = stripe_width;
  pi->cache_target_dirty_ratio_micro =
    g_conf->osd_pool_default_cache_target_dirty_ratio * 1000000;
  pi->cache_target_full_ratio_micro =
    g_conf->osd_pool_default_cache_target_full_ratio * 1000000;
  pi->cache_min_flush_age = g_conf->osd_pool_default_cache_min_flush_age;
  pi->cache_min_evict_age = g_conf->osd_pool_default_cache_min_evict_age;
  pending_inc.new_pool_names[pool] = name;
  return 0;
}

bool OSDMonitor::prepare_set_flag(MMonCommand *m, int flag)
{
  ostringstream ss;
  if (pending_inc.new_flags < 0)
    pending_inc.new_flags = osdmap.get_flags();
  pending_inc.new_flags |= flag;
  ss << "set " << OSDMap::get_flag_string(flag);
  wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, ss.str(),
						    get_last_committed() + 1));
  return true;
}

bool OSDMonitor::prepare_unset_flag(MMonCommand *m, int flag)
{
  ostringstream ss;
  if (pending_inc.new_flags < 0)
    pending_inc.new_flags = osdmap.get_flags();
  pending_inc.new_flags &= ~flag;
  ss << "unset " << OSDMap::get_flag_string(flag);
  wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, ss.str(),
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
       var == "cache_min_flush_age" || var == "cache_min_evict_age")) {
    ss << "pool '" << poolstr << "' is not a tier pool: variable not applicable";
    return -EACCES;
  }

  if (var == "size") {
    if (p.type == pg_pool_t::TYPE_ERASURE) {
      ss << "can not change the size of an erasure-coded pool";
      return -ENOTSUP;
    }
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
    }
    if (n == 0 || n > 10) {
      ss << "pool size must be between 1 and 10";
      return -EINVAL;
    }
    p.size = n;
    if (n < p.min_size)
      p.min_size = n;
  } else if (var == "min_size") {
    if (interr.length()) {
      ss << "error parsing integer value '" << val << "': " << interr;
      return -EINVAL;
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
    p.crush_ruleset = n;
  } else if (var == "hashpspool") {
    // make sure we only compare against 'n' if we didn't receive a string
    if (val == "true" || (interr.empty() && n == 1)) {
      p.flags |= pg_pool_t::FLAG_HASHPSPOOL;
    } else if (val == "false" || (interr.empty() && n == 0)) {
      p.flags &= ~pg_pool_t::FLAG_HASHPSPOOL;
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
  } else {
    ss << "unrecognized variable '" << var << "'";
    return -EINVAL;
  }
  ss << "set pool " << pool << " " << var << " to " << val;
  p.last_change = pending_inc.epoch;
  pending_inc.new_pools[pool] = p;
  return 0;
}

bool OSDMonitor::prepare_command(MMonCommand *m)
{
  stringstream ss;
  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon->reply_command(m, -EINVAL, rs, get_last_committed());
    return true;
  }

  MonSession *session = m->get_session();
  if (!session) {
    mon->reply_command(m, -EACCES, "access denied", get_last_committed());
    return true;
  }

  return prepare_command_impl(m, cmdmap);
}

bool OSDMonitor::prepare_command_impl(MMonCommand *m,
				      map<string,cmd_vartype> &cmdmap)
{
  bool ret = false;
  stringstream ss;
  string rs;
  bufferlist rdata;
  int err = 0;

  string format;
  cmd_getval(g_ceph_context, cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(new_formatter(format));

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  int64_t id;
  string name;
  bool osdid_present = cmd_getval(g_ceph_context, cmdmap, "id", id);
  if (osdid_present) {
    ostringstream oss;
    oss << "osd." << id;
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
    tester.test();
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
    err = newcrush.add_bucket(0, CRUSH_BUCKET_STRAW,
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
  } else if (osdid_present &&
	     (prefix == "osd crush set" || prefix == "osd crush add")) {
    // <OsdName> is 'osd.<id>' or '<id>', passed as int64_t id
    // osd crush set <OsdName> <weight> <loc1> [<loc2> ...]
    // osd crush add <OsdName> <weight> <loc1> [<loc2> ...]

    if (!osdmap.exists(id)) {
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
        && !_get_stable_crush().item_exists(id)) {
      err = -ENOENT;
      ss << "unable to set item id " << id << " name '" << name
         << "' weight " << weight << " at location " << loc
         << ": does not exist";
      goto reply;
    }

    dout(5) << "adding/updating crush item id " << id << " name '"
      << name << "' weight " << weight << " at location "
      << loc << dendl;
    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    string action;
    if (prefix == "osd crush set" ||
        newcrush.check_item_loc(g_ceph_context, id, loc, (int *)NULL)) {
      action = "set";
      err = newcrush.update_item(g_ceph_context, id, weight, name, loc);
    } else {
      action = "add";
      err = newcrush.insert_item(g_ceph_context, id, weight, name, loc);
      if (err == 0)
        err = 1;
    }

    if (err < 0)
      goto reply;

    if (err == 0 && !_have_pending_crush()) {
      ss << action << " item id " << id << " name '" << name << "' weight "
        << weight << " at location " << loc << ": no change";
      goto reply;
    }

    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush);
    ss << action << " item id " << id << " name '" << name << "' weight "
      << weight << " at location " << loc << " to crush map";
    getline(ss, rs);
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
						      get_last_committed() + 1));
    return true;

  } else if (prefix == "osd crush create-or-move") {
    do {
      // osd crush create-or-move <OsdName> <initial_weight> <loc1> [<loc2> ...]
      if (!osdmap.exists(id)) {
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

      err = newcrush.create_or_move_item(g_ceph_context, id, weight, name, loc);
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
	wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
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
	  wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
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

    if (!osdmap.crush->name_exists(name)) {
      err = -ENOENT;
      ss << "item " << name << " does not exist";
      goto reply;
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
	}
      } else {
	ss << "no need to move item id " << id << " name '" << name
           << "' to location " << loc << " in crush map";
	err = 0;
      }
    }
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, err, ss.str(),
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
	wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
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
	wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
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
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
						  get_last_committed() + 1));
    return true;
  } else if (prefix == "osd crush reweight") {
    do {
      // osd crush reweight <name> <weight>
      CrushWrapper newcrush;
      _get_pending_crush(newcrush);

      string name;
      cmd_getval(g_ceph_context, cmdmap, "name", name);
      if (!newcrush.name_exists(name)) {
	err = -ENOENT;
	ss << "device '" << name << "' does not appear in the crush map";
	break;
      }

      int id = newcrush.get_item_id(name);
      if (id < 0) {
	ss << "device '" << name << "' is not a leaf in the crush map";
	break;
      }
      double w;
      if (!cmd_getval(g_ceph_context, cmdmap, "weight", w)) {
        ss << "unable to parse weight value '"
           << cmd_vartype_stringify(cmdmap["weight"]) << "'";
        err = -EINVAL;
        break;
      }

      err = newcrush.adjust_item_weightf(g_ceph_context, id, w);
      if (err >= 0) {
	pending_inc.crush.clear();
	newcrush.encode(pending_inc.crush);
	ss << "reweighted item id " << id << " name '" << name << "' to " << w
	   << " in crush map";
	getline(ss, rs);
	wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
						  get_last_committed() + 1));
	return true;
      }
    } while (false);

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
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
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
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
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
      ss << "rule " << name << " already exists";
      err = 0;
      goto reply;
    }

    CrushWrapper newcrush;
    _get_pending_crush(newcrush);

    if (newcrush.rule_exists(name)) {
      ss << "rule " << name << " already exists";
      err = 0;
    } else {
      int rule = newcrush.add_simple_ruleset(name, root, type, mode,
					     pg_pool_t::TYPE_REPLICATED, &ss);
      if (rule < 0) {
	err = rule;
	goto reply;
      }

      pending_inc.crush.clear();
      newcrush.encode(pending_inc.crush);
    }
    getline(ss, rs);
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
					      get_last_committed() + 1));
    return true;

  } else if (prefix == "osd erasure-code-profile rm") {
    string name;
    cmd_getval(g_ceph_context, cmdmap, "name", name);

    if (erasure_code_profile_in_use(pending_inc.new_pools, name, ss))
      goto wait;

    if (erasure_code_profile_in_use(osdmap.pools, name, ss)) {
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
      wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
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
    err = parse_erasure_code_profile(profile, &profile_map, ss);
    if (err)
      goto reply;

    if (osdmap.has_erasure_code_profile(name)) {
      if (osdmap.get_erasure_code_profile(name) == profile_map) {
	err = 0;
	goto reply;
      }
      if (!force) {
	err = -EPERM;
	ss << "will not override erasure code profile " << name;
	goto reply;
      }
    }

    if (pending_inc.has_erasure_code_profile(name)) {
      dout(20) << "erasure code profile " << name << " try again" << dendl;
      goto wait;
    } else {
      dout(20) << "erasure code profile " << name << " set" << dendl;
      pending_inc.set_erasure_code_profile(name, profile_map);
    }

    getline(ss, rs);
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
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
	dout(20) << "erasure code profile " << profile << " set" << dendl;
	pending_inc.set_erasure_code_profile(profile, profile_map);
	goto wait;
      }
    }

    int ruleset;
    err = crush_ruleset_create_erasure(name, profile, &ruleset, ss);
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
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
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
	ss << "crush rule " << name << " ruleset " << ruleset << " is in use";
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
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
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

    pending_inc.new_max_osd = newmax;
    ss << "set new max_osd = " << pending_inc.new_max_osd;
    getline(ss, rs);
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
					      get_last_committed() + 1));
    return true;

  } else if (prefix == "osd pause") {
    return prepare_set_flag(m, CEPH_OSDMAP_PAUSERD | CEPH_OSDMAP_PAUSEWR);

  } else if (prefix == "osd unpause") {
    return prepare_unset_flag(m, CEPH_OSDMAP_PAUSERD | CEPH_OSDMAP_PAUSEWR);

  } else if (prefix == "osd set") {
    string key;
    cmd_getval(g_ceph_context, cmdmap, "key", key);
    if (key == "pause")
      return prepare_set_flag(m, CEPH_OSDMAP_PAUSERD | CEPH_OSDMAP_PAUSEWR);
    else if (key == "noup")
      return prepare_set_flag(m, CEPH_OSDMAP_NOUP);
    else if (key == "nodown")
      return prepare_set_flag(m, CEPH_OSDMAP_NODOWN);
    else if (key == "noout")
      return prepare_set_flag(m, CEPH_OSDMAP_NOOUT);
    else if (key == "noin")
      return prepare_set_flag(m, CEPH_OSDMAP_NOIN);
    else if (key == "nobackfill")
      return prepare_set_flag(m, CEPH_OSDMAP_NOBACKFILL);
    else if (key == "norecover")
      return prepare_set_flag(m, CEPH_OSDMAP_NORECOVER);
    else if (key == "noscrub")
      return prepare_set_flag(m, CEPH_OSDMAP_NOSCRUB);
    else if (key == "nodeep-scrub")
      return prepare_set_flag(m, CEPH_OSDMAP_NODEEP_SCRUB);
    else if (key == "notieragent")
      return prepare_set_flag(m, CEPH_OSDMAP_NOTIERAGENT);
    else {
      ss << "unrecognized flag '" << key << "'";
      err = -EINVAL;
    }

  } else if (prefix == "osd unset") {
    string key;
    cmd_getval(g_ceph_context, cmdmap, "key", key);
    if (key == "pause")
      return prepare_unset_flag(m, CEPH_OSDMAP_PAUSERD | CEPH_OSDMAP_PAUSEWR);
    else if (key == "noup")
      return prepare_unset_flag(m, CEPH_OSDMAP_NOUP);
    else if (key == "nodown")
      return prepare_unset_flag(m, CEPH_OSDMAP_NODOWN);
    else if (key == "noout")
      return prepare_unset_flag(m, CEPH_OSDMAP_NOOUT);
    else if (key == "noin")
      return prepare_unset_flag(m, CEPH_OSDMAP_NOIN);
    else if (key == "nobackfill")
      return prepare_unset_flag(m, CEPH_OSDMAP_NOBACKFILL);
    else if (key == "norecover")
      return prepare_unset_flag(m, CEPH_OSDMAP_NORECOVER);
    else if (key == "noscrub")
      return prepare_unset_flag(m, CEPH_OSDMAP_NOSCRUB);
    else if (key == "nodeep-scrub")
      return prepare_unset_flag(m, CEPH_OSDMAP_NODEEP_SCRUB);
    else if (key == "notieragent")
      return prepare_unset_flag(m, CEPH_OSDMAP_NOTIERAGENT);
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
      wait_for_finished_proposal(new Monitor::C_Command(mon, m, err, rs,
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
      wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
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
      wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
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
      wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
						get_last_committed() + 1));
      return true;
    }

  } else if (prefix == "osd create") {
    int i = -1;

    // optional uuid provided?
    uuid_d uuid;
    string uuidstr;
    if (cmd_getval(g_ceph_context, cmdmap, "uuid", uuidstr)) {
      if (!uuid.parse(uuidstr.c_str())) {
	err = -EINVAL;
	goto reply;
      }
      dout(10) << " osd create got uuid " << uuid << dendl;
      i = osdmap.identify_osd(uuid);
      if (i >= 0) {
	// osd already exists
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
      i = pending_inc.identify_osd(uuid);
      if (i >= 0) {
	// osd is about to exist
	wait_for_finished_proposal(new C_RetryMessage(this, m));
	return true;
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
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, rdata,
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
	wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
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
	  wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
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
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
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
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "osd pool create") {
    int64_t  pg_num;
    int64_t pgp_num;
    cmd_getval(g_ceph_context, cmdmap, "pg_num", pg_num, int64_t(0));
    if ((pg_num == 0) || (pg_num > g_conf->mon_max_pool_pg_num)) {
      ss << "'pg_num' must be greater than 0 and less than or equal to "
	 << g_conf->mon_max_pool_pg_num
	 << " (you may adjust 'mon max pool pg num' for higher values)";
      err = -ERANGE;
      goto reply;
    }

    cmd_getval(g_ceph_context, cmdmap, "pgp_num", pgp_num, pg_num);
    if ((pgp_num == 0) || (pgp_num > pg_num)) {
      ss << "'pgp_num' must be greater than 0 and lower or equal than 'pg_num'"
	 << ", which in this case is " << pg_num;
      err = -ERANGE;
      goto reply;
    }

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

    err = prepare_new_pool(poolstr, 0, // auid=0 for admin created pool
			   -1, // default crush rule
			   ruleset_name,
			   pg_num, pgp_num,
			   erasure_code_profile, pool_type,
			   ss);
    if (err < 0) {
      switch(err) {
      case -EEXIST:
	ss << "pool '" << poolstr << "' already exists";
	break;
      case -EAGAIN:
	wait_for_finished_proposal(new C_RetryMessage(this, m));
	return true;
      default:
	goto reply;
	break;
      }
    } else {
      ss << "pool '" << poolstr << "' created";
    }
    getline(ss, rs);
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
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
      wait_for_finished_proposal(new C_RetryMessage(this, m));
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
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, ret, rs,
					      get_last_committed() + 1));
    return true;

  } else if (prefix == "osd pool set") {
    err = prepare_command_pool_set(cmdmap, ss);
    if (err == -EAGAIN)
      goto wait;
    if (err < 0)
      goto reply;

    getline(ss, rs);
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
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
    if (p->tiers.count(tierpool_id)) {
      assert(tp->tier_of == pool_id);
      err = 0;
      ss << "pool '" << tierpoolstr << "' is now (or already was) a tier of '" << poolstr << "'";
      goto reply;
    }
    if (tp->is_tier()) {
      ss << "tier pool '" << tierpoolstr << "' is already a tier of '"
	 << osdmap.get_pool_name(tp->tier_of) << "'";
      err = -EINVAL;
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
    // go
    pg_pool_t *np = pending_inc.get_new_pool(pool_id, p);
    pg_pool_t *ntp = pending_inc.get_new_pool(tierpool_id, tp);
    if (np->tiers.count(tierpool_id) || ntp->is_tier()) {
      wait_for_finished_proposal(new C_RetryMessage(this, m));
      return true;
    }
    np->tiers.insert(tierpool_id);
    np->set_snap_epoch(pending_inc.epoch); // tier will update to our snap info
    ntp->tier_of = pool_id;
    ss << "pool '" << tierpoolstr << "' is now (or already was) a tier of '" << poolstr << "'";
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, ss.str(),
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
      wait_for_finished_proposal(new C_RetryMessage(this, m));
      return true;
    }
    np->tiers.erase(tierpool_id);
    ntp->clear_tier();
    ss << "pool '" << tierpoolstr << "' is now (or already was) not a tier of '" << poolstr << "'";
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, ss.str(),
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
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, ss.str(),
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
    // go
    pg_pool_t *np = pending_inc.get_new_pool(pool_id, p);
    np->clear_read_tier();
    np->clear_write_tier();
    np->last_force_op_resend = pending_inc.epoch;
    ss << "there is now (or already was) no overlay for '" << poolstr << "'";
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, ss.str(),
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
     *
     * Hence, these are the allowed transitions:
     *
     *  none -> any
     *  forward -> writeback || any IF num_objects_dirty == 0
     *  writeback -> forward
     *  readonly -> any
     */

    // We check if the transition is valid against the current pool mode, as
    // it is the only committed state thus far.  We will blantly squash
    // whatever mode is on the pending state.

    if (p->cache_mode == pg_pool_t::CACHEMODE_WRITEBACK &&
        mode != pg_pool_t::CACHEMODE_FORWARD) {
      ss << "unable to set cache-mode '" << pg_pool_t::get_cache_mode_name(mode)
         << "' on a '" << pg_pool_t::get_cache_mode_name(p->cache_mode)
         << "' pool; only '"
         << pg_pool_t::get_cache_mode_name(pg_pool_t::CACHEMODE_FORWARD)
        << "' allowed.";
      err = -EINVAL;
      goto reply;
    }
    if (p->cache_mode == pg_pool_t::CACHEMODE_FORWARD &&
               mode != pg_pool_t::CACHEMODE_WRITEBACK) {

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
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, ss.str(),
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
    if (p->tiers.count(tierpool_id)) {
      assert(tp->tier_of == pool_id);
      err = 0;
      ss << "pool '" << tierpoolstr << "' is now (or already was) a tier of '" << poolstr << "'";
      goto reply;
    }
    if (tp->is_tier()) {
      ss << "tier pool '" << tierpoolstr << "' is already a tier of '"
	 << osdmap.get_pool_name(tp->tier_of) << "'";
      err = -EINVAL;
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
      wait_for_finished_proposal(new C_RetryMessage(this, m));
      return true;
    }
    np->tiers.insert(tierpool_id);
    np->set_snap_epoch(pending_inc.epoch); // tier will update to our snap info
    ntp->tier_of = pool_id;
    ntp->cache_mode = mode;
    ntp->hit_set_count = g_conf->osd_tier_default_cache_hit_set_count;
    ntp->hit_set_period = g_conf->osd_tier_default_cache_hit_set_period;
    ntp->min_read_recency_for_promote = g_conf->osd_tier_default_cache_min_read_recency_for_promote;
    ntp->hit_set_params = hsp;
    ntp->target_max_bytes = size;
    ss << "pool '" << tierpoolstr << "' is now (or already was) a cache tier of '" << poolstr << "'";
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, ss.str(),
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
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
					      get_last_committed() + 1));
    return true;

  } else if (prefix == "osd reweight-by-utilization") {
    int64_t oload;
    cmd_getval(g_ceph_context, cmdmap, "oload", oload, int64_t(120));
    string out_str;
    err = reweight_by_utilization(oload, out_str);
    if (err < 0) {
      ss << "FAILED reweight-by-utilization: " << out_str;
    }
    else if (err == 0) {
      ss << "no change: " << out_str;
    } else {
      ss << "SUCCESSFUL reweight-by-utilization: " << out_str;
      getline(ss, rs);
      wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
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
  mon->reply_command(m, err, rs, rdata, get_last_committed());
  return ret;

 update:
  getline(ss, rs);
  wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
					    get_last_committed() + 1));
  return true;

 wait:
  wait_for_finished_proposal(new C_RetryMessage(this, m));
  return true;
}

bool OSDMonitor::preprocess_pool_op(MPoolOp *m) 
{
  if (m->op == POOL_OP_CREATE)
    return preprocess_pool_op_create(m);

  if (!osdmap.get_pg_pool(m->pool)) {
    dout(10) << "attempt to delete non-existent pool id " << m->pool << dendl;
    _pool_op_reply(m, 0, osdmap.get_epoch());
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
      _pool_op_reply(m, -EINVAL, osdmap.get_epoch());
      return true;
    }
    if (snap_exists) {
      _pool_op_reply(m, 0, osdmap.get_epoch());
      return true;
    }
    return false;
  case POOL_OP_CREATE_UNMANAGED_SNAP:
    if (p->is_pool_snaps_mode()) {
      _pool_op_reply(m, -EINVAL, osdmap.get_epoch());
      return true;
    }
    return false;
  case POOL_OP_DELETE_SNAP:
    if (p->is_unmanaged_snaps_mode()) {
      _pool_op_reply(m, -EINVAL, osdmap.get_epoch());
      return true;
    }
    if (!snap_exists) {
      _pool_op_reply(m, 0, osdmap.get_epoch());
      return true;
    }
    return false;
  case POOL_OP_DELETE_UNMANAGED_SNAP:
    if (p->is_pool_snaps_mode()) {
      _pool_op_reply(m, -EINVAL, osdmap.get_epoch());
      return true;
    }
    if (p->is_removed_snap(m->snapid)) {
      _pool_op_reply(m, 0, osdmap.get_epoch());
      return true;
    }
    return false;
  case POOL_OP_DELETE:
    if (osdmap.lookup_pg_pool_name(m->name.c_str()) >= 0) {
      _pool_op_reply(m, 0, osdmap.get_epoch());
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

bool OSDMonitor::preprocess_pool_op_create(MPoolOp *m)
{
  MonSession *session = m->get_session();
  if (!session) {
    _pool_op_reply(m, -EPERM, osdmap.get_epoch());
    return true;
  }
  if (!session->is_capable("osd", MON_CAP_W)) {
    dout(5) << "attempt to create new pool without sufficient auid privileges!"
	    << "message: " << *m  << std::endl
	    << "caps: " << session->caps << dendl;
    _pool_op_reply(m, -EPERM, osdmap.get_epoch());
    return true;
  }

  int64_t pool = osdmap.lookup_pg_pool_name(m->name.c_str());
  if (pool >= 0) {
    _pool_op_reply(m, 0, osdmap.get_epoch());
    return true;
  }

  return false;
}

bool OSDMonitor::prepare_pool_op(MPoolOp *m)
{
  dout(10) << "prepare_pool_op " << *m << dendl;
  if (m->op == POOL_OP_CREATE) {
    return prepare_pool_op_create(m);
  } else if (m->op == POOL_OP_DELETE) {
    return prepare_pool_op_delete(m);
  }

  int ret = 0;
  bool changed = false;

  if (!osdmap.have_pg_pool(m->pool)) {
    _pool_op_reply(m, -ENOENT, osdmap.get_epoch());
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
      _pool_op_reply(m, ret, osdmap.get_epoch());
      return false;

    case POOL_OP_DELETE_UNMANAGED_SNAP:
      // we won't allow removal of an unmanaged snapshot from a pool
      // not in unmanaged snaps mode.
      if (!pool->is_unmanaged_snaps_mode()) {
        _pool_op_reply(m, -ENOTSUP, osdmap.get_epoch());
        return false;
      }
      /* fall-thru */
    case POOL_OP_CREATE_UNMANAGED_SNAP:
      // but we will allow creating an unmanaged snapshot on any pool
      // as long as it is not in 'pool' snaps mode.
      if (pool->is_pool_snaps_mode()) {
        _pool_op_reply(m, -EINVAL, osdmap.get_epoch());
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
  wait_for_finished_proposal(new OSDMonitor::C_PoolOp(this, m, ret, pending_inc.epoch, &reply_data));
  return true;
}

bool OSDMonitor::prepare_pool_op_create(MPoolOp *m)
{
  int err = prepare_new_pool(m);
  wait_for_finished_proposal(new OSDMonitor::C_PoolOp(this, m, err, pending_inc.epoch));
  return true;
}

int OSDMonitor::_check_remove_pool(int64_t pool, const pg_pool_t *p,
				   ostream *ss)
{
  string poolstr = osdmap.get_pool_name(pool);

  if (p->tier_of >= 0) {
    *ss << "pool '" << poolstr << "' is a tier of '"
	<< osdmap.get_pool_name(p->tier_of) << "'";
    return -EBUSY;
  }
  if (!p->tiers.empty()) {
    *ss << "pool '" << poolstr << "' has tiers";
    for(std::set<uint64_t>::iterator i = p->tiers.begin(); i != p->tiers.end(); ++i) {
      const char *name = osdmap.get_pool_name(*i);
      assert(name != NULL);
      *ss << " " << name;
    }
    return -EBUSY;
  }

  if (!g_conf->mon_allow_pool_delete) {
    *ss << "pool deletion is disabled; you must first set the mon_allow_pool_delete config option to true before you can destroy a pool";
    return -EPERM;
  }

  *ss << "pool '" << poolstr << "' removed";
  return 0;
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
  for (map<pg_t,int>::iterator p = osdmap.primary_temp->begin();
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

bool OSDMonitor::prepare_pool_op_delete(MPoolOp *m)
{
  ostringstream ss;
  int ret = _prepare_remove_pool(m->pool, &ss);
  if (ret == -EAGAIN) {
    wait_for_finished_proposal(new C_RetryMessage(this, m));
    return true;
  }
  if (ret < 0)
    dout(10) << __func__ << " got " << ret << " " << ss.str() << dendl;
  wait_for_finished_proposal(new OSDMonitor::C_PoolOp(this, m, ret,
						      pending_inc.epoch));
  return true;
}

void OSDMonitor::_pool_op_reply(MPoolOp *m, int ret, epoch_t epoch, bufferlist *blp)
{
  dout(20) << "_pool_op_reply " << ret << dendl;
  MPoolOpReply *reply = new MPoolOpReply(m->fsid, m->get_tid(),
					 ret, epoch, get_last_committed(), blp);
  mon->send_reply(m, reply);
  m->put();
}
