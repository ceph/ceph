// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
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

#include "include/compat.h"
#include "include/assert.h"
#include "include/stringify.h"
#include "include/util.h"
#include "common/cmdparse.h"
#include "include/str_list.h"

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

  get_mkfs(bl);
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
  newmap.encode(pending_inc.fullmap);
}

void OSDMonitor::update_from_paxos()
{
  version_t version = get_version();
  if (version == osdmap.epoch)
    return;
  assert(version >= osdmap.epoch);

  dout(15) << "update_from_paxos paxos e " << version
	   << ", my e " << osdmap.epoch << dendl;


  /* We no longer have stashed versions. Maybe we can do this by reading
   * from a full map? Maybe we should keep the last full map version on a key
   * as well (say, osdmap_full_version), and consider that the last_committed
   * always contains incrementals, and maybe a full version if
   * osdmap_full_version == last_committed
   *
   * This ^^^^ sounds about right. Do it. We should then change the
   * 'get_stashed_version()' to 'get_full_version(version_t ver)', which should
   * then be read iif
   *	(osdmap.epoch != osd_full_version)
   *	&& (osdmap.epoch <= osdmap_full_version)
   */
  version_t latest_full = get_version_latest_full();
  if ((latest_full > 0) && (latest_full > osdmap.epoch)) {
    bufferlist latest_bl;
    get_version_full(latest_full, latest_bl);
    assert(latest_bl.length() != 0);
    dout(7) << __func__ << " loading latest full map e" << latest_full << dendl;
    osdmap.decode(latest_bl);
  }

  // walk through incrementals
  MonitorDBStore::Transaction t;
  while (version > osdmap.epoch) {
    bufferlist inc_bl;
    int err = get_version(osdmap.epoch+1, inc_bl);
    assert(err == 0);
    assert(inc_bl.length());
    
    dout(7) << "update_from_paxos  applying incremental " << osdmap.epoch+1 << dendl;
    OSDMap::Incremental inc(inc_bl);
    osdmap.apply_incremental(inc);

    // write out the full map for all past epochs
    bufferlist full_bl;
    osdmap.encode(full_bl);
    put_version_full(&t, osdmap.epoch, full_bl);

    // share
    dout(1) << osdmap << dendl;

    if (osdmap.epoch == 1) {
      erase_mkfs(&t);
    }
  }
  if (!t.empty())
    mon->store->apply_transaction(t);

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

  if (mon->is_leader()) {
    // kick pgmon, make sure it's seen the latest map
    mon->pgmon()->check_osd_map(osdmap.epoch);
  }

  send_to_waiting();
  check_subs();

  share_map_with_random_osd();
  update_logger();

  process_failures();

  // make sure our feature bits reflect the latest map
  update_msgr_features();
}

void OSDMonitor::update_msgr_features()
{
  uint64_t mask;
  uint64_t features = osdmap.get_features(&mask);

  set<int> types;
  types.insert((int)entity_name_t::TYPE_OSD);
  types.insert((int)entity_name_t::TYPE_CLIENT);
  types.insert((int)entity_name_t::TYPE_MDS);
  types.insert((int)entity_name_t::TYPE_MON);
  for (set<int>::iterator q = types.begin(); q != types.end(); ++q) {
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
  // let's assume the hash_map iterates in a random-ish order.
  int n = rand() % mon->pgmon()->pg_map.pg_stat.size();
  hash_map<pg_t,pg_stat_t>::iterator p = mon->pgmon()->pg_map.pg_stat.begin();
  hash_map<pg_t,pg_stat_t>::iterator e = mon->pgmon()->pg_map.pg_stat.end();
  while (n--)
    ++p;
  for (int i=0; i<50; i++) {
    vector<int> v;
    for (int j=0; j<3; j++) {
      o = rand() % osdmap.get_num_osds();
      if (osdmap.exists(o) && std::find(v.begin(), v.end(), o) == v.end())
	v.push_back(o);
    }
    if (v.size() < 3) {
      for (vector<int>::iterator q = p->second.acting.begin(); q != p->second.acting.end(); ++q)
	if (std::find(v.begin(), v.end(), *q) == v.end())
	  v.push_back(*q);
    }
    if (!v.empty())
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

  if (thrash_map && thrash())
    propose_pending();

  if (mon->is_leader())
    mon->clog.info() << "osdmap " << osdmap << "\n"; 

  if (!mon->is_leader()) {
    kick_all_failures();
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

void OSDMonitor::remove_redundant_pg_temp()
{
  dout(10) << "remove_redundant_pg_temp" << dendl;

  for (map<pg_t,vector<int> >::iterator p = osdmap.pg_temp->begin();
       p != osdmap.pg_temp->end();
       ++p) {
    if (pending_inc.new_pg_temp.count(p->first) == 0) {
      vector<int> raw_up;
      osdmap.pg_to_raw_up(p->first, raw_up);
      if (raw_up == p->second) {
	dout(10) << " removing unnecessary pg_temp " << p->first << " -> " << p->second << dendl;
	pending_inc.new_pg_temp[p->first].clear();
      }
    }
  }
}

void OSDMonitor::remove_down_pg_temp()
{
  dout(10) << "remove_down_pg_temp" << dendl;
  OSDMap tmpmap(osdmap);
  tmpmap.apply_incremental(pending_inc);

  for (map<pg_t,vector<int> >::iterator p = tmpmap.pg_temp->begin();
       p != tmpmap.pg_temp->end();
       ++p) {
    unsigned num_up = 0;
    for (vector<int>::iterator i = p->second.begin();
	 i != p->second.end();
	 ++i) {
      if (!tmpmap.is_down(*i))
	++num_up;
    }
    if (num_up == 0)
      pending_inc.new_pg_temp[p->first].clear();
  }
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
  for (hash_map<int,osd_stat_t>::const_iterator p = pgm.osd_stat.begin();
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
  remove_redundant_pg_temp();

  // drop any pg_temp entries with no up entries
  remove_down_pg_temp();
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
  assert(get_version() + 1 == pending_inc.epoch);
  ::encode(pending_inc, bl, CEPH_FEATURES_ALL);

  /* put everything in the transaction */
  put_version(t, pending_inc.epoch, bl);
  put_last_committed(t, pending_inc.epoch);
}

void OSDMonitor::encode_full(MonitorDBStore::Transaction *t)
{
  dout(10) << __func__ << " osdmap e " << osdmap.epoch << dendl;
  assert(get_version() == osdmap.epoch);
 
  bufferlist osdmap_bl;
  osdmap.encode(osdmap_bl);
  put_version_full(t, osdmap.epoch, osdmap_bl);
  put_version_latest_full(t, osdmap.epoch);
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

void OSDMonitor::update_trim()
{
  if (mon->pgmon()->is_readable() &&
      mon->pgmon()->pg_map.creating_pgs.empty()) {
    epoch_t floor = mon->pgmon()->pg_map.calc_min_last_epoch_clean();
    dout(10) << " min_last_epoch_clean " << floor << dendl;
    unsigned min = g_conf->mon_min_osdmap_epochs;
    if (floor + min > get_version()) {
      if (min < get_version())
	floor = get_version() - min;
      else
	floor = 0;
    }
    if (floor > get_first_committed())
      if (get_trim_to() < floor)
	set_trim_to(floor);
  }
}

bool OSDMonitor::service_should_trim()
{
  update_trim();
  return (get_trim_to() > 0);
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
  if (osd_weight.size() == (unsigned)osdmap.get_max_osd()) {
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
  if (osdmap.is_down(badboy)) {
    dout(5) << "preprocess_failure dup: " << m->get_target() << ", from " << m->get_orig_source_inst() << dendl;
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

  // check permissions
  if (check_source(m, m->fsid))
    goto didit;

  // first, verify the reporting host is valid
  if (m->get_orig_source().is_osd()) {
    int from = m->get_orig_source().num();
    if (!osdmap.exists(from) ||
	osdmap.get_addr(from) != m->get_orig_source_inst().addr ||
	osdmap.is_down(from)) {
      dout(5) << "preprocess_mark_me_down from dead osd."
	      << from << ", ignoring" << dendl;
      send_incremental(m, m->get_epoch()+1);
      goto didit;
    }
  }

  // no down might be set
  if (!can_mark_down(requesting_down))
    goto didit;

  dout(10) << "MOSDMarkMeDown for: " << m->get_target() << dendl;
  return false;

 didit:
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
    if (old)
      mon->no_reply(old);

    return check_failure(now, target_osd, fi);
  } else {
    // remove the report
    mon->clog.debug() << m->get_target() << " failure report canceled by "
		      << m->get_orig_source_inst() << "\n";
    if (failure_info.count(target_osd)) {
      failure_info_t& fi = failure_info[target_osd];
      fi.cancel_report(reporter);
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

void OSDMonitor::kick_all_failures()
{
  dout(10) << "kick_all_failures on " << failure_info.size() << " osds" << dendl;
  assert(!mon->is_leader());

  list<MOSDFailure*> ls;
  for (map<int,failure_info_t>::iterator p = failure_info.begin();
       p != failure_info.end();
       ++p) {
    p->second.take_report_messages(ls);
  }
  failure_info.clear();

  while (!ls.empty()) {
    dispatch(ls.front());
    ls.pop_front();
  }
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
  
  // already booted?
  if (osdmap.is_up(from) &&
      osdmap.get_inst(from) == m->get_orig_source_inst()) {
    // yup.
    dout(7) << "preprocess_boot dup from " << m->get_orig_source_inst()
	    << " == " << osdmap.get_inst(from) << dendl;
    _booted(m, false);
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
    assert(osdmap.get_inst(from) != m->get_orig_source_inst());  // preproces should have caught it
    
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
    if (!osdmap.exists(from) || osdmap.get_uuid(from) != m->sb.osd_fsid)
      pending_inc.new_uuid[from] = m->sb.osd_fsid;

    // fresh osd?
    if (m->sb.newest_map == 0 && osdmap.exists(from)) {
      const osd_info_t& i = osdmap.get_info(from);
      if (i.up_from > i.lost_at) {
	dout(10) << " fresh osd; marking lost_at too" << dendl;
	pending_inc.new_lost[from] = osdmap.get_epoch();
      }
    }

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
    // removal?
    if (p->second.empty() && osdmap.pg_temp->count(p->first))
      return false;
    // change?
    if (p->second.size() && (osdmap.pg_temp->count(p->first) == 0 ||
			     (*osdmap.pg_temp)[p->first] != p->second))
      return false;
  }

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
  for (map<pg_t,vector<int> >::iterator p = m->pg_temp.begin(); p != m->pg_temp.end(); ++p)
    pending_inc.new_pg_temp[p->first] = p->second;
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
	if (pending_inc.new_pools.count(p->first) == 0)
	  pending_inc.new_pools[p->first] = pi;
	pg_pool_t& newpi = pending_inc.new_pools[p->first];
	newpi.removed_snaps.insert(*q);
	dout(10) << " pool " << p->first << " removed_snaps added " << *q
		 << " (now " << newpi.removed_snaps << ")" << dendl;
	if (*q > newpi.get_snap_seq()) {
	  dout(10) << " pool " << p->first << " snap_seq " << newpi.get_snap_seq() << " -> " << *q << dendl;
	  newpi.set_snap_seq(*q);
	}
	newpi.set_snap_epoch(pending_inc.epoch);
      }
    }
  }

  m->put();
  return true;
}


// ---------------
// map helpers

void OSDMonitor::send_to_waiting()
{
  dout(10) << "send_to_waiting " << osdmap.get_epoch() << dendl;

  map<epoch_t, list<PaxosServiceMessage*> >::iterator p = waiting_for_map.begin();
  while (p != waiting_for_map.end()) {
    epoch_t from = p->first;
    
    if (from) {
      if (from <= osdmap.get_epoch()) {
	while (!p->second.empty()) {
	  send_incremental(p->second.front(), from);
	  p->second.front()->put();
	  p->second.pop_front();
	}
      } else {
	dout(10) << "send_to_waiting from " << from << dendl;
	++p;
	continue;
      }
    } else {
      while (!p->second.empty()) {
	send_full(p->second.front());
	p->second.front()->put();
	p->second.pop_front();
      }
    }

    waiting_for_map.erase(p++);
  }
}

void OSDMonitor::send_latest(PaxosServiceMessage *m, epoch_t start)
{
  if (is_readable()) {
    dout(5) << "send_latest to " << m->get_orig_source_inst()
	    << " start " << start << dendl;
    if (start == 0)
      send_full(m);
    else
      send_incremental(m, start);
    m->put();
  } else {
    dout(5) << "send_latest to " << m->get_orig_source_inst()
	    << " start " << start << " later" << dendl;
    waiting_for_map[start].push_back(m);
  }
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
      get_version("full", e, bl);
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
      dout(10) << " osd." << osd << " should have epoch " << p->second << dendl;
      first = p->second + 1;
      if (first > osdmap.get_epoch())
	return;
    }
  }

  if (first < get_first_committed()) {
    first = get_first_committed();
    bufferlist bl;
    int err = get_version("full", first, bl);
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
    int err = get_version("full", first, bl);
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

  update_from_paxos();
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
	  int type = osdmap.crush->get_type_id(g_conf->mon_osd_down_out_subtree_limit.c_str());
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
  for (hash_map<entity_addr_t,utime_t>::iterator p = osdmap.blacklist.begin();
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
	pg_t pgid = pg_t(pg_t::TYPE_REP, ps, pool, -1);
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

  update_trim();

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
  int num_up_osds = osdmap.get_num_up_osds();
  int num_in_osds = osdmap.get_num_in_osds();

  if (num_osds == 0) {
    summary.push_back(make_pair(HEALTH_ERR, "no osds"));
  } else {
    if (num_up_osds < num_in_osds) {
      ostringstream ss;
      ss << (num_in_osds - num_up_osds) << "/" << num_in_osds << " in osds are down";
      summary.push_back(make_pair(HEALTH_WARN, ss.str()));

      if (detail) {
	for (int i = 0; i < osdmap.get_max_osd(); i++) {
	  if (osdmap.exists(i) && !osdmap.is_up(i)) {
	    const osd_info_t& info = osdmap.get_info(i);
	    ostringstream ss;
	    ss << "osd." << i << " is down since epoch " << info.down_at
	       << ", last address " << osdmap.get_addr(i);
	    detail->push_back(make_pair(HEALTH_WARN, ss.str()));
	  }
	}
      }
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
			 CEPH_OSDMAP_NODEEP_SCRUB)) {
      ostringstream ss;
      ss << osdmap.get_flag_string() << " flag(s) set";
      summary.push_back(make_pair(HEALTH_WARN, ss.str()));
      if (detail)
	detail->push_back(make_pair(HEALTH_WARN, ss.str()));
    }

    get_pools_health(summary, detail);
  }
}

void OSDMonitor::dump_info(Formatter *f)
{
  f->open_object_section("osdmap");
  osdmap.dump(f);
  f->close_section();

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
    mon->reply_command(m, -EINVAL, rs, get_version());
    return true;
  }

  MonSession *session = m->get_session();
  if (!session ||
      (!session->is_capable("osd", MON_CAP_R) &&
       !mon->_allowed_command(session, cmdmap))) {
    mon->reply_command(m, -EACCES, "access denied", rdata, get_version());
    return true;
  }

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  string format;
  cmd_getval(g_ceph_context, cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(new_formatter(format));

  if (prefix == "osd stat") {
    osdmap.print_summary(ds);
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

    OSDMap *p = &osdmap;
    if (epoch) {
      bufferlist b;
      int err = get_version("full", epoch, b);
      if (err == -ENOENT) {
	r = -ENOENT;
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
      p->encode(rdata);
      ss << "got osdmap epoch " << p->get_epoch();
    } else if (prefix == "osd getcrushmap") {
      p->crush->encode(rdata);
      ss << "got crush map from osdmap epoch " << p->get_epoch();
    }
    if (p != &osdmap)
      delete p;
  } else if (prefix == "osd getmaxosd") {
    ds << "max_osd = " << osdmap.get_max_osd() << " in epoch " << osdmap.get_epoch();
    rdata.append(ds);
  } else if (prefix == "osd tell") {
    string whostr;
    cmd_getval(g_ceph_context, cmdmap, "who", whostr);
    vector<string> argvec;
    cmd_getval(g_ceph_context, cmdmap, "args", argvec);
    if (whostr == "*") {
      for (int i = 0; i < osdmap.get_max_osd(); ++i)
	if (osdmap.is_up(i))
	  mon->send_command(osdmap.get_inst(i), argvec, get_version());
      ss << "ok";
    } else {
      errno = 0;
      int who = parse_osd_id(whostr.c_str(), &ss);
      if (who < 0) {
	r = -EINVAL;
      } else {
	if (osdmap.is_up(who)) {
	  mon->send_command(osdmap.get_inst(who), argvec, get_version());
	  ss << "ok";
	} else {
	  ss << "osd." << who << " not up";
	  r = -ENOENT;
	}
      }
    }
  } else if (prefix  == "osd find") {
    int64_t osd;
    cmd_getval(g_ceph_context, cmdmap, "id", osd);
    if (!osdmap.exists(osd)) {
      ss << "osd." << osd << " does not exist";
      r = -ENOENT;
      goto reply;
    }
    string format;
    cmd_getval(g_ceph_context, cmdmap, "format", format, string("json-pretty"));
    boost::scoped_ptr<Formatter> f(new_formatter(format));

    f->open_object_section("osd_location");
    f->dump_int("osd", osd);
    f->dump_stream("ip") << osdmap.get_addr(osd);
    f->open_object_section("crush_location");
    map<string,string> loc = osdmap.crush->get_full_location(osd);
    for (map<string,string>::iterator p = loc.begin(); p != loc.end(); ++p)
      f->dump_string(p->first.c_str(), p->second);
    f->close_section();
    f->close_section();
    f->flush(ds);
    ds << "\n";
    rdata.append(ds);
  } else if (prefix == "osd map") {
    string poolstr, objstr;
    cmd_getval(g_ceph_context, cmdmap, "pool", poolstr);
    cmd_getval(g_ceph_context, cmdmap, "object", objstr);
    int64_t pool = osdmap.lookup_pg_pool_name(poolstr.c_str());
    if (pool < 0) {
      ss << "pool " << poolstr << " does not exist";
      r = -ENOENT;
      goto reply;
    }
    object_locator_t oloc(pool);
    object_t oid(objstr);
    pg_t pgid = osdmap.object_locator_to_pg(oid, oloc);
    pg_t mpgid = osdmap.raw_pg_to_pg(pgid);
    vector<int> up, acting;
    osdmap.pg_to_up_acting_osds(mpgid, up, acting);
    ds << "osdmap e" << osdmap.get_epoch()
       << " pool '" << poolstr << "' (" << pool << ") object '" << oid << "' ->"
       << " pg " << pgid << " (" << mpgid << ")"
       << " -> up " << up << " acting " << acting;
    rdata.append(ds);
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
      ss << " instructed to " << whostr;
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
    for (map<int64_t, pg_pool_t>::iterator p = osdmap.pools.begin();
	 p != osdmap.pools.end();
	 ++p) {
      if (!auid || p->second.auid == (uint64_t)auid) {
	ds << p->first << ' ' << osdmap.pool_name[p->first] << ',';
      }
    }
    rdata.append(ds);
  } else if (prefix == "osd blacklist ls") {
    for (hash_map<entity_addr_t,utime_t>::iterator p = osdmap.blacklist.begin();
	 p != osdmap.blacklist.end();
	 p++) {
      stringstream ss;
      string s;
      ss << p->first << " " << p->second;
      getline(ss, s);
      s += "\n";
      rdata.append(s);
    }
    ss << "listed " << osdmap.blacklist.size() << " entries";
  } else if (prefix == "osd crush rule list" ||
	     prefix == "osd crush rule ls") {
    string format;
    cmd_getval(g_ceph_context, cmdmap, "format", format, string("json-pretty"));
    boost::scoped_ptr<Formatter> f(new_formatter(format));
    f->open_array_section("rules");
    osdmap.crush->list_rules(f.get());
    f->close_section();
    ostringstream rs;
    f->flush(rs);
    rs << "\n";
    rdata.append(rs.str());
  } else if (prefix == "osd crush rule dump") {
    string format;
    cmd_getval(g_ceph_context, cmdmap, "format", format, string("json-pretty"));
    boost::scoped_ptr<Formatter> f(new_formatter(format));
    f->open_array_section("rules");
    osdmap.crush->dump_rules(f.get());
    f->close_section();
    ostringstream rs;
    f->flush(rs);
    rs << "\n";
    rdata.append(rs.str());
  } else if (prefix == "osd crush dump") {
    string format;
    cmd_getval(g_ceph_context, cmdmap, "format", format, string("json-pretty"));
    boost::scoped_ptr<Formatter> f(new_formatter(format));
    f->open_object_section("crush_map");
    osdmap.crush->dump(f.get());
    f->close_section();
    ostringstream rs;
    f->flush(rs);
    rs << "\n";
    rdata.append(rs.str());
  } else {
    // try prepare update
    return false;
  }

 reply:
  string rs;
  getline(ss, rs);
  mon->reply_command(m, r, rs, rdata, get_version());
  return true;
}

void OSDMonitor::update_pool_flags(int64_t pool_id, uint64_t flags)
{
  const pg_pool_t *pool = osdmap.get_pg_pool(pool_id);
  if (pending_inc.new_pools.count(pool_id) == 0)
    pending_inc.new_pools[pool_id] = *pool;
  pending_inc.new_pools[pool_id].flags = flags;
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
           << "' has " << si_t(sum.num_bytes) << " objects"
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
  if (m->auid)
    return prepare_new_pool(m->name, m->auid, m->crush_rule, 0, 0);
  else
    return prepare_new_pool(m->name, session->auid, m->crush_rule, 0, 0);
}

/**
 * @param name The name of the new pool
 * @param auid The auid of the pool owner. Can be -1
 * @param crush_rule The crush rule to use. If <0, will use the system default
 * @param pg_num The pg_num to use. If set to 0, will use the system default
 * @param pgp_num The pgp_num to use. If set to 0, will use the system default
 *
 * @return 0 in all cases. That's silly.
 */
int OSDMonitor::prepare_new_pool(string& name, uint64_t auid, int crush_rule,
                                 unsigned pg_num, unsigned pgp_num)
{
  for (map<int64_t,string>::iterator p = pending_inc.new_pool_names.begin();
       p != pending_inc.new_pool_names.end();
       ++p) {
    if (p->second == name)
      return 0;
  }

  if (-1 == pending_inc.new_pool_max)
    pending_inc.new_pool_max = osdmap.pool_max;
  int64_t pool = ++pending_inc.new_pool_max;
  pending_inc.new_pools[pool].type = pg_pool_t::TYPE_REP;
  pending_inc.new_pools[pool].flags = g_conf->osd_pool_default_flags;
  if (g_conf->osd_pool_default_flag_hashpspool)
    pending_inc.new_pools[pool].flags |= pg_pool_t::FLAG_HASHPSPOOL;

  pending_inc.new_pools[pool].size = g_conf->osd_pool_default_size;
  pending_inc.new_pools[pool].min_size = g_conf->get_osd_pool_default_min_size();
  if (crush_rule >= 0)
    pending_inc.new_pools[pool].crush_ruleset = crush_rule;
  else
    pending_inc.new_pools[pool].crush_ruleset = g_conf->osd_pool_default_crush_rule;
  pending_inc.new_pools[pool].object_hash = CEPH_STR_HASH_RJENKINS;
  pending_inc.new_pools[pool].set_pg_num(pg_num ? pg_num : g_conf->osd_pool_default_pg_num);
  pending_inc.new_pools[pool].set_pgp_num(pgp_num ? pgp_num : g_conf->osd_pool_default_pgp_num);
  pending_inc.new_pools[pool].last_change = pending_inc.epoch;
  pending_inc.new_pools[pool].auid = auid;
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
  wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, ss.str(), get_version()));
  return true;
}

bool OSDMonitor::prepare_unset_flag(MMonCommand *m, int flag)
{
  ostringstream ss;
  if (pending_inc.new_flags < 0)
    pending_inc.new_flags = osdmap.get_flags();
  pending_inc.new_flags &= ~flag;
  ss << "unset " << OSDMap::get_flag_string(flag);
  wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, ss.str(), get_version()));
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

void OSDMonitor::parse_loc_map(const vector<string>& args,  map<string,string> *ploc)
{
  for (unsigned i = 0; i < args.size(); ++i) {
    const char *s = args[i].c_str();
    const char *pos = strchr(s, '=');
    if (!pos)
      break;
    string key(s, 0, pos-s);
    string value(pos+1);
    if (value.length())
      (*ploc)[key] = value;
    else
      ploc->erase(key);
  }
}

bool OSDMonitor::prepare_command(MMonCommand *m)
{
  bool ret = false;
  stringstream ss;
  string rs;
  bufferlist rdata;
  int err = 0;

  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon->reply_command(m, -EINVAL, rs, get_version());
    return true;
  }

  MonSession *session = m->get_session();
  if (!session ||
      (!session->is_capable("osd", MON_CAP_W) &&
       !mon->_allowed_command(session, cmdmap))) {
    mon->reply_command(m, -EACCES, "access denied", get_version());
    return true;
  }

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  int64_t id;
  bool osdid_present = cmd_getval(g_ceph_context, cmdmap, "id", id);

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

    // sanity check: test some inputs to make sure this map isn't totally broken
    dout(10) << " testing map" << dendl;
    stringstream ess;
    CrushTester tester(crush, ess, 1);
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
    int bucketno = newcrush.add_bucket(0, CRUSH_BUCKET_STRAW,
				       CRUSH_HASH_DEFAULT, type, 0, NULL,
				       NULL);
    newcrush.set_item_name(bucketno, name);

    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush);
    ss << "added bucket " << name << " type " << typestr
       << " to crush map";
    goto update;
  } else if (osdid_present && 
	     (prefix == "osd crush set" || prefix == "osd crush add")) {
    do {
      // osd crush set <osd-id> [<osd.* name>] <weight> <loc1> [<loc2> ...]
      // osd crush add <osd-id> [<osd.* name>] <weight> <loc1> [<loc2> ...]
      if (!osdmap.exists(id)) {
	err = -ENOENT;
	ss << "osd." << id << " does not exist.  create it before updating the crush map";
	goto reply;
      }

      string name;
      if (!cmd_getval(g_ceph_context, cmdmap, "name", name)) {
	// new usage; infer name
	name = "osd." + stringify(id);
      }

      double weight;
      cmd_getval(g_ceph_context, cmdmap, "weight", weight);

      string args;
      vector<string> argvec;
      cmd_getval(g_ceph_context, cmdmap, "args", argvec);
      map<string,string> loc;
      parse_loc_map(argvec, &loc);

      dout(0) << "adding/updating crush item id " << id << " name '"
	      << name << "' weight " << weight << " at location "
	      << loc << dendl;
      CrushWrapper newcrush;
      _get_pending_crush(newcrush);

      string action;
      if (prefix == "osd crush set") {
	action = "set";
	err = newcrush.update_item(g_ceph_context, id, weight, name, loc);
      } else {
	action = "add";
	err = newcrush.insert_item(g_ceph_context, id, weight, name, loc);
	if (err == 0)
	  err = 1;
      }
      if (err == 0) {
	ss << action << " item id " << id << " name '" << name << "' weight "
	   << weight << " at location " << loc << ": no change";
	break;
      }
      if (err > 0) {
	pending_inc.crush.clear();
	newcrush.encode(pending_inc.crush);
	ss << action << " item id " << id << " name '" << name << "' weight "
	   << weight << " at location " << loc << " to crush map";
	getline(ss, rs);
	wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	return true;
      }
    } while (false);

  } else if (prefix == "osd crush create-or-move") {
    do {
      // osd crush create-or-move <id> <initial_weight> <loc1> [<loc2> ...]
      int64_t id;
      cmd_getval(g_ceph_context, cmdmap, "id", id);
      if (!osdmap.exists(id)) {
	err = -ENOENT;
	ss << "osd." << id << " does not exist.  create it before updating the crush map";
	goto reply;
      }

      string name = "osd." + stringify(id);
      double weight;
      cmd_getval(g_ceph_context, cmdmap, "weight", weight);

      string args;
      vector<string> argvec;
      cmd_getval(g_ceph_context, cmdmap, "args", argvec);
      map<string,string> loc;
      parse_loc_map(argvec, &loc);

      dout(0) << "create-or-move crush item id " << id << " name '" << name << "' initial_weight " << weight
	      << " at location " << loc << dendl;

      CrushWrapper newcrush;
      _get_pending_crush(newcrush);

      err = newcrush.create_or_move_item(g_ceph_context, id, weight, name, loc);
      if (err == 0) {
	ss << "create-or-move updated item id " << id << " name '" << name << "' weight " << weight
	   << " at location " << loc << " to crush map";
	break;
      }
      if (err > 0) {
	pending_inc.crush.clear();
	newcrush.encode(pending_inc.crush);
	ss << "create-or-move updating item id " << id << " name '" << name << "' weight " << weight
	   << " at location " << loc << " to crush map";
	getline(ss, rs);
	wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	return true;
      }
    } while (false);

  } else if (prefix == "osd crush move") {
    do {
      // osd crush move <name> <loc1> [<loc2> ...]
      string name;
      cmd_getval(g_ceph_context, cmdmap, "name", name);

      string args;
      vector<string> argvec;
      cmd_getval(g_ceph_context, cmdmap, "args", argvec);
      map<string,string> loc;
      parse_loc_map(argvec, &loc);

      dout(0) << "moving crush item name '" << name << "' to location " << loc << dendl;
      CrushWrapper newcrush;
      _get_pending_crush(newcrush);

      if (!newcrush.name_exists(name.c_str())) {
	err = -ENOENT;
	ss << "item " << name << " does not exist";
	break;
      }
      int id = newcrush.get_item_id(name.c_str());

      if (!newcrush.check_item_loc(g_ceph_context, id, loc, (int *)NULL)) {
	err = newcrush.move_bucket(g_ceph_context, id, loc);
	if (err >= 0) {
	  ss << "moved item id " << id << " name '" << name << "' to location " << loc << " in crush map";
	  pending_inc.crush.clear();
	  newcrush.encode(pending_inc.crush);
	  getline(ss, rs);
	  wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	  return true;
	}
      } else {
	ss << "no need to move item id " << id << " name '" << name << "' to location " << loc << " in crush map";
	err = 0;
      }
    } while (false);

  } else if (prefix == "osd crush link") {
    do {
      // osd crush link <name> <loc1> [<loc2> ...]
      string name;
      cmd_getval(g_ceph_context, cmdmap, "name", name);
      vector<string> argvec;
      cmd_getval(g_ceph_context, cmdmap, "args", argvec);
      map<string,string> loc;
      parse_loc_map(argvec, &loc);

      dout(0) << "linking crush item name '" << name << "' at location " << loc << dendl;
      CrushWrapper newcrush;
      _get_pending_crush(newcrush);

      if (!newcrush.name_exists(name.c_str())) {
	err = -ENOENT;
	ss << "item " << name << " does not exist";
	break;
      }
      int id = newcrush.get_item_id(name.c_str());

      if (!newcrush.check_item_loc(g_ceph_context, id, loc, (int *)NULL)) {
	err = newcrush.link_bucket(g_ceph_context, id, loc);
	if (err >= 0) {
	  ss << "linked item id " << id << " name '" << name << "' to location " << loc << " in crush map";
	  pending_inc.crush.clear();
	  newcrush.encode(pending_inc.crush);
	  getline(ss, rs);
	  wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	  return true;
	}
      } else {
	ss << "no need to move item id " << id << " name '" << name << "' to location " << loc << " in crush map";
	err = 0;
      }
    } while (false);
  } else if (prefix == "osd crush rm" ||
	     prefix == "osd crush remove" ||
	     prefix == "osd crush unlink") {
    do {
      // osd crush rm <id> [ancestor]
      CrushWrapper newcrush;
      _get_pending_crush(newcrush);

      string name;
      cmd_getval(g_ceph_context, cmdmap, "name", name);

      if (!newcrush.name_exists(name.c_str())) {
	err = 0;
	ss << "device '" << name << "' does not appear in the crush map";
	break;
      }
      int id = newcrush.get_item_id(name.c_str());
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
	wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	return true;
      }
    } while (false);

  } else if (prefix == "osd crush reweight") {
    do {
      // osd crush reweight <name> <weight>
      CrushWrapper newcrush;
      _get_pending_crush(newcrush);

      string name;
      cmd_getval(g_ceph_context, cmdmap, "name", name);
      if (!newcrush.name_exists(name.c_str())) {
	err = -ENOENT;
	ss << "device '" << name << "' does not appear in the crush map";
	break;
      }

      int id = newcrush.get_item_id(name.c_str());
      if (id < 0) {
	ss << "device '" << name << "' is not a leaf in the crush map";
	break;
      }
      double w;
      cmd_getval(g_ceph_context, cmdmap, "weight", w);

      err = newcrush.adjust_item_weightf(g_ceph_context, id, w);
      if (err == 0) {
	pending_inc.crush.clear();
	newcrush.encode(pending_inc.crush);
	ss << "reweighted item id " << id << " name '" << name << "' to " << w
	   << " in crush map";
	getline(ss, rs);
	wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
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
    } else if (profile == "optimal") {
      newcrush.set_tunables_optimal();
    } else if (profile == "default") {
      newcrush.set_tunables_default();
    } 
    pending_inc.crush.clear();
    newcrush.encode(pending_inc.crush);
    ss << "adjusted tunables profile to " << profile;
    getline(ss, rs);
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
    return true;

  } else if (prefix == "osd crush rule create-simple") {
    string name, root, type;
    cmd_getval(g_ceph_context, cmdmap, "name", name);
    cmd_getval(g_ceph_context, cmdmap, "root", root);
    cmd_getval(g_ceph_context, cmdmap, "type", type);

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
      int rule = newcrush.add_simple_rule(name, root, type, &ss);
      if (rule < 0) {
	err = rule;
	goto reply;
      }

      pending_inc.crush.clear();
      newcrush.encode(pending_inc.crush);
    }
    getline(ss, rs);
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
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
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
    return true;

  } else if (prefix == "osd setmaxosd") {
    int64_t newmax;
    cmd_getval(g_ceph_context, cmdmap, "newmax", newmax);

    if (newmax > g_conf->mon_max_osd) {
      err = -ERANGE;
      ss << "cannot set max_osd to " << newmax << " which is > conf.mon_max_osd ("
	 << g_conf->mon_max_osd << ")";
      goto reply;
    }

    pending_inc.new_max_osd = newmax;
    ss << "set new max_osd = " << pending_inc.new_max_osd;
    getline(ss, rs);
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
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
	// XXX -EINVAL here?
	continue;
      } else if (!osdmap.exists(osd)) {
	ss << "osd." << osd << " does not exist. ";
	err = 0;
	continue;
      }
      if (prefix == "osd down") {
	if (osdmap.is_down(osd)) {
	  ss << "osd." << osd << " is already down. ";
	  err = 0;
	} else {
	  pending_inc.new_state[osd] = CEPH_OSD_UP;
	  ss << "marked down osd." << osd << ". ";
	  any = true;
	}
      } else if (prefix == "osd out") {
	if (osdmap.is_out(osd)) {
	  ss << "osd." << osd << " is already out. ";
	  err = 0;
	} else {
	  pending_inc.new_weight[osd] = CEPH_OSD_OUT;
	  ss << "marked out osd." << osd << ". ";
	  any = true;
	}
      } else if (prefix == "osd in") {
	if (osdmap.is_in(osd)) {
	  ss << "osd." << osd << " is already in. ";
	  err = 0;
	} else {
	  pending_inc.new_weight[osd] = CEPH_OSD_IN;
	  ss << "marked in osd." << osd << ". ";
	  any = true;
	}
      } else if (prefix == "osd rm") {
	if (osdmap.is_up(osd)) {
	ss << "osd." << osd << " is still up; must be down before removal. ";
	} else {
	  pending_inc.new_state[osd] = osdmap.get_state(osd);
	  if (any)
	    ss << ", osd." << osd;
	  else 
	    ss << "removed osd." << osd;
	  any = true;
	}
      }
    }
    if (any) {
      getline(ss, rs);
      wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
      return true;
    }
  } else if (prefix == "osd reweight") {
    int64_t id;
    cmd_getval(g_ceph_context, cmdmap, "id", id);
    double w;
    cmd_getval(g_ceph_context, cmdmap, "weight", w);
    long ww = (int)((double)CEPH_OSD_IN*w);
    if (ww < 0L) {
      ss << "weight must be > 0";
      err = -EINVAL;
      goto reply;
    }
    if (osdmap.exists(id)) {
      pending_inc.new_weight[id] = ww;
      ss << "reweighted osd." << id << " to " << w << " (" << ios::hex << ww << ios::dec << ")";
      getline(ss, rs);
      wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
      return true;
    }

  } else if (prefix == "osd lost") {
    int64_t id;
    cmd_getval(g_ceph_context, cmdmap, "id", id);
    string sure;
    if (!cmd_getval(g_ceph_context, cmdmap, "sure", sure) || sure != "--yes-i-really-mean-it") {
      ss << "are you SURE?  this might mean real, permanent data loss.  pass "
	    "--yes-i-really-mean-it if you really do.";
    } else if (!osdmap.exists(id) || !osdmap.is_down(id)) {
      ss << "osd." << id << " is not down or doesn't exist";
    } else {
      epoch_t e = osdmap.get_info(id).down_at;
      pending_inc.new_lost[id] = e;
      ss << "marked osd lost in epoch " << e;
      getline(ss, rs);
      wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
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
	ss << i;
	getline(ss, rs);
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
    ss << i;
    getline(ss, rs);
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
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
	wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
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
	  wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
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
    } else {
      const pg_pool_t *p = osdmap.get_pg_pool(pool);
      pg_pool_t *pp = 0;
      if (pending_inc.new_pools.count(pool))
	pp = &pending_inc.new_pools[pool];
      string snapname;
      cmd_getval(g_ceph_context, cmdmap, "snap", snapname);
      if (p->snap_exists(snapname.c_str()) ||
	  (pp && pp->snap_exists(snapname.c_str()))) {
	ss << "pool " << poolstr << " snap " << snapname << " already exists";
	err = 0;
      } else {
	if (!pp) {
	  pp = &pending_inc.new_pools[pool];
	  *pp = *p;
	}
	pp->add_snap(snapname.c_str(), ceph_clock_now(g_ceph_context));
	pp->set_snap_epoch(pending_inc.epoch);
	ss << "created pool " << poolstr << " snap " << snapname;
	getline(ss, rs);
	wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	return true;
      }
    }
  } else if (prefix == "osd pool rmsnap") {
    string poolstr;
    cmd_getval(g_ceph_context, cmdmap, "pool", poolstr);
    int64_t pool = osdmap.lookup_pg_pool_name(poolstr.c_str());
    if (pool < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
    } else {
      const pg_pool_t *p = osdmap.get_pg_pool(pool);
      pg_pool_t *pp = 0;
      if (pending_inc.new_pools.count(pool))
	pp = &pending_inc.new_pools[pool];
      string snapname;
      cmd_getval(g_ceph_context, cmdmap, "snap", snapname);
      if (!p->snap_exists(snapname.c_str()) &&
	  (!pp || !pp->snap_exists(snapname.c_str()))) {
	ss << "pool " << poolstr << " snap " << snapname << " does not exist";
	err = 0;
      } else {
	if (!pp) {
	  pp = &pending_inc.new_pools[pool];
	  *pp = *p;
	}
	snapid_t sn = pp->snap_exists(snapname.c_str());
	pp->remove_snap(sn);
	pp->set_snap_epoch(pending_inc.epoch);
	ss << "removed pool " << poolstr << " snap " << snapname;
	getline(ss, rs);
	wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	return true;
      }
    }

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

    string poolstr;
    cmd_getval(g_ceph_context, cmdmap, "pool", poolstr);
    if (osdmap.name_pool.count(poolstr)) {
      ss << "pool '" << poolstr << "' already exists";
      err = 0;
      goto reply;
    }

    err = prepare_new_pool(poolstr, 0, // auid=0 for admin created pool
			   -1,         // default crush rule
			   pg_num, pgp_num);
    if (err < 0 && err != -EEXIST) {
      goto reply;
    }
    if (err == -EEXIST) {
      ss << "pool '" << poolstr << "' already exists";
    } else {
      ss << "pool '" << poolstr << "' created";
    }
    getline(ss, rs);
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
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
    int ret = _prepare_remove_pool(pool);
    if (ret == 0)
      ss << "pool '" << poolstr << "' deleted";
    getline(ss, rs);
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, ret, rs, get_version()));
    return true;
  } else if (prefix == "osd pool rename") {
    string srcpoolstr, destpoolstr;
    cmd_getval(g_ceph_context, cmdmap, "srcpool", srcpoolstr);
    cmd_getval(g_ceph_context, cmdmap, "destpool", destpoolstr);
    int64_t pool = osdmap.lookup_pg_pool_name(srcpoolstr.c_str());
    if (pool < 0) {
      ss << "unrecognized pool '" << srcpoolstr << "'";
      err = -ENOENT;
    } else if (osdmap.lookup_pg_pool_name(destpoolstr.c_str()) >= 0) {
      ss << "pool '" << destpoolstr << "' already exists";
      err = -EEXIST;
    } else {
      int ret = _prepare_rename_pool(pool, destpoolstr);
      if (ret == 0) {
	ss << "pool '" << srcpoolstr << "' renamed to '" << destpoolstr << "'";
      } else {
	ss << "failed to rename pool '" << srcpoolstr << "' to '" << destpoolstr << "': "
	   << cpp_strerror(ret);
      }
      getline(ss, rs);
      wait_for_finished_proposal(new Monitor::C_Command(mon, m, ret, rs, get_version()));
      return true;
    }
  } else if (prefix == "osd pool set") {
    // set a pool variable to a positive int
    string poolstr;
    cmd_getval(g_ceph_context, cmdmap, "pool", poolstr);
    int64_t pool = osdmap.lookup_pg_pool_name(poolstr.c_str());
    if (pool < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
    } else {
      const pg_pool_t *p = osdmap.get_pg_pool(pool);
      int64_t n;
      cmd_getval(g_ceph_context, cmdmap, "val", n);
      string var;
      cmd_getval(g_ceph_context, cmdmap, "var", var);
      if (pending_inc.new_pools.count(pool) == 0)
	pending_inc.new_pools[pool] = *p;
      if (var == "size") {
	if (n == 0 || n > 10) {
	  ss << "pool size must be between 1 and 10";
	  err = -EINVAL;
	  goto reply;
	}
	pending_inc.new_pools[pool].size = n;
	if (n < p->min_size)
	  pending_inc.new_pools[pool].min_size = n;
	ss << "set pool " << pool << " size to " << n;
      } else if (var == "min_size") {
	pending_inc.new_pools[pool].min_size = n;
	ss << "set pool " << pool << " min_size to " << n;
      } else if (var == "crash_replay_interval") {
	pending_inc.new_pools[pool].crash_replay_interval = n;
	ss << "set pool " << pool << " to crash_replay_interval to " << n;
      } else if (var == "pg_num") {
	if (n <= p->get_pg_num()) {
	  ss << "specified pg_num " << n << " <= current " << p->get_pg_num();
	} else if (!mon->pgmon()->pg_map.creating_pgs.empty()) {
	  ss << "currently creating pgs, wait";
	  err = -EAGAIN;
	} else {
	  pending_inc.new_pools[pool].set_pg_num(n);
	  ss << "set pool " << pool << " pg_num to " << n;
	}
      } else if (var == "pgp_num") {
	if (n > p->get_pg_num()) {
	  ss << "specified pgp_num " << n << " > pg_num " << p->get_pg_num();
	} else if (!mon->pgmon()->pg_map.creating_pgs.empty()) {
	  ss << "still creating pgs, wait";
	  err = -EAGAIN;
	} else {
	  pending_inc.new_pools[pool].set_pgp_num(n);
	  ss << "set pool " << pool << " pgp_num to " << n;
	}
      } else if (var == "crush_ruleset") {
	if (osdmap.crush->rule_exists(n)) {
	  pending_inc.new_pools[pool].crush_ruleset = n;
	  ss << "set pool " << pool << " crush_ruleset to " << n;
	} else {
	  ss << "crush ruleset " << n << " does not exist";
	  err = -ENOENT;
	}
      } 
      pending_inc.new_pools[pool].last_change = pending_inc.epoch;
      getline(ss, rs);
      wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
      return true;
    }
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

    if (pending_inc.new_pools.count(pool_id) == 0)
      pending_inc.new_pools[pool_id] = *osdmap.get_pg_pool(pool_id);

    if (field == "max_objects") {
      pending_inc.new_pools[pool_id].quota_max_objects = value;
    } else if (field == "max_bytes") {
      pending_inc.new_pools[pool_id].quota_max_bytes = value;
    } else {
      assert(0 == "unrecognized option");
    }
    ss << "set-quota " << field << " = " << value << " for pool " << poolstr;
    rs = ss.str();
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
    return true;

  } else if (prefix == "osd pool get") {
    string poolstr;
    cmd_getval(g_ceph_context, cmdmap, "pool", poolstr);
    int64_t pool = osdmap.lookup_pg_pool_name(poolstr.c_str());
    if (pool < 0) {
      ss << "unrecognized pool '" << poolstr << "'";
      err = -ENOENT;
      goto reply;
    }

    const pg_pool_t *p = osdmap.get_pg_pool(pool);
    string var;
    cmd_getval(g_ceph_context, cmdmap, "var", var);
    if (var == "pg_num") {
      ss << "pg_num: " << p->get_pg_num();
    }
    if (var == "pgp_num") {
      ss << "pgp_num: " << p->get_pgp_num();
    }
    if (var == "size") {
      ss << "size: " << p->get_size();
    }
    if (var == "min_size") {
      ss << "min_size: " << p->get_min_size();
    }
    if (var == "crash_replay_interval") {
      ss << "crash_replay_interval: " << p->get_crash_replay_interval();
    }
    if (var == "crush_ruleset") {
      ss << "crush_ruleset: " << p->get_crush_ruleset();
    }
    rdata.append(ss);
    ss.str("");
    err = 0;
    goto reply;

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
      wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
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
  mon->reply_command(m, err, rs, rdata, get_version());
  return ret;

 update:
  getline(ss, rs);
  wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
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
  propose_pending();
  return false;
}

bool OSDMonitor::prepare_pool_op_create(MPoolOp *m)
{
  int err = prepare_new_pool(m);
  wait_for_finished_proposal(new OSDMonitor::C_PoolOp(this, m, err, pending_inc.epoch));
  return true;
}

int OSDMonitor::_prepare_remove_pool(uint64_t pool)
{
  dout(10) << "_prepare_remove_pool " << pool << dendl;
  if (pending_inc.old_pools.count(pool)) {
    dout(10) << "_prepare_remove_pool " << pool << " pending removal" << dendl;    
    return 0;  // already removed
  }
  pending_inc.old_pools.insert(pool);

  // remove any pg_temp mappings for this pool too
  for (map<pg_t,vector<int32_t> >::iterator p = osdmap.pg_temp->begin();
       p != osdmap.pg_temp->end();
       ++p) {
    if (p->first.pool() == pool) {
      dout(10) << "_prepare_remove_pool " << pool << " removing obsolete pg_temp "
	       << p->first << dendl;
      pending_inc.new_pg_temp[p->first].clear();
    }
  }
  return 0;
}

int OSDMonitor::_prepare_rename_pool(uint64_t pool, string newname)
{
  dout(10) << "_prepare_rename_pool " << pool << dendl;
  if (pending_inc.old_pools.count(pool)) {
    dout(10) << "_prepare_rename_pool " << pool << " pending removal" << dendl;    
    return -ENOENT;
  }
  for (map<int64_t,string>::iterator p = pending_inc.new_pool_names.begin();
       p != pending_inc.new_pool_names.end();
       ++p) {
    if (p->second == newname) {
      return -EEXIST;
    }
  }

  pending_inc.new_pool_names[pool] = newname;
  return 0;
}

bool OSDMonitor::prepare_pool_op_delete(MPoolOp *m)
{
  int ret = _prepare_remove_pool(m->pool);
  wait_for_finished_proposal(new OSDMonitor::C_PoolOp(this, m, ret, pending_inc.epoch));
  return true;
}

void OSDMonitor::_pool_op_reply(MPoolOp *m, int ret, epoch_t epoch, bufferlist *blp)
{
  dout(20) << "_pool_op_reply " << ret << dendl;
  MPoolOpReply *reply = new MPoolOpReply(m->fsid, m->get_tid(),
					 ret, epoch, get_version(), blp);
  mon->send_reply(m, reply);
  m->put();
}
