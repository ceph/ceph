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

#include "OSDMonitor.h"
#include "Monitor.h"
#include "MDSMonitor.h"
#include "PGMonitor.h"

#include "MonitorStore.h"

#include "crush/CrushWrapper.h"

#include "common/strtol.h"
#include "messages/MOSDFailure.h"
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

#include "common/config.h"

#include <sstream>

#define DOUT_SUBSYS mon
#undef dout_prefix
#define dout_prefix _prefix(mon, osdmap)
static ostream& _prefix(Monitor *mon, OSDMap& osdmap) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)")))
		<< ".osd e" << osdmap.get_epoch() << " ";
}



/************ MAPS ****************/


void OSDMonitor::create_initial(bufferlist& bl)
{
  dout(10) << "create_initial for " << mon->monmap->fsid << dendl;

  OSDMap newmap;
  newmap.decode(bl);
  newmap.set_epoch(1);
  newmap.set_fsid(mon->monmap->fsid);
  newmap.created = newmap.modified = g_clock.now();

  // encode into pending incremental
  newmap.encode(pending_inc.fullmap);
}

bool OSDMonitor::update_from_paxos()
{
  version_t paxosv = paxos->get_version();
  if (paxosv == osdmap.epoch) return true;
  assert(paxosv >= osdmap.epoch);

  dout(15) << "update_from_paxos paxos e " << paxosv 
	   << ", my e " << osdmap.epoch << dendl;

  if (osdmap.epoch == 0 && paxosv > 1) {
    // startup: just load latest full map
    bufferlist latest;
    version_t v = paxos->get_latest(latest);
    if (v) {
      dout(7) << "update_from_paxos startup: loading latest full map e" << v << dendl;
      osdmap.decode(latest);
    }
  } 
  
  // walk through incrementals
  bufferlist bl;
  while (paxosv > osdmap.epoch) {
    bool success = paxos->read(osdmap.epoch+1, bl);
    assert(success);
    
    dout(7) << "update_from_paxos  applying incremental " << osdmap.epoch+1 << dendl;
    OSDMap::Incremental inc(bl);
    osdmap.apply_incremental(inc);

    // write out the full map for all past epochs
    bl.clear();
    osdmap.encode(bl);
    mon->store->put_bl_sn(bl, "osdmap_full", osdmap.epoch);

    // share
    dout(1) << osdmap << dendl;
  }

  // save latest
  paxos->stash_latest(paxosv, bl);

  // populate down -> out map
  for (int o = 0; o < osdmap.get_max_osd(); o++)
    if (osdmap.is_down(o) && osdmap.is_in(o) &&
	down_pending_out.count(o) == 0) {
      dout(10) << " adding osd" << o << " to down_pending_out map" << dendl;
      down_pending_out[o] = g_clock.now();
    }

  if (mon->is_leader()) {
    // kick pgmon, make sure it's seen the latest map
    mon->pgmon()->check_osd_map(osdmap.epoch);
  }

  send_to_waiting();
  check_subs();
   
  return true;
}

void OSDMonitor::remove_redundant_pg_temp()
{
  dout(10) << "remove_redundant_pg_temp" << dendl;
  bool removed = false;

  for (map<pg_t,vector<int> >::iterator p = osdmap.pg_temp.begin();
       p != osdmap.pg_temp.end();
       p++) {
    if (pending_inc.new_pg_temp.count(p->first) == 0) {
      vector<int> raw_up;
      osdmap.pg_to_raw_up(p->first, raw_up);
      if (raw_up == p->second) {
	dout(10) << " removing unnecessary pg_temp " << p->first << " -> " << p->second << dendl;
	pending_inc.new_pg_temp[p->first].clear();
	removed = true;
      }
    }
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
  // average_full
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

  // Assign a lower weight to overloaded OSDs
  float average_full = pgm.osd_sum.kb_used;
  average_full /= pgm.osd_sum.kb;
  float overload_full = average_full;
  overload_full *= oload;
  overload_full /= 100.0;

  ostringstream oss;
  char buf[128];
  snprintf(buf, sizeof(buf), "average_full: %04f, overload_full: %04f. ",
	   average_full, overload_full);
  oss << buf;
  std::string sep;
  oss << "overloaded osds: ";
  for (hash_map<int,osd_stat_t>::const_iterator p = pgm.osd_stat.begin();
       p != pgm.osd_stat.end();
       ++p) {
    float full = p->second.kb_used;
    full /= p->second.kb;
    if (full >= overload_full) {
      sep = ", ";
      float new_weight = (1.0f - full) / (1.0f - overload_full);
      osdmap.set_weightf(p->first, new_weight);
      char buf[128];
      snprintf(buf, sizeof(buf), "%d [%04f]", p->first, new_weight);
      oss << sep << buf;
    }
  }
  if (sep.empty()) {
    oss << "(none)";
  }
  out_str = oss.str();
  dout(0) << "reweight_by_utilization: finished with " << out_str << dendl;
  return 0;
}

void OSDMonitor::create_pending()
{
  pending_inc = OSDMap::Incremental(osdmap.epoch+1);
  pending_inc.fsid = mon->monmap->fsid;
  
  dout(10) << "create_pending e " << pending_inc.epoch << dendl;

  // drop any redundant pg_temp entries
  remove_redundant_pg_temp();
}


void OSDMonitor::encode_pending(bufferlist &bl)
{
  dout(10) << "encode_pending e " << pending_inc.epoch
	   << dendl;
  
  // finalize up pending_inc
  pending_inc.modified = g_clock.now();

  // tell me about it
  for (map<int32_t,uint8_t>::iterator i = pending_inc.new_down.begin();
       i != pending_inc.new_down.end();
       i++) {
    dout(2) << " osd" << i->first << " DOWN clean=" << (int)i->second << dendl;
  }
  for (map<int32_t,entity_addr_t>::iterator i = pending_inc.new_up_client.begin();
       i != pending_inc.new_up_client.end();
       i++) { 
    dout(2) << " osd" << i->first << " UP " << i->second << dendl; //FIXME: insert cluster addresses too
  }
  for (map<int32_t,uint32_t>::iterator i = pending_inc.new_weight.begin();
       i != pending_inc.new_weight.end();
       i++) {
    if (i->second == CEPH_OSD_OUT) {
      dout(2) << " osd" << i->first << " OUT" << dendl;
    } else if (i->second == CEPH_OSD_IN) {
      dout(2) << " osd" << i->first << " IN" << dendl;
    } else {
      dout(2) << " osd" << i->first << " WEIGHT " << hex << i->second << dec << dendl;
    }
  }

  // encode
  assert(paxos->get_version() + 1 == pending_inc.epoch);
  pending_inc.encode(bl);
}


void OSDMonitor::committed()
{
  // tell any osd
  int r = osdmap.get_any_up_osd();
  if (r >= 0) {
    MonSession *s = mon->session_map.get_random_osd_session();
    if (s) {
      dout(10) << "committed, telling random " << s->inst << " all about it" << dendl;
      MOSDMap *m = build_incremental(osdmap.get_epoch() - 1, osdmap.get_epoch());  // whatev, they'll request more if they need it
      mon->messenger->send_message(m, s->inst);
    }
  }
}


// -------------

bool OSDMonitor::preprocess_query(PaxosServiceMessage *m)
{
  dout(10) << "preprocess_query " << *m << " from " << m->get_orig_source_inst() << dendl;

  switch (m->get_type()) {
    // READs
  case MSG_MON_COMMAND:
    return preprocess_command((MMonCommand*)m);

    // damp updates
  case MSG_OSD_FAILURE:
    return preprocess_failure((MOSDFailure*)m);
  case MSG_OSD_BOOT:
    return preprocess_boot((MOSDBoot*)m);
  case MSG_OSD_ALIVE:
    return preprocess_alive((MOSDAlive*)m);
  case MSG_OSD_PGTEMP:
    return preprocess_pgtemp((MOSDPGTemp*)m);

  case CEPH_MSG_POOLOP:
    return preprocess_pool_op((MPoolOp*)m);

  case MSG_REMOVE_SNAPS:
    return preprocess_remove_snaps((MRemoveSnaps*)m);
    
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
  case MSG_OSD_FAILURE:
    return prepare_failure((MOSDFailure*)m);
  case MSG_OSD_BOOT:
    return prepare_boot((MOSDBoot*)m);
  case MSG_OSD_ALIVE:
    return prepare_alive((MOSDAlive*)m);
  case MSG_OSD_PGTEMP:
    return prepare_pgtemp((MOSDPGTemp*)m);

  case MSG_MON_COMMAND:
    return prepare_command((MMonCommand*)m);
    
  case CEPH_MSG_POOLOP:
    return prepare_pool_op((MPoolOp*)m);

  case MSG_REMOVE_SNAPS:
    return prepare_remove_snaps((MRemoveSnaps*)m);

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

bool OSDMonitor::preprocess_failure(MOSDFailure *m)
{
  // who is target_osd
  int badboy = m->get_target().name.num();

  // check permissions
  MonSession *session = m->get_session();
  if (!session)
    goto didit;
  if (!session->caps.check_privileges(PAXOS_OSDMAP, MON_CAP_X)) {
    dout(0) << "got MOSDFailure from entity with insufficient caps "
	    << session->caps << dendl;
    goto didit;
  }

  if (ceph_fsid_compare(&m->fsid, &mon->monmap->fsid)) {
    dout(0) << "preprocess_failure on fsid " << m->fsid << " != " << mon->monmap->fsid << dendl;
    goto didit;
  }

  // first, verify the reporting host is valid
  if (m->get_orig_source().is_osd()) {
    int from = m->get_orig_source().num();
    if (!osdmap.exists(from) ||
	osdmap.get_addr(from) != m->get_orig_source_inst().addr ||
	osdmap.is_down(from)) {
      dout(5) << "preprocess_failure from dead osd" << from << ", ignoring" << dendl;
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

  dout(10) << "preprocess_failure new: " << m->get_target() << ", from " << m->get_orig_source_inst() << dendl;
  return false;

 didit:
  m->put();
  return true;
}

bool OSDMonitor::prepare_failure(MOSDFailure *m)
{
  dout(1) << "prepare_failure " << m->get_target() << " from " << m->get_orig_source_inst()
          << " is reporting failure:" << m->if_osd_failed() << dendl;
  mon->clog.info() << m->get_target() << " failed (by "
		     << m->get_orig_source_inst() << ")\n";
  
  int target_osd = m->get_target().name.num();
  int reporter = m->get_orig_source().num();
  assert(osdmap.is_up(target_osd));
  assert(osdmap.get_addr(target_osd) == m->get_target().addr);
  
  if (m->if_osd_failed()) {
    int reports = 0;
    int reporters = 0;

    if (failed_notes.count(target_osd)) {
      multimap<int, pair<int, int> >::iterator i = failed_notes.lower_bound(target_osd);
      while ((i != failed_notes.end()) && (i->first == target_osd)) {
        if (i->second.first == reporter) {
          ++i->second.second;
          dout(10) << "adding new failure report from osd" << reporter
                   << " on osd" << target_osd << dendl;
          reporter = -1;
        }
        ++reporters;
        reports += i->second.second;
        ++i;
      }
    }
    if (reporter != -1) { //didn't get counted yet
      failed_notes.insert(pair<int, pair<int, int> >
                          (target_osd, pair<int, int>(reporter, 1)));
      ++reporters;
      ++reports;
      dout(10) << "osd" << reporter
               << " is adding failure report on osd" << target_osd << dendl;
    }

    if ((reporters >= g_conf.osd_min_down_reporters) &&
        (reports >= g_conf.osd_min_down_reports)) {
      dout(1) << "have enough reports/reporters to mark osd" << target_osd
              << " as down" << dendl;
      pending_inc.new_down[target_osd] = false;
      paxos->wait_for_commit(new C_Reported(this, m));
      //clear out failure reports
      failed_notes.erase(failed_notes.lower_bound(target_osd),
                         failed_notes.upper_bound(target_osd));
      return true;
    }
  } else { //remove the report
    multimap<int, pair<int, int> >::iterator i = failed_notes.lower_bound(target_osd);
    while ((i != failed_notes.end()) && (i->first == target_osd)
                                && (i->second.first != reporter))
      ++i;
    if ((i == failed_notes.end()) || (i->second.first != reporter))
      dout(0) << "got an OSD not-failed report from osd" << reporter
              << " that hasn't reported failure! (or in previous epoch?)" << dendl;
    else failed_notes.erase(i);
  }
  
  return false;
}

void OSDMonitor::_reported_failure(MOSDFailure *m)
{
  dout(7) << "_reported_failure on " << m->get_target() << ", telling " << m->get_orig_source_inst() << dendl;
  send_latest(m, m->get_epoch());
}


// boot --

bool OSDMonitor::preprocess_boot(MOSDBoot *m)
{
  int from = m->get_orig_source_inst().name.num();

  // check permissions, ignore if failed (no response expected)
  MonSession *session = m->get_session();
  if (!session)
    goto ignore;
  if (!session->caps.check_privileges(PAXOS_OSDMAP, MON_CAP_X)) {
    dout(0) << "got preprocess_boot message from entity with insufficient caps"
	    << session->caps << dendl;
    goto ignore;
  }

  if (ceph_fsid_compare(&m->sb.fsid, &mon->monmap->fsid)) {
    dout(0) << "preprocess_boot on fsid " << m->sb.fsid << " != " << mon->monmap->fsid << dendl;
    goto ignore;
  }

  if (m->get_orig_source_inst().addr.is_blank_addr()) {
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

  dout(10) << "preprocess_boot from " << m->get_orig_source_inst() << dendl;
  return false;

 ignore:
  m->put();
  return true;
}

bool OSDMonitor::prepare_boot(MOSDBoot *m)
{
  dout(7) << "prepare_boot from " << m->get_orig_source_inst() << " sb " << m->sb
	  << " cluster_addr " << m->cluster_addr << " hb_addr " << m->hb_addr
	  << dendl;

  assert(m->get_orig_source().is_osd());
  int from = m->get_orig_source().num();
  
  // does this osd exist?
  if (from >= osdmap.get_max_osd()) {
    dout(1) << "boot from osd" << from << " >= max_osd " << osdmap.get_max_osd() << dendl;
    m->put();
    return false;
  }

  // already up?  mark down first?
  if (osdmap.is_up(from)) {
    dout(7) << "prepare_boot was up, first marking down " << osdmap.get_inst(from) << dendl;
    assert(osdmap.get_inst(from) != m->get_orig_source_inst());  // preproces should have caught it
    
    if (pending_inc.new_down.count(from) == 0) {
      // mark previous guy down
      pending_inc.new_down[from] = false;
    }
    paxos->wait_for_commit(new C_RetryMessage(this, m));
  } else if (pending_inc.new_up_client.count(from)) { //FIXME: should this be using new_up_client?
    // already prepared, just wait
    dout(7) << "prepare_boot already prepared, waiting on " << m->get_orig_source_addr() << dendl;
    paxos->wait_for_commit(new C_RetryMessage(this, m));
  } else {
    // mark new guy up.
    down_pending_out.erase(from);  // if any

    pending_inc.new_up_client[from] = m->get_orig_source_addr();
    if (!m->cluster_addr.is_blank_addr())
      pending_inc.new_up_internal[from] = m->cluster_addr;
    pending_inc.new_hb_up[from] = m->hb_addr;

    // mark in?
    pending_inc.new_weight[from] = CEPH_OSD_IN;

    if (m->sb.weight)
      osd_weight[from] = m->sb.weight;

    // adjust last clean unmount epoch?
    const osd_info_t& info = osdmap.get_info(from);
    dout(10) << " old osd_info: " << info << dendl;
    if (m->sb.mounted > info.last_clean_first ||
	(m->sb.mounted == info.last_clean_first &&
	 m->sb.clean_thru > info.last_clean_last)) {
      epoch_t first = m->sb.mounted;
      epoch_t last = m->sb.clean_thru;

      // adjust clean interval forward to the epoch the osd was actually marked down.
      if (info.up_from == first &&
	  (info.down_at-1) > last)
	last = info.down_at-1;

      dout(10) << "prepare_boot osd" << from << " last_clean_interval "
	       << info.last_clean_first << "-" << info.last_clean_last
	       << " -> " << first << "-" << last
	       << dendl;
      pending_inc.new_last_clean_interval[from] = pair<epoch_t,epoch_t>(first, last);
    }

    // wait
    paxos->wait_for_commit(new C_Booted(this, m));
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
  if (!session->caps.check_privileges(PAXOS_OSDMAP, MON_CAP_X)) {
    dout(0) << "attempt to send MOSDAlive from entity with insufficient privileges:"
	    << session->caps << dendl;
    goto ignore;
  }

  if (osdmap.is_up(from) &&
      osdmap.get_inst(from) == m->get_orig_source_inst() &&
      osdmap.get_up_thru(from) >= m->want) {
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
  paxos->wait_for_commit(new C_ReplyMap(this, m, m->version));
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

  // check caps
  MonSession *session = m->get_session();
  if (!session)
    goto ignore;
  if (!session->caps.check_privileges(PAXOS_OSDMAP, MON_CAP_X)) {
    dout(0) << "attempt to send MOSDPGTemp from entity with insufficient caps "
	    << session->caps << dendl;
    goto ignore;
  }

  for (map<pg_t,vector<int> >::iterator p = m->pg_temp.begin(); p != m->pg_temp.end(); p++) {
    dout(20) << " " << p->first
	     << (osdmap.pg_temp.count(p->first) ? osdmap.pg_temp[p->first] : empty)
	     << " -> " << p->second << dendl;
    // removal?
    if (p->second.empty() && osdmap.pg_temp.count(p->first))
      return false;
    // change?
    if (p->second.size() && (osdmap.pg_temp.count(p->first) == 0 ||
			     osdmap.pg_temp[p->first] != p->second))
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
  for (map<pg_t,vector<int> >::iterator p = m->pg_temp.begin(); p != m->pg_temp.end(); p++)
    pending_inc.new_pg_temp[p->first] = p->second;
  pending_inc.new_up_thru[from] = m->map_epoch;   // set up_thru too, so the osd doesn't have to ask again
  paxos->wait_for_commit(new C_ReplyMap(this, m, m->map_epoch));
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
  if (!session->caps.check_privileges(PAXOS_OSDMAP, MON_CAP_RW)) {
    dout(0) << "got preprocess_remove_snaps from entity with insufficient caps "
	    << session->caps << dendl;
    goto ignore;
  }

  for (map<int, vector<snapid_t> >::iterator q = m->snaps.begin();
       q != m->snaps.end();
       q++) {
    if (!osdmap.have_pg_pool(q->first)) {
      dout(10) << " ignoring removed_snaps " << q->second << " on non-existent pool " << q->first << dendl;
      continue;
    }
    const pg_pool_t *pi = osdmap.get_pg_pool(q->first);
    for (vector<snapid_t>::iterator p = q->second.begin(); 
	 p != q->second.end();
	 p++) {
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
       p++) {
    pg_pool_t& pi = osdmap.pools[p->first];
    for (vector<snapid_t>::iterator q = p->second.begin();
	 q != p->second.end();
	 q++) {
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
	p++;
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
  if (paxos->is_readable()) {
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


void OSDMonitor::send_full(PaxosServiceMessage *m)
{
  dout(5) << "send_full to " << m->get_orig_source_inst() << dendl;
  mon->send_reply(m, new MOSDMap(mon->monmap->fsid, &osdmap));
}

MOSDMap *OSDMonitor::build_incremental(epoch_t from, epoch_t to)
{
  dout(10) << "build_incremental [" << from << ".." << to << "]" << dendl;
  MOSDMap *m = new MOSDMap(mon->monmap->fsid);
  
  for (epoch_t e = to;
       e >= from && e > 0;
       e--) {
    bufferlist bl;
    if (mon->store->get_bl_sn(bl, "osdmap", e) > 0) {
      dout(20) << "send_incremental    inc " << e << " " << bl.length() << " bytes" << dendl;
      m->incremental_maps[e] = bl;
    } 
    else if (mon->store->get_bl_sn(bl, "osdmap_full", e) > 0) {
      dout(20) << "send_incremental   full " << e << " " << bl.length() << " bytes" << dendl;
      m->maps[e] = bl;
    }
    else {
      assert(0);  // we should have all maps.
    }
  }
  return m;
}

void OSDMonitor::send_incremental(PaxosServiceMessage *req, epoch_t first)
{
  dout(5) << "send_incremental [" << first << ".." << osdmap.get_epoch() << "]"
	  << " to " << req->get_orig_source_inst() << dendl;
  MOSDMap *m = build_incremental(first, osdmap.get_epoch());
  mon->send_reply(req, m);
}

void OSDMonitor::send_incremental(epoch_t first, entity_inst_t& dest)
{
  dout(5) << "send_incremental [" << first << ".." << osdmap.get_epoch() << "]"
	  << " to " << dest << dendl;
  while (first <= osdmap.get_epoch()) {
    epoch_t last = MIN(first + 100, osdmap.get_epoch());
    MOSDMap *m = build_incremental(first, last);
    mon->messenger->send_message(m, dest);
    first = last + 1;
  }
}




epoch_t OSDMonitor::blacklist(entity_addr_t a, utime_t until)
{
  dout(10) << "blacklist " << a << " until " << until << dendl;
  pending_inc.new_blacklist[a] = until;
  return pending_inc.epoch;
}


void OSDMonitor::check_subs()
{
  string type = "osdmap";
  xlist<Subscription*>::iterator p = mon->session_map.subs[type].begin();
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
      send_incremental(sub->next, sub->session->inst);
    else
      mon->messenger->send_message(new MOSDMap(mon->monmap->fsid, &osdmap),
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
  if (!paxos->is_active()) return;

  update_from_paxos();
  dout(10) << osdmap << dendl;

  if (!mon->is_leader()) return;

  bool do_propose = false;

  // mark down osds out?
  utime_t now = g_clock.now();
  map<int,utime_t>::iterator i = down_pending_out.begin();
  while (i != down_pending_out.end()) {
    int o = i->first;
    utime_t down = now;
    down -= i->second;
    i++;

    if (osdmap.is_down(o) && osdmap.is_in(o)) {
      if (g_conf.mon_osd_down_out_interval > 0 &&
	  down.sec() >= g_conf.mon_osd_down_out_interval) {
	dout(10) << "tick marking osd" << o << " OUT after " << down
		 << " sec (target " << g_conf.mon_osd_down_out_interval << ")" << dendl;
	pending_inc.new_weight[o] = CEPH_OSD_OUT;
	do_propose = true;
	
	mon->clog.info() << "osd" << o << " out (down for " << down << ")\n";
      } else
	continue;
    }

    down_pending_out.erase(o);
  }

  // expire blacklisted items?
  for (hash_map<entity_addr_t,utime_t>::iterator p = osdmap.blacklist.begin();
       p != osdmap.blacklist.end();
       p++) {
    if (p->second < now) {
      dout(10) << "expiring blacklist item " << p->first << " expired " << p->second << " < now " << now << dendl;
      pending_inc.old_blacklist.push_back(p->first);
      do_propose = true;
    }
  }

  //if map full setting has changed, get that info out there!
  if (mon->pgmon()->paxos->is_readable()) {
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
    for (int pool=0; pool<1; pool++)
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

  if (do_propose ||
      !pending_inc.new_pg_temp.empty())  // also propose if we adjusted pg_temp
    propose_pending();
}


void OSDMonitor::handle_osd_timeouts(const utime_t &now,
			 const std::map<int,utime_t> &last_osd_report)
{
  utime_t timeo(g_conf.mon_osd_report_timeout, 0);
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
      derr << "OSDMonitor::handle_osd_timeouts: never got MOSDPGStat "
	   << "info from osd " << i << ". Marking down!" << dendl;
      pending_inc.new_down[i] = true;
      new_down = true;
    }
    else {
      utime_t diff(now);
      diff -= t->second;
      if (diff > timeo) {
	derr << "OSDMonitor::handle_osd_timeouts: last got MOSDPGStat "
	     << "info from osd " << i << " at " << t->second << ". It has "
	     << "been " << diff << ", so we're marking it down!" << dendl;
	pending_inc.new_down[i] = true;
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
       it++) {
    if (osdmap.is_down(*it)) continue;
    pending_inc.new_down[*it] = true;  // FIXME: am i sure it's clean? we need a proper osd shutdown sequence!
  }

  propose_pending();
}

enum health_status_t OSDMonitor::get_health(std::ostream &ss) const
{
  enum health_status_t ret(HEALTH_OK);

  int num_osds = osdmap.get_num_osds();
  int num_up_osds = osdmap.get_num_up_osds();
  int num_in_osds = osdmap.get_num_in_osds();

  if (num_osds == 0) {
    ss << "no osds";
    ret = HEALTH_ERR;
  } else {
    if (num_up_osds < num_osds) {
      ss << (num_osds - num_up_osds) << "/" << num_osds << " osds down";
      ret = HEALTH_WARN;
    }
    if (num_in_osds < num_osds) {
      if (ret != HEALTH_OK)
	ss << ", ";
      ss << (num_osds - num_in_osds) << "/" << num_osds << " osds out";
      ret = HEALTH_WARN;
    }
  }
  return ret;
}

bool OSDMonitor::preprocess_command(MMonCommand *m)
{
  int r = -1;
  bufferlist rdata;
  stringstream ss;

  if (m->cmd.size() > 1) {
    if (m->cmd[1] == "stat") {
      osdmap.print_summary(ss);
      r = 0;
    }
    else if (m->cmd[1] == "dump" ||
	     m->cmd[1] == "getmap" ||
	     m->cmd[1] == "getcrushmap") {
      OSDMap *p = &osdmap;
      if (m->cmd.size() > 2) {
	epoch_t e = atoi(m->cmd[2].c_str());
	bufferlist b;
	mon->store->get_bl_sn(b,"osdmap_full", e);
	if (!b.length()) {
	  p = 0;
	  r = -ENOENT;
	} else {
	  p = new OSDMap;
	  p->decode(b);
	}
      }
      if (p) {
	if (m->cmd[1] == "dump") {
	  stringstream ds;
	  p->print(ds);
	  rdata.append(ds);
	  ss << "dumped osdmap epoch " << p->get_epoch();
	} else if (m->cmd[1] == "getmap") {
	  p->encode(rdata);
	  ss << "got osdmap epoch " << p->get_epoch();
	} else if (m->cmd[1] == "getcrushmap") {
	  p->crush.encode(rdata);
	  ss << "got crush map from osdmap epoch " << p->get_epoch();
	}
	if (p != &osdmap)
	  delete p;
	r = 0;
      }
    }
    else if (m->cmd[1] == "getmaxosd") {
      ss << "max_osd = " << osdmap.get_max_osd() << " in epoch " << osdmap.get_epoch();
      r = 0;
    }
    else if (m->cmd[1] == "injectargs") {
      if (m->cmd.size() != 4) {
	r = -EINVAL;
	ss << "usage: osd injectargs <who> <args>";
	goto out;
      }
      if (m->cmd[2] == "*") {
	for (int i=0; i<osdmap.get_max_osd(); i++)
	  if (osdmap.is_up(i))
	    mon->inject_args(osdmap.get_inst(i), m->cmd[3]);
	r = 0;
	ss << "ok bcast";
      } else {
	errno = 0;
	int who = strtol(m->cmd[2].c_str(), 0, 10);
	if (!errno && who >= 0 && osdmap.is_up(who)) {
	  mon->inject_args(osdmap.get_inst(who), m->cmd[3]);
	  r = 0;
	  ss << "ok";
	} else 
	  ss << "specify osd number or *";
      }
    }
    else if (m->cmd[1] == "tell") {
      if (m->cmd.size() < 4) {
	r = -EINVAL;
	ss << "usage: osd tell <who> <what>";
	goto out;
      }
      m->cmd.erase(m->cmd.begin()); //take out first two args; don't need them
      m->cmd.erase(m->cmd.begin());
      if (m->cmd[0] == "*") {
	m->cmd.erase(m->cmd.begin()); //and now we're done with the target num
	for (int i = 0; i < osdmap.get_max_osd(); ++i)
	  if (osdmap.is_up(i))
	    mon->send_command(osdmap.get_inst(i), m->cmd, paxos->get_version());
	r = 0;
	ss << "ok";
      } else {
	errno = 0;
	int who = strtol(m->cmd[0].c_str(), 0, 10);
	m->cmd.erase(m->cmd.begin()); //done with target num now
	if (!errno && who >= 0) {
	  if (osdmap.is_up(who)) {
	    mon->send_command(osdmap.get_inst(who), m->cmd, paxos->get_version());
	    r = 0;
	    ss << "ok";
	  } else {
	    ss << "osd" << who << " not up";
	    r = -ENOENT;
	  }
	} else ss << "specify osd number or *";
      }
    }
    else if ((m->cmd[1] == "scrub" || m->cmd[1] == "repair")) {
      if (m->cmd.size() <= 2) {
	r = -EINVAL;
	ss << "usage: osd [scrub|repair] <who>";
	goto out;
      }
      if (m->cmd[2] == "*") {
	ss << "osds ";
	int c = 0;
	for (int i=0; i<osdmap.get_max_osd(); i++)
	  if (osdmap.is_up(i)) {
	    ss << (c++ ? ",":"") << i;
	    mon->try_send_message(new MOSDScrub(osdmap.get_fsid(),
						m->cmd[1] == "repair"),
				  osdmap.get_inst(i));
	  }	    
	r = 0;
	ss << " instructed to " << m->cmd[1];
      } else {
	long osd = strtol(m->cmd[2].c_str(), 0, 10);
	if (osdmap.is_up(osd)) {
	  mon->try_send_message(new MOSDScrub(osdmap.get_fsid(),
					      m->cmd[1] == "repair"),
				osdmap.get_inst(osd));
	  r = 0;
	  ss << "osd" << osd << " instructed to " << m->cmd[1];
	} else 
	  ss << "osd" << osd << " is not up";
      }
    }
    else if (m->cmd[1] == "lspools") {
      uint64_t uid_pools = 0;
      if (m->cmd.size() > 2) {
	uid_pools = strtol(m->cmd[2].c_str(), NULL, 10);
      }
      for (map<int, pg_pool_t>::iterator p = osdmap.pools.begin();
	   p !=osdmap.pools.end();
	   ++p) {
	if (!uid_pools || p->second.v.auid == uid_pools) {
	  ss << p->first << ' ' << osdmap.pool_name[p->first] << ',';
	}
      }
      r = 0;
    }
    else if (m->cmd.size() == 3 && m->cmd[1] == "blacklist" && m->cmd[2] == "ls") {
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
      r = 0;
    }
  }
 out:
  if (r != -1) {
    string rs;
    getline(ss, rs);
    mon->reply_command(m, r, rs, rdata, paxos->get_version());
    return true;
  } else
    return false;
}

int OSDMonitor::prepare_new_pool(MPoolOp *m)
{
  dout(10) << "prepare_new_pool from " << m->get_connection() << dendl;
  MonSession *session = m->get_session();
  if (!session)
    return -EPERM;
  if (m->auid)
    return prepare_new_pool(m->name, m->auid);
  else
    return prepare_new_pool(m->name, session->caps.auid, m->crush_rule);
}

int OSDMonitor::prepare_new_pool(string& name, uint64_t auid, int crush_rule)
{
  if (osdmap.name_pool.count(name)) {
    return -EEXIST;
  }
  if (-1 == pending_inc.new_pool_max)
    pending_inc.new_pool_max = osdmap.pool_max;
  int pool = ++pending_inc.new_pool_max;
  pending_inc.new_pools[pool].v.type = CEPH_PG_TYPE_REP;

  pending_inc.new_pools[pool].v.size = g_conf.osd_pool_default_size;
  if (crush_rule >= 0)
    pending_inc.new_pools[pool].v.crush_ruleset = crush_rule;
  else
    pending_inc.new_pools[pool].v.crush_ruleset = g_conf.osd_pool_default_crush_rule;
  pending_inc.new_pools[pool].v.object_hash = CEPH_STR_HASH_RJENKINS;
  pending_inc.new_pools[pool].v.pg_num = g_conf.osd_pool_default_pg_num;
  pending_inc.new_pools[pool].v.pgp_num = g_conf.osd_pool_default_pgp_num;
  pending_inc.new_pools[pool].v.lpg_num = 0;
  pending_inc.new_pools[pool].v.lpgp_num = 0;
  pending_inc.new_pools[pool].v.last_change = pending_inc.epoch;
  pending_inc.new_pools[pool].v.auid = auid;
  pending_inc.new_pool_names[pool] = name;
  return 0;
}

bool OSDMonitor::prepare_command(MMonCommand *m)
{
  stringstream ss;
  string rs;
  int err = -EINVAL;
  if (m->cmd.size() > 1) {
    if (m->cmd[1] == "setcrushmap") {
      dout(10) << "prepare_command setting new crush map" << dendl;
      bufferlist data(m->get_data());
      try {
	bufferlist::iterator bl(data.begin());
	CrushWrapper crush;
	crush.decode(bl);
      }
      catch (const std::exception &e) {
	err = -EINVAL;
	ss << "Failed to parse crushmap: " << e.what();
	goto out;
      }
      pending_inc.crush = data;
      string rs = "set crush map";
      paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
      return true;
    }
    /*else if (m->cmd[1] == "setmap" && m->cmd.size() == 3) {
      OSDMap map;
      map.decode(m->get_data());
      epoch_t e = atoi(m->cmd[2].c_str());
      if (ceph_fsid_compare(&map.fsid, &mon->monmap->fsid) == 0) {
	if (pending_inc.epoch == e) {
	  map.set_epoch(pending_inc.epoch);  // make sure epoch is correct
	  map.encode(pending_inc.fullmap);
	  string rs = "set osd map";
	  paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	  return true;
	} else
	  ss << "next osdmap epoch " << pending_inc.epoch << " != " << e;
      } else
	  ss << "osdmap fsid " << map.fsid << " does not match monitor fsid " << mon->monmap->fsid;
      err = -EINVAL;
    }
    */
    else if (m->cmd[1] == "setmaxosd" && m->cmd.size() > 2) {
      pending_inc.new_max_osd = atoi(m->cmd[2].c_str());
      ss << "set new max_osd = " << pending_inc.new_max_osd;
      getline(ss, rs);
      paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
      return true;
    }
    else if (m->cmd[1] == "pause") {
      if (pending_inc.new_flags < 0)
	pending_inc.new_flags = osdmap.get_flags();
      pending_inc.new_flags |= CEPH_OSDMAP_PAUSERD | CEPH_OSDMAP_PAUSEWR;
      ss << "pause rd+wr";
      getline(ss, rs);
      paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
      return true;
    }
    else if (m->cmd[1] == "unpause") {
      if (pending_inc.new_flags < 0)
	pending_inc.new_flags = osdmap.get_flags();
      pending_inc.new_flags &= ~(CEPH_OSDMAP_PAUSERD | CEPH_OSDMAP_PAUSEWR);
      ss << "unpause rd+wr";
      getline(ss, rs);
      paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
      return true;
    }
    else if (m->cmd[1] == "down" && m->cmd.size() == 3) {
      long osd = strtol(m->cmd[2].c_str(), 0, 10);
      if (!osdmap.exists(osd)) {
	ss << "osd" << osd << " does not exist";
      } else if (osdmap.is_down(osd)) {
	ss << "osd" << osd << " is already down";
      } else {
	pending_inc.new_down[osd] = false;
	ss << "marked down osd" << osd;
	getline(ss, rs);
	paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	return true;
      }
    }
    else if (m->cmd[1] == "out" && m->cmd.size() == 3) {
      long osd = strtol(m->cmd[2].c_str(), 0, 10);
      if (!osdmap.exists(osd)) {
	ss << "osd" << osd << " does not exist";
      } else if (osdmap.is_out(osd)) {
	ss << "osd" << osd << " is already out";
      } else {
	pending_inc.new_weight[osd] = CEPH_OSD_OUT;
	ss << "marked out osd" << osd;
	getline(ss, rs);
	paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	return true;
      } 
    }
    else if (m->cmd[1] == "in" && m->cmd.size() == 3) {
      long osd = strtol(m->cmd[2].c_str(), 0, 10);
      if (osdmap.is_in(osd)) {
	ss << "osd" << osd << " is already in";
      } else if (!osdmap.exists(osd)) {
	ss << "osd" << osd << " does not exist";
      } else {
	pending_inc.new_weight[osd] = CEPH_OSD_IN;
	ss << "marked in osd" << osd;
	getline(ss, rs);
	paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	return true;
      } 
    }
    else if (m->cmd[1] == "reweight" && m->cmd.size() == 4) {
      long osd = strtol(m->cmd[2].c_str(), 0, 10);
      float w = strtof(m->cmd[3].c_str(), 0);
      long ww = (int)((float)CEPH_OSD_IN*w);
      if (osdmap.exists(osd)) {
	pending_inc.new_weight[osd] = ww;
	ss << "reweighted osd" << osd << " to " << w << " (" << ios::hex << ww << ios::dec << ")";
	getline(ss, rs);
	paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	return true;
      } 
    }
    else if (m->cmd[1] == "lost" && m->cmd.size() >= 3) {
      string err;
      int osd = strict_strtol(m->cmd[2].c_str(), 10, &err);
      if (!err.empty()) {
	ss << err;
      }
      else if ((m->cmd.size() < 4) || m->cmd[3] != "--yes-i-really-mean-it") {
	ss << "are you SURE?  this might mean real, permanent data loss.  pass "
	      "--yes-i-really-mean-it if you really do.";
      }
      else if (!osdmap.exists(osd) || !osdmap.is_down(osd)) {
	ss << "osd" << osd << " is not down or doesn't exist";
      } else {
	epoch_t e = osdmap.get_info(osd).down_at;
	pending_inc.new_lost[osd] = e;
	ss << "marked osd lost in epoch " << e;
	getline(ss, rs);
	paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	return true;
      }
    }
    else if (m->cmd[1] == "blacklist" && m->cmd.size() >= 4) {
      entity_addr_t addr;
      if (!addr.parse(m->cmd[3].c_str(), 0))
	ss << "unable to parse address " << m->cmd[3];
      else if (m->cmd[2] == "add") {

	utime_t expires = g_clock.now();
	double d = 60*60;  // 1 hour default
	if (m->cmd.size() > 4)
	  d = atof(m->cmd[4].c_str());
	expires += d;

	pending_inc.new_blacklist[addr] = expires;
	ss << "blacklisting " << addr << " until " << expires << " (" << d << " sec)";
	getline(ss, rs);
	paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	return true;
      } else if (m->cmd[2] == "rm") {
	if (osdmap.is_blacklisted(addr) || 
	    pending_inc.new_blacklist.count(addr)) {
	  if (osdmap.is_blacklisted(addr))
	    pending_inc.old_blacklist.push_back(addr);
	  else
	    pending_inc.new_blacklist.erase(addr);
	  ss << "un-blacklisting " << addr;
	  getline(ss, rs);
	  paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	  return true;
	}
	ss << addr << " isn't blacklisted";
	err = -ENOENT;
	goto out;
      }
    }
    else if (m->cmd[1] == "pool" && m->cmd.size() >= 3) {
      if (m->cmd.size() >= 5 && m->cmd[2] == "mksnap") {
	int pool = osdmap.lookup_pg_pool_name(m->cmd[3].c_str());
	if (pool < 0) {
	  ss << "unrecognized pool '" << m->cmd[3] << "'";
	  err = -ENOENT;
	} else {
	  const pg_pool_t *p = osdmap.get_pg_pool(pool);
	  pg_pool_t *pp = 0;
	  if (pending_inc.new_pools.count(pool))
	    pp = &pending_inc.new_pools[pool];
	  const string& snapname = m->cmd[4];
	  if (p->snap_exists(snapname.c_str()) ||
	      (pp && pp->snap_exists(snapname.c_str()))) {
	    ss << "pool " << m->cmd[3] << " snap " << snapname << " already exists";
	    err = -EEXIST;
	  } else {
	    if (!pp) {
	      pp = &pending_inc.new_pools[pool];
	      *pp = *p;
	    }
	    pp->add_snap(snapname.c_str(), g_clock.now());
	    pp->set_snap_epoch(pending_inc.epoch);
	    ss << "created pool " << m->cmd[3] << " snap " << snapname;
	    getline(ss, rs);
	    paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	    return true;
	  }
	}
      }
      else if (m->cmd.size() >= 5 && m->cmd[2] == "rmsnap") {
	int pool = osdmap.lookup_pg_pool_name(m->cmd[3].c_str());
	if (pool < 0) {
	  ss << "unrecognized pool '" << m->cmd[3] << "'";
	  err = -ENOENT;
	} else {
	  const pg_pool_t *p = osdmap.get_pg_pool(pool);
	  pg_pool_t *pp = 0;
	  if (pending_inc.new_pools.count(pool))
	    pp = &pending_inc.new_pools[pool];
	  const string& snapname = m->cmd[4];
	  if (!p->snap_exists(snapname.c_str()) &&
	      (!pp || !pp->snap_exists(snapname.c_str()))) {
	    ss << "pool " << m->cmd[3] << " snap " << snapname << " does not exists";
	    err = -ENOENT;
	  } else {
	    if (!pp) {
	      pp = &pending_inc.new_pools[pool];
	      *pp = *p;
	    }
	    snapid_t sn = pp->snap_exists(snapname.c_str());
	    pp->remove_snap(sn);
	    pp->set_snap_epoch(pending_inc.epoch);
	    ss << "removed pool " << m->cmd[3] << " snap " << snapname;
	    getline(ss, rs);
	    paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	    return true;
	  }
	}
      }
      else if (m->cmd[2] == "create" && m->cmd.size() >= 4) {
        int ret = prepare_new_pool(m->cmd[3]);
        if (ret < 0) {
          if (ret == -EEXIST)
            ss << "pool '" << m->cmd[3] << "' exists";
          err = ret;
          goto out;
        }
	ss << "pool '" << m->cmd[3] << "' created";
	getline(ss, rs);
	paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	return true;
      } else if (m->cmd[2] == "delete" && m->cmd.size() >= 4) {
	//hey, let's delete a pool!
	int pool = osdmap.lookup_pg_pool_name(m->cmd[3].c_str());
	if (pool < 0) {
	  ss << "unrecognized pool '" << m->cmd[3] << "'";
	  err = -ENOENT;
	} else {
	  pending_inc.old_pools.insert(pool);
	  ss << "pool '" << m->cmd[3] << "' deleted";
	  getline(ss, rs);
	  paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	  return true;
	}
      } else if (m->cmd[2] == "set") {
	if (m->cmd.size() != 6) {
	  err = -EINVAL;
	  ss << "usage: osd pool set <poolname> <field> <value>";
	  goto out;
	}
	int pool = osdmap.lookup_pg_pool_name(m->cmd[3].c_str());
	if (pool < 0) {
	  ss << "unrecognized pool '" << m->cmd[3] << "'";
	  err = -ENOENT;
	} else {
	  const pg_pool_t *p = osdmap.get_pg_pool(pool);
	  int n = atoi(m->cmd[5].c_str());
	  if (n) {
	    if (m->cmd[4] == "size") {
	      pending_inc.new_pools[pool] = *p;
	      pending_inc.new_pools[pool].v.size = n;
	      ss << "set pool " << pool << " size to " << n;
	      getline(ss, rs);
	      paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	      return true;
	    } else if (m->cmd[4] == "pg_num") {
	      if (n <= p->get_pg_num()) {
		ss << "specified pg_num " << n << " <= current " << p->get_pg_num();
	      } else if (!mon->pgmon()->pg_map.creating_pgs.empty()) {
		ss << "currently creating pgs, wait";
		err = -EAGAIN;
	      } else {
		pending_inc.new_pools[pool] = osdmap.pools[pool];
		pending_inc.new_pools[pool].v.pg_num = n;
		ss << "set pool " << pool << " pg_num to " << n;
		getline(ss, rs);
		paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
		return true;
	      }
	    } else if (m->cmd[4] == "pgp_num") {
	      if (n <= p->get_pgp_num()) {
		ss << "specified pgp_num " << n << " <= current " << p->get_pgp_num();
	      } else if (n > p->get_pg_num()) {
		ss << "specified pgp_num " << n << " > pg_num " << p->get_pg_num();
	      } else if (!mon->pgmon()->pg_map.creating_pgs.empty()) {
		ss << "still creating pgs, wait";
		err = -EAGAIN;
	      } else {
		pending_inc.new_pools[pool] = osdmap.pools[pool];
		pending_inc.new_pools[pool].v.pgp_num = n;
		ss << "set pool " << pool << " pgp_num to " << n;
		getline(ss, rs);
		paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
		return true;
	      }
	    } else {
	      ss << "unrecognized pool field " << m->cmd[4];
	    }
	  }
	}
      }
      else if (m->cmd[2] == "get") {
	if (m->cmd.size() != 5) {
	  err = -EINVAL;
	  ss << "usage: osd pool get <poolname> <field>";
	  goto out;
	}
	int pool = osdmap.lookup_pg_pool_name(m->cmd[3].c_str());
	if (pool < 0) {
	  ss << "unrecognized pool '" << m->cmd[3] << "'";
	  err = -ENOENT;
	  goto out;
	}

	const pg_pool_t *p = osdmap.get_pg_pool(pool);
	if (m->cmd[4] == "pg_num") {
	  ss << "PG_NUM: " << p->get_pg_num();
	  err = 0;
	  goto out;
	}
	if (m->cmd[4] == "pgp_num") {
	  ss << "PGP_NUM: " << p->get_pgp_num();
	  err = 0;
	  goto out;
	}
	if (m->cmd[4] == "lpg_num") {
	  ss << "LPG_NUM: " << p->get_lpg_num();
	  err = 0;
	  goto out;
	}
	if (m->cmd[4] == "lpgp_num") {
	  ss << "LPPG_NUM: " << p->get_lpgp_num();
	  err = 0;
	  goto out;
	}
	ss << "don't know how to get pool field " << m->cmd[4];
	goto out;
      }
    }
    else if ((m->cmd.size() > 1) &&
	     (m->cmd[1] == "reweight-by-utilization")) {
      int oload = 120;
      if (m->cmd.size() > 2) {
	oload = atoi(m->cmd[2].c_str());
      }
      string out_str;
      err = reweight_by_utilization(oload, out_str);
      if (err) {
	ss << "FAILED to reweight-by-utilization: " << out_str;
      }
      else {
	ss << "SUCCESSFUL reweight-by-utilization: " << out_str;
      }
    }
    else {
      ss << "unknown command " << m->cmd[1];
    }
  } else {
    ss << "no command?";
  }
out:
  getline(ss, rs);
  mon->reply_command(m, err, rs, paxos->get_version());
  return false;
}

bool OSDMonitor::preprocess_pool_op(MPoolOp *m) 
{
  dout(10) << "m->op=" << m->op << dendl;
  if (m->op == POOL_OP_CREATE) {
    return preprocess_pool_op_create(m);
  }

  bool snap_exists = false;
  pg_pool_t *pp = 0;
  if (pending_inc.new_pools.count(m->pool))
    pp = &pending_inc.new_pools[m->pool];
  // check if the snap and snapname exists
  if (!osdmap.get_pg_pool(m->pool)) {
    // uh-oh, bad pool num!
    dout(10) << "attempt to delete non-existent pool id " << m->pool << dendl;
    _pool_op(m, -ENODATA, pending_inc.epoch);
    return true;
  }
  if ((osdmap.get_pg_pool(m->pool)->snap_exists(m->name.c_str())) ||
      (pp && pp->snap_exists(m->name.c_str()))) snap_exists = true;

  switch (m->op) {
  case POOL_OP_CREATE_SNAP:
    if(snap_exists) {
      _pool_op(m, -EEXIST, pending_inc.epoch);
      return true;
    }
    return false; //this message needs to go through preparation
  case POOL_OP_CREATE_UNMANAGED_SNAP:
    return false; //gotta go through the leader and allocate a unique snap_id
  case POOL_OP_DELETE_SNAP:
    if (!snap_exists) {
      _pool_op(m, -ENOENT, pending_inc.epoch);
      return true; //done with this message
    }
    return false;
  case POOL_OP_DELETE_UNMANAGED_SNAP:
    return false;
  case POOL_OP_DELETE: //can't delete except on master
    return false;
  case POOL_OP_AUID_CHANGE:
    return false; //can't change except on master
  default:
    assert(0);
    break;
  }

  return false;
}

bool OSDMonitor::preprocess_pool_op_create ( MPoolOp *m)
{
  MonSession *session = m->get_session();
  if (!session) {
    _pool_op(m, -EPERM, pending_inc.epoch);
    return true;
  }
  if ((m->auid && !session->caps.check_privileges(PAXOS_OSDMAP, MON_CAP_W, m->auid)) ||
      (!m->auid && !session->caps.check_privileges(PAXOS_OSDMAP, MON_CAP_W))) {
    if (session)
      dout(5) << "attempt to create new pool without sufficient auid privileges!"
	      << "message: " << *m  << std::endl
	      << "caps: " << m->get_session()->caps << dendl;
    _pool_op(m, -EPERM, pending_inc.epoch);
    return true;
  }

  int pool = osdmap.lookup_pg_pool_name(m->name.c_str());
  if (pool >= 0) {
    _pool_op(m, -EEXIST, pending_inc.epoch);
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
  } else if (m->op == POOL_OP_AUID_CHANGE) {
    return prepare_pool_op_auid(m);
  }

  // pool snaps vs unmanaged snaps are mutually exclusive
  const pg_pool_t *p = osdmap.get_pg_pool(m->pool);
  int rc = 0;
  switch (m->op) {
  case POOL_OP_CREATE_SNAP:
  case POOL_OP_DELETE_SNAP:
    if (!p->removed_snaps.empty())
      rc = -EINVAL;
    break;

  case POOL_OP_CREATE_UNMANAGED_SNAP:
  case POOL_OP_DELETE_UNMANAGED_SNAP:
    if (!p->snaps.empty())
      rc = -EINVAL;
    break;
  }
  if (rc) {
    _pool_op(m, rc, 0);
    return false;
  }

  pg_pool_t* pp = 0;
  bufferlist *blp = NULL;
  uint64_t snapid(0);

  // if the pool isn't already in the update, add it
  if (!pending_inc.new_pools.count(m->pool))
    pending_inc.new_pools[m->pool] = *p;
  pp = &pending_inc.new_pools[m->pool];

  switch (m->op) {
  case POOL_OP_CREATE_SNAP:
    pp->add_snap(m->name.c_str(), g_clock.now());
    dout(10) << "create snap in pool " << m->pool << " " << m->name << " seq " << pp->get_snap_epoch() << dendl;
    break;

  case POOL_OP_DELETE_SNAP:
    pp->remove_snap(pp->snap_exists(m->name.c_str()));
    break;

  case POOL_OP_CREATE_UNMANAGED_SNAP:
    blp = new bufferlist();
    pp->add_unmanaged_snap(snapid);
    ::encode(snapid, *blp);
    break;

  case POOL_OP_DELETE_UNMANAGED_SNAP:
    pp->remove_unmanaged_snap(m->snapid);
    break;

  default:
    assert(0);
    break;
  }
  pp->set_snap_epoch(pending_inc.epoch);

  paxos->wait_for_commit(new OSDMonitor::C_PoolOp(this, m, 0, pending_inc.epoch, blp));
  propose_pending();
  return false;
}

bool OSDMonitor::prepare_pool_op_create(MPoolOp *m)
{
  int err = prepare_new_pool(m);
  if (!err) {
    paxos->wait_for_commit(new OSDMonitor::C_PoolOp(this, m, err, pending_inc.epoch));
  } else {
    dout(10) << "prepare_new_pool returned err " << strerror(-err) << dendl;
    _pool_op(m, err, pending_inc.epoch);
  }
  return true;
}

bool OSDMonitor::prepare_pool_op_delete(MPoolOp *m)
{
  pending_inc.old_pools.insert(m->pool);
  paxos->wait_for_commit(new OSDMonitor::C_PoolOp(this, m, 0, pending_inc.epoch));
  return true;
}

bool OSDMonitor::prepare_pool_op_auid(MPoolOp *m)
{
  // check that current user can write to new auid
  MonSession *session = m->get_session();
  if (!session)
    goto fail;
  if (session->caps.check_privileges(PAXOS_OSDMAP, MON_CAP_W, m->auid)) {
    // check that current user can write to old auid
    int old_auid = osdmap.get_pg_pool(m->pool)->v.auid;
    if (session->caps.check_privileges(PAXOS_OSDMAP, MON_CAP_W, old_auid)) {
      // update pg_pool_t with new auid
      pending_inc.new_pools[m->pool] = *(osdmap.get_pg_pool(m->pool));
      pending_inc.new_pools[m->pool].v.auid = m->auid;
      paxos->wait_for_commit(new OSDMonitor::C_PoolOp(this, m, 0, pending_inc.epoch));
      return true;
    }
  }

 fail:
  // if it gets here it failed a permissions check
  _pool_op(m, -EPERM, pending_inc.epoch);
  return true;
}

void OSDMonitor::_pool_op(MPoolOp *m, int replyCode, epoch_t epoch, bufferlist *blp)
{
  dout(20) << "_pool_op returning with replyCode " << replyCode << dendl;
  MPoolOpReply *reply = new MPoolOpReply(m->fsid, m->get_tid(),
					 replyCode, epoch, paxos->get_version(), blp);
  mon->send_reply(m, reply);
  m->put();
}
