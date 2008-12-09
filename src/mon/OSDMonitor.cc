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

#include "messages/MOSDFailure.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDGetMap.h"
#include "messages/MOSDBoot.h"
#include "messages/MOSDAlive.h"
#include "messages/MMonCommand.h"
#include "messages/MRemoveSnaps.h"
#include "messages/MOSDScrub.h"

#include "common/Timer.h"

#include "config.h"

#include <sstream>

#define DOUT_SUBSYS mon
#undef dout_prefix
#define dout_prefix _prefix(mon, osdmap)
static ostream& _prefix(Monitor *mon, OSDMap& osdmap) {
  return *_dout << dbeginl 
		<< "mon" << mon->whoami
		<< (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)")))
		<< ".osd e" << osdmap.get_epoch() << " ";
}


// FAKING

class C_Mon_FakeOSDFailure : public Context {
  OSDMonitor *mon;
  int osd;
  bool down;
public:
  C_Mon_FakeOSDFailure(OSDMonitor *m, int o, bool d) : mon(m), osd(o), down(d) {}
  void finish(int r) {
    mon->fake_osd_failure(osd,down);
  }
};

void OSDMonitor::fake_osd_failure(int osd, bool down) 
{
  if (down) {
    dout(1) << "fake_osd_failure DOWN osd" << osd << dendl;
    pending_inc.new_down[osd] = false;
  } else {
    dout(1) << "fake_osd_failure OUT osd" << osd << dendl;
    pending_inc.new_weight[osd] = CEPH_OSD_OUT;
  }
  propose_pending();

  // fixme
  //bcast_latest_osd();
  //bcast_latest_mds();
}

void OSDMonitor::fake_osdmap_update()
{
  dout(1) << "fake_osdmap_update" << dendl;
  propose_pending();

  // tell a random osd
  int osd = rand() % g_conf.num_osd;
  send_latest(osdmap.get_inst(osd));
}


void OSDMonitor::fake_reorg() 
{
  int r = rand() % g_conf.num_osd;
  
  if (osdmap.is_out(r)) {
    dout(1) << "fake_reorg marking osd" << r << " in" << dendl;
    pending_inc.new_weight[r] = CEPH_OSD_IN;
  } else {
    dout(1) << "fake_reorg marking osd" << r << " out" << dendl;
    pending_inc.new_weight[r] = CEPH_OSD_OUT;
  }

  propose_pending();
  send_latest(osdmap.get_inst(r));  // after
}


ostream& operator<<(ostream& out, OSDMonitor& om)
{
  om.osdmap.print_summary(out);
  return out;
}


/************ MAPS ****************/


void OSDMonitor::create_initial()
{
  dout(10) << "create_initial for " << mon->monmap->fsid << " from g_conf" << dendl;

  OSDMap newmap;
  newmap.epoch = 1;
  newmap.set_fsid(mon->monmap->fsid);
  newmap.ctime = g_clock.now();
  newmap.crush.create(); // empty crush map

  // encode into pending incremental
  newmap.encode(pending_inc.fullmap);
}





bool OSDMonitor::update_from_paxos()
{
  assert(paxos->is_active());

  version_t paxosv = paxos->get_version();
  if (paxosv == osdmap.epoch) return true;
  assert(paxosv >= osdmap.epoch);

  dout(15) << "update_from_paxos paxos e " << paxosv 
	   << ", my e " << osdmap.epoch << dendl;

  if (osdmap.epoch == 0 && paxosv > 1) {
    // startup: just load latest full map
    epoch_t lastfull = mon->store->get_int("osdmap_full","last_epoch");
    if (lastfull) {
      dout(7) << "update_from_paxos startup: loading latest full map e" << lastfull << dendl;
      bufferlist bl;
      mon->store->get_bl_sn(bl, "osdmap_full", lastfull);
      osdmap.decode(bl);
    }
  } 
  
  // walk through incrementals
  while (paxosv > osdmap.epoch) {
    bufferlist bl;
    bool success = paxos->read(osdmap.epoch+1, bl);
    assert(success);
    
    dout(7) << "update_from_paxos  applying incremental " << osdmap.epoch+1 << dendl;
    OSDMap::Incremental inc(bl);
    osdmap.apply_incremental(inc);

    // write out the full map, too.
    bl.clear();
    osdmap.encode(bl);
    mon->store->put_bl_sn(bl, "osdmap_full", osdmap.epoch);

    // share
    dout(1) << *this << dendl;
  }
  mon->store->put_int(osdmap.epoch, "osdmap_full","last_epoch");

  if (mon->is_leader()) {
    // kick pgmon, make sure it's seen the latest map
    mon->pgmon()->check_osd_map(osdmap.epoch);

    bcast_latest_mds();
  }

  send_to_waiting();
    
  return true;
}


void OSDMonitor::create_pending()
{
  pending_inc = OSDMap::Incremental(osdmap.epoch+1);
  pending_inc.fsid = mon->monmap->fsid;
  
  dout(10) << "create_pending e " << pending_inc.epoch << dendl;
}


/*
struct RetryClearMkpg : public Context {
  OSDMonitor *osdmon;
  RetryClearMkpg(OSDMonitor *o) : osdmon(o) {}
  void finish(int r) {
    osdmon->clear_mkpg_flag();
  }
};
*/

void OSDMonitor::encode_pending(bufferlist &bl)
{
  dout(10) << "encode_pending e " << pending_inc.epoch
	   << dendl;
  
  // finalize up pending_inc
  pending_inc.ctime = g_clock.now();

  // tell me about it
  for (map<int32_t,uint8_t>::iterator i = pending_inc.new_down.begin();
       i != pending_inc.new_down.end();
       i++) {
    dout(2) << " osd" << i->first << " DOWN clean=" << (int)i->second << dendl;
    derr(0) << " osd" << i->first << " DOWN clean=" << (int)i->second << dendl;
    // no: this screws up map delivery on shutdown
    //mon->messenger->mark_down(osdmap.get_addr(i->first));
  }
  for (map<int32_t,entity_addr_t>::iterator i = pending_inc.new_up.begin();
       i != pending_inc.new_up.end(); 
       i++) { 
    dout(2) << " osd" << i->first << " UP " << i->second << dendl;
    derr(0) << " osd" << i->first << " UP " << i->second << dendl;
  }
  for (map<int32_t,uint32_t>::iterator i = pending_inc.new_weight.begin();
       i != pending_inc.new_weight.end();
       i++) {
    if (i->second == CEPH_OSD_OUT) {
      dout(2) << " osd" << i->first << " OUT" << dendl;
      derr(0) << " osd" << i->first << " OUT" << dendl;
    } else if (i->second == CEPH_OSD_IN) {
      dout(2) << " osd" << i->first << " IN" << dendl;
      derr(0) << " osd" << i->first << " IN" << dendl;
    } else {
      dout(2) << " osd" << i->first << " WEIGHT " << hex << i->second << dec << dendl;
      derr(0) << " osd" << i->first << " WEIGHT " << hex << i->second << dec << dendl;
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
    dout(10) << "committed, telling random osd" << r << " all about it" << dendl;
    send_latest(osdmap.get_inst(r), osdmap.get_epoch() - 1);  // whatev, they'll request more if they need it
  }
}


// -------------

bool OSDMonitor::preprocess_query(Message *m)
{
  dout(10) << "preprocess_query " << *m << " from " << m->get_orig_source_inst() << dendl;

  switch (m->get_type()) {
    // READs
  case CEPH_MSG_OSD_GETMAP:
    handle_osd_getmap((MOSDGetMap*)m);
    return true;
    
  case MSG_MON_COMMAND:
    return preprocess_command((MMonCommand*)m);

    // damp updates
  case MSG_OSD_FAILURE:
    return preprocess_failure((MOSDFailure*)m);
  case MSG_OSD_BOOT:
    return preprocess_boot((MOSDBoot*)m);
  case MSG_OSD_ALIVE:
    return preprocess_alive((MOSDAlive*)m);
    /*
  case MSG_OSD_OUT:
    return preprocess_out((MOSDOut*)m);
    */

  case MSG_REMOVE_SNAPS:
    return preprocess_remove_snaps((MRemoveSnaps*)m);
    
  default:
    assert(0);
    delete m;
    return true;
  }
}

bool OSDMonitor::prepare_update(Message *m)
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

  case MSG_MON_COMMAND:
    return prepare_command((MMonCommand*)m);
    
    /*
  case MSG_OSD_OUT:
    return prepare_out((MOSDOut*)m);
    */

  case MSG_REMOVE_SNAPS:
    return prepare_remove_snaps((MRemoveSnaps*)m);

  default:
    assert(0);
    delete m;
  }

  return false;
}

bool OSDMonitor::should_propose(double& delay)
{
  dout(10) << "should_propose" << dendl;

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

void OSDMonitor::handle_osd_getmap(MOSDGetMap *m)
{
  dout(7) << "handle_osd_getmap from " << m->get_orig_source()
	  << " start " << m->get_start_epoch()
	  << dendl;
  
  if (!ceph_fsid_equal(&m->fsid, &mon->monmap->fsid)) {
    dout(0) << "handle_osd_getmap on fsid " << m->fsid << " != " << mon->monmap->fsid << dendl;
    goto out;
  }

  if (m->get_start_epoch()) {
    if (m->get_start_epoch() <= osdmap.get_epoch())
	send_incremental(m->get_orig_source_inst(), m->get_start_epoch());
    else
      waiting_for_map[m->get_orig_source_inst()] = m->get_start_epoch();
  } else
    send_full(m->get_orig_source_inst());
  
 out:
  delete m;
}



// ---------------------------
// UPDATEs

// failure --

bool OSDMonitor::preprocess_failure(MOSDFailure *m)
{
  // who is failed
  int badboy = m->get_failed().name.num();

  if (!ceph_fsid_equal(&m->fsid, &mon->monmap->fsid)) {
    dout(0) << "preprocess_failure on fsid " << m->fsid << " != " << mon->monmap->fsid << dendl;
    goto didit;
  }

  /*
   * FIXME
   * this whole thing needs a rework of some sort.  we shouldn't
   * be taking any failure report on faith.  if A and B can't talk
   * to each other either A or B should be killed, but we should
   * make some attempt to make sure we choose the right one.
   */

  // first, verify the reporting host is valid
  if (m->get_orig_source().is_osd()) {
    int from = m->get_orig_source().num();
    if (!osdmap.exists(from) ||
	osdmap.get_addr(from) != m->get_orig_source_inst().addr ||
	osdmap.is_down(from)) {
      dout(5) << "preprocess_failure from dead osd" << from << ", ignoring" << dendl;
      send_incremental(m->get_orig_source_inst(), m->get_epoch()+1);
      goto didit;
    }
  }
  

  // weird?
  if (!osdmap.have_inst(badboy)) {
    dout(5) << "preprocess_failure dne(/dup?): " << m->get_failed() << ", from " << m->get_orig_source_inst() << dendl;
    if (m->get_epoch() < osdmap.get_epoch())
      send_incremental(m->get_orig_source_inst(), m->get_epoch()+1);
    goto didit;
  }
  if (osdmap.get_inst(badboy) != m->get_failed()) {
    dout(5) << "preprocess_failure wrong osd: report " << m->get_failed() << " != map's " << osdmap.get_inst(badboy)
	    << ", from " << m->get_orig_source_inst() << dendl;
    if (m->get_epoch() < osdmap.get_epoch())
      send_incremental(m->get_orig_source_inst(), m->get_epoch()+1);
    goto didit;
  }
  // already reported?
  if (osdmap.is_down(badboy)) {
    dout(5) << "preprocess_failure dup: " << m->get_failed() << ", from " << m->get_orig_source_inst() << dendl;
    if (m->get_epoch() < osdmap.get_epoch())
      send_incremental(m->get_orig_source_inst(), m->get_epoch()+1);
    goto didit;
  }

  dout(10) << "preprocess_failure new: " << m->get_failed() << ", from " << m->get_orig_source_inst() << dendl;
  return false;

 didit:
  delete m;
  return true;
}

bool OSDMonitor::prepare_failure(MOSDFailure *m)
{
  dout(1) << "prepare_failure " << m->get_failed() << " from " << m->get_orig_source_inst() << dendl;
  
  // FIXME
  // take their word for it
  int badboy = m->get_failed().name.num();
  assert(osdmap.is_up(badboy));
  assert(osdmap.get_addr(badboy) == m->get_failed().addr);
  
  pending_inc.new_down[badboy] = false;
  
  if (osdmap.is_in(badboy))
    down_pending_out[badboy] = g_clock.now();

  paxos->wait_for_commit(new C_Reported(this, m));
  
  return true;
}

void OSDMonitor::_reported_failure(MOSDFailure *m)
{
  dout(7) << "_reported_failure on " << m->get_failed() << ", telling " << m->get_orig_source_inst() << dendl;
  send_latest(m->get_orig_source_inst(), m->get_epoch());
  delete m;
}


// boot --

bool OSDMonitor::preprocess_boot(MOSDBoot *m)
{
  if (!ceph_fsid_equal(&m->sb.fsid, &mon->monmap->fsid)) {
    dout(0) << "preprocess_boot on fsid " << m->sb.fsid << " != " << mon->monmap->fsid << dendl;
    delete m;
    return true;
  }

  assert(m->get_orig_source_inst().name.is_osd());
  int from = m->get_orig_source_inst().name.num();
  
  // already booted?
  if (osdmap.is_up(from) &&
      osdmap.get_inst(from) == m->get_orig_source_inst()) {
    // yup.
    dout(7) << "preprocess_boot dup from " << m->get_orig_source_inst() << dendl;
    _booted(m);
    return true;
  }

  dout(10) << "preprocess_boot from " << m->get_orig_source_inst() << dendl;
  return false;
}

bool OSDMonitor::prepare_boot(MOSDBoot *m)
{
  dout(7) << "prepare_boot from " << m->get_orig_source_inst() << " sb " << m->sb << dendl;
  assert(m->get_orig_source().is_osd());
  int from = m->get_orig_source().num();
  
  // does this osd exist?
  if (!osdmap.exists(from)) {
    dout(1) << "boot from non-existent osd" << from << ", increase max_osd?" << dendl;
    delete m;
    return false;
  }

  // already up?  mark down first?
  if (osdmap.is_up(from)) {
    dout(7) << "prepare_boot was up, first marking down " << osdmap.get_inst(from) << dendl;
    assert(osdmap.get_inst(from) != m->get_orig_source_inst());  // preproces should have caught it
    
    // mark previous guy down
    pending_inc.new_down[from] = false;
    
    paxos->wait_for_commit(new C_RetryMessage(this, m));
  } else {
    // mark new guy up.
    down_pending_out.erase(from);  // if any
    pending_inc.new_up[from] = m->get_orig_source_addr();
    
    // mark in?
    pending_inc.new_weight[from] = CEPH_OSD_IN;

    if (m->sb.weight)
      osd_weight[from] = m->sb.weight;

    // adjust last clean unmount epoch?
    const osd_info_t& info = osdmap.get_info(from);
    dout(10) << " old osd_info: " << info << dendl;
    if (m->sb.epoch_mounted > info.last_clean_first ||
	(m->sb.epoch_mounted == info.last_clean_first &&
	 m->sb.epoch_unmounted > info.last_clean_last)) {
      epoch_t first = m->sb.epoch_mounted;
      epoch_t last = m->sb.epoch_unmounted;

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

void OSDMonitor::_booted(MOSDBoot *m)
{
  dout(7) << "_booted " << m->get_orig_source_inst() 
	  << " w " << m->sb.weight << " from " << m->sb.current_epoch << dendl;
  send_latest(m->get_orig_source_inst(), m->sb.current_epoch+1);
  delete m;
}


// -------------
// in

bool OSDMonitor::preprocess_alive(MOSDAlive *m)
{
  int from = m->get_orig_source().num();
  if (osdmap.is_up(from) &&
      osdmap.get_inst(from) == m->get_orig_source_inst() &&
      osdmap.get_up_thru(from) >= m->map_epoch) {
    // yup.
    dout(7) << "preprocess_alive e" << m->map_epoch << " dup from " << m->get_orig_source_inst() << dendl;
    _alive(m);
    return true;
  }
  
  dout(10) << "preprocess_alive e" << m->map_epoch
	   << " from " << m->get_orig_source_inst() << dendl;
  return false;
}

bool OSDMonitor::prepare_alive(MOSDAlive *m)
{
  int from = m->get_orig_source().num();

  dout(7) << "prepare_alive e" << m->map_epoch << " from " << m->get_orig_source_inst() << dendl;
  pending_inc.new_up_thru[from] = m->map_epoch;
  paxos->wait_for_commit(new C_Alive(this,m ));
  return true;
}

void OSDMonitor::_alive(MOSDAlive *m)
{
  dout(7) << "_alive e" << m->map_epoch
	  << " from " << m->get_orig_source_inst()
	  << dendl;
  send_latest(m->get_orig_source_inst(), m->map_epoch);
  delete m;
}


// ---

bool OSDMonitor::preprocess_remove_snaps(MRemoveSnaps *m)
{
  dout(7) << "preprocess_remove_snaps " << *m << dendl;
  
  for (vector<snapid_t>::iterator p = m->snaps.begin(); 
       p != m->snaps.end();
       p++) {
    if (*p > osdmap.max_snap ||
	!osdmap.removed_snaps.contains(*p))
      return false;
  }
  delete m;
  return true;
}

bool OSDMonitor::prepare_remove_snaps(MRemoveSnaps *m)
{
  dout(7) << "prepare_remove_snaps " << *m << dendl;

  snapid_t max;
  for (vector<snapid_t>::iterator p = m->snaps.begin(); 
       p != m->snaps.end();
       p++) {
    if (*p > max)
      max = *p;

    if (!osdmap.removed_snaps.contains(*p) &&
	!pending_inc.removed_snaps.contains(*p)) {
      dout(10) << " adding " << *p << " to removed_snaps" << dendl;
      pending_inc.removed_snaps.insert(*p);
    }
  }

  if (max > osdmap.max_snap && 
      max > pending_inc.new_max_snap) {
    dout(10) << " new_max_snap " << max << dendl;
    pending_inc.new_max_snap = max;
  } else {
    dout(10) << " max_snap " << osdmap.max_snap << " still >= " << max << dendl;
  }

  delete m;
  return true;
}


// ---------------
// map helpers

void OSDMonitor::send_to_waiting()
{
  dout(10) << "send_to_waiting " << osdmap.get_epoch() << dendl;

  map<entity_inst_t,epoch_t>::iterator i = waiting_for_map.begin();
  while (i != waiting_for_map.end()) {
    if (i->second) {
      if (i->second <= osdmap.get_epoch())
	send_incremental(i->first, i->second);
      else {
	dout(10) << "send_to_waiting skipping " << i->first
		 << " wants " << i->second
		 << dendl;
	i++;
	continue;
      }
    } else
      send_full(i->first);

    waiting_for_map.erase(i++);
  }
}

void OSDMonitor::send_latest(entity_inst_t who, epoch_t start)
{
  if (paxos->is_readable()) {
    dout(5) << "send_latest to " << who << " now" << dendl;
    if (start == 0)
      send_full(who);
    else
      send_incremental(who, start);
  } else {
    dout(5) << "send_latest to " << who << " later" << dendl;
    waiting_for_map[who] = start;
  }
}


void OSDMonitor::send_full(entity_inst_t who)
{
  dout(5) << "send_full to " << who << dendl;
  mon->messenger->send_message(new MOSDMap(mon->monmap->fsid, &osdmap), who);
}

void OSDMonitor::send_incremental(entity_inst_t dest, epoch_t from)
{
  dout(5) << "send_incremental from " << from << " -> " << osdmap.get_epoch()
	  << " to " << dest << dendl;
  
  MOSDMap *m = new MOSDMap(mon->monmap->fsid);
  
  for (epoch_t e = osdmap.get_epoch();
       e >= from;
       e--) {
    bufferlist bl;
    if (mon->store->get_bl_sn(bl, "osdmap", e) > 0) {
      dout(20) << "send_incremental    inc " << e << " " << bl.length() << " bytes" << dendl;
      m->incremental_maps[e] = bl;
    } 
    else if (mon->store->get_bl_sn(bl, "osdmap_full", e) > 0) {
      dout(20) << "send_incremental   full " << e << dendl;
      m->maps[e] = bl;
    }
    else {
      assert(0);  // we should have all maps.
    }
  }
  
  mon->messenger->send_message(m, dest);
}


void OSDMonitor::bcast_latest_mds()
{
  epoch_t e = osdmap.get_epoch();
  dout(1) << "bcast_latest_mds epoch " << e << dendl;
  
  // tell mds
  set<int> up;
  mon->mdsmon()->mdsmap.get_up_mds_set(up);
  for (set<int>::iterator i = up.begin();
       i != up.end();
       i++) {
    send_incremental(mon->mdsmon()->mdsmap.get_inst(*i), osdmap.get_epoch());
  }
}

void OSDMonitor::bcast_latest_osd()
{
  epoch_t e = osdmap.get_epoch();
  dout(1) << "bcast_latest_osd epoch " << e << dendl;

  // tell osds
  set<int32_t> osds;
  osdmap.get_all_osds(osds);
  for (set<int32_t>::iterator it = osds.begin();
       it != osds.end();
       it++) {
    if (osdmap.is_down(*it)) continue;
    
    send_incremental(osdmap.get_inst(*it), osdmap.get_epoch());
  }  
}

void OSDMonitor::bcast_full_osd()
{
  epoch_t e = osdmap.get_epoch();
  dout(1) << "bcast_full_osd epoch " << e << dendl;

  // tell osds
  set<int32_t> osds;
  osdmap.get_all_osds(osds);
  for (set<int32_t>::iterator it = osds.begin();
       it != osds.end();
       it++) {
    if (osdmap.is_down(*it)) continue;
    send_full(osdmap.get_inst(*it));
  }  
}


// TICK


void OSDMonitor::tick()
{
  if (!paxos->is_active()) return;

  update_from_paxos();
  dout(10) << *this << dendl;

  if (!mon->is_leader()) return;


  // mark down osds out?
  utime_t now = g_clock.now();
  list<int> mark_out;
  for (map<int,utime_t>::iterator i = down_pending_out.begin();
       i != down_pending_out.end();
       i++) {
    utime_t down = now;
    down -= i->second;
    
    if (down.sec() >= g_conf.mon_osd_down_out_interval) {
      dout(10) << "tick marking osd" << i->first << " OUT after " << down << " sec" << dendl;
      mark_out.push_back(i->first);
    }
  }
  for (list<int>::iterator i = mark_out.begin();
       i != mark_out.end();
       i++) {
    down_pending_out.erase(*i);
    pending_inc.new_weight[*i] = CEPH_OSD_OUT;
  }
  if (!mark_out.empty()) {
    propose_pending();
  }


#define SWAP_PRIMARIES_AT_START 0
#define SWAP_TIME 1

  if (!SWAP_PRIMARIES_AT_START) return;

  // For all PGs that have OSD 0 as the primary,
  // switch them to use the first replca


  ps_t numps = osdmap.get_pg_num();
  int minrep = 1; 
  int maxrep = MIN(g_conf.num_osd, g_conf.osd_max_rep);
  for (int pool=0; pool<1; pool++)
    for (int nrep = minrep; nrep <= maxrep; nrep++) { 
      for (ps_t ps = 0; ps < numps; ++ps) {
	pg_t pgid = pg_t(pg_t::TYPE_REP, nrep, ps, pool, -1);
	vector<int> osds;
	osdmap.pg_to_osds(pgid, osds); 
	if (osds[0] == 0) {
	  pending_inc.new_pg_swap_primary[pgid] = osds[1];
	  dout(3) << "Changing primary for PG " << pgid << " from " << osds[0] << " to "
		  << osds[1] << dendl;
	}
      }
    }
  propose_pending();
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
    else if (m->cmd[1] == "dump") {
      ss << "ok";
      r = 0;
      stringstream ds;
      osdmap.print(ds);
      rdata.append(ds);
    }
    else if (m->cmd[1] == "getmap") {
      osdmap.encode(rdata);
      ss << "got osdmap epoch " << osdmap.get_epoch();
      r = 0;
    }
    else if (m->cmd[1] == "getcrushmap") {
      osdmap.crush.encode(rdata);
      ss << "got crush map from osdmap epoch " << osdmap.get_epoch();
      r = 0;
    }
    else if (m->cmd[1] == "getmaxosd") {
      ss << "max_osd = " << osdmap.get_max_osd() << " in epoch " << osdmap.get_epoch();
      r = 0;
    }
    else if (m->cmd[1] == "injectargs" && m->cmd.size() == 4) {
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
    else if (m->cmd[1] == "scrub" && m->cmd.size() > 2) {
      if (m->cmd[2] == "*") {
	ss << "osds ";
	int c = 0;
	for (int i=0; i<osdmap.get_max_osd(); i++)
	  if (osdmap.is_up(i)) {
	    ss << (c++ ? ",":"") << i;
	    mon->messenger->send_message(new MOSDScrub(osdmap.get_fsid()),
					 osdmap.get_inst(i));
	  }	    
	r = 0;
	ss << " instructed to scrub";
      } else {
	long osd = strtol(m->cmd[2].c_str(), 0, 10);
	if (osdmap.is_up(osd)) {
	  mon->messenger->send_message(new MOSDScrub(osdmap.get_fsid()),
				       osdmap.get_inst(osd));
	  r = 0;
	  ss << "osd" << osd << " instructed to scrub";
	} else 
	  ss << "osd" << osd << " is not up";
      }
    }

  }
  if (r != -1) {
    string rs;
    getline(ss, rs);
    mon->reply_command(m, r, rs, rdata);
    return true;
  } else
    return false;
}

bool OSDMonitor::prepare_command(MMonCommand *m)
{
  stringstream ss;
  string rs;
  int err = -EINVAL;
  if (m->cmd.size() > 1) {
    if (m->cmd[1] == "setcrushmap") {
      dout(10) << "prepare_command setting new crush map" << dendl;
      pending_inc.crush = m->get_data();
      string rs = "set crush map";
      paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs));
      return true;
    }
    else if (m->cmd[1] == "setmap") {
      OSDMap map;
      map.decode(m->get_data());
      if (ceph_fsid_equal(&map.fsid, &mon->monmap->fsid)) {
	map.epoch = pending_inc.epoch;  // make sure epoch is correct
	map.encode(pending_inc.fullmap);
	string rs = "set osd map";
	paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs));
	return true;
      }
      ss << "osdmap fsid " << map.fsid << " does not match monitor fsid " << mon->monmap->fsid;
      err = -EINVAL;
    }
    else if (m->cmd[1] == "setmaxosd" && m->cmd.size() > 2) {
      pending_inc.new_max_osd = atoi(m->cmd[2].c_str());
      ss << "set new max_osd = " << pending_inc.new_max_osd;
      getline(ss, rs);
      paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs));
      return true;
    }
    else if (m->cmd[1] == "setpgnum" && m->cmd.size() > 2) {
      int n = atoi(m->cmd[2].c_str());
      if (n <= osdmap.get_pg_num()) {
	ss << "specified pg_num " << n << " <= current " << osdmap.get_pg_num();
      } else if (!mon->pgmon()->pg_map.creating_pgs.empty()) {
	ss << "currently creating pgs, wait";
	err = -EAGAIN;
      } else {
	ss << "set new pg_num = " << n;
	pending_inc.new_pg_num = n;
	getline(ss, rs);
	paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs));
	return true;
      } 
    }
    else if (m->cmd[1] == "setpgpnum" && m->cmd.size() > 2) {
      int n = atoi(m->cmd[2].c_str());
      if (n <= osdmap.get_pgp_num()) {
	ss << "specified pgp_num " << n << " <= current " << osdmap.get_pgp_num();
      } else if (n > osdmap.get_pg_num()) {
	ss << "specified pgp_num " << n << " > pg_num " << osdmap.get_pg_num();
      } else if (!mon->pgmon()->pg_map.creating_pgs.empty()) {
	ss << "still creating pgs, wait";
	err = -EAGAIN;
      } else {
	ss << "set new pgp_num = " << n;
	pending_inc.new_pgp_num = n;
	getline(ss, rs);
	paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs));
	return true;
      }
    }
    else if (m->cmd[1] == "down" && m->cmd.size() == 3) {
      long osd = strtol(m->cmd[2].c_str(), 0, 10);
      if (osdmap.is_down(osd)) {
	ss << "osd" << osd << " is already down";
      } else if (!osdmap.exists(osd)) {
	ss << "osd" << osd << " does not exist";
      } else {
	pending_inc.new_down[osd] = false;
	ss << "marked down osd" << osd;
	getline(ss, rs);
	paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs));
	
	// send them the new map when it updates, so they know it
	waiting_for_map[osdmap.get_inst(osd)] = osdmap.get_epoch();

	return true;
      }
    }
    else if (m->cmd[1] == "out" && m->cmd.size() == 3) {
      long osd = strtol(m->cmd[2].c_str(), 0, 10);
      if (osdmap.is_out(osd)) {
	ss << "osd" << osd << " is already out";
      } else if (!osdmap.exists(osd)) {
	ss << "osd" << osd << " does not exist";
      } else {
	pending_inc.new_weight[osd] = CEPH_OSD_OUT;
	ss << "marked out osd" << osd;
	getline(ss, rs);
	paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs));
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
	paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs));
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
	paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs));
	return true;
      } 
    }
    else if (m->cmd[1] == "lost" && m->cmd.size() >= 3) {
      long osd = strtol(m->cmd[2].c_str(), 0, 10);
      if (m->cmd.size() < 4 ||
	  m->cmd[3] != "--yes-i-really-mean-it") {
	ss << "are you SURE?  this might mean real, permanent data loss.  pass --yes-i-really-mean-it if you really do.";
      }
      else if (!osdmap.exists(osd) || !osdmap.is_down(osd)) {
	ss << "osd" << osd << " is not down or doesn't exist";
      } else {
	epoch_t e = osdmap.get_info(osd).down_at;
	pending_inc.new_lost[osd] = e;
	ss << "marked osd lost in epoch " << e;
	getline(ss, rs);
	paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs));
	return true;
      }
    }
    else {
      ss << "unknown command " << m->cmd[1];
    }
  } else {
    ss << "no command?";
  }
  getline(ss, rs);
  mon->reply_command(m, err, rs);
  return false;
}


