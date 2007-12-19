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

#include "MonitorStore.h"

#include "crush/CrushWrapper.h"

#include "messages/MOSDFailure.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDGetMap.h"
#include "messages/MOSDBoot.h"
#include "messages/MOSDIn.h"
#include "messages/MOSDOut.h"

#include "common/Timer.h"

#include "config.h"


#define  dout(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) *_dout << dbeginl << g_clock.now() << " mon" << mon->whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << ".osd e" << osdmap.get_epoch() << " "
#define  derr(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) *_derr << dbeginl << g_clock.now() << " mon" << mon->whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << ".osd e" << osdmap.get_epoch() << " "


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
    pending_inc.new_offload[osd] = CEPH_OSD_OUT;
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
    pending_inc.new_offload[r] = CEPH_OSD_IN;
  } else {
    dout(1) << "fake_reorg marking osd" << r << " out" << dendl;
    pending_inc.new_offload[r] = CEPH_OSD_OUT;
  }

  propose_pending();
  send_latest(osdmap.get_inst(r));  // after
}



/************ MAPS ****************/

void OSDMonitor::create_initial()
{
  assert(mon->is_leader());
  assert(paxos->get_version() == 0);

  dout(1) << "create_initial for " << mon->monmap->fsid << " from g_conf" << dendl;

  // <HACK set up OSDMap from g_conf>
  OSDMap newmap;
  newmap.set_fsid(mon->monmap->fsid);
  newmap.mon_epoch = mon->mon_epoch;
  newmap.ctime = g_clock.now();

  newmap.set_pg_num(g_conf.num_osd << g_conf.osd_pg_bits);
  
  // start at epoch 1 until all osds boot
  newmap.inc_epoch();  // = 1
  assert(newmap.get_epoch() == 1);

  map<int,double> weights;
  build_crush_map(newmap.crush, weights);

  // -- test --
#if 0
  {
    //vector<int> t;
    //crush.do_rule(2, 132, t, 4, -1);

    // 3x5p2
    int n = 4;
    int x = 0;
    int p = 2;
    vector<int> r(n);
    newmap.mark_down(0, false);
    newmap.mark_out(0);
    newmap.crush.do_rule(CRUSH_REP_RULE(n), x, r, n, p);
    dout(0) << "test out " << r << dendl;
  }
#endif

  newmap.set_max_osd(g_conf.num_osd);
  for (int i=0; i<g_conf.num_osd; i++) {
    newmap.set_state(i, CEPH_OSD_EXISTS|CEPH_OSD_CLEAN);
    newmap.set_offload(i, CEPH_OSD_IN);
  }
  
  if (g_conf.mds_local_osd) {
    newmap.set_max_osd(g_conf.num_mds+g_conf.num_osd);

    // add mds local osds, but don't put them in the crush mapping func
    for (int i=0; i<g_conf.num_mds; i++) {
      newmap.set_max_osd(i+g_conf.num_osd);
      newmap.set_state(i, CEPH_OSD_EXISTS);
      newmap.set_offload(i, CEPH_OSD_IN);
    }
  }
  
  // </HACK>
  
  // fake osd failures
  for (map<int,float>::iterator i = g_fake_osd_down.begin();
	   i != g_fake_osd_down.end();
	   i++) {
	dout(0) << "will fake osd" << i->first << " DOWN after " << i->second << dendl;
	mon->timer.add_event_after(i->second, new C_Mon_FakeOSDFailure(this, i->first, 1));
  }
  for (map<int,float>::iterator i = g_fake_osd_out.begin();
	   i != g_fake_osd_out.end();
	   i++) {
	dout(0) << "will fake osd" << i->first << " OUT after " << i->second << dendl;
	mon->timer.add_event_after(i->second, new C_Mon_FakeOSDFailure(this, i->first, 0));
  }

  // encode into pending incremental
  newmap.encode(pending_inc.fullmap);
}


void OSDMonitor::build_crush_map(CrushWrapper& crush,
				 map<int,double>& weights)
{
  // new
  crush.create();

  if (g_conf.num_osd >= 12) {
    int ndom = g_conf.osd_max_rep;
    int ritems[ndom];
    int rweights[ndom];

    int nper = ((g_conf.num_osd - 1) / ndom) + 1;
    derr(0) << ndom << " failure domains, " << nper << " osds each" << dendl;

    int o = 0;
    for (int i=0; i<ndom; i++) {
      int items[nper];
      //int w[nper];
      int j;
      rweights[i] = 0;
      for (j=0; j<nper; j++, o++) {
	if (o == g_conf.num_osd) break;
	dout(20) << "added osd" << o << dendl;
	items[j] = o;
	//w[j] = weights[o] ? (0x10000 - (int)(weights[o] * 0x10000)):0x10000;
	//rweights[i] += w[j];
	rweights[i] += 0x10000;
      }

      crush_bucket_uniform *domain = crush_make_uniform_bucket(1, j, items, 0x10000);
      ritems[i] = crush_add_bucket(crush.map, (crush_bucket*)domain);
      dout(20) << "added domain bucket i " << ritems[i] << " of size " << j << dendl;
    }
    
    // root
    crush_bucket_list *root = crush_make_list_bucket(2, ndom, ritems, rweights);
    int rootid = crush_add_bucket(crush.map, (crush_bucket*)root);
    
    // rules
    // replication
    for (int i=1; i<=ndom; i++) {
      crush_rule *rule = crush_make_rule(4);
      crush_rule_set_step(rule, 0, CRUSH_RULE_TAKE, rootid, 0);
      crush_rule_set_step(rule, 1, CRUSH_RULE_CHOOSE_FIRSTN, i, 1);
      crush_rule_set_step(rule, 2, CRUSH_RULE_CHOOSE_FIRSTN, 1, 0);
      crush_rule_set_step(rule, 3, CRUSH_RULE_EMIT, 0, 0);
      crush_add_rule(crush.map, CRUSH_REP_RULE(i), rule);
    }

    // raid
    for (int i=g_conf.osd_min_raid_width; i <= g_conf.osd_max_raid_width; i++) {
      if (ndom >= i) {
	crush_rule *rule = crush_make_rule(4);
	crush_rule_set_step(rule, 0, CRUSH_RULE_TAKE, rootid, 0);
	crush_rule_set_step(rule, 1, CRUSH_RULE_CHOOSE_INDEP, i, 1);
	crush_rule_set_step(rule, 2, CRUSH_RULE_CHOOSE_INDEP, 1, 0);
	crush_rule_set_step(rule, 3, CRUSH_RULE_EMIT, 0, 0);
	crush_add_rule(crush.map, CRUSH_RAID_RULE(i), rule);
      } else {
	crush_rule *rule = crush_make_rule(3);
	crush_rule_set_step(rule, 0, CRUSH_RULE_TAKE, rootid, 0);
	crush_rule_set_step(rule, 1, CRUSH_RULE_CHOOSE_INDEP, i, 0);
	crush_rule_set_step(rule, 2, CRUSH_RULE_EMIT, 0, 0);
	crush_add_rule(crush.map, CRUSH_RAID_RULE(i), rule);
      }
    }
    
  } else {
    // one bucket

    int items[g_conf.num_osd];
    for (int i=0; i<g_conf.num_osd; i++) 
      items[i] = i;
    
    crush_bucket_uniform *b = crush_make_uniform_bucket(1, g_conf.num_osd, items, 0x10000);
    int root = crush_add_bucket(crush.map, (crush_bucket*)b);
    
    // rules
    // replication
    for (int i=1; i<=g_conf.osd_max_rep; i++) {
      crush_rule *rule = crush_make_rule(3);
      crush_rule_set_step(rule, 0, CRUSH_RULE_TAKE, root, 0);
      crush_rule_set_step(rule, 1, CRUSH_RULE_CHOOSE_FIRSTN, i, 0);
      crush_rule_set_step(rule, 2, CRUSH_RULE_EMIT, 0, 0);
      crush_add_rule(crush.map, CRUSH_REP_RULE(i), rule);
    }
    // raid4
    for (int i=g_conf.osd_min_raid_width; i <= g_conf.osd_max_raid_width; i++) {
      crush_rule *rule = crush_make_rule(3);
      crush_rule_set_step(rule, 0, CRUSH_RULE_TAKE, root, 0);
      crush_rule_set_step(rule, 1, CRUSH_RULE_CHOOSE_INDEP, i, 0);
      crush_rule_set_step(rule, 2, CRUSH_RULE_EMIT, 0, 0);
      crush_add_rule(crush.map, CRUSH_RAID_RULE(i), rule);
    }
  }
  
  crush.finalize();

  // mark all in
  for (int i=0; i<g_conf.num_osd; i++)
    crush.set_offload(i, CEPH_OSD_IN);

  dout(20) << "crush max_devices " << crush.map->max_devices << dendl;
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
    OSDMap::Incremental inc;
    int off = 0;
    inc.decode(bl, off);
    osdmap.apply_incremental(inc);
    
    // write out the full map, too.
    bl.clear();
    osdmap.encode(bl);
    mon->store->put_bl_sn(bl, "osdmap_full", osdmap.epoch);

    // share
    dout(1) << osdmap.get_num_osds() << " osds, "
	    << osdmap.get_num_up_osds() << " up, " 
	    << osdmap.get_num_in_osds() << " in" 
	    << dendl;
  }
  mon->store->put_int(osdmap.epoch, "osdmap_full","last_epoch");

  // new map!
  bcast_latest_mds();
  send_to_waiting();
    
  return true;
}


void OSDMonitor::create_pending()
{
  pending_inc = OSDMap::Incremental(osdmap.epoch+1);
  pending_inc.fsid = mon->monmap->fsid;
  
  dout(10) << "create_pending e " << pending_inc.epoch << dendl;
}

void OSDMonitor::encode_pending(bufferlist &bl)
{
  dout(10) << "encode_pending e " << pending_inc.epoch
	   << dendl;
  
  // finish up pending_inc
  pending_inc.ctime = g_clock.now();
  pending_inc.mon_epoch = mon->mon_epoch;
  
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
  for (map<int32_t,uint32_t>::iterator i = pending_inc.new_offload.begin();
       i != pending_inc.new_offload.end();
       i++) {
    if (i->second == CEPH_OSD_OUT) {
      dout(2) << " osd" << i->first << " OUT" << dendl;
      derr(0) << " osd" << i->first << " OUT" << dendl;
    } else if (i->second == CEPH_OSD_IN) {
      dout(2) << " osd" << i->first << " IN" << dendl;
      derr(0) << " osd" << i->first << " IN" << dendl;
    } else {
      dout(2) << " osd" << i->first << " OFFLOAD " << i->second << dendl;
      derr(0) << " osd" << i->first << " OFFLOAD " << i->second << dendl;
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
  if (r >= 0)
    send_latest(osdmap.get_inst(r));
}


// -------------

bool OSDMonitor::preprocess_query(Message *m)
{
  dout(10) << "preprocess_query " << *m << " from " << m->get_source_inst() << dendl;

  switch (m->get_type()) {
    // READs
  case CEPH_MSG_OSD_GETMAP:
    handle_osd_getmap((MOSDGetMap*)m);
    return true;
    
    // damp updates
  case MSG_OSD_FAILURE:
    return preprocess_failure((MOSDFailure*)m);
  case MSG_OSD_BOOT:
    return preprocess_boot((MOSDBoot*)m);
    /*
  case MSG_OSD_IN:
    return preprocess_in((MOSDIn*)m);
  case MSG_OSD_OUT:
    return preprocess_out((MOSDOut*)m);
    */
    
  default:
    assert(0);
    delete m;
    return true;
  }
}

bool OSDMonitor::prepare_update(Message *m)
{
  dout(7) << "prepare_update " << *m << " from " << m->get_source_inst() << dendl;
  
  switch (m->get_type()) {
    // damp updates
  case MSG_OSD_FAILURE:
    return prepare_failure((MOSDFailure*)m);
  case MSG_OSD_BOOT:
    return prepare_boot((MOSDBoot*)m);

    /*
  case MSG_OSD_IN:
    return prepare_in((MOSDIn*)m);
  case MSG_OSD_OUT:
    return prepare_out((MOSDOut*)m);
    */

  default:
    assert(0);
    delete m;
  }

  return false;
}

bool OSDMonitor::should_propose(double& delay)
{
  if (osdmap.epoch == 1) {
    if (pending_inc.new_up.size() == (unsigned)g_conf.num_osd) {
      delay = 0.0;
      if (g_conf.osd_auto_weight) {
	CrushWrapper crush;
	build_crush_map(crush, osd_weight);
	crush._encode(pending_inc.crush);
      }
      return true;
    } else 
      return false;
  }
  return PaxosService::should_propose(delay);
}



// ---------------------------
// READs

void OSDMonitor::handle_osd_getmap(MOSDGetMap *m)
{
  dout(7) << "handle_osd_getmap from " << m->get_source() << " from " << m->get_start_epoch() << dendl;
  
  if (m->get_start_epoch()) {
    if (m->get_want_epoch() <= osdmap.get_epoch())
	send_incremental(m->get_source_inst(), m->get_start_epoch());
    else
      waiting_for_map[m->get_source_inst()] = pair<epoch_t,epoch_t>(m->get_start_epoch(),
								    m->get_want_epoch());
  } else
    send_full(m->get_source_inst());
  
  delete m;
}



// ---------------------------
// UPDATEs

// failure --

bool OSDMonitor::preprocess_failure(MOSDFailure *m)
{
  /*
   * FIXME
   * this whole thing needs a rework of some sort.  we shouldn't
   * be taking any failure report on faith.  if A and B can't talk
   * to each other either A or B should be killed, but we should
   * make some attempt to make sure we choose the right one.
   */

  // first, verify the reporting host is valid
  if (m->get_source().is_osd()) {
    int from = m->get_source().num();
    if (!osdmap.exists(from) ||
	osdmap.get_addr(from) != m->get_source_inst().addr ||
	osdmap.is_down(from)) {
      dout(5) << "preprocess_failure from dead osd" << from << ", ignoring" << dendl;
      send_incremental(m->get_from(), m->get_epoch()+1);
      delete m;
      return true;
    }
  }
  
  // who is failed
  int badboy = m->get_failed().name.num();

  // weird?
  if (!osdmap.have_inst(badboy)) {
    dout(5) << "preprocess_failure dne(/dup?): " << m->get_failed() << ", from " << m->get_from() << dendl;
    if (m->get_epoch() < osdmap.get_epoch())
      send_incremental(m->get_from(), m->get_epoch()+1);
    return true;
  }
  if (osdmap.get_inst(badboy) != m->get_failed()) {
    dout(5) << "preprocess_failure wrong osd: report " << m->get_failed() << " != map's " << osdmap.get_inst(badboy)
	    << ", from " << m->get_from() << dendl;
    if (m->get_epoch() < osdmap.get_epoch())
      send_incremental(m->get_from(), m->get_epoch()+1);
    return true;
  }
  // already reported?
  if (osdmap.is_down(badboy)) {
    dout(5) << "preprocess_failure dup: " << m->get_failed() << ", from " << m->get_from() << dendl;
    if (m->get_epoch() < osdmap.get_epoch())
      send_incremental(m->get_from(), m->get_epoch()+1);
    return true;
  }

  dout(10) << "preprocess_failure new: " << m->get_failed() << ", from " << m->get_from() << dendl;
  return false;
}

bool OSDMonitor::prepare_failure(MOSDFailure *m)
{
  dout(1) << "prepare_failure " << m->get_failed() << " from " << m->get_from() << dendl;
  
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
  dout(7) << "_reported_failure on " << m->get_failed() << ", telling " << m->get_from() << dendl;
  send_latest(m->get_from(), m->get_epoch());
}


// boot --

bool OSDMonitor::preprocess_boot(MOSDBoot *m)
{
  assert(m->inst.name.is_osd());
  int from = m->inst.name.num();
  
  // already booted?
  if (osdmap.is_up(from) &&
      osdmap.get_inst(from) == m->inst) {
    // yup.
    dout(7) << "preprocess_boot dup from " << m->inst << dendl;
    _booted(m);
    return true;
  }
  
  dout(10) << "preprocess_boot from " << m->inst << dendl;
  return false;
}

bool OSDMonitor::prepare_boot(MOSDBoot *m)
{
  dout(7) << "prepare_boot from " << m->inst << dendl;
  assert(m->inst.name.is_osd());
  int from = m->inst.name.num();
  
  // does this osd exist?
  if (!osdmap.exists(from)) {
    dout(1) << "boot from non-existent osd" << from << dendl;
    delete m;
    return true;
  }

  // already up?  mark down first?
  if (osdmap.is_up(from)) {
    dout(7) << "prepare_boot was up, first marking down " << osdmap.get_inst(from) << dendl;
    assert(osdmap.get_inst(from) != m->inst);  // preproces should have caught it
    
    // mark previous guy down
    pending_inc.new_down[from] = false;
    
    paxos->wait_for_commit(new C_RetryMessage(this, m));
  } else {
    // mark new guy up.
    down_pending_out.erase(from);  // if any
    pending_inc.new_up[from] = m->inst.addr;
    
    // mark in?
    pending_inc.new_offload[from] = CEPH_OSD_IN;
    
    osd_weight[from] = m->sb.weight;

    // wait
    paxos->wait_for_commit(new C_Booted(this, m));
  }
  return true;
}

void OSDMonitor::_booted(MOSDBoot *m)
{
  dout(7) << "_booted " << m->inst << " w " << m->sb.weight << dendl;
  send_latest(m->inst, m->sb.current_epoch);
  delete m;
}





// ---------------
// map helpers

void OSDMonitor::send_to_waiting()
{
  dout(10) << "send_to_waiting " << osdmap.get_epoch() << dendl;

  map<entity_inst_t,pair<epoch_t,epoch_t> >::iterator i = waiting_for_map.begin();
  while (i != waiting_for_map.end()) {
    if (i->second.first) {
      if (i->second.second <= osdmap.get_epoch())
	send_incremental(i->first, i->second.first);
      else {
	dout(10) << "send_to_waiting skipping " << i->first
		 << " has " << i->second.first
		 << " wants " << i->second.second
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
    waiting_for_map[who] = pair<epoch_t,epoch_t>(start, 0);
  }
}


void OSDMonitor::send_full(entity_inst_t who)
{
  dout(5) << "send_full to " << who << dendl;
  mon->messenger->send_message(new MOSDMap(&osdmap), who);
}

void OSDMonitor::send_incremental(entity_inst_t dest, epoch_t from)
{
  dout(5) << "send_incremental from " << from << " -> " << osdmap.get_epoch()
	  << " to " << dest << dendl;
  
  MOSDMap *m = new MOSDMap;
  
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
  mon->mdsmon->mdsmap.get_up_mds_set(up);
  for (set<int>::iterator i = up.begin();
       i != up.end();
       i++) {
    send_incremental(mon->mdsmon->mdsmap.get_inst(*i), osdmap.get_epoch());
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
  if (!mon->is_leader()) return;
  if (!paxos->is_active()) return;

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
    pending_inc.new_offload[*i] = CEPH_OSD_OUT;
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
  for (int nrep = minrep; nrep <= maxrep; nrep++) { 
    for (ps_t ps = 0; ps < numps; ++ps) {
      pg_t pgid = pg_t(pg_t::TYPE_REP, nrep, ps, -1);
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


int OSDMonitor::do_command(vector<string>& cmd, bufferlist& data, 
			   bufferlist& rdata, string &rs)
{
  rs = "unknown command";
  return -EINVAL;
}
