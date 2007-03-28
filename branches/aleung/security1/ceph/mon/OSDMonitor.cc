// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

#include "messages/MOSDFailure.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDGetMap.h"
#include "messages/MOSDBoot.h"
#include "messages/MOSDIn.h"
#include "messages/MOSDOut.h"

#include "messages/MMonOSDMapInfo.h"
#include "messages/MMonOSDMapLease.h"
#include "messages/MMonOSDMapLeaseAck.h"
#include "messages/MMonOSDMapUpdatePrepare.h"
#include "messages/MMonOSDMapUpdateAck.h"
#include "messages/MMonOSDMapUpdateCommit.h"

#include "common/Timer.h"

#include "config.h"
#undef dout
#define  dout(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cout << g_clock.now() << " mon" << mon->whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << ".osd(" << (state == STATE_INIT ? (const char*)"init":(state == STATE_SYNC ? (const char*)"sync":(state == STATE_LOCK ? (const char*)"lock":(state == STATE_UPDATING ? (const char*)"updating":(const char*)"?\?")))) << ") e" << osdmap.get_epoch() << " "
#define  derr(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cerr << g_clock.now() << " mon" << mon->whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << ".osd(" << (state == STATE_INIT ? (const char*)"init":(state == STATE_SYNC ? (const char*)"sync":(state == STATE_LOCK ? (const char*)"lock":(state == STATE_UPDATING ? (const char*)"updating":(const char*)"?\?")))) << ") e" << osdmap.get_epoch() << " "


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


void OSDMonitor::fake_osdmap_update()
{
  dout(1) << "fake_osdmap_update" << endl;
  accept_pending();

  // tell a random osd
  int osd = rand() % g_conf.num_osd;
  send_incremental(osdmap.get_epoch()-1,                     // ick! FIXME
		   osdmap.get_inst(osd));
}


void OSDMonitor::fake_reorg() 
{
  int r = rand() % g_conf.num_osd;
  
  if (osdmap.is_out(r)) {
    dout(1) << "fake_reorg marking osd" << r << " in" << endl;
    pending_inc.new_in.push_back(r);
  } else {
    dout(1) << "fake_reorg marking osd" << r << " out" << endl;
    pending_inc.new_out.push_back(r);
  }

  accept_pending();
  
  // tell him!
  send_incremental(osdmap.get_epoch()-1, osdmap.get_inst(r));
  
  // do it again?
  /*
  if (g_conf.num_osd - d > 4 &&
      g_conf.num_osd - d > g_conf.num_osd/2)
    mon->timer.add_event_after(g_conf.fake_osdmap_expand,
                            new C_Mon_Faker(this));
  */
}



/*
void OSDMonitor::init()
{
  // start with blank map

  // load my last state from the store
  bufferlist bl;
  if (get_map_bl(0, bl)) {  // FIXME
    // yay!
    osdmap.decode(bl);
    dout(1) << "init got epoch " << osdmap.get_epoch() << " from store" << endl;

    // set up pending_inc
    pending_inc.epoch = osdmap.get_epoch()+1;
  }
}
*/




/************ MAPS ****************/
/**********
 * Creates an initital OSD map
 **********/
void OSDMonitor::create_initial()
{
  dout(1) << "create_initial generating osdmap from g_conf" << endl;

  // <HACK set up OSDMap from g_conf>
  osdmap.mon_epoch = mon->mon_epoch;
  osdmap.ctime = g_clock.now();

  // set up osd placement groups
  if (g_conf.osd_pg_bits) {
    osdmap.set_pg_bits(g_conf.osd_pg_bits);
  } else {
    int osdbits = 1;
    int n = g_conf.num_osd;
    while (n) {
      n = n >> 1;
      osdbits++;
    }

    // 2 bits per osd.
    osdmap.set_pg_bits(osdbits + 7);
  }
  
  // start at epoch 0 until all osds boot
  //osdmap.inc_epoch();  // = 1
  //assert(osdmap.get_epoch() == 1);
  
  if (g_conf.num_osd >= 12) {
    int ndom = g_conf.osd_max_rep;
    UniformBucket *domain[ndom];
    int domid[ndom];
    for (int i=0; i<ndom; i++) {
      domain[i] = new UniformBucket(1, 0);
      domid[i] = osdmap.crush.add_bucket(domain[i]);
    }
    
    // add osds
    int nper = ((g_conf.num_osd - 1) / ndom) + 1;
    cerr << ndom << " failure domains, " << nper << " osds each" << endl;
    int i = 0;
    for (int dom=0; dom<ndom; dom++) {
      for (int j=0; j<nper; j++) {
	osdmap.osds.insert(i);
	domain[dom]->add_item(i, 1.0);
	//cerr << "osd" << i << " in domain " << dom << endl;
	i++;
	if (i == g_conf.num_osd) break;
      }
    }
    
    // root
    Bucket *root = new ListBucket(2);
    for (int i=0; i<ndom; i++) {
      //cerr << "dom " << i << " w " << domain[i]->get_weight() << endl;
      root->add_item(domid[i], domain[i]->get_weight());
    }
    int nroot = osdmap.crush.add_bucket(root);    
    
    // rules
    for (int i=1; i<=ndom; i++) {
      osdmap.crush.rules[i].steps.push_back(RuleStep(CRUSH_RULE_TAKE, nroot));
      osdmap.crush.rules[i].steps.push_back(RuleStep(CRUSH_RULE_CHOOSE, i, 1));
      osdmap.crush.rules[i].steps.push_back(RuleStep(CRUSH_RULE_CHOOSE, 1, 0));      
      osdmap.crush.rules[i].steps.push_back(RuleStep(CRUSH_RULE_EMIT));
    }
    
    // test
    //vector<int> out;
    //osdmap.pg_to_osds(0x40200000110ULL, out);
    
  } else {
    // one bucket
    Bucket *b = new UniformBucket(1, 0);
    int root = osdmap.crush.add_bucket(b);
    for (int i=0; i<g_conf.num_osd; i++) {
      osdmap.osds.insert(i);
      b->add_item(i, 1.0);
    }
    
    for (int i=1; i<=g_conf.osd_max_rep; i++) {
      osdmap.crush.rules[i].steps.push_back(RuleStep(CRUSH_RULE_TAKE, root));
      osdmap.crush.rules[i].steps.push_back(RuleStep(CRUSH_RULE_CHOOSE, i, 0));
      osdmap.crush.rules[i].steps.push_back(RuleStep(CRUSH_RULE_EMIT));
    }
  }
  
  /* Adds MDS OSDs to the map, they need keys...
   This just adds them to the set in the map */
  if (g_conf.mds_local_osd) {
    // add mds osds, but don't put them in the crush mapping func
    for (int i=0; i<g_conf.num_mds; i++) 
      osdmap.osds.insert(i+10000);
  }
  
  // </HACK>
  
  // fake osd failures
  for (map<int,float>::iterator i = g_fake_osd_down.begin();
	   i != g_fake_osd_down.end();
	   i++) {
	dout(0) << "will fake osd" << i->first << " DOWN after " << i->second << endl;
	mon->timer.add_event_after(i->second, new C_Mon_FakeOSDFailure(this, i->first, 1));
  }
  for (map<int,float>::iterator i = g_fake_osd_out.begin();
	   i != g_fake_osd_out.end();
	   i++) {
	dout(0) << "will fake osd" << i->first << " OUT after " << i->second << endl;
	mon->timer.add_event_after(i->second, new C_Mon_FakeOSDFailure(this, i->first, 0));
  }
}


bool OSDMonitor::get_map_bl(epoch_t epoch, bufferlist& bl)
{
  if (!mon->store->exists_bl_sn("osdmap", epoch))
    return false;
  int r = mon->store->get_bl_sn(bl, "osdmap", epoch);
  assert(r > 0);
  return true;  
}

bool OSDMonitor::get_inc_map_bl(epoch_t epoch, bufferlist& bl)
{
  if (!mon->store->exists_bl_sn("osdincmap", epoch))
    return false;
  int r = mon->store->get_bl_sn(bl, "osdincmap", epoch);
  assert(r > 0);
  return true;  
}


void OSDMonitor::save_map()
{
  bufferlist bl;
  osdmap.encode(bl);

  mon->store->put_bl_sn(bl, "osdmap", osdmap.get_epoch());
  mon->store->put_int(osdmap.get_epoch(), "osd_epoch");
}

void OSDMonitor::save_inc_map(OSDMap::Incremental &inc)
{
  bufferlist bl;
  osdmap.encode(bl);

  bufferlist incbl;
  inc.encode(incbl);

  mon->store->put_bl_sn(bl, "osdmap", osdmap.get_epoch());
  mon->store->put_bl_sn(incbl, "osdincmap", osdmap.get_epoch());
  mon->store->put_int(osdmap.get_epoch(), "osd_epoch");
}



void OSDMonitor::dispatch(Message *m)
{
  switch (m->get_type()) {
    
    // services
  case MSG_OSD_GETMAP:
    handle_osd_getmap((MOSDGetMap*)m);
    break;
  case MSG_OSD_FAILURE:
    handle_osd_failure((MOSDFailure*)m);
    break;
  case MSG_OSD_BOOT:
    handle_osd_boot((MOSDBoot*)m);
    break;
  case MSG_OSD_IN:
    handle_osd_in((MOSDIn*)m);
    break;
  case MSG_OSD_OUT:
    handle_osd_out((MOSDOut*)m);
    break;
    
    // replication
  case MSG_MON_OSDMAP_INFO:
    handle_info((MMonOSDMapInfo*)m);
    break;
  case MSG_MON_OSDMAP_LEASE:
    handle_lease((MMonOSDMapLease*)m);
    break;
  case MSG_MON_OSDMAP_LEASE_ACK:
    handle_lease_ack((MMonOSDMapLeaseAck*)m);
    break;
  case MSG_MON_OSDMAP_UPDATE_PREPARE:
    handle_update_prepare((MMonOSDMapUpdatePrepare*)m);
    break;
  case MSG_MON_OSDMAP_UPDATE_ACK:
    handle_update_ack((MMonOSDMapUpdateAck*)m);
    break;
  case MSG_MON_OSDMAP_UPDATE_COMMIT:
    handle_update_commit((MMonOSDMapUpdateCommit*)m);
    break;
    
  default:
    assert(0);
  }
}



void OSDMonitor::handle_osd_failure(MOSDFailure *m)
{
  dout(1) << "osd failure: " << m->get_failed() << " from " << m->get_source() << endl;
  
  // FIXME
  // take their word for it
  int from = m->get_failed().name.num();
  if (osdmap.is_up(from) &&
      (osdmap.osd_inst.count(from) == 0 ||
       osdmap.osd_inst[from] == m->get_failed())) {
    pending_inc.new_down[from] = m->get_failed();
    
    if (osdmap.is_in(from))
      down_pending_out[from] = g_clock.now();
    
    //awaiting_maps[pending_inc.epoch][m->get_source()] = 
    
    accept_pending();
    
    send_incremental(m->get_epoch(), m->get_source_inst());
    
    send_waiting();
    bcast_latest_mds();   
  }
  
  delete m;
}


void OSDMonitor::fake_osd_failure(int osd, bool down) 
{
  if (down) {
    dout(1) << "fake_osd_failure DOWN osd" << osd << endl;
    pending_inc.new_down[osd] = osdmap.osd_inst[osd];
  } else {
    dout(1) << "fake_osd_failure OUT osd" << osd << endl;
    pending_inc.new_out.push_back(osd);
  }
  accept_pending();
  bcast_latest_osd();
  bcast_latest_mds();
}

void OSDMonitor::mark_all_down()
{
  dout(7) << "mark_all_down" << endl;

  for (set<int>::iterator it = osdmap.get_osds().begin();
       it != osdmap.get_osds().end();
       it++) {
    if (osdmap.is_down(*it)) continue;
    pending_inc.new_down[*it] = osdmap.get_inst(*it);
  }
  accept_pending();
}




void OSDMonitor::handle_osd_boot(MOSDBoot *m)
{
  dout(7) << "osd_boot from " << m->get_source() << endl;
  assert(m->get_source().is_osd());
  int from = m->get_source().num();
  
  if (osdmap.get_epoch() == 0) {
    // waiting for boot!
    // add the OSD instance to the map?
    osdmap.osd_inst[from] = m->get_source_inst();

    // adds the key to the map
    //osdmap.osd_str_keys[from] = m->get_public_key();

    if (osdmap.osd_inst.size() == osdmap.osds.size()) {
      dout(7) << "osd_boot all osds booted." << endl;
      osdmap.inc_epoch();
      
      save_map();
      
      pending_inc.epoch = osdmap.get_epoch()+1;     // 2

      bcast_latest_osd();
      bcast_latest_mds();
    } else {
      dout(7) << "osd_boot waiting for " 
              << (osdmap.osds.size() - osdmap.osd_inst.size())
              << " osds to boot" << endl;
    }

    delete m;
    return;
  }
  // if epoch != 0 then its incremental

  // already up?  mark down first?
  if (osdmap.is_up(from)) {
    pending_inc.new_down[from] = osdmap.osd_inst[from];
    accept_pending();
  }
  
  // mark up.
  down_pending_out.erase(from);
  assert(osdmap.is_down(from));
  pending_inc.new_up[from] = m->get_source_inst();
  
  // mark in?
  if (osdmap.out_osds.count(from)) 
    pending_inc.new_in.push_back(from);
  
  accept_pending();
  
  // the booting osd will spread word
  send_incremental(m->sb.current_epoch, m->get_source_inst());
  delete m;

  // tell mds
  bcast_latest_mds();
}

void OSDMonitor::handle_osd_in(MOSDIn *m)
{
  dout(7) << "osd_in from " << m->get_source() << endl;
  int from = m->get_source().num();
  
  if (osdmap.is_out(from)) 
    pending_inc.new_in.push_back(from);
  accept_pending();
  send_incremental(m->map_epoch, m->get_source_inst());
}

void OSDMonitor::handle_osd_out(MOSDOut *m)
{
  dout(7) << "osd_out from " << m->get_source() << endl;
  int from = m->get_source().num();
  if (osdmap.is_in(from)) {
    pending_inc.new_out.push_back(from);
    accept_pending();
    send_incremental(m->map_epoch, m->get_source_inst());
  }
}

void OSDMonitor::handle_osd_getmap(MOSDGetMap *m)
{
  dout(7) << "osd_getmap from " << m->get_source() << " since " << m->get_since() << endl;
  
  if (osdmap.get_epoch() == 0) {
    awaiting_map[m->get_source()].first = m->get_source_inst();
    awaiting_map[m->get_source()].second = m->get_since();
  } else {
    //if (m->get_since())
    send_incremental(m->get_since(), m->get_source_inst());
    //else
    //send_full(m->get_source(), m->get_source_inst());
  }
  delete m;
}



void OSDMonitor::accept_pending()
{
  dout(-10) << "accept_pending " << osdmap.get_epoch() << " -> " << pending_inc.epoch << endl;

  // accept pending into a new map!
  pending_inc.ctime = g_clock.now();
  pending_inc.mon_epoch = mon->mon_epoch;

  // advance!
  osdmap.apply_incremental(pending_inc);
  
  // save it.
  save_inc_map( pending_inc );
  
  // tell me about it
  for (map<int,entity_inst_t>::iterator i = pending_inc.new_up.begin();
       i != pending_inc.new_up.end(); 
       i++) { 
    dout(0) << "osd" << i->first << " UP " << i->second << endl;
    derr(0) << "osd" << i->first << " UP " << i->second << endl;
  }
  for (map<int,entity_inst_t>::iterator i = pending_inc.new_down.begin();
       i != pending_inc.new_down.end();
       i++) {
    dout(0) << "osd" << i->first << " DOWN " << i->second << endl;
    derr(0) << "osd" << i->first << " DOWN " << i->second << endl;
    messenger->mark_down(i->second.addr);
  }
  for (list<int>::iterator i = pending_inc.new_in.begin();
       i != pending_inc.new_in.end();
       i++) {
    dout(0) << "osd" << *i << " IN" << endl;
    derr(0) << "osd" << *i << " IN" << endl;
  }
  for (list<int>::iterator i = pending_inc.new_out.begin();
       i != pending_inc.new_out.end();
       i++) {
    dout(0) << "osd" << *i << " OUT" << endl;
    derr(0) << "osd" << *i << " OUT" << endl;
  }

  // clear new pending
  OSDMap::Incremental next(osdmap.get_epoch() + 1);
  pending_inc = next;
}

void OSDMonitor::send_waiting()
{
  dout(10) << "send_waiting " << osdmap.get_epoch() << endl;

  for (map<entity_name_t,pair<entity_inst_t,epoch_t> >::iterator i = awaiting_map.begin();
       i != awaiting_map.end();
       i++)
    send_incremental(i->second.second, i->second.first);
}


void OSDMonitor::send_full(entity_inst_t who)
{
  messenger->send_message(new MOSDMap(&osdmap), who);
}

void OSDMonitor::send_incremental(epoch_t since, entity_inst_t dest)
{
  dout(5) << "osd_send_incremental " << since << " -> " << osdmap.get_epoch()
	  << " to " << dest << endl;
  
  MOSDMap *m = new MOSDMap;
  
  for (epoch_t e = osdmap.get_epoch();
       e > since;
       e--) {
    bufferlist bl;
    if (get_inc_map_bl(e, bl)) {
      dout(10) << "osd_send_incremental    inc " << e << endl;
      m->incremental_maps[e] = bl;
    } 
    else if (get_map_bl(e, bl)) {
      dout(10) << "osd_send_incremental   full " << e << endl;
      m->maps[e] = bl;
    }
    else {
      assert(0);  // we should have all maps.
    }
  }
  
  messenger->send_message(m, dest);
}



void OSDMonitor::bcast_latest_mds()
{
  epoch_t e = osdmap.get_epoch();
  dout(1) << "bcast_latest_mds epoch " << e << endl;
  
  // tell mds
  set<int> up;
  mon->mdsmon->mdsmap.get_up_mds_set(up);
  for (set<int>::iterator i = up.begin();
       i != up.end();
       i++) {
    send_incremental(osdmap.get_epoch()-1, mon->mdsmon->mdsmap.get_inst(*i));
  }
}

void OSDMonitor::bcast_latest_osd()
{
  epoch_t e = osdmap.get_epoch();
  dout(1) << "bcast_latest_osd epoch " << e << endl;

  // tell osds
  set<int> osds;
  osdmap.get_all_osds(osds);
  for (set<int>::iterator it = osds.begin();
       it != osds.end();
       it++) {
    if (osdmap.is_down(*it)) continue;
    
    send_incremental(osdmap.get_epoch()-1, osdmap.get_inst(*it));
  }  
}



void OSDMonitor::tick()
{
  // mark down osds out?
  utime_t now = g_clock.now();
  list<int> mark_out;
  for (map<int,utime_t>::iterator i = down_pending_out.begin();
       i != down_pending_out.end();
       i++) {
    utime_t down = now;
    down -= i->second;
    
    if (down.sec() >= g_conf.mon_osd_down_out_interval) {
      dout(10) << "tick marking osd" << i->first << " OUT after " << down << " sec" << endl;
      mark_out.push_back(i->first);
    }
  }
  for (list<int>::iterator i = mark_out.begin();
       i != mark_out.end();
       i++) {
    down_pending_out.erase(*i);
    pending_inc.new_out.push_back( *i );
  }
  if (!mark_out.empty()) {
    accept_pending();
    
    // hrmpf.  bcast map for now.  FIXME FIXME.
    bcast_latest_osd();
  }
}

void OSDMonitor::election_starting()
{
  dout(10) << "election_starting" << endl;
}

void OSDMonitor::election_finished()
{
  dout(10) << "election_finished" << endl;

  if (mon->is_leader()) {
    if (g_conf.mkfs) {
      create_initial();
      save_map();
    } else {
      //
      epoch_t epoch = mon->store->get_int("osd_epoch");
      dout(10) << " last epoch was " << epoch << endl;
      bufferlist bl, blinc;
      int r = mon->store->get_bl_sn(bl, "osdmap", epoch);
      assert(r>0);
      osdmap.decode(bl);

      // pending_inc
      pending_inc.epoch = epoch+1;
    }

  }

  /*
  state = STATE_INIT;

  // map?
  if (osdmap.get_epoch() == 0 &&
      mon->is_leader()) {
    create_initial();
  }



  if (mon->is_leader()) {
    // leader.
    if (mon->monmap->num_mon == 1) {
      // hmm, it's just me!
      state = STATE_SYNC;
    }
  } 
  else if (mon->is_peon()) {
    // peon. send info
    //messenger->send_message(new MMonOSDMapInfo(osdmap.epoch, osdmap.mon_epoch),
    //			    mon->monmap->get_inst(mon->leader));
  }
  */
}



void OSDMonitor::handle_info(MMonOSDMapInfo *m)
{
  dout(10) << "handle_info from " << m->get_source()
	   << " epoch " << m->get_epoch() << " in mon_epoch " << m->get_mon_epoch()
	   << endl;
  
  epoch_t epoch = m->get_epoch();

  // did they have anything?
  if (epoch > 0) {
    // make sure it's current.
    if (epoch == osdmap.get_epoch()) {
      if (osdmap.mon_epoch != m->get_mon_epoch()) {
	dout(10) << "handle_info had divergent epoch " << m->get_epoch() 
		 << ", mon_epoch " << m->get_mon_epoch() << " != " << osdmap.mon_epoch << endl;
	epoch--;
      }
    } else {
      bufferlist bl;
      get_map_bl(epoch, bl);
      
      OSDMap old;
      old.decode(bl);
      
      if (old.mon_epoch != m->get_mon_epoch()) {
	dout(10) << "handle_info had divergent epoch " << m->get_epoch() 
		 << ", mon_epoch " << m->get_mon_epoch() << " != " << old.mon_epoch << endl;
	epoch--;
      }
    }
  }
  
  // bring up to date
  if (epoch < osdmap.get_epoch()) 
    send_incremental(epoch, m->get_source_inst());
  
  delete m;
}


void OSDMonitor::issue_leases()
{
  dout(10) << "issue_leases" << endl;
  assert(mon->is_leader());

  // set lease endpoint
  lease_expire = g_clock.now();
  lease_expire += g_conf.mon_lease;

  pending_ack.clear();
  
  for (set<int>::iterator i = mon->quorum.begin();
       i != mon->quorum.end();
       i++) {
    if (*i == mon->whoami) continue;
    messenger->send_message(new MMonOSDMapLease(osdmap.get_epoch(), lease_expire),
			    mon->monmap->get_inst(*i));
    pending_ack.insert(*i);
  }
}

void OSDMonitor::handle_lease(MMonOSDMapLease *m)
{
  if (m->get_epoch() != osdmap.get_epoch() + 1) {
    dout(10) << "map_lease from " << m->get_source() 
	     << " on epoch " << m->get_epoch() << ", but i am " << osdmap.get_epoch() << endl;
    assert(0);
    delete m;
    return;
  }
  
  dout(10) << "map_lease from " << m->get_source() << " expires " << lease_expire << endl;
  lease_expire = m->get_lease_expire();
  
  delete m;
}

void OSDMonitor::handle_lease_ack(MMonOSDMapLeaseAck *m)
{
  // right epoch?
  if (m->get_epoch() != osdmap.get_epoch()) {
    dout(10) << "map_lease_ack from " << m->get_source() 
	     << " on old epoch " << m->get_epoch() << ", dropping" << endl;
    delete m;
    return;
  }
  
  // within time limit?
  if (g_clock.now() >= lease_expire) {
    dout(10) << "map_lease_ack from " << m->get_source() 
	     << ", but lease expired, calling election" << endl;
    mon->call_election();
    delete m;
    return;
  }
  
  assert(m->get_source().is_mon());
  int from = m->get_source().num();

  assert(pending_ack.count(from));
  pending_ack.erase(from);

  if (pending_ack.empty()) {
    dout(10) << "map_lease_ack from " << m->get_source() 
	     << ", last one" << endl;
  } else {
    dout(10) << "map_lease_ack from " << m->get_source() 
	     << ", still waiting on " << pending_ack << endl;
  }
  
  delete m;
}


void OSDMonitor::update_map()
{
  // lock map
  state = STATE_UPDATING;
  pending_ack.clear();
  
  // set lease endpoint
  lease_expire += g_conf.mon_lease;

  // send prepare
  epoch_t epoch = osdmap.get_epoch();
  bufferlist map_bl, inc_map_bl;
  if (!get_inc_map_bl(epoch, inc_map_bl))
	get_map_bl(epoch, map_bl);

  for (set<int>::iterator i = mon->quorum.begin();
       i != mon->quorum.end();
       i++) {
    if (*i == mon->whoami) continue;
    messenger->send_message(new MMonOSDMapUpdatePrepare(epoch, 
							map_bl, inc_map_bl),
			    mon->monmap->get_inst(*i));
    pending_ack.insert(*i);
  }
}



void OSDMonitor::handle_update_prepare(MMonOSDMapUpdatePrepare *m)
{
  dout(10) << "map_update_prepare from " << m->get_source() << " epoch " << m->get_epoch() << endl;
  // accept map
  assert(m->get_epoch() == osdmap.get_epoch() + 1);
  
  if (m->inc_map_bl.length()) {
    int off = 0;
    pending_inc.decode(m->inc_map_bl, off);
    accept_pending();
  } else {
    osdmap.decode(m->map_bl);
  }
  
  // state
  state = STATE_LOCK;
  //lease_expire = m->lease_expire;
  
  // ack
  messenger->send_message(new MMonOSDMapUpdateAck(osdmap.get_epoch()),
						  m->get_source_inst());
  delete m;
}

void OSDMonitor::handle_update_ack(MMonOSDMapUpdateAck *m)
{
  /*
  // right epoch?
  if (m->get_epoch() != osdmap.get_epoch()) {
	dout(10) << "map_update_ack from " << m->get_source() 
			 << " on old epoch " << m->get_epoch() << ", dropping" << endl;
	delete m;
	return;
  }

  // within time limit?
  if (g_clock.now() >= lease_expire) {
	dout(10) << "map_update_ack from " << m->get_source() 
			 << ", but lease expired, calling election" << endl;
	state = STATE_SYNC;
	mon->call_election();
	return;
  }

  assert(m->get_source().is_mon());
  int from = m->get_source().num();

  assert(pending_lease_ack.count(from));
  pending_lease_ack.erase(from);

  if (pending_lease_ack.empty()) {
	dout(10) << "map_update_ack from " << m->get_source() 
			 << ", last one" << endl;
	state = STATE_SYNC;
	
	// send lease commit
	for (map<int>::iterator i = mon->quorum.begin();
		 i != mon->quorum.end();
		 i++) {
	  if (i == mon->whoami) continue;
	  messenger->send_message(new MMonOSDMapLeaseCommit(osdmap),
							  MSG_ADDR_MON(*i), mon->monmap->get_inst(*i));
	}
  } else {
	dout(10) << "map_update_ack from " << m->get_source() 
			 << ", still waiting on " << pending_lease_ack << endl;
  }
*/
}

void OSDMonitor::handle_update_commit(MMonOSDMapUpdateCommit *m)
{
}
