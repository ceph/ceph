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

#include "messages/MOSDFailure.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDGetMap.h"
#include "messages/MOSDBoot.h"
#include "messages/MOSDIn.h"
#include "messages/MOSDOut.h"

#include "common/Timer.h"

#include "config.h"
#undef dout
#define  dout(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cout << g_clock.now()<< " mon" << mon->whoami << ".osd e" << (osdmap ? osdmap->get_epoch():0) << " "
#define  derr(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cerr << g_clock.now()<< " mon" << mon->whoami << ".osd e" << (osdmap ? osdmap->get_epoch():0) << " "


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
  send_incremental(osdmap->get_epoch()-1,                    // ick! FIXME
				   MSG_ADDR_OSD(rand() % g_conf.num_osd));
}


void OSDMonitor::fake_reorg() 
{
  int r = rand() % g_conf.num_osd;
  
  if (osdmap->is_out(r)) {
    dout(1) << "fake_reorg marking osd" << r << " in" << endl;
    pending_inc.new_in.push_back(r);
  } else {
    dout(1) << "fake_reorg marking osd" << r << " out" << endl;
    pending_inc.new_out.push_back(r);
  }

  accept_pending();
  
  // tell him!
  send_incremental(osdmap->get_epoch()-1, MSG_ADDR_OSD(r));
  
  // do it again?
  /*
  if (g_conf.num_osd - d > 4 &&
      g_conf.num_osd - d > g_conf.num_osd/2)
    g_timer.add_event_after(g_conf.fake_osdmap_expand,
                            new C_Mon_Faker(this));
  */
}




void OSDMonitor::create_initial()
{
  // <HACK set up OSDMap from g_conf>
  osdmap = new OSDMap();
  osdmap->ctime = g_clock.now();
  osdmap->set_pg_bits(g_conf.osd_pg_bits);
  
  // start at epoch 0 until all osds boot
  //osdmap->inc_epoch();  // = 1
  //assert(osdmap->get_epoch() == 1);
  
  if (g_conf.num_osd >= 12) {
	int ndom = g_conf.osd_max_rep;
	UniformBucket *domain[ndom];
	int domid[ndom];
	for (int i=0; i<ndom; i++) {
	  domain[i] = new UniformBucket(1, 0);
	  domid[i] = osdmap->crush.add_bucket(domain[i]);
	}
	
	// add osds
	int nper = ((g_conf.num_osd - 1) / ndom) + 1;
	cerr << ndom << " failure domains, " << nper << " osds each" << endl;
	int i = 0;
	for (int dom=0; dom<ndom; dom++) {
	  for (int j=0; j<nper; j++) {
		osdmap->osds.insert(i);
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
	int nroot = osdmap->crush.add_bucket(root);    
	
	// rules
	for (int i=1; i<=ndom; i++) {
	  osdmap->crush.rules[i].steps.push_back(RuleStep(CRUSH_RULE_TAKE, nroot));
	  osdmap->crush.rules[i].steps.push_back(RuleStep(CRUSH_RULE_CHOOSE, i, 1));
	  osdmap->crush.rules[i].steps.push_back(RuleStep(CRUSH_RULE_CHOOSE, 1, 0));      
	  osdmap->crush.rules[i].steps.push_back(RuleStep(CRUSH_RULE_EMIT));
	}
	
	// test
	vector<int> out;
	osdmap->pg_to_osds(0x40200000110ULL, out);
	
  } else {
	// one bucket
	Bucket *b = new UniformBucket(1, 0);
	int root = osdmap->crush.add_bucket(b);
	for (int i=0; i<g_conf.num_osd; i++) {
	  osdmap->osds.insert(i);
	  b->add_item(i, 1.0);
	}
	
	for (int i=1; i<=g_conf.osd_max_rep; i++) {
	  osdmap->crush.rules[i].steps.push_back(RuleStep(CRUSH_RULE_TAKE, root));
	  osdmap->crush.rules[i].steps.push_back(RuleStep(CRUSH_RULE_CHOOSE, i, 0));
	  osdmap->crush.rules[i].steps.push_back(RuleStep(CRUSH_RULE_EMIT));
	}
  }
  
  if (g_conf.mds_local_osd) {
	// add mds osds, but don't put them in the crush mapping func
	for (int i=0; i<g_conf.num_mds; i++) 
	  osdmap->osds.insert(i+10000);
  }
  
  //osdmap->encode(maps[osdmap->get_epoch()]); // 1
  //pending_inc.epoch = osdmap->get_epoch()+1;     // 2
  // </HACK>
  
  // fake osd failures
  for (map<int,float>::iterator i = g_fake_osd_down.begin();
	   i != g_fake_osd_down.end();
	   i++) {
	dout(0) << "will fake osd" << i->first << " DOWN after " << i->second << endl;
	g_timer.add_event_after(i->second, new C_Mon_FakeOSDFailure(this, i->first, 1));
  }
  for (map<int,float>::iterator i = g_fake_osd_out.begin();
	   i != g_fake_osd_out.end();
	   i++) {
	dout(0) << "will fake osd" << i->first << " OUT after " << i->second << endl;
	g_timer.add_event_after(i->second, new C_Mon_FakeOSDFailure(this, i->first, 0));
  }
}



void OSDMonitor::dispatch(Message *m)
{
  switch (m->get_type()) {

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

  default:
	assert(0);
  }
}



void OSDMonitor::handle_osd_failure(MOSDFailure *m)
{
  dout(1) << "osd failure: " << m->get_failed() << " from " << m->get_source() << endl;
  
  // FIXME
  // take their word for it
  int from = m->get_failed().num();
  if (osdmap->is_up(from) &&
      (osdmap->osd_inst.count(from) == 0 ||
       osdmap->osd_inst[from] == m->get_inst())) {
    pending_inc.new_down[from] = m->get_inst();

    if (osdmap->is_in(from))
      down_pending_out[from] = g_clock.now();
    
    //awaiting_maps[pending_inc.epoch][m->get_source()] = 

    accept_pending();

    bcast_latest_mds();   

    send_incremental(m->get_epoch(), m->get_source());
  }

  delete m;
}


void OSDMonitor::fake_osd_failure(int osd, bool down) 
{
  lock.Lock();
  {
    if (down) {
      dout(1) << "fake_osd_failure DOWN osd" << osd << endl;
      pending_inc.new_down[osd] = osdmap->osd_inst[osd];
    } else {
    dout(1) << "fake_osd_failure OUT osd" << osd << endl;
    pending_inc.new_out.push_back(osd);
    }
    accept_pending();
    bcast_latest_osd();
    bcast_latest_mds();
  }
  lock.Unlock();
}


void OSDMonitor::handle_osd_boot(MOSDBoot *m)
{
  dout(7) << "osd_boot from " << m->get_source() << endl;
  assert(m->get_source().is_osd());
  int from = m->get_source().num();
  
  if (osdmap->get_epoch() == 0) {
    // waiting for boot!
    osdmap->osd_inst[from] = m->get_source_inst();

    if (osdmap->osd_inst.size() == osdmap->osds.size()) {
      dout(-7) << "osd_boot all osds booted." << endl;
      osdmap->inc_epoch();
      osdmap->encode(maps[osdmap->get_epoch()]); // 1
      pending_inc.epoch = osdmap->get_epoch()+1;     // 2

      bcast_latest_osd();
      bcast_latest_mds();
    } else {
      dout(7) << "osd_boot waiting for " 
              << (osdmap->osds.size() - osdmap->osd_inst.size())
              << " osds to boot" << endl;
    }
    return;
  }

  // already up?  mark down first?
  if (osdmap->is_up(from)) {
    assert(m->get_source_inst() > osdmap->osd_inst[from]);   // this better be newer!  
	pending_inc.new_down[from] = osdmap->osd_inst[from];
	accept_pending();
  }
  
  // mark up.
  down_pending_out.erase(from);
  assert(osdmap->is_down(from));
  pending_inc.new_up[from] = m->get_source_inst();
  
  // mark in?
  if (osdmap->out_osds.count(from)) 
    pending_inc.new_in.push_back(from);
  
  accept_pending();

  // the booting osd will spread word
  send_incremental(m->sb.current_epoch, m->get_source());
  delete m;

  // tell mds
  bcast_latest_mds();
}

void OSDMonitor::handle_osd_in(MOSDIn *m)
{
  dout(7) << "osd_in from " << m->get_source() << endl;
  int from = m->get_source().num();

  if (osdmap->is_out(from)) 
    pending_inc.new_in.push_back(from);
  accept_pending();
  send_incremental(m->map_epoch, m->get_source());
}

void OSDMonitor::handle_osd_out(MOSDOut *m)
{
  dout(7) << "osd_out from " << m->get_source() << endl;
  int from = m->get_source().num();
  if (osdmap->is_in(from)) {
    pending_inc.new_out.push_back(from);
    accept_pending();
    send_incremental(m->map_epoch, m->get_source());
  }
}

void OSDMonitor::handle_osd_getmap(MOSDGetMap *m)
{
  dout(7) << "osd_getmap from " << m->get_source() << " since " << m->get_since() << endl;
  
  if (osdmap->get_epoch() == 0) {
    awaiting_map[1][m->get_source()] = m->get_since();
  } else {
    if (m->get_since())
      send_incremental(m->get_since(), m->get_source());
    else
      send_full(m->get_source());
  }
  delete m;
}



void OSDMonitor::accept_pending()
{
  dout(-10) << "accept_pending " << osdmap->get_epoch() << " -> " << pending_inc.epoch << endl;

  // accept pending into a new map!
  pending_inc.ctime = g_clock.now();
  pending_inc.encode( inc_maps[ pending_inc.epoch ] );
  
  // advance!
  osdmap->apply_incremental(pending_inc);

  
  // tell me about it
  for (map<int,entity_inst_t>::iterator i = pending_inc.new_up.begin();
       i != pending_inc.new_up.end(); 
       i++) { 
    dout(0) << "osd" << i->first << " UP " << i->second << endl;
    derr(0) << "osd" << i->first << " UP " << i->second << endl;
    messenger->mark_up(MSG_ADDR_OSD(i->first), i->second);
  }
  for (map<int,entity_inst_t>::iterator i = pending_inc.new_down.begin();
       i != pending_inc.new_down.end();
       i++) {
    dout(0) << "osd" << i->first << " DOWN " << i->second << endl;
    derr(0) << "osd" << i->first << " DOWN " << i->second << endl;
    messenger->mark_down(MSG_ADDR_OSD(i->first), i->second);
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
  OSDMap::Incremental next(osdmap->get_epoch() + 1);
  pending_inc = next;
}

void OSDMonitor::send_current()
{
  dout(10) << "send_current " << osdmap->get_epoch() << endl;

  map<msg_addr_t,epoch_t> s;
  s.swap( awaiting_map[osdmap->get_epoch()] );
  awaiting_map.erase(osdmap->get_epoch());

  for (map<msg_addr_t,epoch_t>::iterator i = s.begin();
       i != s.end();
       i++)
    send_incremental(i->second, i->first);
}


void OSDMonitor::send_full(msg_addr_t who)
{
  messenger->send_message(new MOSDMap(osdmap), who);
}

void OSDMonitor::send_incremental(epoch_t since, msg_addr_t dest)
{
  dout(5) << "osd_send_incremental " << since << " -> " << osdmap->get_epoch()
           << " to " << dest << endl;
  
  MOSDMap *m = new MOSDMap;
  
  for (epoch_t e = osdmap->get_epoch();
       e > since;
       e--) {
    bufferlist bl;
    if (inc_maps.count(e)) {
      dout(10) << "osd_send_incremental    inc " << e << endl;
      m->incremental_maps[e] = inc_maps[e];
    } else if (maps.count(e)) {
      dout(10) << "osd_send_incremental   full " << e << endl;
      m->maps[e] = maps[e];
      //if (!full) break;
    }
    else {
      assert(0);  // we should have all maps.
    }
  }
  
  messenger->send_message(m, dest);
}



void OSDMonitor::bcast_latest_mds()
{
  epoch_t e = osdmap->get_epoch();
  dout(1) << "osd_bcast_latest_mds epoch " << e << endl;
  
  // tell mds
  for (int i=0; i<g_conf.num_mds; i++) {
    //send_full(MSG_ADDR_MDS(i));
    send_incremental(osdmap->get_epoch()-1, MSG_ADDR_MDS(i));
  }
}

void OSDMonitor::bcast_latest_osd()
{
  epoch_t e = osdmap->get_epoch();
  dout(1) << "osd_bcast_latest_osd epoch " << e << endl;

  // tell osds
  set<int> osds;
  osdmap->get_all_osds(osds);
  for (set<int>::iterator it = osds.begin();
       it != osds.end();
       it++) {
    if (osdmap->is_down(*it)) continue;

    send_incremental(osdmap->get_epoch()-1, MSG_ADDR_OSD(*it));
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
