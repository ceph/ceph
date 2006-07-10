// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
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

#include "osd/OSDMap.h"

#include "msg/Message.h"
#include "msg/Messenger.h"

#include "messages/MPing.h"
#include "messages/MPingAck.h"
#include "messages/MFailure.h"
#include "messages/MFailureAck.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDBoot.h"

#include "common/Timer.h"
#include "common/Clock.h"

#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "mon" << whoami << " "



class C_OM_Faker : public Context {
public:
  OSDMonitor *om;
  C_OM_Faker(OSDMonitor *m) { 
	this->om = m;
  }
  void finish(int r) {
	om->fake_reorg();
  }
};

class C_OM_FakeOSDFailure : public Context {
  OSDMonitor *mon;
  int osd;
  bool down;
public:
  C_OM_FakeOSDFailure(OSDMonitor *m, int o, bool d) : mon(m), osd(o), down(d) {}
  void finish(int r) {
	mon->fake_osd_failure(osd,down);
  }
};




void OSDMonitor::fake_reorg() 
{
  
  // HACK osd map change
  static int d = 0;

  if (d > 0) {
	dout(1) << "changing OSD map, marking osd" << d-1 << " out" << endl;
	osdmap->mark_out(d-1);
  }

  dout(1) << "changing OSD map, marking osd" << d << " down" << endl;
  osdmap->mark_down(d);

  osdmap->inc_epoch();
  d++;
  
  // bcast
  bcast_osd_map();
    
  // do it again?
  if (g_conf.num_osd - d > 4 &&
	  g_conf.num_osd - d > g_conf.num_osd/2)
	g_timer.add_event_after(g_conf.fake_osdmap_expand,
							new C_OM_Faker(this));
}


void OSDMonitor::init()
{
  dout(1) << "init" << endl;

  // <HACK set up OSDMap from g_conf>
  osdmap = new OSDMap();
  osdmap->set_pg_bits(g_conf.osd_pg_bits);
  osdmap->inc_epoch();  // = 1
  assert(osdmap->get_epoch() == 1);

  if (g_conf.mkfs) osdmap->set_mkfs();

  Bucket *b = new UniformBucket(1, 0);
  int root = osdmap->crush.add_bucket(b);
  for (int i=0; i<g_conf.num_osd; i++) {
	osdmap->osds.insert(i);
	b->add_item(i, 1);
  }
  
  for (int i=1; i<5; i++) {
	osdmap->crush.rules[i].steps.push_back(RuleStep(CRUSH_RULE_TAKE, root));
	osdmap->crush.rules[i].steps.push_back(RuleStep(CRUSH_RULE_CHOOSE, i, 0));
	osdmap->crush.rules[i].steps.push_back(RuleStep(CRUSH_RULE_EMIT));
  }

  if (g_conf.mds_local_osd) {
	// add mds osds, but don't put them in the crush mapping func
	for (int i=0; i<g_conf.num_mds; i++) 
	  osdmap->osds.insert(i+10000);
  }

  // </HACK>


  
  if (whoami == 0 &&
	  g_conf.num_osd > 4 &&
	  g_conf.fake_osdmap_expand) {
	dout(1) << "scheduling OSD map reorg at " << g_conf.fake_osdmap_expand << endl;
	g_timer.add_event_after(g_conf.fake_osdmap_expand,
							new C_OM_Faker(this));
  }

  if (whoami == 0) {
	// fake osd failures
	for (map<int,float>::iterator i = g_fake_osd_down.begin();
		 i != g_fake_osd_down.end();
		 i++) {
	  dout(0) << "osd" << i->first << " DOWN after " << i->second << endl;
	  g_timer.add_event_after(i->second, new C_OM_FakeOSDFailure(this, i->first, 1));
	}
	for (map<int,float>::iterator i = g_fake_osd_out.begin();
		 i != g_fake_osd_out.end();
		 i++) {
	  dout(0) << "osd" << i->first << " OUT after " << i->second << endl;
	  g_timer.add_event_after(i->second, new C_OM_FakeOSDFailure(this, i->first, 0));
	}
  }


  // i'm ready!
  messenger->set_dispatcher(this);
}


void OSDMonitor::dispatch(Message *m)
{
  switch (m->get_type()) {
  case MSG_FAILURE:
	handle_failure((MFailure*)m);
	break;

  case MSG_PING_ACK:
	handle_ping_ack((MPingAck*)m);
	break;

  case MSG_OSD_GETMAP:
	handle_osd_getmap(m);
	return;

  case MSG_OSD_BOOT:
	handle_osd_boot((MOSDBoot*)m);
	return;

  case MSG_SHUTDOWN:
	handle_shutdown(m);
	return;

  default:
	dout(0) << "unknown message " << *m << endl;
	assert(0);
  }
}


void OSDMonitor::handle_shutdown(Message *m)
{
  dout(1) << "shutdown from " << m->get_source() << endl;
  messenger->shutdown();
  delete m;
}

void OSDMonitor::handle_ping_ack(MPingAck *m)
{
  // ...
  
  delete m;
}

void OSDMonitor::handle_failure(MFailure *m)
{
  dout(1) << "osd failure: " << MSG_ADDR_NICE(m->get_failed()) << " from " << MSG_ADDR_NICE(m->get_source()) << endl;
  
  // ack
  messenger->send_message(new MFailureAck(m),
						  m->get_source(), m->get_source_port());
  delete m;
}



void OSDMonitor::fake_osd_failure(int osd, bool down) 
{
  if (down) {
	dout(1) << "fake_osd_failure DOWN osd" << osd << endl;
	osdmap->down_osds.insert(osd);
  } else {
	dout(1) << "fake_osd_failure OUT osd" << osd << endl;
	osdmap->out_osds.insert(osd);
  }
  osdmap->inc_epoch();
  bcast_osd_map();
}


void OSDMonitor::handle_osd_boot(MOSDBoot *m)
{
  dout(7) << "osd_boot from " << m->get_source() << endl;
  
  // FIXME: check for reboots, etc.
  // ...mark up in map...
  
  messenger->send_message(new MOSDMap(osdmap),
						  m->get_source());
  delete m;
}

void OSDMonitor::handle_osd_getmap(Message *m)
{
  dout(7) << "osd_getmap from " << MSG_ADDR_NICE(m->get_source()) << endl;
  
  messenger->send_message(new MOSDMap(osdmap),
						  m->get_source());
  delete m;
}


void OSDMonitor::bcast_osd_map()
{
  dout(1) << "bcast_osd_map epoch " << osdmap->get_epoch() << endl;

  // tell mds
  for (int i=0; i<g_conf.num_mds; i++) {
	messenger->send_message(new MOSDMap(osdmap),
							MSG_ADDR_MDS(i));
  }
  
  // tell osds
  set<int> osds;
  osdmap->get_all_osds(osds);
  for (set<int>::iterator it = osds.begin();
	   it != osds.end();
	   it++) {
	messenger->send_message(new MOSDMap(osdmap),
							MSG_ADDR_OSD(*it));
  }  
}

