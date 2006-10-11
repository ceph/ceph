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


#include "MDSMonitor.h"
#include "Monitor.h"

#include "messages/MMDSBoot.h"
#include "messages/MMDSMap.h"
#include "messages/MMDSGetMap.h"
//#include "messages/MMDSFailure.h"

#include "common/Timer.h"

#include "config.h"
#undef dout
#define  dout(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cout << g_clock.now() << " mon" << mon->whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << ".mds e" << mdsmap.get_epoch() << " "
#define  derr(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cerr << g_clock.now() << " mon" << mon->whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << ".mds e" << mdsmap.get_epoch() << " "



/********* MDS map **************/

void MDSMonitor::create_initial()
{
  mdsmap.epoch = 0;  // until everyone boots
  mdsmap.ctime = g_clock.now();
  for (int i=0; i<g_conf.num_mds; i++) {
	mdsmap.all_mds.insert(i);
	mdsmap.down_mds.insert(i);
  }
}

void MDSMonitor::dispatch(Message *m)
{
  switch (m->get_type()) {

  case MSG_MDS_BOOT:
	handle_mds_boot((MMDSBoot*)m);
	break;
	
  case MSG_MDS_GETMAP:
	handle_mds_getmap((MMDSGetMap*)m);
	break;

	/*
  case MSG_MDS_FAILURE:
	handle_mds_failure((MMDSFailure*)m);
	break;
	*/

  default:
	assert(0);
  }  
}

void MDSMonitor::handle_mds_boot(MMDSBoot *m)
{
  dout(7) << "mds_boot from " << m->get_source() << " at " << m->get_source_inst() << endl;
  assert(m->get_source().is_mds());
  int from = m->get_source().num();

  if (mdsmap.get_epoch() == 0) {
    // waiting for boot!
    mdsmap.mds_inst[from] = m->get_source_inst();
	mdsmap.down_mds.erase(from);

    if ((int)mdsmap.mds_inst.size() == mdsmap.get_num_mds()) {
      mdsmap.inc_epoch();
      dout(-7) << "mds_boot all MDSs booted." << endl;
      mdsmap.encode(maps[mdsmap.get_epoch()]); // 1

      bcast_latest_mds();
	  send_current();
    } else {
      dout(7) << "mds_boot waiting for " 
              << (mdsmap.get_num_mds() - mdsmap.mds_inst.size())
              << " mdss to boot" << endl;
    }
    return;
  } else {
    dout(0) << "mds_boot everyone already booted, so who is this?  write me." << endl;
    assert(0);
  }
}

void MDSMonitor::handle_mds_getmap(MMDSGetMap *m)
{
  dout(7) << "mds_getmap from " << m->get_source() << " " << m->get_source_inst() << endl;
  if (mdsmap.get_epoch() > 0)
	send_full(m->get_source(), m->get_source_inst());
  else
	awaiting_map[m->get_source()] = m->get_source_inst();
}


void MDSMonitor::bcast_latest_mds()
{
  dout(10) << "bcast_latest_mds " << mdsmap.get_epoch() << endl;
  
  // tell mds
  for (set<int>::iterator p = mdsmap.get_mds().begin();
       p != mdsmap.get_mds().end();
       p++) {
    if (mdsmap.is_down(*p)) continue;
	send_full(MSG_ADDR_MDS(*p), mdsmap.get_inst(*p));
  }
}

void MDSMonitor::send_full(msg_addr_t dest, const entity_inst_t& inst)
{
  dout(11) << "send_full to " << dest << " inst " << inst << endl;
  messenger->send_message(new MMDSMap(&mdsmap), dest, inst);
}

void MDSMonitor::send_current()
{
  dout(10) << "mds_send_current " << mdsmap.get_epoch() << endl;
  for (map<msg_addr_t,entity_inst_t>::iterator i = awaiting_map.begin();
       i != awaiting_map.end();
       i++) 
	send_full(i->first, i->second);
  awaiting_map.clear();
}

