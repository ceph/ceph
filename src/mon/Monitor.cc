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

// TODO: missing run() method, which creates the two main timers, refreshTimer and readTimer

#include "Monitor.h"

#include "osd/OSDMap.h"

#include "MonitorStore.h"

#include "msg/Message.h"
#include "msg/Messenger.h"

#include "messages/MPing.h"
#include "messages/MPingAck.h"
#include "messages/MGenericMessage.h"
#include "messages/MMonCommand.h"
#include "messages/MMonCommandAck.h"

#include "messages/MMonPaxos.h"

#include "common/Timer.h"
#include "common/Clock.h"

#include "OSDMonitor.h"
#include "MDSMonitor.h"
#include "ClientMonitor.h"
#include "PGMonitor.h"

#include "config.h"

#define  dout(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) *_dout << dbeginl << g_clock.now() << " mon" << whoami << (is_starting() ? (const char*)"(starting)":(is_leader() ? (const char*)"(leader)":(is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << " "
#define  derr(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) *_derr << dbeginl << g_clock.now() << " mon" << whoami << (is_starting() ? (const char*)"(starting)":(is_leader() ? (const char*)"(leader)":(is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << " "



void Monitor::init()
{
  lock.Lock();
  
  dout(1) << "init" << dendl;
  
  // store
  char s[80];
  sprintf(s, "mondata/mon%d", whoami);
  store = new MonitorStore(s);
  
  if (g_conf.mkfs) 
    store->mkfs();
  
  store->mount();
  
  // create 
  osdmon = new OSDMonitor(this, &paxos_osdmap);
  mdsmon = new MDSMonitor(this, &paxos_mdsmap);
  clientmon = new ClientMonitor(this, &paxos_clientmap);
  pgmon = new PGMonitor(this, &paxos_pgmap);
  
  // init paxos
  paxos_osdmap.init();
  paxos_mdsmap.init();
  paxos_clientmap.init();
  paxos_pgmap.init();
  
  // i'm ready!
  messenger->set_dispatcher(this);
  
  // start ticker
  reset_tick();
  
  // call election?
  if (monmap->size() > 1) {
    assert(monmap->size() != 2); 
    call_election();
  } else {
    // we're standalone.
    set<int> q;
    q.insert(whoami);
    win_election(1, q);
  }
  
  lock.Unlock();
}

void Monitor::shutdown()
{
  dout(1) << "shutdown" << dendl;
  
  elector.shutdown();
  
  if (is_leader()) {
    // stop osds.
    set<int32_t> ls;
    osdmon->osdmap.get_all_osds(ls);
    for (set<int32_t>::iterator it = ls.begin(); it != ls.end(); it++) {
      if (osdmon->osdmap.is_down(*it)) continue;
      dout(10) << "sending shutdown to osd" << *it << dendl;
      messenger->send_message(new MGenericMessage(CEPH_MSG_SHUTDOWN),
			      osdmon->osdmap.get_inst(*it));
    }
    osdmon->mark_all_down();
    
    // monitors too.
    for (unsigned i=0; i<monmap->size(); i++)
      if ((int)i != whoami)
	messenger->send_message(new MGenericMessage(CEPH_MSG_SHUTDOWN), 
				monmap->get_inst(i));
  }
  
  // cancel all events
  cancel_tick();
  timer.cancel_all();
  timer.join();
  
  // unmount my local storage
  if (store) 
    delete store;
  
  // clean up
  if (osdmon) delete osdmon;
  if (mdsmon) delete mdsmon;
  if (clientmon) delete clientmon;
  if (pgmon) delete pgmon;
  
  // die.
  messenger->shutdown();
}


void Monitor::call_election()
{
  if (monmap->size() == 1) return;
  
  dout(10) << "call_election" << dendl;
  state = STATE_STARTING;
  
  // tell paxos
  paxos_mdsmap.election_starting();
  paxos_osdmap.election_starting();
  paxos_clientmap.election_starting();
  
  // call a new election
  elector.call_election();
}

void Monitor::win_election(epoch_t epoch, set<int>& active) 
{
  state = STATE_LEADER;
  leader = whoami;
  mon_epoch = epoch;
  quorum = active;
  dout(10) << "win_election, epoch " << mon_epoch << " quorum is " << quorum << dendl;
  
  // init paxos
  paxos_mdsmap.leader_init();
  paxos_osdmap.leader_init();
  paxos_clientmap.leader_init();
  paxos_pgmap.leader_init();
  
  // init
  osdmon->election_finished();
  mdsmon->election_finished();
  clientmon->election_finished();
  pgmon->election_finished();
} 

void Monitor::lose_election(epoch_t epoch, int l) 
{
  state = STATE_PEON;
  mon_epoch = epoch;
  leader = l;
  dout(10) << "lose_election, epoch " << mon_epoch << " leader is mon" << leader << dendl;
  
  // init paxos
  paxos_mdsmap.peon_init();
  paxos_osdmap.peon_init();
  paxos_clientmap.peon_init();
  paxos_pgmap.peon_init();
  
  // init
  osdmon->election_finished();
  mdsmon->election_finished();
  clientmon->election_finished();
  pgmon->election_finished();
}


int Monitor::do_command(vector<string>& cmd, bufferlist& data, 
			bufferlist& rdata, string &rs)
{
  if (cmd.empty()) {
    rs = "no command";
    return -EINVAL;
  }

  if (cmd[0] == "stop") {
    rs = "stopping";
    do_stop();
    return 0;
  }
  if (cmd[0] == "mds") 
    return mdsmon->do_command(cmd, data, rdata, rs);

  if (cmd[0] == "osd") 
    return osdmon->do_command(cmd, data, rdata, rs);

  // huh.
  rs = "unrecognized subsystem '" + cmd[0] + "'";
  return -EINVAL;
}

void Monitor::handle_command(MMonCommand *m)
{
  dout(0) << "handle_command " << *m << dendl;
  
  string rs;         // return string
  bufferlist rdata;  // return data
  int rc = do_command(m->cmd, m->get_data(), rdata, rs);

  MMonCommandAck *reply = new MMonCommandAck(rc, rs);
  reply->set_data(rdata);
  messenger->send_message(reply, m->get_source_inst());
  delete m;
}


void Monitor::do_stop()
{
  dout(0) << "do_stop -- initiating shutdown" << dendl;
  stopping = true;
  mdsmon->do_stop();
}


void Monitor::dispatch(Message *m)
{
  lock.Lock();
  {
    switch (m->get_type()) {
      
      // misc
    case CEPH_MSG_PING_ACK:
      handle_ping_ack((MPingAck*)m);
      break;
      
    case CEPH_MSG_SHUTDOWN:
      if (m->get_source().is_osd()) 
	osdmon->dispatch(m);
      else
	handle_shutdown(m);
      break;
      
    case MSG_MON_COMMAND:
      handle_command((MMonCommand*)m);
      break;


      // OSDs
    case CEPH_MSG_OSD_GETMAP:
    case MSG_OSD_FAILURE:
    case MSG_OSD_BOOT:
    case MSG_OSD_IN:
    case MSG_OSD_OUT:
      osdmon->dispatch(m);
      break;

      
      // MDSs
    case MSG_MDS_BEACON:
    case CEPH_MSG_MDS_GETMAP:
      mdsmon->dispatch(m);
      break;

      // clients
    case CEPH_MSG_CLIENT_MOUNT:
    case CEPH_MSG_CLIENT_UNMOUNT:
      clientmon->dispatch(m);
      break;

      // pg
    case CEPH_MSG_STATFS:
    case MSG_PGSTATS:
      pgmon->dispatch(m);
      break;


      // paxos
    case MSG_MON_PAXOS:
      {
	MMonPaxos *pm = (MMonPaxos*)m;

	// sanitize
	if (pm->epoch > mon_epoch) 
	  call_election();
	if (pm->epoch != mon_epoch) {
	  delete pm;
	  break;
	}

	// send it to the right paxos instance
	switch (pm->machine_id) {
	case PAXOS_OSDMAP:
	  paxos_osdmap.dispatch(m);
	  break;
	case PAXOS_MDSMAP:
	  paxos_mdsmap.dispatch(m);
	  break;
	case PAXOS_CLIENTMAP:
	  paxos_clientmap.dispatch(m);
	  break;
	default:
	  assert(0);
	}
      }
      break;

      // elector messages
    case MSG_MON_ELECTION:
      elector.dispatch(m);
      break;

      
    default:
      dout(0) << "unknown message " << m << " " << *m << " from " << m->get_source_inst() << dendl;
      assert(0);
    }
  }
  lock.Unlock();
}


void Monitor::handle_shutdown(Message *m)
{
  assert(m->get_source().is_mon());
  if (m->get_source().num() == get_leader()) {
    dout(1) << "shutdown from leader " << m->get_source() << dendl;
    shutdown();
  } else {
    dout(1) << "ignoring shutdown from non-leader " << m->get_source() << dendl;
  }
  delete m;
}

void Monitor::handle_ping_ack(MPingAck *m)
{
  // ...
  
  delete m;
}




/************ TICK ***************/

class C_Mon_Tick : public Context {
  Monitor *mon;
public:
  C_Mon_Tick(Monitor *m) : mon(m) {}
  void finish(int r) {
    mon->tick();
  }
};

void Monitor::cancel_tick()
{
  if (tick_timer) timer.cancel_event(tick_timer);
}

void Monitor::reset_tick()
{
  cancel_tick();
  tick_timer = new C_Mon_Tick(this);
  timer.add_event_after(g_conf.mon_tick_interval, tick_timer);
}


void Monitor::tick()
{
  tick_timer = 0;

  // ok go.
  dout(11) << "tick" << dendl;
  
  osdmon->tick();
  mdsmon->tick();
  
  // next tick!
  reset_tick();
}







