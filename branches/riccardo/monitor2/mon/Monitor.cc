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

// TODO: missing run() method, which creates the two main timers, refreshTimer and readTimer

#include "Monitor.h"

#include "osd/OSDMap.h"

#include "ebofs/Ebofs.h"

#include "msg/Message.h"
#include "msg/Messenger.h"

#include "messages/MPing.h"
#include "messages/MPingAck.h"
#include "messages/MGenericMessage.h"

#include "common/Timer.h"
#include "common/Clock.h"

#include "OSDMonitor.h"
#include "MDSMonitor.h"
#include "ClientMonitor.h"

#include "config.h"
#undef dout
#define  dout(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cout << g_clock.now() << " mon" << whoami << (is_starting() ? (const char*)"(starting)":(is_leader() ? (const char*)"(leader)":(is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << " "
#define  derr(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cerr << g_clock.now() << " mon" << whoami << (is_starting() ? (const char*)"(starting)":(is_leader() ? (const char*)"(leader)":(is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << " "



void Monitor::init()
{
  dout(1) << "init" << endl;
  
  // store
  char s[80];
  sprintf(s, "dev/mon%d", whoami);
  store = new Ebofs(s);

  if (g_conf.mkfs)
    store->mkfs();
  int r = store->mount();
  assert(r >= 0);

  // create 
  osdmon = new OSDMonitor(this, messenger, lock);
  mdsmon = new MDSMonitor(this, messenger, lock);
  clientmon = new ClientMonitor(this, messenger, lock);

  // i'm ready!
  messenger->set_dispatcher(this);
  
  // start ticker
  reset_tick();

  // call election?
  if (monmap->num_mon > 1) {
    assert(monmap->num_mon != 2); 
    call_election();
  } else {
    // we're standalone.
    set<int> q;
    q.insert(whoami);
    win_election(q);
  }
}

void Monitor::shutdown()
{
  dout(1) << "shutdown" << endl;

  cancel_tick();

  if (store) {
    store->umount();
    delete store;
  }
  
  // stop osds.
  for (set<int>::iterator it = osdmon->osdmap.get_osds().begin();
       it != osdmon->osdmap.get_osds().end();
       it++) {
    if (osdmon->osdmap.is_down(*it)) continue;
    dout(10) << "sending shutdown to osd" << *it << endl;
    messenger->send_message(new MGenericMessage(MSG_SHUTDOWN),
			    MSG_ADDR_OSD(*it), osdmon->osdmap.get_inst(*it));
  }
  
  // monitors too.
  for (int i=0; i<monmap->num_mon; i++)
    if (i != whoami)
      messenger->send_message(new MGenericMessage(MSG_SHUTDOWN), 
			      MSG_ADDR_MON(i), monmap->get_inst(i));

  // clean up
  if (monmap) delete monmap;
  if (osdmon) delete osdmon;
  if (mdsmon) delete mdsmon;
  if (clientmon) delete clientmon;

  // die.
  messenger->shutdown();
  delete messenger;
}


void Monitor::call_election()
{
  if (monmap->num_mon == 1) return;

  dout(10) << "call_election" << endl;
  state = STATE_STARTING;

  elector.start();

  osdmon->election_starting();
  //mdsmon->election_starting();
}

void Monitor::win_election(set<int>& active) 
{
  state = STATE_LEADER;
  leader = whoami;
  quorum = active;
  dout(10) << "win_election, quorum is " << quorum << endl;

  // init
  osdmon->election_finished();
  //mdsmon->election_finished();

  // init paxos
  test_paxos.leader_start();
} 

void Monitor::lose_election(int l) 
{
  state = STATE_PEON;
  leader = l;
  dout(10) << "lose_election, leader is mon" << leader << endl;
}



void Monitor::dispatch(Message *m)
{
  lock.Lock();
  {
    switch (m->get_type()) {

      // misc
    case MSG_PING_ACK:
      handle_ping_ack((MPingAck*)m);
      break;

    case MSG_SHUTDOWN:
      if (m->get_source().is_mds()) {
	mdsmon->dispatch(m);
	if (mdsmon->mdsmap.get_num_mds() == 0) 
	  shutdown();
      }
      else if (m->get_source().is_osd()) {
	osdmon->dispatch(m);
      }
      break;


      // OSDs
    case MSG_OSD_GETMAP:
    case MSG_OSD_FAILURE:
    case MSG_OSD_BOOT:
    case MSG_OSD_IN:
    case MSG_OSD_OUT:
      osdmon->dispatch(m);
      break;

      
      // MDSs
    case MSG_MDS_BOOT:
    case MSG_MDS_GETMAP:
      mdsmon->dispatch(m);
      break;

      // clients
    case MSG_CLIENT_BOOT:
      clientmon->dispatch(m);
      break;


      // elector messages
    case MSG_MON_ELECTION_PROPOSE:
    case MSG_MON_ELECTION_ACK:
    case MSG_MON_ELECTION_VICTORY:
      elector.dispatch(m);
      break;

      
    default:
      dout(0) << "unknown message " << *m << endl;
      assert(0);
    }
  }
  lock.Unlock();
}


void Monitor::handle_shutdown(Message *m)
{
  dout(1) << "shutdown from " << m->get_source() << endl;

  shutdown();
  delete m;
}

void Monitor::handle_ping_ack(MPingAck *m)
{
  // ...
  
  delete m;
}




/************ TIMER ***************/

class C_Mon_Tick : public Context {
  Monitor *mon;
public:
  C_Mon_Tick(Monitor *m) : mon(m) {}
  void finish(int r) {
    mon->tick(this);
  }
};


void Monitor::cancel_tick()
{
  if (!tick_timer) return;

  if (g_timer.cancel_event(tick_timer)) {
    dout(10) << "cancel_tick canceled" << endl;
  } else {
    // already dispatched!
    dout(10) << "cancel_tick timer dispatched, waiting to cancel" << endl;
    tick_timer = (Context*)1;  // hackish.
    while (tick_timer)
      tick_timer_cond.Wait(lock);    
  }
}

void Monitor::reset_tick()
{
  if (tick_timer) 
    cancel_tick();
  tick_timer = new C_Mon_Tick(this);
  g_timer.add_event_after(g_conf.mon_tick_interval, tick_timer);
}


void Monitor::tick(Context *timer)
{
  lock.Lock();
  {
    if (tick_timer != timer) {
      dout(10) << "tick - canceled" << endl;
      tick_timer = 0;
      tick_timer_cond.Signal();
      lock.Unlock();
      return;
    }

    tick_timer = 0;

    // ok go.
    dout(10) << "tick" << endl;

    osdmon->tick();

    // next tick!
    reset_tick();
  }
  lock.Unlock();
}







