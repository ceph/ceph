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

#include "MonitorStore.h"

#include "msg/Message.h"
#include "msg/Messenger.h"

#include "messages/MPing.h"
#include "messages/MPingAck.h"
#include "messages/MGenericMessage.h"

#include "messages/MMonPaxos.h"

#include "common/Timer.h"
#include "common/Clock.h"

#include "OSDMonitor.h"
#include "MDSMonitor.h"
#include "ClientMonitor.h"

#include "config.h"
#undef dout
#define  dout(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cout << g_clock.now() << " mon" << whoami << (is_starting() ? (const char*)"(starting)":(is_leader() ? (const char*)"(leader)":(is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << " "
#define  derr(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cerr << g_clock.now() << " mon" << whoami << (is_starting() ? (const char*)"(starting)":(is_leader() ? (const char*)"(leader)":(is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << " "


//void Monitor::set_new_private_key(string& pk)
void Monitor::set_new_private_key(char *pk)
{
  dout(10) << "set_new_private_key" << endl;
  myPrivKey = _fromStr_esignPrivKey(string(pk, ESIGNPRIVSIZE));
  myPubKey = esignPubKey(myPrivKey);
  
}

void Monitor::init()
{
  lock.Lock();
  
  dout(1) << "init" << endl;
  
  // store
  char s[80];
  sprintf(s, "mondata/mon%d", whoami);
  store = new MonitorStore(s);

  if (g_conf.mkfs) {
    store->mkfs();
    store->mount();
    
    // i should have already been provided a key via set_new_private_key().
    // save it.
    // FIXME.
    bufferlist bl;
    //bl.append(myPrivKey.c_str(), myPrivKey.length());
    string prvString = privToString(myPrivKey);
    string pubString = pubToString(myPubKey);
    ::_encode(prvString, bl);
    ::_encode(pubString, bl);
    store->put_bl_ss(bl, "private_key", 0);
    //assert(0);
  }
  else {
    store->mount();

    // load private key
    // FIXME.
    bufferlist bl;
    store->get_bl_ss(bl, "private_key", 0);
    //myPrivKey = bl.c_str();
    int off = 0;
    string prvString, pubString;
    ::_decode(prvString, bl, off);
    ::_decode(pubString, bl, off);
    // get keys from strings
    myPrivKey = _fromStr_esignPrivKey(prvString);
    myPubKey = _fromStr_esignPubKey(pubString);
    // der?
    //myPrivKey = esignPrivKey("crypto/esig1536.dat");
    //myPubKey = esignPubKey(myPrivKey);
    
  }

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

  lock.Unlock();
}

void Monitor::shutdown()
{
  dout(1) << "shutdown" << endl;

  // cancel all events
  cancel_tick();
  timer.cancel_all();
  timer.join();
  
  // stop osds.
  for (set<int>::iterator it = osdmon->osdmap.get_osds().begin();
       it != osdmon->osdmap.get_osds().end();
       it++) {
    if (osdmon->osdmap.is_down(*it)) continue;
    dout(10) << "sending shutdown to osd" << *it << endl;
    messenger->send_message(new MGenericMessage(MSG_SHUTDOWN),
			    osdmon->osdmap.get_inst(*it));
  }
  osdmon->mark_all_down();
  
  // monitors too.
  for (int i=0; i<monmap->num_mon; i++)
    if (i != whoami)
      messenger->send_message(new MGenericMessage(MSG_SHUTDOWN), 
			      monmap->get_inst(i));

  // unmount my local storage
  if (store) 
    delete store;
  
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
  mdsmon->election_finished();

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
      assert(m->get_source().is_osd());
      osdmon->dispatch(m);
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
    case MSG_MDS_BEACON:
    case MSG_MDS_GETMAP:
      mdsmon->dispatch(m);

      // hackish: did all mds's shut down?
      if (g_conf.mon_stop_with_last_mds &&
	  mdsmon->mdsmap.get_num_up_or_failed_mds() == 0) 
	shutdown();

      break;

      // clients
    case MSG_CLIENT_BOOT:
    case MSG_CLIENT_AUTH_USER:
      clientmon->dispatch(m);
      break;


      // paxos
    case MSG_MON_PAXOS:
      // send it to the right paxos instance
      switch (((MMonPaxos*)m)->machine_id) {
      case PAXOS_TEST:
	test_paxos.dispatch(m);
	break;
      case PAXOS_OSDMAP:
	//...
	
      default:
	assert(0);
      }
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
  dout(11) << "tick" << endl;
  
  osdmon->tick();
  mdsmon->tick();
  
  // next tick!
  reset_tick();
}







