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

#include "msg/Message.h"
#include "msg/Messenger.h"

#include "messages/MPing.h"
#include "messages/MPingAck.h"
#include "messages/MGenericMessage.h"

#include "common/Timer.h"
#include "common/Clock.h"

#include "OSDMonitor.h"
#include "MDSMonitor.h"

#include "config.h"
#undef dout
#define  dout(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cout << g_clock.now()<< " mon" << whoami << " "
#define  derr(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cerr << g_clock.now()<< " mon" << whoami << " "



class C_Mon_Tick : public Context {
  Monitor *mon;
public:
  C_Mon_Tick(Monitor *m) : mon(m) {}
  void finish(int r) {
    mon->tick();
  }
};


void Monitor::init()
{
  dout(1) << "init" << endl;
  
  // create 
  osdmon = new OSDMonitor(this, messenger, lock);
  mdsmon = new MDSMonitor(this, messenger, lock);

  // i'm ready!
  messenger->set_dispatcher(this);
  
  // start ticker
  g_timer.add_event_after(g_conf.mon_tick_interval, new C_Mon_Tick(this));
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
      handle_shutdown(m);
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


      // elector messages
    case MSG_MON_ELECTION_ACK:
    case MSG_MON_ELECTION_STATUS:
    case MSG_MON_ELECTION_COLLECT:
    case MSG_MON_ELECTION_REFRESH:
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

  // die.
  messenger->shutdown();
  delete messenger;
  delete m;
}

void Monitor::handle_ping_ack(MPingAck *m)
{
  // ...
  
  delete m;
}




void Monitor::tick()
{
  lock.Lock();
  {
    dout(10) << "tick" << endl;
    
    osdmon->tick();
  }
  lock.Unlock();
}






