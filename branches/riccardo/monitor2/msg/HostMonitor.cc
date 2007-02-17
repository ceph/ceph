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



#include "HostMonitor.h"

#include "msg/Message.h"
#include "msg/Messenger.h"

#include "messages/MPing.h"
#include "messages/MPingAck.h"
#include "messages/MFailure.h"
#include "messages/MFailureAck.h"

#include "common/Timer.h"
#include "common/Clock.h"

#define DBL  10


#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << whoami << " hostmon: "


// timer contexts

class C_HM_InitiateHeartbeat : public Context {
  HostMonitor *hm;
public:
  C_HM_InitiateHeartbeat(HostMonitor *hm) {
     this->hm = hm;
  }
  void finish(int r) {
    //cout << "HEARTBEAT" << endl;
    hm->pending_events.erase(this);
    hm->initiate_heartbeat();
  }
};

class C_HM_CheckHeartbeat : public Context {
  HostMonitor *hm;
public:
  C_HM_CheckHeartbeat(HostMonitor *hm) {
    this->hm = hm;
  }
  void finish(int r) {
    //cout << "CHECK" << endl;
    hm->pending_events.erase(this);
    hm->check_heartbeat();
  }
};



// startup/shutdown

void HostMonitor::init()
{
  dout(DBL) << "init" << endl;

  // hack params for now
  heartbeat_interval = 10;
  max_ping_time = 2;
  max_heartbeat_misses = 3;
  notify_retry_interval = 10;
  
  // schedule first hb
  schedule_heartbeat();
}


void HostMonitor::shutdown()
{
  // cancel any events
  for (set<Context*>::iterator it = pending_events.begin();
       it != pending_events.end();
       it++) {
    g_timer.cancel_event(*it);
    delete *it;
  }
  pending_events.clear();
}


// schedule next heartbeat

void HostMonitor::schedule_heartbeat()
{
  dout(DBL) << "schedule_heartbeat" << endl;
  Context *e = new C_HM_InitiateHeartbeat(this);
  pending_events.insert(e);
  g_timer.add_event_after(heartbeat_interval, e);
}


// take note of a live host

void HostMonitor::host_is_alive(entity_name_t host)
{
  if (hosts.count(host))
    status[host].last_heard_from = g_clock.gettime();
}


// do heartbeat

void HostMonitor::initiate_heartbeat()
{
  time_t now = g_clock.gettime();
  
  // send out pings
  inflight_pings.clear();
  for (set<entity_name_t>::iterator it = hosts.begin();
       it != hosts.end();
       it++) {
    // have i heard from them recently?
    if (now - status[*it].last_heard_from < heartbeat_interval) {
      dout(DBL) << "skipping " << *it << ", i heard from them recently" << endl;
    } else {
      dout(DBL) << "pinging " << *it << endl;
      status[*it].last_pinged = now;
      inflight_pings.insert(*it);

      messenger->send_message(new MPing(1), *it, 0);
    }
  }
  
  // set timer to check results
  Context *e = new C_HM_CheckHeartbeat(this);
  pending_events.insert(e);
  g_timer.add_event_after(max_ping_time, e);
  dout(10) << "scheduled check " << e << endl;

  schedule_heartbeat();  // schedule next heartbeat
}


// check results

void HostMonitor::check_heartbeat()
{
  dout(DBL) << "check_heartbeat()" << endl;

  // check inflight pings
  for (set<entity_name_t>::iterator it = inflight_pings.begin();
       it != inflight_pings.end();
       it++) {
    status[*it].num_heartbeats_missed++;

    dout(DBL) << "no response from " << *it << " for " << status[*it].num_heartbeats_missed << " beats" << endl;
    
    if (status[*it].num_heartbeats_missed >= max_heartbeat_misses) {
      if (acked_failures.count(*it)) {
        dout(DBL) << *it << " is already failed" << endl;
      } else {
        if (unacked_failures.count(*it)) {
          dout(DBL) << *it << " is already failed, but unacked, sending another failure message" << endl;
        } else {
          dout(DBL) << "failing " << *it << endl;
          unacked_failures.insert(*it);
        }
        
        /*if (false)   // do this in NewMessenger for now!  FIXME
        for (set<msg_addr_t>::iterator nit = notify.begin();
             nit != notify.end();
             nit++) {
          messenger->send_message(new MFailure(*it, messenger->get_inst(*it)),
                                  *nit, notify_port, 0);
        }
        */
      }
    }
  }
 
  // forget about the pings.
  inflight_pings.clear();
}


// incoming messages

void HostMonitor::proc_message(Message *m)
{
  switch (m->get_type()) {

  case MSG_PING_ACK:
    handle_ping_ack((MPingAck*)m);
    break;

  case MSG_FAILURE_ACK:
    handle_failure_ack((MFailureAck*)m);
    break;

  }
}

void HostMonitor::handle_ping_ack(MPingAck *m)
{
  entity_name_t from = m->get_source();

  dout(DBL) << "ping ack from " << from << endl;
  status[from].last_pinged = g_clock.gettime();
  status[from].num_heartbeats_missed = 0;
  inflight_pings.erase(from);

  delete m;
}

void HostMonitor::handle_failure_ack(MFailureAck *m)
{

  // FIXME: this doesn't handle failed -> alive transitions gracefully at all..

  // the higher-up's acknowledged our failure notification, we can stop resending it.
  entity_name_t failed = m->get_failed();
  dout(DBL) << "handle_failure_ack " << failed << endl;
  unacked_failures.erase(failed);
  acked_failures.insert(failed);

  delete m;
}


