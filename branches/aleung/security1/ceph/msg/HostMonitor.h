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


#ifndef __HOSTMONITOR_H
#define __HOSTMONITOR_H

#include <time.h>

#include <map>
#include <set>
using namespace std;

#include "include/Context.h"
#include "msg/Message.h"

class Message;
class Messenger;

typedef struct {
  time_t last_heard_from;
  time_t last_pinged;
  int    num_heartbeats_missed;
} monitor_rec_t;

class HostMonitor {
  Messenger *messenger;
  string whoami;

  // hosts i monitor
  set<entity_name_t>  hosts;

  // who i tell when they fail
  set<entity_name_t>  notify;
  int              notify_port;

  // their status
  map<entity_name_t,monitor_rec_t>  status;

  set<entity_name_t>  inflight_pings;    // pings we sent that haven't replied yet

  set<entity_name_t>  unacked_failures;  // failed hosts that haven't been acked yet.
  set<entity_name_t>  acked_failures;    // these failures have been acked.

  float heartbeat_interval;    // how often to do a heartbeat
  float max_ping_time;         // how long before it's a miss
  int   max_heartbeat_misses;  // how many misses before i tell
  float notify_retry_interval; // how often to retry failure notification

 public:
  set<Context*>  pending_events;

 private:
  void schedule_heartbeat();

 public:
  HostMonitor(Messenger *m, string& whoami) {
    this->messenger = m;
    this->whoami = whoami;
    notify_port = 0;
  }
  set<entity_name_t>& get_hosts() { return hosts; }
  set<entity_name_t>& get_notify() { return notify; }
  void set_notify_port(int p) { notify_port = p; }

  void remove_host(entity_name_t h) {
    hosts.erase(h);
    status.erase(h);
    unacked_failures.erase(h);
    acked_failures.erase(h);
  }

  void init();
  void shutdown();
  
  void host_is_alive(entity_name_t who);

  void proc_message(Message *m);
  void handle_ping_ack(class MPingAck *m);
  void handle_failure_ack(class MFailureAck *m);

  void initiate_heartbeat();
  void check_heartbeat();

};

#endif
