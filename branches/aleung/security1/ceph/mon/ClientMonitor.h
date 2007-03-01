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

#ifndef __CLIENTMONITOR_H
#define __CLIENTMONITOR_H

#include <map>
#include <set>
using namespace std;

#include "include/types.h"
#include "msg/Messenger.h"

#include "mds/MDSMap.h"

#include "crypto/CryptoLib.h"
using namespace CryptoLib;

#include "crypto/Ticket.h"

class Monitor;

class ClientMonitor : public Dispatcher {
  Monitor *mon;
  Messenger *messenger;
  Mutex &lock;

 private:
  int num_clients;
  map<entity_name_t,entity_addr_t> client_map;
  map<uid_t, Ticket*> user_tickets;

  void bcast_latest_mds();

  //void accept_pending();   // accept pending, new map.
  //void send_incremental(epoch_t since, msg_addr_t dest);

  void handle_client_boot(class MClientBoot *m);
  void handle_client_auth_user(class MClientAuthUser *m);
  void send_ticket(const entity_inst_t& inst);

 public:
  ClientMonitor(Monitor *mn, Messenger *m, Mutex& l) : mon(mn), messenger(m), lock(l),
						       num_clients(0) { }
  
  void dispatch(Message *m);
  void tick();  // check state, take actions
};

#endif
