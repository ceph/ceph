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

#ifndef __CLIENTMAP_H
#define __CLIENTMAP_H

#include "msg/Message.h"

#include <set>
using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;


/*
 * this structure is used by the MDS purely so that
 * it can remember client addresses (entity_inst_t)
 * while processing request(s) on behalf of clients.
 * as such it's only really a sort of short-term cache.
 * 
 * it also remembers which clients mounted via this MDS,
 * for the same reason (so that mounted clients can be 
 * contacted if necessary).
 */
class ClientMap {
  hash_map<int,entity_inst_t> client_inst;
  set<int>           client_mount;
  hash_map<int, int> client_ref;
  
  void inc_ref(int client, const entity_inst_t& inst) {
    if (client_inst.count(client)) {
      assert(client_inst[client] == inst);
      assert(client_ref.count(client));
    } else {
      client_inst[client] = inst;
    }
    client_ref[client]++;
  }
  void dec_ref(int client) {
    assert(client_ref.count(client));
    assert(client_ref[client] > 0);
    client_ref[client]--;
    if (client_ref[client] == 0) {
      client_ref.erase(client);
      client_inst.erase(client);
    }
  }
  
public:
  const entity_inst_t& get_inst(int client) {
    assert(client_inst.count(client));
    return client_inst[client];
  }
  const set<int>& get_mount_set() { return client_mount; }
  
  void add_mount(int client, const entity_inst_t& inst) {
    inc_ref(client, inst);
    client_mount.insert(client);
  }
  void rem_mount(int client) {
    dec_ref(client);
    client_mount.erase(client);
  }
  
  
  void add_open(int client, const entity_inst_t& inst) {
    inc_ref(client, inst);
  }
  void dec_open(int client) {
    dec_ref(client);
  }
};

#endif
