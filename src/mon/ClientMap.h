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

/*
 * The Client Monitor is used for traking the filesystem's clients.
 */

#ifndef __CLIENTMAP_H
#define __CLIENTMAP_H

#include <map>
#include <set>
using namespace std;

#include "include/types.h"
#include "auth/ClientTicket.h"

struct client_info_t {
  ClientTicket ticket;
  bufferlist signed_ticket;

  entity_addr_t addr() { return entity_addr_t(ticket.addr); }
  utime_t created() { return utime_t(ticket.created); }

  void encode(bufferlist& bl) const {
    ::encode(ticket, bl);
    ::encode(signed_ticket, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(ticket, bl);
    ::decode(signed_ticket, bl);
  }
};

WRITE_CLASS_ENCODER(client_info_t)


class ClientMap {
public:
  version_t version;
  uint32_t next_client;
  map<uint32_t,client_info_t> client_info;
  map<entity_addr_t,uint32_t> addr_client;  // reverse map

  class Incremental {
  public:

    version_t version;
    uint32_t next_client;
    map<int32_t, client_info_t> mount;
    set<int32_t> unmount;

    Incremental() : version(0), next_client() {}

    bool is_empty() { return mount.empty() && unmount.empty(); }
    void add_mount(uint32_t client, client_info_t& info) {
      next_client = MAX(next_client, client+1);
      mount[client] = info;
    }
    void add_unmount(uint32_t client) {
      assert(client < next_client);
      if (mount.count(client))
	mount.erase(client);
      else
	unmount.insert(client);
    }

    void encode(bufferlist &bl) const {
      ::encode(version, bl);
      ::encode(next_client, bl);
      ::encode(mount, bl);
      ::encode(unmount, bl);
    }
    void decode(bufferlist::iterator &bl) {
      ::decode(version, bl);
      ::decode(next_client, bl);
      ::decode(mount, bl);
      ::decode(unmount, bl);
    }
  };

  ClientMap() : version(0), next_client(0) {}

  void reverse() {
    addr_client.clear();
    for (map<uint32_t,client_info_t>::iterator p = client_info.begin();
	 p != client_info.end();
	 ++p) {
      addr_client[p->second.addr()] = p->first;
    }
  }
  void apply_incremental(Incremental &inc) {
    assert(inc.version == version+1);
    version = inc.version;
    next_client = inc.next_client;
    for (map<int32_t,client_info_t>::iterator p = inc.mount.begin();
	   p != inc.mount.end();
	   ++p) {
	client_info[p->first] = p->second;
	addr_client[p->second.addr()] = p->first;
    }

    for (set<int32_t>::iterator p = inc.unmount.begin();
	   p != inc.unmount.end();
	   ++p) {
	assert(client_info.count(*p));
	addr_client.erase(client_info[*p].addr());
	client_info.erase(*p);
    }
  }

  void encode(bufferlist &bl) const {
    ::encode(version, bl);
    ::encode(next_client, bl);
    ::encode(client_info, bl);
  }
  void decode(bufferlist::iterator &bl) {
    ::decode(version, bl);
    ::decode(next_client, bl);
    ::decode(client_info, bl);
    reverse();
  }

  void print_summary(ostream& out) {
    out << "v" << version << ": "
	<< client_info.size() << " clients, next is client" << next_client;
  }
};

inline ostream& operator<<(ostream& out, ClientMap& cm) {
  cm.print_summary(out);
  return out;
}

#endif
