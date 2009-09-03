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

class ClientMap {
public:
  version_t version;
  client_t next_client;

  ClientMap() : version(0), next_client(0) {}

  void encode(bufferlist &bl) const {
    ::encode(version, bl);
    ::encode(next_client, bl);
  }
  void decode(bufferlist::iterator &bl) {
    ::decode(version, bl);
    ::decode(next_client, bl);
  }

  void print_summary(ostream& out) {
    out << "v" << version << ": next is " << next_client;
  }
};

inline ostream& operator<<(ostream& out, ClientMap& cm) {
  cm.print_summary(out);
  return out;
}

#endif
