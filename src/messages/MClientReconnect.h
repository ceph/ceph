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

#ifndef __MCLIENTRECONNECT_H
#define __MCLIENTRECONNECT_H

#include "msg/Message.h"
#include "mds/mdstypes.h"

class MClientReconnect : public Message {
public:
  map<inodeno_t, cap_reconnect_t>  caps;   // only head inodes
  vector<ceph_mds_snaprealm_reconnect> realms;

  MClientReconnect() : Message(CEPH_MSG_CLIENT_RECONNECT) {}
private:
  ~MClientReconnect() {}

public:
  const char *get_type_name() { return "client_reconnect"; }
  void print(ostream& out) {
    out << "client_reconnect("
	<< caps.size() << " caps)";
  }

  void add_cap(inodeno_t ino, uint64_t cap_id, inodeno_t pathbase, const string& path,
	       int wanted, int issued,
	       loff_t sz, utime_t mt, utime_t at,
	       inodeno_t sr) {
    caps[ino] = cap_reconnect_t(cap_id, pathbase, path, wanted, issued, sz, mt, at, sr);
  }
  void add_snaprealm(inodeno_t ino, snapid_t seq, inodeno_t parent) {
    ceph_mds_snaprealm_reconnect r;
    r.ino = ino;
    r.seq = seq;
    r.parent = parent;
    realms.push_back(r);
  }

  void encode_payload() {
    ::encode(caps, data);
    ::encode_nohead(realms, data);
  }
  void decode_payload() {
    bufferlist::iterator p = data.begin();
    ::decode(caps, p);
    while (p.end()) {
      realms.push_back(ceph_mds_snaprealm_reconnect());
      ::decode(realms.back(), p);
    }
  }

};


#endif
