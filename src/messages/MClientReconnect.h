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
  map<inodeno_t, inode_caps_reconnect_t>  inode_caps;
  map<inodeno_t, string> inode_path;
  __u8 closed;  // true if this session was closed by the client.

  MClientReconnect() : Message(CEPH_MSG_CLIENT_RECONNECT),
		       closed(false) { }

  const char *get_type_name() { return "client_reconnect"; }
  void print(ostream& out) {
    out << "client_reconnect(" << inode_caps.size() << " caps)";
  }

  void add_inode_caps(inodeno_t ino, 
		      int wanted, int issued,
		      off_t sz, utime_t mt, utime_t at) {
    inode_caps[ino] = inode_caps_reconnect_t(wanted, issued, sz, mt, at);
  }
  void add_inode_path(inodeno_t ino, const string& path) {
    inode_path[ino] = path;
  }

  void encode_payload() {
    __u32 n = inode_caps.size();
    ::encode(closed, payload);
    ::encode(n, payload);
    for (map<inodeno_t, inode_caps_reconnect_t>::iterator p = inode_caps.begin();
	 p != inode_caps.end();
	 p++) {
      ::encode(p->first, payload);
      ::encode(p->second, payload);
      ::encode(inode_path[p->first], payload);
    }
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(closed, p);
    __u32 n;
    ::decode(n, p);
    while (n--) {
      inodeno_t ino;
      ::decode(ino, p);
      ::decode(inode_caps[ino], p);
      ::decode(inode_path[ino], p);
    }
  }

};


#endif
