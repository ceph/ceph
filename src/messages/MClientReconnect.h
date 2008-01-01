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
  bool closed;  // true if this session was closed by the client.

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
    ::_encode(closed, payload);
    ::_encode(inode_caps, payload);
    ::_encode(inode_path, payload);
  }
  void decode_payload() {
    int off = 0;
    ::_decode(closed, payload, off);
    ::_decode(inode_caps, payload, off);
    ::_decode(inode_path, payload, off);
  }

};


#endif
