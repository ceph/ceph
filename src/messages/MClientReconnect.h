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
  map<vinodeno_t, cap_reconnect_t>  caps;
  __u8 closed;  // true if this session was closed by the client.

  MClientReconnect() : Message(CEPH_MSG_CLIENT_RECONNECT),
		       closed(false) { }

  const char *get_type_name() { return "client_reconnect"; }
  void print(ostream& out) {
    out << "client_reconnect("
	<< (closed ? "closed":"")
	<< caps.size() << " caps)";
  }

  void add_cap(vinodeno_t ino, const string& path,
	       int wanted, int issued,
	       loff_t sz, utime_t mt, utime_t at) {
    caps[ino] = cap_reconnect_t(path, wanted, issued, sz, mt, at);
  }

  void encode_payload() {
    ::encode(closed, payload);
    ::encode(caps, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(closed, p);
    ::decode(caps, p);
  }

};


#endif
