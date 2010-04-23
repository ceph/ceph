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

#ifndef __MCLIENTCAPRELEASE_H
#define __MCLIENTCAPRELEASE_H

#include "msg/Message.h"


class MClientCapRelease : public Message {
 public:
  struct ceph_mds_cap_release head;
  vector<ceph_mds_cap_item> caps;

  MClientCapRelease() : 
    Message(CEPH_MSG_CLIENT_CAPRELEASE) {
    memset(&head, 0, sizeof(head));
  }
private:
  ~MClientCapRelease() {}

public:
  const char *get_type_name() { return "client_cap_release";}
  void print(ostream& out) {
    out << "client_cap_release(" << head.num << ")";
  }
  
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(head, p);
    ::decode_nohead(head.num, caps, p);
  }
  void encode_payload() {
    head.num = caps.size();
    ::encode(head, payload);
    ::encode_nohead(caps, payload);
  }
};

#endif
