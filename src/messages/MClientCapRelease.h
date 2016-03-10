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

#ifndef CEPH_MCLIENTCAPRELEASE_H
#define CEPH_MCLIENTCAPRELEASE_H

#include "msg/Message.h"


class MClientCapRelease : public Message {
  static const int HEAD_VERSION = 2;
  static const int COMPAT_VERSION = 1;
 public:
  struct ceph_mds_cap_release head;
  vector<ceph_mds_cap_item> caps;

  // The message receiver must wait for this OSD epoch
  // before actioning this cap release.
  epoch_t osd_epoch_barrier;

  MClientCapRelease() : 
    Message(CEPH_MSG_CLIENT_CAPRELEASE, HEAD_VERSION, COMPAT_VERSION),
    osd_epoch_barrier(0)
  {
    memset(&head, 0, sizeof(head));
  }
private:
  ~MClientCapRelease() {}

public:
  const char *get_type_name() const { return "client_cap_release";}
  void print(ostream& out) const {
    out << "client_cap_release(" << caps.size() << ")";
  }
  
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(head, p);
    ::decode_nohead(head.num, caps, p);
    if (header.version >= 2) {
      ::decode(osd_epoch_barrier, p);
    }
  }
  void encode_payload(uint64_t features) {
    head.num = caps.size();
    ::encode(head, payload);
    ::encode_nohead(caps, payload);
    ::encode(osd_epoch_barrier, payload);
  }
};
REGISTER_MESSAGE(MClientCapRelease, CEPH_MSG_CLIENT_CAPRELEASE);
#endif
