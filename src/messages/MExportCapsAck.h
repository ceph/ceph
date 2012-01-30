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


#ifndef CEPH_MEXPORTCAPSACK_H
#define CEPH_MEXPORTCAPSACK_H

#include "msg/Message.h"


class MExportCapsAck : public Message {
 public:  
  inodeno_t ino;

  MExportCapsAck() :
    Message(MSG_MDS_EXPORTCAPSACK) {}
  MExportCapsAck(inodeno_t i) :
    Message(MSG_MDS_EXPORTCAPSACK), ino(i) {}
private:
  ~MExportCapsAck() {}

public:
  const char *get_type_name() const { return "export_caps_ack"; }
  void print(ostream& o) const {
    o << "export_caps_ack(" << ino << ")";
  }

  virtual void encode_payload(uint64_t features) {
    ::encode(ino, payload);
  }
  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(ino, p);
  }

};

#endif
