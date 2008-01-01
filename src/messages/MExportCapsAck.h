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


#ifndef __MEXPORTCAPSACK_H
#define __MEXPORTCAPSACK_H

#include "msg/Message.h"


class MExportCapsAck : public Message {
 public:  
  inodeno_t ino;

  MExportCapsAck() :
    Message(MSG_MDS_EXPORTCAPSACK) {}
  MExportCapsAck(inodeno_t i) :
    Message(MSG_MDS_EXPORTCAPSACK), ino(i) {}

  const char *get_type_name() { return "export_caps_ack"; }
  void print(ostream& o) {
    o << "export_caps_ack(" << ino << ")";
  }

  virtual void decode_payload() {
    int off = 0;
    ::_decode(ino, payload, off);
  }
  virtual void encode_payload() {
    ::_encode(ino, payload);
  }

};

#endif
