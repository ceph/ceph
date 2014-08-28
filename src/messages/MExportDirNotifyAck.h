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

#ifndef CEPH_MEXPORTDIRNOTIFYACK_H
#define CEPH_MEXPORTDIRNOTIFYACK_H

#include "msg/Message.h"

class MExportDirNotifyAck : public Message {
  dirfrag_t dirfrag;

 public:
  dirfrag_t get_dirfrag() { return dirfrag; }
  
  MExportDirNotifyAck() {}
  MExportDirNotifyAck(dirfrag_t df, uint64_t tid) :
    Message(MSG_MDS_EXPORTDIRNOTIFYACK), dirfrag(df) {
    set_tid(tid);
  }
private:
  ~MExportDirNotifyAck() {}

public:
  const char *get_type_name() const { return "ExNotA"; }
  void print(ostream& o) const {
    o << "export_notify_ack(" << dirfrag << ")";
  }

  void encode_payload(uint64_t features) {
    ::encode(dirfrag, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(dirfrag, p);
  }
  
};

#endif
