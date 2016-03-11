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

#ifndef CEPH_MEXPORTDIRACK_H
#define CEPH_MEXPORTDIRACK_H

#include "MExportDir.h"

class MExportDirAck : public Message {
public:
  dirfrag_t dirfrag;
  bufferlist imported_caps;

  dirfrag_t get_dirfrag() { return dirfrag; }
  
  MExportDirAck() : Message(MSG_MDS_EXPORTDIRACK) {}
  MExportDirAck(dirfrag_t df, uint64_t tid) :
    Message(MSG_MDS_EXPORTDIRACK), dirfrag(df) {
    set_tid(tid);
  }
private:
  ~MExportDirAck() {}

public:
  const char *get_type_name() const { return "ExAck"; }
    void print(ostream& o) const {
    o << "export_ack(" << dirfrag << ")";
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(dirfrag, p);
    ::decode(imported_caps, p);
  }
  void encode_payload(uint64_t features) {
    ::encode(dirfrag, payload);
    ::encode(imported_caps, payload);
  }

};
REGISTER_MESSAGE(MExportDirAck, MSG_MDS_EXPORTDIRACK);
#endif
