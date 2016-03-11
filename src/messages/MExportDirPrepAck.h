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

#ifndef CEPH_MEXPORTDIRPREPACK_H
#define CEPH_MEXPORTDIRPREPACK_H

#include "msg/Message.h"
#include "include/types.h"

class MExportDirPrepAck : public Message {
  dirfrag_t dirfrag;
  bool success;

 public:
  dirfrag_t get_dirfrag() { return dirfrag; }
  
  MExportDirPrepAck() {}
  MExportDirPrepAck(dirfrag_t df, bool s, uint64_t tid) :
    Message(MSG_MDS_EXPORTDIRPREPACK), dirfrag(df), success(s) {
    set_tid(tid);
  }
private:
  ~MExportDirPrepAck() {}

public:  
  bool is_success() { return success; }
  const char *get_type_name() const { return "ExPAck"; }
  void print(ostream& o) const {
    o << "export_prep_ack(" << dirfrag << (success ? " success)" : " fail)");
  }

  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(dirfrag, p);
    ::decode(success, p);
  }
  void encode_payload(uint64_t features) {
    ::encode(dirfrag, payload);
    ::encode(success, payload);
  }
};
REGISTER_MESSAGE(MExportDirPrepAck, MSG_MDS_EXPORTDIRPREPACK);
#endif
