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

#ifndef CEPH_MEXPORTDIRDISCOVER_H
#define CEPH_MEXPORTDIRDISCOVER_H

#include "msg/Message.h"
#include "include/types.h"

class MExportDirDiscover : public Message {
  mds_rank_t from;
  dirfrag_t dirfrag;
  filepath path;

 public:
  mds_rank_t get_source_mds() { return from; }
  inodeno_t get_ino() { return dirfrag.ino; }
  dirfrag_t get_dirfrag() { return dirfrag; }
  filepath& get_path() { return path; }

  bool started;

  MExportDirDiscover() :     
    Message(MSG_MDS_EXPORTDIRDISCOVER),
    started(false) { }
  MExportDirDiscover(dirfrag_t df, filepath& p, mds_rank_t f, uint64_t tid) :
    Message(MSG_MDS_EXPORTDIRDISCOVER),
    from(f), dirfrag(df), path(p), started(false) {
    set_tid(tid);
  }
private:
  ~MExportDirDiscover() {}

public:
  const char *get_type_name() const { return "ExDis"; }
  void print(ostream& o) const {
    o << "export_discover(" << dirfrag << " " << path << ")";
  }

  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(from, p);
    ::decode(dirfrag, p);
    ::decode(path, p);
  }

  virtual void encode_payload(uint64_t features) {
    ::encode(from, payload);
    ::encode(dirfrag, payload);
    ::encode(path, payload);
  }
};
REGISTER_MESSAGE(MExportDirDiscover, MSG_MDS_EXPORTDIRDISCOVER);
#endif
