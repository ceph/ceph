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


#ifndef __MDIRUPDATE_H
#define __MDIRUPDATE_H

#include "msg/Message.h"

class MDirUpdate : public Message {
  struct {
    dirfrag_t dirfrag;
    int dir_rep;
    int discover;
  } st;
  set<int> dir_rep_by;
  filepath path;

 public:
  dirfrag_t get_dirfrag() { return st.dirfrag; }
  int get_dir_rep() { return st.dir_rep; }
  set<int>& get_dir_rep_by() { return dir_rep_by; } 
  bool should_discover() { return st.discover > 0; }
  filepath& get_path() { return path; }

  void tried_discover() {
    if (st.discover) st.discover--;
  }

  MDirUpdate() {}
  MDirUpdate(dirfrag_t dirfrag,
             int dir_rep,
             set<int>& dir_rep_by,
             filepath& path,
             bool discover = false) :
    Message(MSG_MDS_DIRUPDATE) {
    this->st.dirfrag = dirfrag;
    this->st.dir_rep = dir_rep;
    this->dir_rep_by = dir_rep_by;
    if (discover) this->st.discover = 5;
    this->path = path;
  }
  const char *get_type_name() { return "dir_update"; }
  void print(ostream& out) {
    out << "dir_update(" << get_dirfrag() << ")";
  }

  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(st), (char*)&st);
    off += sizeof(st);
    ::_decode(dir_rep_by, payload, off);
    path._decode(payload, off);
  }

  virtual void encode_payload() {
    payload.append((char*)&st, sizeof(st));
    ::_encode(dir_rep_by, payload);
    path._encode(payload);
  }
};

#endif
