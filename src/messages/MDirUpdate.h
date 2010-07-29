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


#ifndef CEPH_MDIRUPDATE_H
#define CEPH_MDIRUPDATE_H

#include "msg/Message.h"

class MDirUpdate : public Message {
  int32_t from_mds;
  dirfrag_t dirfrag;
  int32_t dir_rep;
  int32_t discover;
  set<int32_t> dir_rep_by;
  filepath path;

 public:
  int get_source_mds() { return from_mds; }
  dirfrag_t get_dirfrag() { return dirfrag; }
  int get_dir_rep() { return dir_rep; }
  set<int>& get_dir_rep_by() { return dir_rep_by; } 
  bool should_discover() { return discover > 0; }
  filepath& get_path() { return path; }

  void tried_discover() {
    if (discover) discover--;
  }

  MDirUpdate() {}
  MDirUpdate(int f, 
	     dirfrag_t dirfrag,
             int dir_rep,
             set<int>& dir_rep_by,
             filepath& path,
             bool discover = false) :
    Message(MSG_MDS_DIRUPDATE) {
    this->from_mds = f;
    this->dirfrag = dirfrag;
    this->dir_rep = dir_rep;
    this->dir_rep_by = dir_rep_by;
    if (discover) this->discover = 5;
    this->path = path;
  }
private:
  ~MDirUpdate() {}

public:
  const char *get_type_name() { return "dir_update"; }
  void print(ostream& out) {
    out << "dir_update(" << get_dirfrag() << ")";
  }

  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(from_mds, p);
    ::decode(dirfrag, p);
    ::decode(dir_rep, p);
    ::decode(discover, p);
    ::decode(dir_rep_by, p);
    ::decode(path, p);
  }

  virtual void encode_payload() {
    ::encode(from_mds, payload);
    ::encode(dirfrag, payload);
    ::encode(dir_rep, payload);
    ::encode(discover, payload);
    ::encode(dir_rep_by, payload);
    ::encode(path, payload);
  }
};

#endif
