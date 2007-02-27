// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

typedef struct {
  inodeno_t ino;
  int dir_rep;
  int discover;
} MDirUpdate_st;

class MDirUpdate : public Message {
  MDirUpdate_st st;
  set<int> dir_rep_by;
  string path;

 public:
  inodeno_t get_ino() { return st.ino; }
  int get_dir_rep() { return st.dir_rep; }
  set<int>& get_dir_rep_by() { return dir_rep_by; } 
  bool should_discover() { return st.discover > 0; }
  string& get_path() { return path; }

  void tried_discover() {
    if (st.discover) st.discover--;
  }

  MDirUpdate() {}
  MDirUpdate(inodeno_t ino,
             int dir_rep,
             set<int>& dir_rep_by,
             string& path,
             bool discover = false) :
    Message(MSG_MDS_DIRUPDATE) {
    this->st.ino = ino;
    this->st.dir_rep = dir_rep;
    this->dir_rep_by = dir_rep_by;
    if (discover) this->st.discover = 5;
    this->path = path;
  }
  virtual char *get_type_name() { return "dup"; }

  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(st), (char*)&st);
    off += sizeof(st);
    ::_decode(dir_rep_by, payload, off);
    ::_decode(path, payload, off);
  }

  virtual void encode_payload() {
    payload.append((char*)&st, sizeof(st));
    ::_encode(dir_rep_by, payload);
    ::_encode(path, payload);
  }
};

#endif
