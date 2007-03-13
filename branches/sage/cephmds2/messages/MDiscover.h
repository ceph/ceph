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


#ifndef __MDISCOVER_H
#define __MDISCOVER_H

#include "msg/Message.h"
#include "mds/CDir.h"
#include "include/filepath.h"

#include <vector>
#include <string>
using namespace std;


class MDiscover : public Message {
  int             asker;
  inodeno_t       base_ino;          // 1 -> root
  bool            want_base_dir;
  bool            want_root_inode;
  frag_t          base_dir_frag;

  filepath        want;   // ... [/]need/this/stuff

 public:
  int       get_asker() { return asker; }
  inodeno_t get_base_ino() { return base_ino; }
  filepath& get_want() { return want; }
  const string&   get_dentry(int n) { return want[n]; }
  bool      wants_base_dir() { return want_base_dir; }
  frag_t    get_base_dir_frag() { return base_dir_frag; }

  void set_base_dir_frag(frag_t f) { base_dir_frag = f; }

  MDiscover() { }
  MDiscover(int asker, 
            inodeno_t base_ino,
            filepath& want,
            bool want_base_dir = true,
            bool want_root_inode = false) :
    Message(MSG_MDS_DISCOVER) {
    this->asker = asker;
    this->base_ino = base_ino;
    this->want = want;
    this->want_base_dir = want_base_dir;
    this->want_root_inode = want_root_inode;
  }
  virtual char *get_type_name() { return "Dis"; }

  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(asker), (char*)&asker);
    off += sizeof(asker);
    payload.copy(off, sizeof(base_ino), (char*)&base_ino);
    off += sizeof(base_ino);
    payload.copy(off, sizeof(want_base_dir), (char*)&want_base_dir);
    off += sizeof(want_base_dir);
    payload.copy(off, sizeof(base_dir_frag), (char*)&base_dir_frag);
    off += sizeof(base_dir_frag);
    want._decode(payload, off);
  }
  virtual void encode_payload() {
    payload.append((char*)&asker, sizeof(asker));
    payload.append((char*)&base_ino, sizeof(base_ino));
    payload.append((char*)&want_base_dir, sizeof(want_base_dir));
    payload.append((char*)&base_dir_frag, sizeof(base_dir_frag));
    want._encode(payload);
  }

};

#endif
