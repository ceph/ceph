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
  frag_t          base_dir_frag;
  bool            want_base_dir;

  filepath        want;   // ... [/]need/this/stuff
  inodeno_t       want_ino;

 public:
  int       get_asker() { return asker; }
  inodeno_t get_base_ino() { return base_ino; }
  frag_t    get_base_dir_frag() { return base_dir_frag; }
  filepath& get_want() { return want; }
  inodeno_t get_want_ino() { return want_ino; }
  const string&   get_dentry(int n) { return want[n]; }
  bool      wants_base_dir() { return want_base_dir; }

  void set_base_dir_frag(frag_t f) { base_dir_frag = f; }

  MDiscover() { }
  MDiscover(int asker, 
            inodeno_t base_ino,
            filepath& want,
            bool want_base_dir = true) :
    Message(MSG_MDS_DISCOVER) {
    this->asker = asker;
    this->base_ino = base_ino;
    this->want = want;
    want_ino = 0;
    this->want_base_dir = want_base_dir;
  }
  MDiscover(int asker, 
            dirfrag_t base_dirfrag,
            inodeno_t want_ino,
            bool want_base_dir = true) :
    Message(MSG_MDS_DISCOVER) {
    this->asker = asker;
    this->base_ino = base_dirfrag.ino;
    this->base_dir_frag = base_dirfrag.frag;
    this->want_ino = want_ino;
    this->want_base_dir = want_base_dir;
  }

  char *get_type_name() { return "Dis"; }
  void print(ostream &out) {
    out << "discover(" << base_ino << "." << base_dir_frag
	<< " " << want;
    if (want_ino) out << want_ino;
    out << ")";
  }

  void decode_payload() {
    int off = 0;
    ::_decode(asker, payload, off);
    ::_decode(base_ino, payload, off);
    ::_decode(base_dir_frag, payload, off);
    ::_decode(want_base_dir, payload, off);
    want._decode(payload, off);
    ::_decode(want_ino, payload, off);
  }
  void encode_payload() {
    payload.append((char*)&asker, sizeof(asker));
    payload.append((char*)&base_ino, sizeof(base_ino));
    payload.append((char*)&base_dir_frag, sizeof(base_dir_frag));
    payload.append((char*)&want_base_dir, sizeof(want_base_dir));
    want._encode(payload);
    ::_encode(want_ino, payload);
  }

};

#endif
