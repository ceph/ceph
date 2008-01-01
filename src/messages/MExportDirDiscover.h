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

#ifndef __MEXPORTDIRDISCOVER_H
#define __MEXPORTDIRDISCOVER_H

#include "msg/Message.h"
#include "mds/CInode.h"
#include "include/types.h"

class MExportDirDiscover : public Message {
  dirfrag_t dirfrag;
  filepath path;

 public:
  inodeno_t get_ino() { return dirfrag.ino; }
  dirfrag_t get_dirfrag() { return dirfrag; }
  filepath& get_path() { return path; }

  bool started;

  MExportDirDiscover() :     
    Message(MSG_MDS_EXPORTDIRDISCOVER),
    started(false) { }
  MExportDirDiscover(CDir *dir) : 
    Message(MSG_MDS_EXPORTDIRDISCOVER),
    started(false) {
    dir->get_inode()->make_path(path);
    dirfrag = dir->dirfrag();
  }
  const char *get_type_name() { return "ExDis"; }
  void print(ostream& o) {
    o << "export_discover(" << dirfrag << " " << path << ")";
  }

  virtual void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::_decode_simple(dirfrag, p);
    path._decode(p);
  }

  virtual void encode_payload() {
    ::_encode_simple(dirfrag, payload);
    path._encode(payload);
  }
};

#endif
