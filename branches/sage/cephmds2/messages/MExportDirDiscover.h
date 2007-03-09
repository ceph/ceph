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

#ifndef __MEXPORTDIRDISCOVER_H
#define __MEXPORTDIRDISCOVER_H

#include "msg/Message.h"
#include "mds/CInode.h"
#include "include/types.h"

class MExportDirDiscover : public Message {
  dirfrag_t dirfrag;
  string path;

 public:
  inodeno_t get_ino() { return dirfrag.ino; }
  dirfrag_t get_dirfrag() { return dirfrag; }
  string& get_path() { return path; }

  MExportDirDiscover() {}
  MExportDirDiscover(CDir *dir) : 
    Message(MSG_MDS_EXPORTDIRDISCOVER) {
    dir->get_inode()->make_path(path);
    dirfrag = dir->dirfrag();
  }
  virtual char *get_type_name() { return "ExDis"; }
  void print(ostream& o) {
    o << "export_discover " << dirfrag << " " << path;
  }

  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(dirfrag), (char*)&dirfrag);
    off += sizeof(dirfrag);
    ::_decode(path, payload, off);
  }

  virtual void encode_payload() {
    payload.append((char*)&dirfrag, sizeof(dirfrag));
    ::_encode(path, payload);
  }
};

#endif
