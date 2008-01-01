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

#ifndef __MEXPORTDIRWARNING_H
#define __MEXPORTDIRWARNING_H

#include "msg/Message.h"
#include "mds/CInode.h"
#include "include/types.h"

class MExportDirWarning : public Message {
  inodeno_t ino;
  int new_dir_auth;

 public:
  inodeno_t get_ino() { return ino; }
  int get_new_dir_auth() { return new_dir_auth; }

  MExportDirWarning() {}
  MExportDirWarning(inodeno_t i, int nda) : 
    Message(MSG_MDS_EXPORTDIRWARNING),
    ino(i), new_dir_auth(nda) {}

  const char *get_type_name() { return "ExW"; }

  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(ino), (char*)&ino);
    off += sizeof(ino);
    payload.copy(off, sizeof(new_dir_auth), (char*)&new_dir_auth);
    off += sizeof(new_dir_auth);
  }
  virtual void encode_payload() {
    payload.append((char*)&ino, sizeof(ino));
    payload.append((char*)&new_dir_auth, sizeof(new_dir_auth));
  }
};

#endif
