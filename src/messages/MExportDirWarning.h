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
#include "include/types.h"

class MExportDirWarning : public Message {
  inodeno_t ino;
  __s32 new_dir_auth;

 public:
  inodeno_t get_ino() { return ino; }
  int get_new_dir_auth() { return new_dir_auth; }

  MExportDirWarning() {}
  MExportDirWarning(inodeno_t i, int nda) : 
    Message(MSG_MDS_EXPORTDIRWARNING),
    ino(i), new_dir_auth(nda) {}
private:
  ~MExportDirWarning() {}

public:
  const char *get_type_name() { return "ExW"; }

  void encode_payload() {
    ::encode(ino, payload);
    ::encode(new_dir_auth, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(ino, p);
    ::decode(new_dir_auth, p);
  }
};

#endif
