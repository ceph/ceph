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

#ifndef __MEXPORTDIRWARNINGACK_H
#define __MEXPORTDIRWARNINGACK_H

#include "msg/Message.h"
#include "include/types.h"

class MExportDirWarningAck : public Message {
  inodeno_t ino;

 public:
  inodeno_t get_ino() { return ino; }

  MExportDirWarningAck() {}
  MExportDirWarningAck(inodeno_t i) : 
    Message(MSG_MDS_EXPORTDIRWARNINGACK),
    ino(i) {}

  const char *get_type_name() { return "ExWAck"; }

  void encode_payload() {
    ::encode(ino, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(ino, p);
  }
};

#endif
