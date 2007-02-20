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

#ifndef __MEXPORTDIRDISCOVERACK_H
#define __MEXPORTDIRDISCOVERACK_H

#include "msg/Message.h"
#include "mds/CInode.h"
#include "include/types.h"

class MExportDirDiscoverAck : public Message {
  inodeno_t ino;
  bool success;

 public:
  inodeno_t get_ino() { return ino; }
  bool is_success() { return success; }

  MExportDirDiscoverAck() {}
  MExportDirDiscoverAck(inodeno_t ino, bool success=true) : 
    Message(MSG_MDS_EXPORTDIRDISCOVERACK) {
    this->ino = ino;
    this->success = false;
  }
  virtual char *get_type_name() { return "ExDisA"; }


  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(ino), (char*)&ino);
    off += sizeof(ino);
    payload.copy(off, sizeof(success), (char*)&success);
    off += sizeof(success);
  }

  virtual void encode_payload() {
    payload.append((char*)&ino, sizeof(ino));
    payload.append((char*)&success, sizeof(success));
  }
};

#endif
