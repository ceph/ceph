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


#ifndef __MEXPORTDIRACK_H
#define __MEXPORTDIRACK_H

#include "MExportDir.h"

class MExportDirAck : public Message {
  inodeno_t ino;

 public:
  inodeno_t get_ino() { return ino; }
  
  MExportDirAck() {}
  MExportDirAck(MExportDir *req) :
    Message(MSG_MDS_EXPORTDIRACK) {
    ino = req->get_ino();
  }  
  virtual char *get_type_name() { return "ExAck"; }
  
  virtual void decode_payload(crope& s) {
    s.copy(0, sizeof(ino), (char*)&ino);
  }
  virtual void encode_payload(crope& s) {
    s.append((char*)&ino, sizeof(ino));
  }

};

#endif
