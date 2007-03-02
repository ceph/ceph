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
  MExportDirAck(inodeno_t i) :
    Message(MSG_MDS_EXPORTDIRACK), ino(i) { }

  virtual char *get_type_name() { return "ExAck"; }
    void print(ostream& o) {
    o << "export_ack(" << ino << ")";
  }

  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(ino), (char*)&ino);
    off += sizeof(ino);
  }
  virtual void encode_payload() {
    payload.append((char*)&ino, sizeof(ino));
  }

};

#endif
