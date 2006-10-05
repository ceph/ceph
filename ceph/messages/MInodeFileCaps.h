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


#ifndef __MINODEFILECAPS_H
#define __MINODEFILECAPS_H

class MInodeFileCaps : public Message {
  inodeno_t ino;
  int       from;
  int       caps;

 public:
  inodeno_t get_ino() { return ino; }
  int       get_from() { return from; }
  int       get_caps() { return caps; }

  MInodeFileCaps() {}
  // from auth
  MInodeFileCaps(inodeno_t ino, int from, int caps) :
    Message(MSG_MDS_INODEFILECAPS) {

    this->ino = ino;
    this->from = from;
    this->caps = caps;
  }

  virtual char *get_type_name() { return "Icap";}
  
  virtual void decode_payload(crope& s, int& off) {
    s.copy(off, sizeof(from), (char*)&from);
    off += sizeof(from);
    s.copy(off, sizeof(ino), (char*)&ino);
    off += sizeof(ino);
    s.copy(off, sizeof(caps), (char*)&caps);
    off += sizeof(caps);
  }
  virtual void encode_payload(crope& s) {
    s.append((char*)&from, sizeof(from));
    s.append((char*)&ino, sizeof(ino));
    s.append((char*)&caps, sizeof(caps));
  }
};

#endif
