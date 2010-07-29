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


#ifndef CEPH_MINODEFILECAPS_H
#define CEPH_MINODEFILECAPS_H

class MInodeFileCaps : public Message {
  inodeno_t ino;
  __s32     from;
  __u32     caps;

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
private:
  ~MInodeFileCaps() {}

public:
  const char *get_type_name() { return "Icap";}
  
  void encode_payload() {
    ::encode(from, payload);
    ::encode(ino, payload);
    ::encode(caps, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(from, p);
    ::decode(ino, p);
    ::decode(caps, p);
  }
};

#endif
