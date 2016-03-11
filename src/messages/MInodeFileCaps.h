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
  __u32     caps;

 public:
  inodeno_t get_ino() { return ino; }
  int       get_caps() { return caps; }

  MInodeFileCaps() : Message(MSG_MDS_INODEFILECAPS) {}
  MInodeFileCaps(inodeno_t ino, int caps) :
    Message(MSG_MDS_INODEFILECAPS) {
    this->ino = ino;
    this->caps = caps;
  }
private:
  ~MInodeFileCaps() {}

public:
  const char *get_type_name() const { return "inode_file_caps";}
  void print(ostream& out) const {
    out << "inode_file_caps(" << ino << " " << ccap_string(caps) << ")";
  }
  
  void encode_payload(uint64_t features) {
    ::encode(ino, payload);
    ::encode(caps, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(ino, p);
    ::decode(caps, p);
  }
};
REGISTER_MESSAGE(MInodeFileCaps, MSG_MDS_INODEFILECAPS);
#endif
