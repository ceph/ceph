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


#ifndef __MRENAMEREQ_H
#define __MRENAMEREQ_H

class MRenameReq : public Message {
  int initiator;
  inodeno_t srcdirino;
  string srcname;
  inodeno_t destdirino;
  string destname;
  string destpath;
  int destauth;

 public:
  int get_initiator() { return initiator; }
  inodeno_t get_srcdirino() { return srcdirino; }
  string& get_srcname() { return srcname; }
  inodeno_t get_destdirino() { return destdirino; }
  string& get_destname() { return destname; }
  string& get_destpath() { return destpath; }
  int get_destauth() { return destauth; }

  MRenameReq() {}
  MRenameReq(int initiator,
             inodeno_t srcdirino,
             const string& srcname,
             inodeno_t destdirino,
             const string& destname,
             const string& destpath, 
             int destauth) :
    Message(MSG_MDS_RENAMEREQ) {
    this->initiator = initiator;
    this->srcdirino = srcdirino;
    this->srcname = srcname;
    this->destdirino = destdirino;
    this->destname = destname;
    this->destpath = destpath;
    this->destauth = destauth;
  }
  virtual char *get_type_name() { return "RnReq";}

  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(initiator), (char*)&initiator);
    off += sizeof(initiator);
    payload.copy(off, sizeof(srcdirino), (char*)&srcdirino);
    off += sizeof(srcdirino);
    payload.copy(off, sizeof(destdirino), (char*)&destdirino);
    off += sizeof(destdirino);
    ::_decode(srcname, payload, off);
    ::_decode(destname, payload, off);
    ::_decode(destpath, payload, off);
    payload.copy(off, sizeof(destauth), (char*)&destauth);
    off += sizeof(destauth);
  }
  virtual void encode_payload() {
    payload.append((char*)&initiator,sizeof(initiator));
    payload.append((char*)&srcdirino,sizeof(srcdirino));
    payload.append((char*)&destdirino,sizeof(destdirino));
    ::_encode(srcname, payload);
    ::_encode(destname, payload);
    ::_encode(destpath, payload);
    payload.append((char*)&destauth, sizeof(destauth));
  }
};

#endif
