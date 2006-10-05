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


#ifndef __MRENAME_H
#define __MRENAME_H

class MRename : public Message {
  inodeno_t srcdirino;
  string srcname;
  inodeno_t destdirino;
  string destname;
  int initiator;

  bufferlist inode_state;

 public:
  int get_initiator() { return initiator; }
  inodeno_t get_srcdirino() { return srcdirino; }
  string& get_srcname() { return srcname; }
  inodeno_t get_destdirino() { return destdirino; }
  string& get_destname() { return destname; }
  bufferlist& get_inode_state() { return inode_state; }

  MRename() {}
  MRename(int initiator,
          inodeno_t srcdirino,
          const string& srcname,
          inodeno_t destdirino,
          const string& destname,
          bufferlist& inode_state) :
    Message(MSG_MDS_RENAME) {
    this->initiator = initiator;
    this->srcdirino = srcdirino;
    this->srcname = srcname;
    this->destdirino = destdirino;
    this->destname = destname;
    this->inode_state.claim( inode_state );
  }
  virtual char *get_type_name() { return "Rn";}
  
  virtual void decode_payload() {
    int off = 0;
    payload.copy(off, sizeof(initiator), (char*)&initiator);
    off += sizeof(initiator);
    payload.copy(off, sizeof(srcdirino), (char*)&srcdirino);
    off += sizeof(srcdirino);
    payload.copy(off, sizeof(destdirino), (char*)&destdirino);
    off += sizeof(destdirino);
    _decode(srcname, payload, off);
    _decode(destname, payload, off);
    size_t len;
    payload.copy(off, sizeof(len), (char*)&len);
    off += sizeof(len);
    inode_state.substr_of(payload, off, len);
    off += len;
  }
  virtual void encode_payload() {
    payload.append((char*)&initiator,sizeof(initiator));
    payload.append((char*)&srcdirino,sizeof(srcdirino));
    payload.append((char*)&destdirino,sizeof(destdirino));
    _encode(srcname, payload);
    _encode(destname, payload);
    size_t len = inode_state.length();
    payload.append((char*)&len, sizeof(len));
    payload.claim_append(inode_state);
  }
};

#endif
