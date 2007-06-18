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


#ifndef __MINODELINK_H
#define __MINODELINK_H

class MInodeLink : public Message {
public:
  static const int OP_PREPARE  = 1;
  static const int OP_AGREE    = 2;
  static const int OP_COMMIT   = 3;
  static const int OP_ACK      = 4;
  static const int OP_ROLLBACK = 5;

  const char *get_opname(int o) {
    switch (o) {
    case OP_PREPARE: return "prepare";
    case OP_AGREE: return "agree";
    case OP_COMMIT: return "commit";
    case OP_ACK: return "ack";
    case OP_ROLLBACK: return "rollback";
    default: assert(0);
    }
  }

private:
  struct _st {
    inodeno_t ino;      // inode to nlink++
    metareqid_t reqid;  // relevant request
    int op;             // see above
    bool inc;           // true == ++, false == --

    utime_t ctime;
  } st;

public:
  inodeno_t get_ino() { return st.ino; }
  metareqid_t get_reqid() { return st.reqid; }
  int get_op() { return st.op; }
  bool get_inc() { return st.inc; }

  utime_t get_ctime() { return st.ctime; }
  void set_ctime(utime_t ct) { st.ctime = ct; }

  MInodeLink() {}
  MInodeLink(int op, inodeno_t ino, bool inc, metareqid_t ri) :
    Message(MSG_MDS_INODELINK) {
    st.op = op;
    st.ino = ino;
    st.inc = inc;
    st.reqid = ri;
  }

  virtual char *get_type_name() { return "inode_link"; }
  void print(ostream& o) {
    o << "inode_link(" << get_opname(st.op)
      << " " << st.ino 
      << " nlink" << (st.inc ? "++":"--")
      << " " << st.reqid << ")";
  }
  
  virtual void decode_payload() {
    int off = 0;
    _decoderaw(st, payload, off);
  }
  virtual void encode_payload() {
    _encode(st, payload);
  }
};

#endif
