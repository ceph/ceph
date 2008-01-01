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


#ifndef __MMDSSLAVEREQUEST_H
#define __MMDSSLAVEREQUEST_H

#include "msg/Message.h"
#include "mds/mdstypes.h"
#include "include/encodable.h"

class MMDSSlaveRequest : public Message {
 public:
  static const int OP_XLOCK =       1;
  static const int OP_XLOCKACK =   -1;
  static const int OP_UNXLOCK =     2;
  static const int OP_AUTHPIN =     3;
  static const int OP_AUTHPINACK = -3;

  static const int OP_LINKPREP =     4;
  static const int OP_UNLINKPREP =   5;
  static const int OP_LINKPREPACK = -4;

  static const int OP_RENAMEPREP =     7;
  static const int OP_RENAMEPREPACK = -7;

  static const int OP_FINISH = 17;  

  static const int OP_ABORT =  20;  // used for recovery only
  //static const int OP_COMMIT = 21;  // used for recovery only


  const static char *get_opname(int o) {
    switch (o) { 
    case OP_XLOCK: return "xlock";
    case OP_XLOCKACK: return "xlock_ack";
    case OP_UNXLOCK: return "unxlock";
    case OP_AUTHPIN: return "authpin";
    case OP_AUTHPINACK: return "authpin_ack";

    case OP_LINKPREP: return "link_prep";
    case OP_LINKPREPACK: return "link_prep_ack";
    case OP_UNLINKPREP: return "unlink_prep";

    case OP_RENAMEPREP: return "rename_prep";
    case OP_RENAMEPREPACK: return "rename_prep_ack";

    case OP_FINISH: return "finish"; // commit
    case OP_ABORT: return "abort";
      //case OP_COMMIT: return "commit";

    default: assert(0); return 0;
    }
  }

 private:
  metareqid_t reqid;
  char op;

  // for locking
  char lock_type;  // lock object type
  MDSCacheObjectInfo object_info;
  
  // for authpins
  list<MDSCacheObjectInfo> authpins;

 public:
  // for rename prep
  filepath srcdnpath;
  filepath destdnpath;
  set<int> witnesses;
  bufferlist inode_export;
  version_t inode_export_v;
  bufferlist srci_replica;
  utime_t now;

  bufferlist stray;  // stray dir + dentry

public:
  metareqid_t get_reqid() { return reqid; }
  int get_op() { return op; }
  bool is_reply() { return op < 0; }

  int get_lock_type() { return lock_type; }
  MDSCacheObjectInfo &get_object_info() { return object_info; }

  list<MDSCacheObjectInfo>& get_authpins() { return authpins; }

  void set_lock_type(int t) { lock_type = t; }

  // ----
  MMDSSlaveRequest() : Message(MSG_MDS_SLAVE_REQUEST) { }
  MMDSSlaveRequest(metareqid_t ri, int o) : 
    Message(MSG_MDS_SLAVE_REQUEST),
    reqid(ri), op(o) { }
  void encode_payload() {
    ::_encode(reqid, payload);
    ::_encode(op, payload);
    ::_encode(lock_type, payload);
    object_info._encode(payload);
    ::_encode_complex(authpins, payload);
    srcdnpath._encode(payload);
    destdnpath._encode(payload);
    ::_encode(witnesses, payload);
    ::_encode(now, payload);
    ::_encode(inode_export, payload);
    ::_encode(inode_export_v, payload);
    ::_encode(srci_replica, payload);
    ::_encode(stray, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::_decode_simple(reqid, p);
    ::_decode_simple(op, p);
    ::_decode_simple(lock_type, p);
    object_info._decode(p);
    ::_decode_complex(authpins, p);
    srcdnpath._decode(p);
    destdnpath._decode(p);
    ::_decode_simple(witnesses, p);
    ::_decode_simple(now, p);
    ::_decode_simple(inode_export, p);
    ::_decode_simple(inode_export_v, p);
    ::_decode_simple(srci_replica, p);
    ::_decode_simple(stray, p);
  }

  const char *get_type_name() { return "slave_request"; }
  void print(ostream& out) {
    out << "slave_request(" << reqid
	<< " " << get_opname(op) 
	<< ")";
  }  
	
};

#endif
