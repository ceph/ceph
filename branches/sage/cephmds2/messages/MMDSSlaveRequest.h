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

  static const int OP_PINDN =       5;
  static const int OP_PINDNACK =   -5;
  static const int OP_UNPINDN =     6;

  static const int OP_RENAMEPREP =     7;
  static const int OP_RENAMEPREPACK = -7;

  static const int OP_FINISH =     17;

  const static char *get_opname(int o) {
    switch (o) { 
    case OP_XLOCK: return "xlock";
    case OP_XLOCKACK: return "xlock_ack";
    case OP_UNXLOCK: return "unxlock";
    case OP_AUTHPIN: return "authpin";
    case OP_AUTHPINACK: return "authpin_ack";

    case OP_RENAMEPREP: return "rename_prep";
    case OP_RENAMEPREPACK: return "rename_prep_ack";

    case OP_PINDN: return "pin_dn";
    case OP_PINDNACK: return "pin_dn_ack";
    case OP_UNPINDN: return "unpin_dn";

    case OP_FINISH: return "finish";
    default: assert(0); return 0;
    }
  }

 private:
  metareqid_t reqid;
  char op;

  // for locking
  char lock_type;  // lock object type
  MDSCacheObjectInfo object_info;
  
  // for dn pins
  inodeno_t dnpathbase;
  string dnpath;

  // for authpins
  list<MDSCacheObjectInfo> authpins;

 public:
  // for rename prep
  string srcdnpath;
  string destdnpath;
  set<int> srcdn_replicas;
  utime_t now;

public:
  metareqid_t get_reqid() { return reqid; }
  int get_op() { return op; }
  bool is_reply() { return op < 0; }

  int get_lock_type() { return lock_type; }
  MDSCacheObjectInfo &get_object_info() { return object_info; }

  inodeno_t get_dnpathbase() { return dnpathbase; }
  const string& get_dnpath() { return dnpath; }

  list<MDSCacheObjectInfo>& get_authpins() { return authpins; }

  void set_lock_type(int t) { lock_type = t; }
  void set_dnpath(string& p, inodeno_t i) { dnpath = p; dnpathbase = i; }

  // ----
  MMDSSlaveRequest() : Message(MSG_MDS_SLAVE_REQUEST) { }
  MMDSSlaveRequest(metareqid_t ri, int o) : 
    Message(MSG_MDS_SLAVE_REQUEST),
    reqid(ri), op(o)
  { }
  
  void encode_payload() {
    ::_encode(reqid, payload);
    ::_encode(op, payload);
    ::_encode(lock_type, payload);
    object_info._encode(payload);
    ::_encode(dnpath, payload);
    ::_encode(dnpathbase, payload);
    ::_encode_complex(authpins, payload);
    ::_encode(srcdnpath, payload);
    ::_encode(destdnpath, payload);
    ::_encode(srcdn_replicas, payload);
    ::_encode(now, payload);
  }
  void decode_payload() {
    int off = 0;
    ::_decode(reqid, payload, off);
    ::_decode(op, payload, off);
    ::_decode(lock_type, payload, off);
    object_info._decode(payload, off);
    ::_decode(dnpath, payload, off);
    ::_decode(dnpathbase, payload, off);
    ::_decode_complex(authpins, payload, off);
    ::_decode(srcdnpath, payload, off);
    ::_decode(destdnpath, payload, off);
    ::_decode(srcdn_replicas, payload, off);
    ::_decode(now, payload, off);
  }

  char *get_type_name() { return "slave_request"; }
  void print(ostream& out) {
    out << "slave_request(" << reqid
	<< " " << get_opname(op) 
	<< ")";
  }  
	
};

#endif
