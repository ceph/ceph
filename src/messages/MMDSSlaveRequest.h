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


#ifndef CEPH_MMDSSLAVEREQUEST_H
#define CEPH_MMDSSLAVEREQUEST_H

#include "msg/Message.h"
#include "mds/mdstypes.h"

class MMDSSlaveRequest : public MessageInstance<MMDSSlaveRequest> {
public:
  friend factory;

  static constexpr int OP_XLOCK =       1;
  static constexpr int OP_XLOCKACK =   -1;
  static constexpr int OP_UNXLOCK =     2;
  static constexpr int OP_AUTHPIN =     3;
  static constexpr int OP_AUTHPINACK = -3;

  static constexpr int OP_LINKPREP =     4;
  static constexpr int OP_UNLINKPREP =   5;
  static constexpr int OP_LINKPREPACK = -4;

  static constexpr int OP_RENAMEPREP =     7;
  static constexpr int OP_RENAMEPREPACK = -7;

  static constexpr int OP_WRLOCK = 8;
  static constexpr int OP_WRLOCKACK = -8;
  static constexpr int OP_UNWRLOCK = 9;

  static constexpr int OP_RMDIRPREP = 10;
  static constexpr int OP_RMDIRPREPACK = -10;

  static constexpr int OP_DROPLOCKS	= 11;

  static constexpr int OP_RENAMENOTIFY = 12;
  static constexpr int OP_RENAMENOTIFYACK = -12;

  static constexpr int OP_FINISH = 17;
  static constexpr int OP_COMMITTED = -18;

  static constexpr int OP_ABORT =  20;  // used for recovery only
  //static constexpr int OP_COMMIT = 21;  // used for recovery only


  static const char *get_opname(int o) {
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
    case OP_COMMITTED: return "committed";

    case OP_WRLOCK: return "wrlock";
    case OP_WRLOCKACK: return "wrlock_ack";
    case OP_UNWRLOCK: return "unwrlock";

    case OP_RMDIRPREP: return "rmdir_prep";
    case OP_RMDIRPREPACK: return "rmdir_prep_ack";

    case OP_DROPLOCKS: return "drop_locks";

    case OP_RENAMENOTIFY: return "rename_notify";
    case OP_RENAMENOTIFYACK: return "rename_notify_ack";

    case OP_ABORT: return "abort";
      //case OP_COMMIT: return "commit";

    default: ceph_abort(); return 0;
    }
  }

 private:
  metareqid_t reqid;
  __u32 attempt;
  __s16 op;
  mutable __u16 flags; /* XXX HACK for mark_interrupted */

  static constexpr unsigned FLAG_NONBLOCK       =	1<<0;
  static constexpr unsigned FLAG_WOULDBLOCK     =	1<<1;
  static constexpr unsigned FLAG_NOTJOURNALED   =	1<<2;
  static constexpr unsigned FLAG_EROFS          =	1<<3;
  static constexpr unsigned FLAG_ABORT          =	1<<4;
  static constexpr unsigned FLAG_INTERRUPTED    =	1<<5;

  // for locking
  __u16 lock_type;  // lock object type
  MDSCacheObjectInfo object_info;
  
  // for authpins
  vector<MDSCacheObjectInfo> authpins;

 public:
  // for rename prep
  filepath srcdnpath;
  filepath destdnpath;
  set<mds_rank_t> witnesses;
  bufferlist inode_export;
  version_t inode_export_v;
  mds_rank_t srcdn_auth;
  utime_t op_stamp;

  mutable bufferlist straybl;  // stray dir + dentry
  bufferlist srci_snapbl;
  bufferlist desti_snapbl;

public:
  metareqid_t get_reqid() const { return reqid; }
  __u32 get_attempt() const { return attempt; }
  int get_op() const { return op; }
  bool is_reply() const { return op < 0; }

  int get_lock_type() const { return lock_type; }
  const MDSCacheObjectInfo &get_object_info() const { return object_info; }
  MDSCacheObjectInfo &get_object_info() { return object_info; }
  const MDSCacheObjectInfo &get_authpin_freeze() const { return object_info; }
  MDSCacheObjectInfo &get_authpin_freeze() { return object_info; }

  const vector<MDSCacheObjectInfo>& get_authpins() const { return authpins; }
  vector<MDSCacheObjectInfo>& get_authpins() { return authpins; }
  void mark_nonblock() { flags |= FLAG_NONBLOCK; }
  bool is_nonblock() const { return (flags & FLAG_NONBLOCK); }
  void mark_error_wouldblock() { flags |= FLAG_WOULDBLOCK; }
  bool is_error_wouldblock() const { return (flags & FLAG_WOULDBLOCK); }
  void mark_not_journaled() { flags |= FLAG_NOTJOURNALED; }
  bool is_not_journaled() const { return (flags & FLAG_NOTJOURNALED); }
  void mark_error_rofs() { flags |= FLAG_EROFS; }
  bool is_error_rofs() const { return (flags & FLAG_EROFS); }
  bool is_abort() const { return (flags & FLAG_ABORT); }
  void mark_abort() { flags |= FLAG_ABORT; }
  bool is_interrupted() const { return (flags & FLAG_INTERRUPTED); }
  void mark_interrupted() const { flags |= FLAG_INTERRUPTED; }

  void set_lock_type(int t) { lock_type = t; }
  const bufferlist& get_lock_data() const { return inode_export; }
  bufferlist& get_lock_data() { return inode_export; }

protected:
  MMDSSlaveRequest() : MessageInstance(MSG_MDS_SLAVE_REQUEST) { }
  MMDSSlaveRequest(metareqid_t ri, __u32 att, int o) : 
    MessageInstance(MSG_MDS_SLAVE_REQUEST),
    reqid(ri), attempt(att), op(o), flags(0), lock_type(0),
    inode_export_v(0), srcdn_auth(MDS_RANK_NONE) { }
  ~MMDSSlaveRequest() override {}

public:
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(reqid, payload);
    encode(attempt, payload);
    encode(op, payload);
    encode(flags, payload);
    encode(lock_type, payload);
    encode(object_info, payload);
    encode(authpins, payload);
    encode(srcdnpath, payload);
    encode(destdnpath, payload);
    encode(witnesses, payload);
    encode(op_stamp, payload);
    encode(inode_export, payload);
    encode(inode_export_v, payload);
    encode(srcdn_auth, payload);
    encode(straybl, payload);
    encode(srci_snapbl, payload);
    encode(desti_snapbl, payload);
  }
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(reqid, p);
    decode(attempt, p);
    decode(op, p);
    decode(flags, p);
    decode(lock_type, p);
    decode(object_info, p);
    decode(authpins, p);
    decode(srcdnpath, p);
    decode(destdnpath, p);
    decode(witnesses, p);
    decode(op_stamp, p);
    decode(inode_export, p);
    decode(inode_export_v, p);
    decode(srcdn_auth, p);
    decode(straybl, p);
    decode(srci_snapbl, p);
    decode(desti_snapbl, p);
  }

  std::string_view get_type_name() const override { return "slave_request"; }
  void print(ostream& out) const override {
    out << "slave_request(" << reqid
	<< "." << attempt
	<< " " << get_opname(op) 
	<< ")";
  }  
	
};

#endif
