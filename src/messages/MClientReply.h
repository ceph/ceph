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


#ifndef CEPH_MCLIENTREPLY_H
#define CEPH_MCLIENTREPLY_H

#include "include/types.h"
#include "include/fs_types.h"
#include "MClientRequest.h"

#include "msg/Message.h"
#include "include/ceph_features.h"
#include "common/errno.h"

/***
 *
 * MClientReply - container message for MDS reply to a client's MClientRequest
 *
 * key fields:
 *  long tid - transaction id, so the client can match up with pending request
 *  int result - error code, or fh if it was open
 *
 * for most requests:
 *  trace is a vector of InodeStat's tracing from root to the file/dir/whatever
 *  the operation referred to, so that the client can update it's info about what
 *  metadata lives on what MDS.
 *
 * for readdir replies:
 *  dir_contents is a vector of InodeStat*'s.  
 * 
 * that's mostly it, i think!
 *
 */


struct LeaseStat {
  // this matches ceph_mds_reply_lease
  __u16 mask;
  __u32 duration_ms;  
  __u32 seq;

  LeaseStat() : mask(0), duration_ms(0), seq(0) {}
  LeaseStat(__u16 msk, __u32 dur, __u32 sq) : mask{msk}, duration_ms{dur}, seq{sq} {}

  void decode(bufferlist::const_iterator &bl, const uint64_t features) {
    using ceph::decode;
    if (features == (uint64_t)-1) {
      DECODE_START(1, bl);
      decode(mask, bl);
      decode(duration_ms, bl);
      decode(seq, bl);
      DECODE_FINISH(bl);
    }
    else {
      decode(mask, bl);
      decode(duration_ms, bl);
      decode(seq, bl);
    }
  }
};

inline ostream& operator<<(ostream& out, const LeaseStat& l) {
  return out << "lease(mask " << l.mask << " dur " << l.duration_ms << ")";
}

struct DirStat {
  // mds distribution hints
  frag_t frag;
  __s32 auth;
  set<__s32> dist;
  
  DirStat() : auth(CDIR_AUTH_PARENT) {}
  DirStat(bufferlist::const_iterator& p, const uint64_t features) {
    decode(p, features);
  }

  void decode(bufferlist::const_iterator& p, const uint64_t features) {
    using ceph::decode;
    if (features == (uint64_t)-1) {
      DECODE_START(1, p);
      decode(frag, p);
      decode(auth, p);
      decode(dist, p);
      DECODE_FINISH(p);
    }
    else {
      decode(frag, p);
      decode(auth, p);
      decode(dist, p);
    }
  }

  // see CDir::encode_dirstat for encoder.
};

struct InodeStat {
  vinodeno_t vino;
  uint32_t rdev = 0;
  version_t version = 0;
  version_t xattr_version = 0;
  ceph_mds_reply_cap cap;
  file_layout_t layout;
  utime_t ctime, btime, mtime, atime;
  uint32_t time_warp_seq = 0;
  uint64_t size = 0, max_size = 0;
  uint64_t change_attr = 0;
  uint64_t truncate_size = 0;
  uint32_t truncate_seq = 0;
  uint32_t mode = 0, uid = 0, gid = 0, nlink = 0;
  frag_info_t dirstat;
  nest_info_t rstat;

  fragtree_t dirfragtree;
  string  symlink;   // symlink content (if symlink)

  ceph_dir_layout dir_layout;

  bufferlist xattrbl;

  bufferlist inline_data;
  version_t inline_version;

  quota_info_t quota;

  mds_rank_t dir_pin;

 public:
  InodeStat() {}
  InodeStat(bufferlist::const_iterator& p, const uint64_t features) {
    decode(p, features);
  }

  void decode(bufferlist::const_iterator &p, const uint64_t features) {
    using ceph::decode;
    if (features == (uint64_t)-1) {
      DECODE_START(2, p);
      decode(vino.ino, p);
      decode(vino.snapid, p);
      decode(rdev, p);
      decode(version, p);
      decode(xattr_version, p);
      decode(cap, p);
      {
        ceph_file_layout legacy_layout;
        decode(legacy_layout, p);
        layout.from_legacy(legacy_layout);
      }
      decode(ctime, p);
      decode(mtime, p);
      decode(atime, p);
      decode(time_warp_seq, p);
      decode(size, p);
      decode(max_size, p);
      decode(truncate_size, p);
      decode(truncate_seq, p);
      decode(mode, p);
      decode(uid, p);
      decode(gid, p);
      decode(nlink, p);
      decode(dirstat.nfiles, p);
      decode(dirstat.nsubdirs, p);
      decode(rstat.rbytes, p);
      decode(rstat.rfiles, p);
      decode(rstat.rsubdirs, p);
      decode(rstat.rctime, p);
      decode(dirfragtree, p);
      decode(symlink, p);
      decode(dir_layout, p);
      decode(xattrbl, p);
      decode(inline_version, p);
      decode(inline_data, p);
      decode(quota, p);
      decode(layout.pool_ns, p);
      decode(btime, p);
      decode(change_attr, p);
      if (struct_v > 1) {
        decode(dir_pin, p);
      } else {
        dir_pin = -ENODATA;
      }
      DECODE_FINISH(p);
    }
    else {
      decode(vino.ino, p);
      decode(vino.snapid, p);
      decode(rdev, p);
      decode(version, p);
      decode(xattr_version, p);
      decode(cap, p);
      {
        ceph_file_layout legacy_layout;
        decode(legacy_layout, p);
        layout.from_legacy(legacy_layout);
      }
      decode(ctime, p);
      decode(mtime, p);
      decode(atime, p);
      decode(time_warp_seq, p);
      decode(size, p);
      decode(max_size, p);
      decode(truncate_size, p);
      decode(truncate_seq, p);
      decode(mode, p);
      decode(uid, p);
      decode(gid, p);
      decode(nlink, p);
      decode(dirstat.nfiles, p);
      decode(dirstat.nsubdirs, p);
      decode(rstat.rbytes, p);
      decode(rstat.rfiles, p);
      decode(rstat.rsubdirs, p);
      decode(rstat.rctime, p);
      decode(dirfragtree, p);
      decode(symlink, p);
      if (features & CEPH_FEATURE_DIRLAYOUTHASH)
        decode(dir_layout, p);
      else
        memset(&dir_layout, 0, sizeof(dir_layout));

      decode(xattrbl, p);

      if (features & CEPH_FEATURE_MDS_INLINE_DATA) {
        decode(inline_version, p);
        decode(inline_data, p);
      } else {
        inline_version = CEPH_INLINE_NONE;
      }

      if (features & CEPH_FEATURE_MDS_QUOTA)
        decode(quota, p);
      else
        quota = quota_info_t{};

      if ((features & CEPH_FEATURE_FS_FILE_LAYOUT_V2))
        decode(layout.pool_ns, p);

      if ((features & CEPH_FEATURE_FS_BTIME)) {
        decode(btime, p);
        decode(change_attr, p);
      } else {
        btime = utime_t();
        change_attr = 0;
      }
    }
  }
  
  // see CInode::encode_inodestat for encoder.
};


class MClientReply : public MessageInstance<MClientReply> {
public:
  friend factory;

  // reply data
  struct ceph_mds_reply_head head {};
  bufferlist trace_bl;
  bufferlist extra_bl;
  bufferlist snapbl;

  int get_op() const { return head.op; }

  void set_mdsmap_epoch(epoch_t e) { head.mdsmap_epoch = e; }
  epoch_t get_mdsmap_epoch() const { return head.mdsmap_epoch; }

  int get_result() const {
    return ceph_to_hostos_errno((__s32)(__u32)head.result);
  }

  void set_result(int r) { head.result = r; }

  void set_unsafe() { head.safe = 0; }

  bool is_safe() const { return head.safe; }

protected:
  MClientReply() : MessageInstance(CEPH_MSG_CLIENT_REPLY) {}
  MClientReply(const MClientRequest &req, int result = 0) :
    MessageInstance(CEPH_MSG_CLIENT_REPLY) {
    memset(&head, 0, sizeof(head));
    header.tid = req.get_tid();
    head.op = req.get_op();
    head.result = result;
    head.safe = 1;
  }
  ~MClientReply() override {}

public:
  std::string_view get_type_name() const override { return "creply"; }
  void print(ostream& o) const override {
    o << "client_reply(???:" << get_tid();
    o << " = " << get_result();
    if (get_result() <= 0) {
      o << " " << cpp_strerror(get_result());
    }
    if (head.op & CEPH_MDS_OP_WRITE) {
      if (head.safe)
	o << " safe";
      else
	o << " unsafe";
    }
    o << ")";
  }

  // serialization
  void decode_payload() override {
    auto p = payload.cbegin();
    decode(head, p);
    decode(trace_bl, p);
    decode(extra_bl, p);
    decode(snapbl, p);
    ceph_assert(p.end());
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    encode(head, payload);
    encode(trace_bl, payload);
    encode(extra_bl, payload);
    encode(snapbl, payload);
  }


  // dir contents
  void set_extra_bl(bufferlist& bl) {
    extra_bl.claim(bl);
  }
  bufferlist& get_extra_bl() {
    return extra_bl;
  }
  const bufferlist& get_extra_bl() const {
    return extra_bl;
  }

  // trace
  void set_trace(bufferlist& bl) {
    trace_bl.claim(bl);
  }
  bufferlist& get_trace_bl() {
    return trace_bl;
  }
  const bufferlist& get_trace_bl() const {
    return trace_bl;
  }
};

#endif
