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

#ifndef CEPH_MCLIENTCAPS_H
#define CEPH_MCLIENTCAPS_H

#include "msg/Message.h"
#include "include/ceph_features.h"


class MClientCaps : public Message {
  static const int HEAD_VERSION = 9;
  static const int COMPAT_VERSION = 1;

 public:
  struct ceph_mds_caps_head head;

  uint64_t size, max_size, truncate_size;
  uint32_t truncate_seq;
  utime_t mtime, atime, ctime, btime;
  file_layout_t layout;
  uint32_t time_warp_seq;

  struct ceph_mds_cap_peer peer;

  bufferlist snapbl;
  bufferlist xattrbl;
  bufferlist flockbl;
  version_t  inline_version;
  bufferlist inline_data;

  // Receivers may not use their new caps until they have this OSD map
  epoch_t osd_epoch_barrier;
  ceph_tid_t oldest_flush_tid;
  uint32_t caller_uid;
  uint32_t caller_gid;

  int      get_caps() { return head.caps; }
  int      get_wanted() { return head.wanted; }
  int      get_dirty() { return head.dirty; }
  ceph_seq_t get_seq() { return head.seq; }
  ceph_seq_t get_issue_seq() { return head.issue_seq; }
  ceph_seq_t get_mseq() { return head.migrate_seq; }

  inodeno_t get_ino() { return inodeno_t(head.ino); }
  inodeno_t get_realm() { return inodeno_t(head.realm); }
  uint64_t get_cap_id() { return head.cap_id; }

  uint64_t get_size() { return size;  }
  uint64_t get_max_size() { return max_size;  }
  __u32 get_truncate_seq() { return truncate_seq; }
  uint64_t get_truncate_size() { return truncate_size; }
  utime_t get_ctime() { return ctime; }
  utime_t get_btime() { return btime; }
  utime_t get_mtime() { return mtime; }
  utime_t get_atime() { return atime; }
  __u32 get_time_warp_seq() { return time_warp_seq; }

  const file_layout_t& get_layout() {
    return layout;
  }

  int       get_migrate_seq() { return head.migrate_seq; }
  int       get_op() { return head.op; }

  uint64_t get_client_tid() { return get_tid(); }
  void set_client_tid(uint64_t s) { set_tid(s); }

  snapid_t get_snap_follows() { return snapid_t(head.snap_follows); }
  void set_snap_follows(snapid_t s) { head.snap_follows = s; }

  void set_caps(int c) { head.caps = c; }
  void set_wanted(int w) { head.wanted = w; }

  void set_max_size(uint64_t ms) { max_size = ms; }

  void set_migrate_seq(unsigned m) { head.migrate_seq = m; }
  void set_op(int o) { head.op = o; }

  void set_size(loff_t s) { size = s; }
  void set_mtime(const utime_t &t) { mtime = t; }
  void set_ctime(const utime_t &t) { ctime = t; }
  void set_atime(const utime_t &t) { atime = t; }

  void set_cap_peer(uint64_t id, ceph_seq_t seq, ceph_seq_t mseq, int mds, int flags) {
    peer.cap_id = id;
    peer.seq = seq;
    peer.mseq = mseq;
    peer.mds = mds;
    peer.flags = flags;
  }

  void set_oldest_flush_tid(ceph_tid_t tid) { oldest_flush_tid = tid; }
  ceph_tid_t get_oldest_flush_tid() { return oldest_flush_tid; }

  void clear_dirty() { head.dirty = 0; }

  MClientCaps()
    : Message(CEPH_MSG_CLIENT_CAPS, HEAD_VERSION, COMPAT_VERSION),
      size(0),
      max_size(0),
      truncate_size(0),
      truncate_seq(0),
      time_warp_seq(0),
      osd_epoch_barrier(0),
      oldest_flush_tid(0),
      caller_uid(0), caller_gid(0) {
    inline_version = 0;
  }
  MClientCaps(int op,
	      inodeno_t ino,
	      inodeno_t realm,
	      uint64_t id,
	      long seq,
	      int caps,
	      int wanted,
	      int dirty,
	      int mseq,
              epoch_t oeb)
    : Message(CEPH_MSG_CLIENT_CAPS, HEAD_VERSION, COMPAT_VERSION),
      size(0),
      max_size(0),
      truncate_size(0),
      truncate_seq(0),
      time_warp_seq(0),
      osd_epoch_barrier(oeb),
      oldest_flush_tid(0),
      caller_uid(0), caller_gid(0) {
    memset(&head, 0, sizeof(head));
    head.op = op;
    head.ino = ino;
    head.realm = realm;
    head.cap_id = id;
    head.seq = seq;
    head.caps = caps;
    head.wanted = wanted;
    head.dirty = dirty;
    head.migrate_seq = mseq;
    memset(&peer, 0, sizeof(peer));
    inline_version = 0;
  }
  MClientCaps(int op,
	      inodeno_t ino, inodeno_t realm,
	      uint64_t id, int mseq, epoch_t oeb)
    : Message(CEPH_MSG_CLIENT_CAPS, HEAD_VERSION, COMPAT_VERSION),
      size(0),
      max_size(0),
      truncate_size(0),
      truncate_seq(0),
      time_warp_seq(0),
      osd_epoch_barrier(oeb),
      oldest_flush_tid(0),
      caller_uid(0), caller_gid(0) {
    memset(&head, 0, sizeof(head));
    head.op = op;
    head.ino = ino;
    head.realm = realm;
    head.cap_id = id;
    head.migrate_seq = mseq;
    memset(&peer, 0, sizeof(peer));
    inline_version = 0;
  }
private:
  ~MClientCaps() {}

public:
  const char *get_type_name() const { return "Cfcap";}
  void print(ostream& out) const {
    out << "client_caps(" << ceph_cap_op_name(head.op)
	<< " ino " << inodeno_t(head.ino)
	<< " " << head.cap_id
	<< " seq " << head.seq;
    if (get_tid())
      out << " tid " << get_tid();
    out << " caps=" << ccap_string(head.caps)
	<< " dirty=" << ccap_string(head.dirty)
	<< " wanted=" << ccap_string(head.wanted);
    out << " follows " << snapid_t(head.snap_follows);
    if (head.migrate_seq)
      out << " mseq " << head.migrate_seq;

    out << " size " << size << "/" << max_size;
    if (truncate_seq)
      out << " ts " << truncate_seq << "/" << truncate_size;
    out << " mtime " << mtime;
    if (time_warp_seq)
      out << " tws " << time_warp_seq;

    if (head.xattr_version)
      out << " xattrs(v=" << head.xattr_version << " l=" << xattrbl.length() << ")";

    out << ")";
  }
  
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(head, p);
    ceph_mds_caps_body_legacy body;
    ::decode(body, p);
    if (head.op == CEPH_CAP_OP_EXPORT) {
      peer = body.peer;
    } else {
      size = body.size;
      max_size = body.max_size;
      truncate_size = body.truncate_size;
      truncate_seq = body.truncate_seq;
      mtime = utime_t(body.mtime);
      atime = utime_t(body.atime);
      ctime = utime_t(body.ctime);
      layout.from_legacy(body.layout);
      time_warp_seq = body.time_warp_seq;
    }
    ::decode_nohead(head.snap_trace_len, snapbl, p);

    assert(middle.length() == head.xattr_len);
    if (head.xattr_len)
      xattrbl = middle;

    // conditionally decode flock metadata
    if (header.version >= 2)
      ::decode(flockbl, p);

    if (header.version >= 3) {
      if (head.op == CEPH_CAP_OP_IMPORT)
	::decode(peer, p);
    }

    if (header.version >= 4) {
      ::decode(inline_version, p);
      ::decode(inline_data, p);
    } else {
      inline_version = CEPH_INLINE_NONE;
    }

    if (header.version >= 5) {
      ::decode(osd_epoch_barrier, p);
    }
    if (header.version >= 6) {
      ::decode(oldest_flush_tid, p);
    }
    if (header.version >= 7) {
      ::decode(caller_uid, p);
      ::decode(caller_gid, p);
    }
    if (header.version >= 8) {
      ::decode(layout.pool_ns, p);
    }
    if (header.version >= 9) {
      ::decode(btime, p);
    }
  }
  void encode_payload(uint64_t features) {
    header.version = HEAD_VERSION;
    head.snap_trace_len = snapbl.length();
    head.xattr_len = xattrbl.length();

    ::encode(head, payload);
    ceph_mds_caps_body_legacy body;
    if (head.op == CEPH_CAP_OP_EXPORT) {
      body.peer = peer;
    } else {
      body.size = size;
      body.max_size = max_size;
      body.truncate_size = truncate_size;
      body.truncate_seq = truncate_seq;
      mtime.encode_timeval(&body.mtime);
      atime.encode_timeval(&body.atime);
      ctime.encode_timeval(&body.ctime);
      layout.to_legacy(&body.layout);
      body.time_warp_seq = time_warp_seq;
    }
    ::encode(body, payload);
    ::encode_nohead(snapbl, payload);

    middle = xattrbl;

    // conditionally include flock metadata
    if (features & CEPH_FEATURE_FLOCK) {
      ::encode(flockbl, payload);
    } else {
      header.version = 1;
      return;
    }

    if (features & CEPH_FEATURE_EXPORT_PEER) {
      if (head.op == CEPH_CAP_OP_IMPORT)
	::encode(peer, payload);
    } else {
      header.version = 2;
      return;
    }

    if (features & CEPH_FEATURE_MDS_INLINE_DATA) {
      ::encode(inline_version, payload);
      ::encode(inline_data, payload);
    } else {
      ::encode(inline_version, payload);
      ::encode(bufferlist(), payload);
    }

    ::encode(osd_epoch_barrier, payload);
    ::encode(oldest_flush_tid, payload);
    ::encode(caller_uid, payload);
    ::encode(caller_gid, payload);

    ::encode(layout.pool_ns, payload);
    ::encode(btime, payload);
  }
};

#endif
