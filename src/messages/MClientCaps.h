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
#include "mds/mdstypes.h"
#include "include/ceph_features.h"

class MClientCaps : public SafeMessage {
private:

  static constexpr int HEAD_VERSION = 11;
  static constexpr int COMPAT_VERSION = 1;

 public:
  static constexpr unsigned FLAG_SYNC		= (1<<0);
  static constexpr unsigned FLAG_NO_CAPSNAP		= (1<<1); // unused
  static constexpr unsigned FLAG_PENDING_CAPSNAP	= (1<<2);

  struct ceph_mds_caps_head head;

  uint64_t size = 0;
  uint64_t max_size = 0;
  uint64_t truncate_size = 0;
  uint64_t change_attr = 0;
  uint32_t truncate_seq = 0;
  utime_t mtime, atime, ctime, btime;
  uint32_t time_warp_seq = 0;
  int64_t nfiles = -1;		// files in dir
  int64_t nsubdirs = -1;	// subdirs in dir

  struct ceph_mds_cap_peer peer;

  ceph::buffer::list snapbl;
  ceph::buffer::list xattrbl;
  ceph::buffer::list flockbl;
  version_t  inline_version = 0;
  ceph::buffer::list inline_data;

  // Receivers may not use their new caps until they have this OSD map
  epoch_t osd_epoch_barrier = 0;
  ceph_tid_t oldest_flush_tid = 0;
  uint32_t caller_uid = 0;
  uint32_t caller_gid = 0;

  /* advisory CLIENT_CAPS_* flags to send to mds */
  unsigned flags = 0;

  int      get_caps() const { return head.caps; }
  int      get_wanted() const { return head.wanted; }
  int      get_dirty() const { return head.dirty; }
  ceph_seq_t get_seq() const { return head.seq; }
  ceph_seq_t get_issue_seq() const { return head.issue_seq; }
  ceph_seq_t get_mseq() const { return head.migrate_seq; }

  inodeno_t get_ino() const { return inodeno_t(head.ino); }
  inodeno_t get_realm() const { return inodeno_t(head.realm); }
  uint64_t get_cap_id() const { return head.cap_id; }

  uint64_t get_size() const { return size;  }
  uint64_t get_max_size() const { return max_size;  }
  __u32 get_truncate_seq() const { return truncate_seq; }
  uint64_t get_truncate_size() const { return truncate_size; }
  utime_t get_ctime() const { return ctime; }
  utime_t get_btime() const { return btime; }
  utime_t get_mtime() const { return mtime; }
  utime_t get_atime() const { return atime; }
  __u64 get_change_attr() const { return change_attr; }
  __u32 get_time_warp_seq() const { return time_warp_seq; }
  uint64_t get_nfiles() const { return nfiles; }
  uint64_t get_nsubdirs() const { return nsubdirs; }
  bool dirstat_is_valid() const { return nfiles != -1 || nsubdirs != -1; }

  const file_layout_t& get_layout() const {
    return layout;
  }

  void set_layout(const file_layout_t &l) {
    layout = l;
  }

  int       get_migrate_seq() const { return head.migrate_seq; }
  int       get_op() const { return head.op; }

  uint64_t get_client_tid() const { return get_tid(); }
  void set_client_tid(uint64_t s) { set_tid(s); }

  snapid_t get_snap_follows() const { return snapid_t(head.snap_follows); }
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
  ceph_tid_t get_oldest_flush_tid() const { return oldest_flush_tid; }

  void clear_dirty() { head.dirty = 0; }

protected:
  MClientCaps()
    : SafeMessage{CEPH_MSG_CLIENT_CAPS, HEAD_VERSION, COMPAT_VERSION} {}
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
    : SafeMessage{CEPH_MSG_CLIENT_CAPS, HEAD_VERSION, COMPAT_VERSION},
      osd_epoch_barrier(oeb) {
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
  }
  MClientCaps(int op,
	      inodeno_t ino, inodeno_t realm,
	      uint64_t id, int mseq, epoch_t oeb)
    : SafeMessage{CEPH_MSG_CLIENT_CAPS, HEAD_VERSION, COMPAT_VERSION},
      osd_epoch_barrier(oeb) {
    memset(&head, 0, sizeof(head));
    head.op = op;
    head.ino = ino;
    head.realm = realm;
    head.cap_id = id;
    head.migrate_seq = mseq;
    memset(&peer, 0, sizeof(peer));
  }
  ~MClientCaps() override {}

private:
  file_layout_t layout;

public:
  std::string_view get_type_name() const override { return "Cfcap";}
  void print(std::ostream& out) const override {
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

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(head, p);
    if (head.op == CEPH_CAP_OP_EXPORT) {
      ceph_mds_caps_export_body body;
      decode(body, p);
      peer = body.peer;
      p += (sizeof(ceph_mds_caps_non_export_body) -
	    sizeof(ceph_mds_caps_export_body));
    } else {
      ceph_mds_caps_non_export_body body;
      decode(body, p);
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
    ceph::decode_nohead(head.snap_trace_len, snapbl, p);

    ceph_assert(middle.length() == head.xattr_len);
    if (head.xattr_len)
      xattrbl = middle;

    // conditionally decode flock metadata
    if (header.version >= 2)
      decode(flockbl, p);

    if (header.version >= 3) {
      if (head.op == CEPH_CAP_OP_IMPORT)
	decode(peer, p);
    }

    if (header.version >= 4) {
      decode(inline_version, p);
      decode(inline_data, p);
    } else {
      inline_version = CEPH_INLINE_NONE;
    }

    if (header.version >= 5) {
      decode(osd_epoch_barrier, p);
    }
    if (header.version >= 6) {
      decode(oldest_flush_tid, p);
    }
    if (header.version >= 7) {
      decode(caller_uid, p);
      decode(caller_gid, p);
    }
    if (header.version >= 8) {
      decode(layout.pool_ns, p);
    }
    if (header.version >= 9) {
      decode(btime, p);
      decode(change_attr, p);
    }
    if (header.version >= 10) {
      decode(flags, p);
    }
    if (header.version >= 11) {
      decode(nfiles, p);
      decode(nsubdirs, p);
    }
  }
  void encode_payload(uint64_t features) override {
    using ceph::encode;
    header.version = HEAD_VERSION;
    head.snap_trace_len = snapbl.length();
    head.xattr_len = xattrbl.length();

    encode(head, payload);
    static_assert(sizeof(ceph_mds_caps_non_export_body) >
		  sizeof(ceph_mds_caps_export_body));
    if (head.op == CEPH_CAP_OP_EXPORT) {
      ceph_mds_caps_export_body body;
      body.peer = peer;
      encode(body, payload);
      payload.append_zero(sizeof(ceph_mds_caps_non_export_body) -
			  sizeof(ceph_mds_caps_export_body));
    } else {
      ceph_mds_caps_non_export_body body;
      body.size = size;
      body.max_size = max_size;
      body.truncate_size = truncate_size;
      body.truncate_seq = truncate_seq;
      mtime.encode_timeval(&body.mtime);
      atime.encode_timeval(&body.atime);
      ctime.encode_timeval(&body.ctime);
      layout.to_legacy(&body.layout);
      body.time_warp_seq = time_warp_seq;
      encode(body, payload);
    }
    ceph::encode_nohead(snapbl, payload);

    middle = xattrbl;

    // conditionally include flock metadata
    if (features & CEPH_FEATURE_FLOCK) {
      encode(flockbl, payload);
    } else {
      header.version = 1;
      return;
    }

    if (features & CEPH_FEATURE_EXPORT_PEER) {
      if (head.op == CEPH_CAP_OP_IMPORT)
	encode(peer, payload);
    } else {
      header.version = 2;
      return;
    }

    if (features & CEPH_FEATURE_MDS_INLINE_DATA) {
      encode(inline_version, payload);
      encode(inline_data, payload);
    } else {
      encode(inline_version, payload);
      encode(ceph::buffer::list(), payload);
    }

    encode(osd_epoch_barrier, payload);
    encode(oldest_flush_tid, payload);
    encode(caller_uid, payload);
    encode(caller_gid, payload);

    encode(layout.pool_ns, payload);
    encode(btime, payload);
    encode(change_attr, payload);
    encode(flags, payload);
    encode(nfiles, payload);
    encode(nsubdirs, payload);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
