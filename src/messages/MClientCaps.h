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

#ifndef __MCLIENTCAPS_H
#define __MCLIENTCAPS_H

#include "msg/Message.h"


class MClientCaps : public Message {
 public:
  struct ceph_mds_caps head;
  bufferlist snapbl;
  bufferlist xattrbl;

  int      get_caps() { return head.caps; }
  int      get_wanted() { return head.wanted; }
  capseq_t get_seq() { return head.seq; }
  capseq_t get_mseq() { return head.migrate_seq; }

  inodeno_t get_ino() { return inodeno_t(head.ino); }

  __u64 get_size() { return head.size;  }
  __u64 get_max_size() { return head.max_size;  }
  __u64 get_truncate_seq() { return head.truncate_seq; }
  utime_t get_ctime() { return utime_t(head.ctime); }
  utime_t get_mtime() { return utime_t(head.mtime); }
  utime_t get_atime() { return utime_t(head.atime); }
  __u64 get_time_warp_seq() { return head.time_warp_seq; }

  ceph_file_layout& get_layout() { return head.layout; }

  int       get_migrate_seq() { return head.migrate_seq; }
  int       get_op() { return head.op; }

  snapid_t get_snap_follows() { return snapid_t(head.snap_follows); }
  void set_snap_follows(snapid_t s) { head.snap_follows = s; }

  void set_caps(int c) { head.caps = c; }
  void set_wanted(int w) { head.wanted = w; }

  void set_max_size(__u64 ms) { head.max_size = ms; }

  void set_migrate_seq(unsigned m) { head.migrate_seq = m; }
  void set_op(int o) { head.op = o; }

  void set_size(loff_t s) { head.size = s; }
  void set_mtime(const utime_t &t) { t.encode_timeval(&head.mtime); }
  void set_atime(const utime_t &t) { t.encode_timeval(&head.atime); }

  MClientCaps() {}
  MClientCaps(int op,
	      inode_t& inode,
	      inodeno_t realm,
	      long seq,
	      int caps,
	      int wanted,
	      int mseq) :
    Message(CEPH_MSG_CLIENT_CAPS) {
    memset(&head, 0, sizeof(head));
    head.op = op;
    head.ino = inode.ino;
    head.seq = seq;
    head.caps = caps;
    head.wanted = wanted;
    head.migrate_seq = mseq;

    head.uid = inode.uid;
    head.gid = inode.gid;
    head.mode = inode.mode;

    head.xattr_len = 0; // FIXME

    head.layout = inode.layout;
    head.size = inode.size;
    head.max_size = inode.max_size;
    head.truncate_seq = inode.truncate_seq;
    inode.mtime.encode_timeval(&head.mtime);
    inode.atime.encode_timeval(&head.atime);
    inode.ctime.encode_timeval(&head.ctime);
    head.time_warp_seq = inode.time_warp_seq;
  }
  MClientCaps(int op,
	      inodeno_t ino,
	      int mseq) :
    Message(CEPH_MSG_CLIENT_CAPS) {
    memset(&head, 0, sizeof(head));
    head.op = op;
    head.ino = ino;
    head.migrate_seq = mseq;
  }

  const char *get_type_name() { return "Cfcap";}
  void print(ostream& out) {
    out << "client_caps(" << ceph_cap_op_name(head.op)
	<< " ino " << inodeno_t(head.ino)
	<< " seq " << head.seq 
	<< " caps=" << ccap_string(head.caps)
	<< " wanted=" << ccap_string(head.wanted)
	<< " size " << head.size << "/" << head.max_size;
    if (head.truncate_seq)
      out << " ts " << head.truncate_seq;
    out << " mtime " << utime_t(head.mtime);
    if (head.time_warp_seq)
      out << " tws " << head.time_warp_seq;
    out << " follows " << snapid_t(head.snap_follows);
    if (head.migrate_seq)
      out << " mseq " << head.migrate_seq;
    out << ")";
  }
  
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(head, p);
    ::decode_nohead(head.snap_trace_len, snapbl, p);
    ::decode_nohead(head.xattr_len, xattrbl, p);
  }
  void encode_payload() {
    head.snap_trace_len = snapbl.length();
    ::encode(head, payload);
    ::encode_nohead(snapbl, payload);
    ::encode_nohead(xattrbl, payload);
  }
};

#endif
