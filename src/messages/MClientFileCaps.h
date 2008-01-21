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

#ifndef __MCLIENTFILECAPS_H
#define __MCLIENTFILECAPS_H

#include "msg/Message.h"


class MClientFileCaps : public Message {
 public:
  static const char* get_opname(int op) {
    switch (op) {
    case CEPH_CAP_OP_GRANT: return "grant";
    case CEPH_CAP_OP_ACK: return "ack";
    case CEPH_CAP_OP_EXPORT: return "export";
    case CEPH_CAP_OP_IMPORT: return "import";
    default: assert(0); return 0;
    }
  }

 private:
  struct ceph_mds_file_caps h;

 public:
  int       get_caps() { return le32_to_cpu(h.caps); }
  int       get_wanted() { return le32_to_cpu(h.wanted); }
  capseq_t  get_seq() { return le32_to_cpu(h.seq); }

  inodeno_t get_ino() { return le64_to_cpu(h.ino); }
  __u64 get_size() { return le64_to_cpu(h.size);  }
  utime_t get_mtime() { return utime_t(h.mtime); }
  utime_t get_atime() { return utime_t(h.atime); }

  // for cap migration
  int       get_migrate_mds() { return le32_to_cpu(h.migrate_mds); }
  int       get_migrate_seq() { return le32_to_cpu(h.migrate_seq); }
  int       get_op() { return le32_to_cpu(h.op); }

  void set_caps(int c) { h.caps = cpu_to_le32(c); }
  void set_wanted(int w) { h.wanted = cpu_to_le32(w); }

  void set_migrate_mds(int m) { h.migrate_mds = cpu_to_le32(m); }
  void set_migrate_seq(int m) { h.migrate_seq = cpu_to_le32(m); }
  void set_op(int o) { h.op = cpu_to_le32(o); }

  void set_size(loff_t s) { h.size = cpu_to_le64(s); }
  void set_mtime(const utime_t &t) { t.encode_timeval(&h.mtime); }
  void set_atime(const utime_t &t) { t.encode_timeval(&h.atime); }

  MClientFileCaps() {}
  MClientFileCaps(int op,
		  inode_t& inode,
                  long seq,
                  int caps,
                  int wanted,
                  int mmds=0,
		  int mseq=0) :
    Message(CEPH_MSG_CLIENT_FILECAPS) {
    h.op = cpu_to_le32(op);
    h.seq = cpu_to_le32(seq);
    h.caps = cpu_to_le32(caps);
    h.wanted = cpu_to_le32(wanted);
    h.ino = cpu_to_le64(inode.ino);
    h.size = cpu_to_le64(inode.size);
    h.migrate_mds = cpu_to_le32(mmds);
    h.migrate_seq = cpu_to_le32(mseq);
    inode.mtime.encode_timeval(&h.mtime);
    inode.atime.encode_timeval(&h.atime);
  }

  const char *get_type_name() { return "Cfcap";}
  void print(ostream& out) {
    out << "client_file_caps(" << get_opname(le32_to_cpu(h.op))
	<< " ino " << inodeno_t(le64_to_cpu(h.ino))
	<< " seq " << le32_to_cpu(h.seq) 
	<< " caps " << cap_string(le32_to_cpu(h.caps)) 
	<< " wanted" << cap_string(le32_to_cpu(h.wanted)) 
	<< ")";
  }
  
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::_decode_simple(h, p);
  }
  void encode_payload() {
    ::_encode_simple(h, payload);
  }
};

#endif
