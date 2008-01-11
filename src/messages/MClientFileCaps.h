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
#include "mds/Capability.h"


class MClientFileCaps : public Message {
 public:
  static const int OP_GRANT   = 0;  // mds->client grant.
  static const int OP_ACK     = 1;  // client->mds ack (if prior grant was a recall)
  static const int OP_RELEASE = 2;  // mds->client release cap (*)
  static const int OP_EXPORT  = 3;  // mds has exported the cap
  static const int OP_IMPORT  = 4;  // mds has imported the cap from get_mds()
  /* 
   * (*) it's a bit counterintuitive, but the mds has to 
   *  close the cap because the client isn't able to tell
   *  if a concurrent open() would map to the same inode.
   */
  static const char* get_opname(int op) {
    switch (op) {
    case OP_GRANT: return "grant";
    case OP_ACK: return "ack";
    case OP_RELEASE: return "release";
    case OP_EXPORT: return "export";
    case OP_IMPORT: return "import";
    default: assert(0); return 0;
    }
  }

 private:
  struct ceph_mds_file_caps h;

 public:
  int       get_caps() { return le32_to_cpu(h.caps); }
  int       get_wanted() { return le32_to_cpu(h.wanted); }
  capseq_t  get_seq() { return le64_to_cpu(h.seq); }

  inodeno_t get_ino() { return le64_to_cpu(h.ino); }
  __u64 get_size() { return le64_to_cpu(h.size);  }
  utime_t get_mtime() { return utime_t(h.mtime); }
  utime_t get_atime() { return utime_t(h.atime); }

  // for cap migration
  int       get_mds() { return le32_to_cpu(h.mds); }
  int       get_op() { return le32_to_cpu(h.op); }

  void set_caps(int c) { h.caps = cpu_to_le32(c); }
  void set_wanted(int w) { h.wanted = cpu_to_le32(w); }

  void set_mds(int m) { h.mds = cpu_to_le32(m); }
  void set_op(int o) { h.op = cpu_to_le32(o); }

  void set_size(loff_t s) { h.size = cpu_to_le64(s); }
  void set_mtime(utime_t t) { h.mtime = t.tv_ref(); }
  void set_atime(utime_t t) { h.atime = t.tv_ref(); }

  MClientFileCaps() {}
  MClientFileCaps(int op,
		  inode_t& inode,
                  long seq,
                  int caps,
                  int wanted,
                  int mds=0) :
    Message(CEPH_MSG_CLIENT_FILECAPS) {
    h.op = cpu_to_le32(op);
    h.mds = cpu_to_le32(mds);
    h.seq = cpu_to_le64(seq);
    h.caps = cpu_to_le32(caps);
    h.wanted = cpu_to_le32(wanted);
    h.ino = cpu_to_le64(inode.ino);
    h.size = cpu_to_le64(inode.size);
    h.mtime = inode.mtime.tv_ref();
    h.atime = inode.atime.tv_ref();
  }

  const char *get_type_name() { return "Cfcap";}
  void print(ostream& out) {
    out << "client_file_caps(" << le32_to_cpu(h.op)
	<< " " << le64_to_cpu(h.ino)
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
